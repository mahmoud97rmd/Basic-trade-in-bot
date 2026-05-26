import asyncio
import aiohttp
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, timezone
from metaapi_cloud_sdk import MetaApi
from aiohttp import web

# --- METAAPI & OANDA CONFIGURATION ---
METAAPI_TOKEN = ""
ACCOUNT_ID    = ""
TG_TOKEN      = "8647261254:AAH7AyzhBYvc9QjGmzgFW7NBb0a_SOAYCjc"
OANDA_ID      = "101-001-39389982-001"
OANDA_API     = "d05b25b3f1ce0c8fa105ffefa45efb01-a5c26f544a26a4f810f1809913a2795f"
OANDA_URL     = "https://api-fxpractice.oanda.com/v3"


def c_log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================
# GLOBAL STATE
# tolerance_mode:
#   'LEVEL' = invalidate setup only when Stoch crosses back out of zone
#   'TIME'  = invalidate setup after max_tolerance_candles new candles
#
# setup_state: per-timeframe state machine tracking active setups
# =============================================================
_TFS = ['1m', '2m', '3m', '5m', '15m']

bot_state = {
    'status': 'RUNNING',
    'symbol': 'XAUUSD@',
    'live_connected': False,
    'timeframes': _TFS,
    'active_tfs': {'1m': False, '2m': True, '3m': True, '5m': False, '15m': False},
    'lot_size': 0.05,
    'pip_value': 0.1,
    'spread_pips': 2.2,
    'chat_id': None,
    'last_update_id': 0,
    'tp_pips': {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80},
    'sl_pips': {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},

    # ── New: Tolerance Mode (mutually exclusive) ──
    'tolerance_mode': 'LEVEL',   # 'LEVEL' or 'TIME'
    'max_tolerance_candles': 3,

    # ── Per-timeframe state machine ──
    'setup_state': {
        tf: {'buy_active': False, 'sell_active': False, 'wait_count': 0}
        for tf in _TFS
    },

    # ── Time filters ──
    'use_time_filter':   False,
    'use_danger_filter': True,

    # ── Risk controls ──
    'use_be': False,
    'use_atr': False,
    'use_max_spread': True,
    'max_spread_pips': 3.0,
    'atr_mult_tp': 1.5,
    'atr_mult_sl': 3.0,
    'tp_tolerance_pips': 5.0,

    # ── Live display ──
    'market_data': {tf: "⏸ بانتظار الاتصال (Offline)" for tf in _TFS},
    'last_signal_time': {tf: None for tf in _TFS},
    'connection_obj': None,
    'account_obj': None,
    'is_backtesting': False,
}


# =============================================================
# INDICATOR ENGINE  (THE CORE MATH)
# =============================================================
# Sub-window 1 : Stochastic (10, 2, 10)  → K line tracked
#
# Sub-window 2 : Three nested indicators
#   ① RSI(2)            – base, applied to Close
#   ② OsMA(200,5,200)   – applied to RSI(2) array   → RED histogram
#   ③ MACD(1000,5,5)    – applied to OsMA/RSI data  → GREEN histogram
#
# Normalisation: rolling Min-Max (window=200) maps each unbounded
# array into [0, 100] so we can compare against the fixed levels
# (10 / 90) that the MT5 sub-window shows visually.
# =============================================================

def _rsi(series: pd.Series, period: int) -> pd.Series:
    """Wilder-smoothed RSI."""
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_g = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_l = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _macd_line(series: pd.Series, fast: int, slow: int) -> pd.Series:
    """MACD main line = EMA(fast) − EMA(slow)."""
    return _ema(series, fast) - _ema(series, slow)


def _osma(series: pd.Series, fast: int, slow: int, signal: int) -> pd.Series:
    """OsMA = MACD_line − Signal  (Moving Average of Oscillator)."""
    macd   = _macd_line(series, fast, slow)
    sig    = _ema(macd, signal)
    return macd - sig


def _rolling_minmax(series: pd.Series, window: int = 200) -> pd.Series:
    """Scale series into [0, 100] using a rolling Min-Max window."""
    roll_min = series.rolling(window, min_periods=1).min()
    roll_max = series.rolling(window, min_periods=1).max()
    denom    = (roll_max - roll_min).replace(0, np.nan)
    scaled   = 100.0 * (series - roll_min) / denom
    return scaled.fillna(50.0)           # default to midpoint when flat


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates all indicators required by the new signal engine.

    Columns added:
        K, D            – Stochastic (10, 2, 10)
        rsi2            – RSI(2) of Close  [0..100]
        osma_raw        – OsMA(200,5,200) applied to rsi2
        macd_raw        – MACD(1000,5,5) applied to osma_raw
        osma_norm       – osma_raw rolled-MinMax → [0..100]   RED histogram
        macd_norm       – macd_raw rolled-MinMax → [0..100]   GREEN histogram
        atr             – ATR(14) for risk sizing
    """
    if df.empty:
        return df

    # ── Stochastic (10, 2, 10) ───────────────────────────────
    stoch_k_period = 10
    stoch_smooth   = 2
    stoch_d_period = 10

    low_min  = df['low'].rolling(stoch_k_period).min()
    high_max = df['high'].rolling(stoch_k_period).max()
    denom    = (high_max - low_min).replace(0, 1e-10)
    k_raw    = 100.0 * (df['close'] - low_min) / denom
    df['K']  = k_raw.ewm(span=stoch_smooth, adjust=False).mean()
    df['D']  = df['K'].ewm(span=stoch_d_period, adjust=False).mean()

    # ── ① RSI(2) on Close ────────────────────────────────────
    df['rsi2'] = _rsi(df['close'], 2)

    # ── ② OsMA(200, 5, 200) applied to RSI(2) ───────────────
    #   Fast EMA=200, Slow EMA=5, Signal SMA=200
    #   MT5 "Apply to: First Indicator's Data" → uses rsi2
    df['osma_raw'] = _osma(df['rsi2'], fast=200, slow=5, signal=200)

    # ── ③ MACD(1000, 5, 5) applied to OsMA output ───────────
    #   Fast EMA=1000, Slow EMA=5, Signal SMA=5
    #   MT5 "Apply to: Previous Indicator's Data" → uses osma_raw
    macd_line      = _macd_line(df['osma_raw'], fast=1000, slow=5)
    signal_line    = _ema(macd_line, 5)
    df['macd_raw'] = macd_line - signal_line

    # ── Normalise both histograms into [0, 100] ──────────────
    df['osma_norm'] = _rolling_minmax(df['osma_raw'], window=200)   # RED
    df['macd_norm'] = _rolling_minmax(df['macd_raw'], window=200)   # GREEN

    # ── ATR(14) for risk sizing ──────────────────────────────
    tr0       = abs(df['high'] - df['low'])
    tr1       = abs(df['high'] - df['close'].shift())
    tr2       = abs(df['low']  - df['close'].shift())
    df['atr'] = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1).rolling(14).mean().bfill()

    return df


# =============================================================
# STATE MACHINE  (per-timeframe, per-bar)
# =============================================================

def evaluate_state_machine(tf: str, curr: pd.Series) -> tuple[bool, bool]:
    """
    Evaluates one closed candle for the given timeframe.
    Updates bot_state['setup_state'][tf] in place.

    Returns:
        (buy_signal, sell_signal)  – at most ONE of them True per call.
    """
    state   = bot_state['setup_state'][tf]
    mode    = bot_state['tolerance_mode']
    max_cnt = bot_state['max_tolerance_candles']

    stoch_k    = curr['K']
    macd_norm  = curr['macd_norm']   # GREEN → buy trigger
    osma_norm  = curr['osma_norm']   # RED  → sell trigger

    buy_signal  = False
    sell_signal = False

    # ── BUY side ─────────────────────────────────────────────
    if not state['buy_active']:
        # Trigger: Stochastic drops into or below 30
        if stoch_k <= 30:
            state['buy_active']  = True
            state['wait_count']  = 0
            c_log(f"[{tf}] 🟡 BUY SETUP activated (K={stoch_k:.1f})")
    else:
        # Already in setup — check execution condition first
        if macd_norm <= 10:
            buy_signal          = True
            state['buy_active'] = False
            state['wait_count'] = 0
            c_log(f"[{tf}] 🟢 BUY SIGNAL (K={stoch_k:.1f}, MACD_norm={macd_norm:.1f})")
        else:
            # Invalidation
            if mode == 'LEVEL':
                if stoch_k > 30:
                    state['buy_active'] = False
                    state['wait_count'] = 0
                    c_log(f"[{tf}] ❌ BUY setup invalidated (K={stoch_k:.1f} > 30, LEVEL mode)")
            else:  # TIME
                state['wait_count'] += 1
                if state['wait_count'] >= max_cnt:
                    state['buy_active'] = False
                    state['wait_count'] = 0
                    c_log(f"[{tf}] ❌ BUY setup expired ({max_cnt} candles, TIME mode)")

    # ── SELL side ─────────────────────────────────────────────
    if not state['sell_active']:
        # Trigger: Stochastic rises into or above 70
        if stoch_k >= 70:
            state['sell_active'] = True
            state['wait_count']  = 0
            c_log(f"[{tf}] 🟡 SELL SETUP activated (K={stoch_k:.1f})")
    else:
        # Already in setup — check execution condition first
        if osma_norm >= 90:
            sell_signal          = True
            state['sell_active'] = False
            state['wait_count']  = 0
            c_log(f"[{tf}] 🔴 SELL SIGNAL (K={stoch_k:.1f}, OsMA_norm={osma_norm:.1f})")
        else:
            # Invalidation
            if mode == 'LEVEL':
                if stoch_k < 70:
                    state['sell_active'] = False
                    state['wait_count']  = 0
                    c_log(f"[{tf}] ❌ SELL setup invalidated (K={stoch_k:.1f} < 70, LEVEL mode)")
            else:  # TIME
                state['wait_count'] += 1
                if state['wait_count'] >= max_cnt:
                    state['sell_active'] = False
                    state['wait_count']  = 0
                    c_log(f"[{tf}] ❌ SELL setup expired ({max_cnt} candles, TIME mode)")

    # Mutual exclusivity: if both fired on same bar, prefer the stronger
    if buy_signal and sell_signal:
        # Prefer whichever indicator is more extreme
        if macd_norm < (100 - osma_norm):
            sell_signal = False
        else:
            buy_signal = False

    return buy_signal, sell_signal


def evaluate_state_machine_backtest(tf: str, state: dict, curr: pd.Series,
                                    mode: str, max_cnt: int) -> tuple[bool, bool]:
    """
    Stateless version for backtesting — mutates the passed `state` dict.
    Identical logic to evaluate_state_machine but does not touch bot_state.
    """
    stoch_k   = curr['K']
    macd_norm = curr['macd_norm']
    osma_norm = curr['osma_norm']

    buy_signal  = False
    sell_signal = False

    # BUY
    if not state['buy_active']:
        if stoch_k <= 30:
            state['buy_active'] = True
            state['wait_count'] = 0
    else:
        if macd_norm <= 10:
            buy_signal          = True
            state['buy_active'] = False
            state['wait_count'] = 0
        else:
            if mode == 'LEVEL':
                if stoch_k > 30:
                    state['buy_active'] = False
                    state['wait_count'] = 0
            else:
                state['wait_count'] += 1
                if state['wait_count'] >= max_cnt:
                    state['buy_active'] = False
                    state['wait_count'] = 0

    # SELL
    if not state['sell_active']:
        if stoch_k >= 70:
            state['sell_active'] = True
            state['wait_count']  = 0
    else:
        if osma_norm >= 90:
            sell_signal          = True
            state['sell_active'] = False
            state['wait_count']  = 0
        else:
            if mode == 'LEVEL':
                if stoch_k < 70:
                    state['sell_active'] = False
                    state['wait_count']  = 0
            else:
                state['wait_count'] += 1
                if state['wait_count'] >= max_cnt:
                    state['sell_active'] = False
                    state['wait_count']  = 0

    if buy_signal and sell_signal:
        if macd_norm < (100 - osma_norm):
            sell_signal = False
        else:
            buy_signal = False

    return buy_signal, sell_signal


# =============================================================
# TIME FILTERS
# =============================================================

def is_danger_time(dt_utc: datetime) -> bool:
    """Block 19:00–22:00 Damascus time (= 16:00–19:00 UTC in summer)."""
    dh = (dt_utc.hour + 3) % 24
    return 19 <= dh <= 21


# =============================================================
# OANDA & TELEGRAM HELPERS
# =============================================================

async def fetch_oanda_candles(instrument, granularity, count=5000, end_time=None):
    tf_map = {'s5': 'S5', '1m': 'M1', '2m': 'M2', '3m': 'M3',
              '5m': 'M5', '15m': 'M15', '1h': 'H1'}
    url     = f"{OANDA_URL}/instruments/{instrument}/candles"
    headers = {"Authorization": f"Bearer {OANDA_API}"}
    params  = {"granularity": tf_map.get(granularity, 'M5'),
               "count": count, "price": "M"}
    if end_time:
        params["to"] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data    = await resp.json()
                    candles = []
                    for c in data.get('candles', []):
                        if c['complete']:
                            candles.append({
                                'time':  pd.to_datetime(c['time']),
                                'open':  float(c['mid']['o']),
                                'high':  float(c['mid']['h']),
                                'low':   float(c['mid']['l']),
                                'close': float(c['mid']['c']),
                            })
                    return candles
        except Exception as e:
            c_log(f"❌ خطأ Oanda: {e}")
    return []


async def send_tg_msg(text, reply_markup=None):
    if not bot_state['chat_id']:
        return
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup:
        payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(url, json=payload)
        except:
            pass


async def edit_tg_msg(chat_id, message_id, text, reply_markup=None):
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/editMessageText"
    payload = {'chat_id': chat_id, 'message_id': message_id,
               'text': text, 'parse_mode': 'HTML'}
    if reply_markup:
        payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(url, json=payload)
        except:
            pass


async def answer_callback(callback_query_id, text=None):
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery"
    payload = {'callback_query_id': callback_query_id}
    if text:
        payload['text'] = text
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(url, json=payload)
        except:
            pass


async def send_tg_document(file_path, caption):
    if not bot_state['chat_id']:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
    async with aiohttp.ClientSession() as s:
        try:
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('chat_id', str(bot_state['chat_id']))
                data.add_field('document', f)
                data.add_field('caption', caption)
                await s.post(url, data=data)
        except Exception as e:
            c_log(f"❌ خطأ إرسال الملف: {e}")


# =============================================================
# TELEGRAM KEYBOARDS
# =============================================================

def get_main_keyboard():
    live_icon   = "🟢 متصل"  if bot_state['live_connected'] else "🔴 غير متصل"
    status_icon = "🟢 RUN"   if bot_state['status'] == 'RUNNING' else "🔴 PAUSE"
    tol_lbl     = ("⏱ TIME" if bot_state['tolerance_mode'] == 'TIME'
                   else "📏 LEVEL")
    return {"inline_keyboard": [
        [{"text": f"🔌 سيرفر التداول الحي: {live_icon}",
          "callback_data": "toggle_live_conn"}],
        [{"text": f"Status: {status_icon}", "callback_data": "toggle_status"},
         {"text": f"Tolerance: {tol_lbl}",  "callback_data": "toggle_tolerance"}],
        [{"text": "🎛 فلاتر وإعدادات",   "callback_data": "menu_filters"},
         {"text": "⏱ فريمات",            "callback_data": "menu_tfs"}],
        [{"text": "📊 Live Report",        "callback_data": "report"},
         {"text": "💳 Account",            "callback_data": "account"}],
        [{"text": "🛠 إعدادات المخاطرة",  "callback_data": "menu_settings"},
         {"text": "🔬 BACKTEST",           "callback_data": "menu_backtest"}],
        [{"text": "🛑 إغلاق جميع الصفقات", "callback_data": "close_all"}],
    ]}


def get_filters_keyboard():
    t_icon = "🟢" if bot_state['use_time_filter']   else "🔴"
    d_icon = "🟢" if bot_state['use_danger_filter'] else "🔴"

    tol_level_icon = "✅" if bot_state['tolerance_mode'] == 'LEVEL' else "⬜"
    tol_time_icon  = "✅" if bot_state['tolerance_mode'] == 'TIME'  else "⬜"
    cnt            = bot_state['max_tolerance_candles']

    return {"inline_keyboard": [
        [{"text": "━━ نافذة السماحية (اختر واحدة فقط) ━━", "callback_data": "noop"}],
        [{"text": f"{tol_level_icon} LEVEL: إلغاء فقط عند خروج Stoch من المنطقة",
          "callback_data": "set_tol_level"}],
        [{"text": f"{tol_time_icon} TIME:  إلغاء بعد {cnt} شمعة",
          "callback_data": "set_tol_time"}],
        [{"text": "━━ عدد شموع السماحية (TIME فقط) ━━", "callback_data": "noop"}],
        [{"text": "➖",  "callback_data": "dec_tol_cnt"},
         {"text": f"السماحية = {cnt} شموع", "callback_data": "noop"},
         {"text": "➕",  "callback_data": "inc_tol_cnt"}],
        [{"text": "━━ فلاتر الوقت ━━", "callback_data": "noop"}],
        [{"text": f"Time Filter 08-17 UTC: {t_icon}", "callback_data": "toggle_time"},
         {"text": f"حظر 19:00-22:00 دمشق: {d_icon}", "callback_data": "toggle_danger"}],
        [{"text": "🔙 القائمة الرئيسية", "callback_data": "menu_main"}],
    ]}


def get_tf_keyboard():
    kb, row = [], []
    for tf in bot_state['timeframes']:
        row.append({"text": f"{tf}: {'🟢' if bot_state['active_tfs'][tf] else '🔴'}",
                    "callback_data": f"toggle_tf_{tf}"})
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    kb.append([{"text": "🔙 رجوع", "callback_data": "menu_main"}])
    return {"inline_keyboard": kb}


def get_settings_keyboard():
    be_i  = "🟢" if bot_state['use_be']         else "🔴"
    atr_i = "🟢" if bot_state['use_atr']        else "🔴"
    spr_i = "🟢" if bot_state['use_max_spread'] else "🔴"
    return {"inline_keyboard": [
        [{"text": f"تأمين الدخول (BE 20p): {be_i}",  "callback_data": "toggle_be"}],
        [{"text": f"أهداف ATR: {atr_i}",             "callback_data": "toggle_atr"}],
        [{"text": f"حماية السبريد: {spr_i}",         "callback_data": "toggle_spread"}],
        [{"text": f"LOT SIZE: {bot_state['lot_size']}", "callback_data": "noop"}],
        [{"text": "➕ Lot", "callback_data": "inc_lot"},
         {"text": "➖ Lot", "callback_data": "dec_lot"}],
        [{"text": "📖 عرض TP/SL", "callback_data": "view_tpsl"}],
        [{"text": "🔙 رجوع", "callback_data": "menu_main"}],
    ]}


# =============================================================
# BACKTEST ENGINE — Standard
# =============================================================

async def run_oanda_backtest(start_dt):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة.")
        return
    bot_state['is_backtesting'] = True
    c_log("بدء الباك تيست...")

    fname       = f"BT_{datetime.now().strftime('%H%M%S')}.xlsx"
    trade_logs  = []
    total_prof  = 0.0
    peak_equity = 0.0
    max_dd      = 0.0
    total_win   = 0.0
    total_loss  = 0.0
    win_count   = 0
    loss_count  = 0
    be_count    = 0

    tol_desc = bot_state['tolerance_mode']
    if tol_desc == 'TIME':
        tol_desc += f"({bot_state['max_tolerance_candles']})"

    await send_tg_msg(
        f"⏳ <b>بدء الباك تيست</b>\n"
        f"من: {start_dt.strftime('%Y-%m-%d')}\n"
        f"السماحية: {tol_desc}"
    )

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]:
                continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300:
                continue
            df = calculate_indicators(
                pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))

            # Per-TF state machine (isolated for backtest)
            bt_state = {'buy_active': False, 'sell_active': False, 'wait_count': 0}
            mode     = bot_state['tolerance_mode']
            max_cnt  = bot_state['max_tolerance_candles']

            for i in df[df['time'] >= start_dt].index:
                if i < 10:
                    continue
                curr = df.loc[i]

                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17):
                    continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']):
                    continue

                buy_sig, sell_sig = evaluate_state_machine_backtest(
                    tf, bt_state, curr, mode, max_cnt)

                if not (buy_sig or sell_sig):
                    continue
                if i + 1 >= len(df):
                    continue

                next_c  = df.loc[i + 1]
                entry_p = next_c['open']
                entry_t = next_c['time']
                m       = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])

                if bot_state['use_atr']:
                    tp_dist = curr['atr'] * bot_state['atr_mult_tp']
                    sl_dist = curr['atr'] * bot_state['atr_mult_sl']
                else:
                    tp_dist = bot_state['tp_pips'][tf] * bot_state['pip_value']
                    sl_dist = bot_state['sl_pips'][tf] * bot_state['pip_value']

                tp_p   = round(act_ent + (m * tp_dist), 2)
                sl_p   = round(act_ent - (m * sl_dist), 2)
                tol    = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp = (tp_p - tol) if buy_sig else (tp_p + tol)

                max_ext = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                val_c   = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                outcome = "EXPIRED"
                exit_t  = max_ext
                be_act  = False
                be_tgt  = act_ent + (m * 20 * bot_state['pip_value'])

                for vc in [v for v in val_c if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                            sl_p   = act_ent
                            be_act = True
                        if vc['low'] <= sl_p:
                            outcome = "BREAK-EVEN" if be_act else "LOSS"
                            exit_t  = vc['time']
                            break
                        if vc['high'] >= eff_tp:
                            outcome = "WIN"
                            exit_t  = vc['time']
                            break
                    else:
                        if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                            sl_p   = act_ent
                            be_act = True
                        if vc['high'] >= sl_p:
                            outcome = "BREAK-EVEN" if be_act else "LOSS"
                            exit_t  = vc['time']
                            break
                        if vc['low'] <= eff_tp:
                            outcome = "WIN"
                            exit_t  = vc['time']
                            break

                if outcome == "BREAK-EVEN":
                    p_usd = 0.0
                    be_count += 1
                elif outcome in ("WIN", "LOSS"):
                    exit_price = tp_p if outcome == "WIN" else sl_p
                    p_usd = round(
                        abs(act_ent - exit_price) * 100 * bot_state['lot_size'], 2
                    ) * (1 if outcome == "WIN" else -1)
                    if outcome == "WIN":
                        total_win  += p_usd
                        win_count  += 1
                    else:
                        total_loss += p_usd
                        loss_count += 1
                else:
                    p_usd = 0.0

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)

                trade_logs.append({
                    'Timeframe':   tf,
                    'Type':        "BUY" if buy_sig else "SELL",
                    'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price': round(act_ent, 2),
                    'TP': tp_p, 'SL': sl_p,
                    'Pips': round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p))
                        / bot_state['pip_value'], 1
                    ) if outcome in ("WIN", "LOSS") else 0,
                    'Outcome':    outcome,
                    'Profit ($)': p_usd,
                    'K':          round(curr['K'], 1),
                    'MACD_norm':  round(curr['macd_norm'], 1),
                    'OsMA_norm':  round(curr['osma_norm'], 1),
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على أي صفقات.")
            return

        from openpyxl.styles import PatternFill, Font
        df_logs      = pd.DataFrame(trade_logs)
        total_trades = win_count + loss_count
        win_rate     = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct       = round(max_dd / peak_equity * 100, 1)     if peak_equity else 0

        summary_data = {
            'البند': ['✅ الربح الكلي', '❌ الخسارة الكلية', '💰 المحصلة النهائية',
                      '🎯 نسبة الفوز', '📉 أقصى سحب (DD)', '🔄 بريك إيفن',
                      '📌 السماحية'],
            'القيمة': [
                f'{win_count} صفقة | +${round(total_win, 2)}',
                f'{loss_count} صفقة | -${abs(round(total_loss, 2))}',
                f'${round(total_prof, 2)}',
                f'{win_rate}% ({total_trades} صفقة)',
                f'${round(max_dd, 2)} ({dd_pct}%)',
                str(be_count),
                tol_desc,
            ],
        }

        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            df_logs.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='الملخص', index=False)

            ws = writer.sheets['الصفقات']
            gf = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
            rf = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            hf = PatternFill(start_color='2E4057', end_color='2E4057', fill_type='solid')
            for cell in ws[1]:
                cell.fill = hf
                cell.font = Font(color='FFFFFF', bold=True)
            oc = next((i + 1 for i, c in enumerate(ws[1]) if c.value == 'Outcome'), 9)
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                val = str(row[oc - 1].value) if len(row) >= oc else ''
                if val == 'WIN':
                    for cell in row: cell.fill = gf
                elif val == 'LOSS':
                    for cell in row: cell.fill = rf
            for col in ws.columns:
                ws.column_dimensions[col[0].column_letter].width = min(
                    max((len(str(c.value or '')) for c in col), default=8) + 3, 28)

        await send_tg_document(
            fname,
            f"📊 <b>الباك تيست</b>\n"
            f"✅ +${round(total_win, 2)} ({win_count})\n"
            f"❌ -${abs(round(total_loss, 2))} ({loss_count})\n"
            f"💰 ${round(total_prof, 2)} | 🎯 {win_rate}% | 📉 DD:{round(max_dd, 2)}"
        )
        os.remove(fname)

    except Exception as e:
        c_log(f"❌ خطأ باك تيست: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally:
        bot_state['is_backtesting'] = False


# =============================================================
# BACKTEST ENGINE — Advanced (MT5 Style)
# =============================================================

async def run_advanced_backtest(days: int = 7):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة.")
        return
    bot_state['is_backtesting'] = True
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)

    tol_desc = bot_state['tolerance_mode']
    if tol_desc == 'TIME':
        tol_desc += f"({bot_state['max_tolerance_candles']})"

    await send_tg_msg(
        f"⏳ <b>Advanced Backtest</b>\n"
        f"من: {start_dt.strftime('%Y-%m-%d')} ({days} أيام)\n"
        f"السماحية: {tol_desc}"
    )

    trade_logs   = []
    total_prof   = 0.0
    peak_equity  = 0.0
    max_dd       = 0.0
    total_win    = 0.0
    total_loss   = 0.0
    win_count    = 0
    loss_count   = 0
    be_count     = 0
    long_win     = 0
    long_loss    = 0
    short_win    = 0
    short_loss   = 0
    all_profits  = []
    consec_win   = 0
    consec_loss  = 0
    max_consec_win       = 0
    max_consec_loss      = 0
    max_consec_win_usd   = 0.0
    max_consec_loss_usd  = 0.0
    cur_w_usd    = 0.0
    cur_l_usd    = 0.0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]:
                continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300:
                continue
            df = calculate_indicators(
                pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))

            bt_state = {'buy_active': False, 'sell_active': False, 'wait_count': 0}
            mode     = bot_state['tolerance_mode']
            max_cnt  = bot_state['max_tolerance_candles']

            for i in df[df['time'] >= start_dt].index:
                if i < 10:
                    continue
                curr = df.loc[i]

                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17):
                    continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']):
                    continue

                buy_sig, sell_sig = evaluate_state_machine_backtest(
                    tf, bt_state, curr, mode, max_cnt)

                if not (buy_sig or sell_sig):
                    continue
                if i + 1 >= len(df):
                    continue

                next_c  = df.loc[i + 1]
                entry_p = next_c['open']
                entry_t = next_c['time']
                m       = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])

                tp_dist = (curr['atr'] * bot_state['atr_mult_tp']
                           if bot_state['use_atr']
                           else bot_state['tp_pips'][tf] * bot_state['pip_value'])
                sl_dist = (curr['atr'] * bot_state['atr_mult_sl']
                           if bot_state['use_atr']
                           else bot_state['sl_pips'][tf] * bot_state['pip_value'])
                tp_p    = round(act_ent + (m * tp_dist), 2)
                sl_p    = round(act_ent - (m * sl_dist), 2)
                tol     = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp  = (tp_p - tol) if buy_sig else (tp_p + tol)

                max_ext = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                val_c   = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                outcome = "EXPIRED"
                exit_t  = max_ext
                be_act  = False
                be_tgt  = act_ent + (m * 20 * bot_state['pip_value'])

                for vc in [v for v in val_c if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['low'] <= sl_p:
                            outcome = "BREAK-EVEN" if be_act else "LOSS"
                            exit_t  = vc['time']
                            break
                        if vc['high'] >= eff_tp:
                            outcome = "WIN"
                            exit_t  = vc['time']
                            break
                    else:
                        if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['high'] >= sl_p:
                            outcome = "BREAK-EVEN" if be_act else "LOSS"
                            exit_t  = vc['time']
                            break
                        if vc['low'] <= eff_tp:
                            outcome = "WIN"
                            exit_t  = vc['time']
                            break

                if outcome == "BREAK-EVEN":
                    p_usd = 0.0
                    be_count += 1
                elif outcome in ("WIN", "LOSS"):
                    exit_price = tp_p if outcome == "WIN" else sl_p
                    p_usd = round(
                        abs(act_ent - exit_price) * 100 * bot_state['lot_size'], 2
                    ) * (1 if outcome == "WIN" else -1)
                else:
                    p_usd = 0.0

                if outcome == "WIN":
                    total_win  += p_usd; win_count  += 1
                    consec_win += 1; cur_w_usd += p_usd
                    consec_loss = 0;  cur_l_usd = 0.0
                    if consec_win > max_consec_win:
                        max_consec_win = consec_win
                        max_consec_win_usd = cur_w_usd
                    if buy_sig: long_win  += 1
                    else:       short_win += 1
                elif outcome == "LOSS":
                    total_loss  += p_usd; loss_count  += 1
                    consec_loss += 1; cur_l_usd += p_usd
                    consec_win  = 0;  cur_w_usd = 0.0
                    if consec_loss > max_consec_loss:
                        max_consec_loss = consec_loss
                        max_consec_loss_usd = cur_l_usd
                    if buy_sig: long_loss  += 1
                    else:       short_loss += 1

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)
                all_profits.append(p_usd)
                _dh = (curr['time'].hour + 3) % 24

                trade_logs.append({
                    'Timeframe':    tf,
                    'Type':         "BUY" if buy_sig else "SELL",
                    'Entry Time':   (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':    (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price':  round(act_ent, 2),
                    'TP': tp_p, 'SL': sl_p,
                    'Pips': round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p))
                        / bot_state['pip_value'], 1
                    ) if outcome in ("WIN", "LOSS") else 0,
                    'Outcome':    outcome,
                    'Profit ($)': p_usd,
                    'K':          round(curr['K'], 1),
                    'MACD_norm':  round(curr['macd_norm'], 1),
                    'OsMA_norm':  round(curr['osma_norm'], 1),
                    'Hour_Damascus': _dh,
                    'Weekday': curr['time'].strftime('%a'),
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على صفقات.")
            return

        total_trades    = win_count + loss_count
        win_rate        = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct          = round(max_dd / peak_equity * 100, 1)     if peak_equity else 0
        profit_factor   = round(total_win / abs(total_loss), 2)    if total_loss   else 999
        expected_payoff = round(total_prof / total_trades, 2)      if total_trades else 0
        recovery_factor = round(total_prof / max_dd, 2)            if max_dd       else 999
        wins_only       = [p for p in all_profits if p > 0]
        losses_only     = [p for p in all_profits if p < 0]
        avg_win         = round(sum(wins_only)   / len(wins_only),   2) if wins_only   else 0
        avg_loss        = round(sum(losses_only) / len(losses_only), 2) if losses_only else 0
        largest_win     = round(max(wins_only),   2) if wins_only   else 0
        largest_loss    = round(min(losses_only), 2) if losses_only else 0

        df_t        = pd.DataFrame(trade_logs)
        actv        = df_t[df_t['Outcome'].isin(['WIN', 'LOSS'])]
        hour_counts = actv.groupby('Hour_Damascus').size()
        day_counts  = actv.groupby('Weekday').size()

        def bar_chart(dd, width=18):
            if not dd:
                return "(لا بيانات)"
            mx = max(dd.values())
            return "\n".join(
                f"  {str(k):>4} |{'█' * int(v / mx * width):<{width}}| {v}"
                for k, v in sorted(dd.items())
            )

        report = (
            f"📊 <b>Advanced Strategy Report — {days} يوم</b>\n"
            f"📌 السماحية: {tol_desc}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>💰 الأرباح</b>\n"
            f"  صافي الربح:      ${round(total_prof, 2)}\n"
            f"  إجمالي الربح:    +${round(total_win, 2)}\n"
            f"  إجمالي الخسارة:  -${abs(round(total_loss, 2))}\n"
            f"  Profit Factor:   {profit_factor}\n"
            f"  Expected Payoff: ${expected_payoff}\n"
            f"  Recovery Factor: {recovery_factor}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📉 Drawdown</b>\n"
            f"  أقصى DD: ${round(max_dd, 2)} ({dd_pct}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📈 الصفقات</b>\n"
            f"  الإجمالي: {total_trades} | فوز: {win_count} ({win_rate}%) | خسارة: {loss_count}\n"
            f"  Long  W/L: {long_win}/{long_loss} | Short W/L: {short_win}/{short_loss}\n"
            f"  بريك إيفن: {be_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🔢 إحصاءات</b>\n"
            f"  أكبر ربح: +${largest_win} | أكبر خسارة: ${largest_loss}\n"
            f"  متوسط ربح: +${avg_win} | متوسط خسارة: ${avg_loss}\n"
            f"  أكبر سلسلة فوز:   {max_consec_win} (+${round(max_consec_win_usd, 2)})\n"
            f"  أكبر سلسلة خسارة: {max_consec_loss} (-${abs(round(max_consec_loss_usd, 2))})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🕐 بالساعة (دمشق):</b>\n<pre>{bar_chart(hour_counts.to_dict())}</pre>\n"
            f"<b>📅 بالأيام:</b>\n<pre>{bar_chart(day_counts.to_dict())}</pre>"
        )
        await send_tg_msg(report)

        from openpyxl.styles import PatternFill, Font
        xlsx_adv = f"ADV_{datetime.now().strftime('%H%M%S')}.xlsx"
        df_exec  = df_t.drop(columns=['Hour_Damascus', 'Weekday'], errors='ignore')
        stats    = {
            'المقياس': [
                'صافي الربح', 'إجمالي الربح', 'إجمالي الخسارة',
                'Profit Factor', 'Expected Payoff', 'Recovery Factor',
                'أقصى DD', 'DD%', 'إجمالي الصفقات', 'فوز', 'خسارة',
                'نسبة الفوز', 'بريك إيفن', 'Long W/L', 'Short W/L',
                'أكبر ربح', 'أكبر خسارة', 'متوسط ربح', 'متوسط خسارة',
                'أكبر سلسلة فوز', 'أكبر سلسلة خسارة', 'السماحية',
            ],
            'القيمة': [
                f'${round(total_prof, 2)}', f'+${round(total_win, 2)}',
                f'-${abs(round(total_loss, 2))}', profit_factor,
                expected_payoff, recovery_factor,
                f'${round(max_dd, 2)}', f'{dd_pct}%',
                total_trades, win_count, loss_count,
                f'{win_rate}%', be_count,
                f'{long_win}/{long_loss}', f'{short_win}/{short_loss}',
                f'+${largest_win}', f'${largest_loss}',
                f'+${avg_win}', f'${avg_loss}',
                f'{max_consec_win}(+${round(max_consec_win_usd, 2)})',
                f'{max_consec_loss}(-${abs(round(max_consec_loss_usd, 2))})',
                tol_desc,
            ],
        }
        with pd.ExcelWriter(xlsx_adv, engine='openpyxl') as writer:
            df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(stats).to_excel(writer, sheet_name='الإحصاءات', index=False)
            ws  = writer.sheets['الصفقات']
            gf  = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
            rf  = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            hf  = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')
            for cell in ws[1]:
                cell.fill = hf
                cell.font = Font(color='FFFFFF', bold=True)
            oc = next((i + 1 for i, c in enumerate(ws[1]) if c.value == 'Outcome'), 9)
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                val = str(row[oc - 1].value) if len(row) >= oc else ''
                if val == 'WIN':
                    for cell in row: cell.fill = gf
                elif val == 'LOSS':
                    for cell in row: cell.fill = rf
            for col in ws.columns:
                ws.column_dimensions[col[0].column_letter].width = min(
                    max((len(str(c.value or '')) for c in col), default=8) + 3, 28)

        await send_tg_document(xlsx_adv, f"📊 Advanced Report — {days} يوم")
        os.remove(xlsx_adv)

    except Exception as e:
        c_log(f"❌ خطأ Advanced BT: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally:
        bot_state['is_backtesting'] = False


# =============================================================
# LIVE POSITION MONITOR  (Break-Even)
# =============================================================

async def position_monitor():
    while True:
        try:
            if (bot_state['live_connected']
                    and bot_state['use_be']
                    and bot_state['connection_obj']):
                positions = await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] != bot_state['symbol']:
                        continue
                    op = p['openPrice']
                    tp = p.get('takeProfit')
                    sl = p.get('stopLoss')
                    cp = p['currentPrice']
                    if (tp and sl != op
                            and abs(cp - op) >= 20 * bot_state['pip_value']):
                        is_buy = tp > op
                        if (is_buy and cp > op) or (not is_buy and cp < op):
                            await bot_state['connection_obj'].modify_position(
                                p['id'], stop_loss=op)
                            await send_tg_msg(
                                f"🛡️ <b>BE</b> تأمين الدخول لصفقة: {p['id']}")
        except:
            pass
        await asyncio.sleep(5)


# =============================================================
# LIVE TIMEFRAME SCANNER
# =============================================================

async def timeframe_scanner(tf: str):
    c_log(f"✅ ماسح [{tf}] يعمل.")
    # We keep a rolling buffer of candles for this timeframe
    candle_buffer: list = []

    while True:
        try:
            if bot_state['status'] == 'RUNNING' and bot_state['active_tfs'][tf]:
                if not bot_state['live_connected'] or not bot_state['account_obj']:
                    bot_state['market_data'][tf] = "⏸ بانتظار الاتصال (Offline)"
                    await asyncio.sleep(5)
                    continue

                try:
                    raw = await bot_state['account_obj'].get_historical_candles(
                        bot_state['symbol'], tf, limit=500)
                except:
                    await asyncio.sleep(15)
                    continue

                df   = calculate_indicators(pd.DataFrame(raw))
                curr = df.iloc[-2]     # last CLOSED candle
                prev_close = df.iloc[-1]
                now_utc = datetime.now(timezone.utc)

                danger_now = (bot_state['use_danger_filter']
                              and is_danger_time(now_utc))
                time_block = (bot_state['use_time_filter']
                              and not (8 <= now_utc.hour <= 17))

                if time_block or danger_now:
                    bot_state['market_data'][tf] = (
                        f"⏸ خمول | {prev_close['close']:.2f}")
                else:
                    bot_state['market_data'][tf] = (
                        f"{prev_close['close']:.2f} | "
                        f"K:{curr['K']:.1f} "
                        f"MACD%:{curr['macd_norm']:.1f} "
                        f"OsMA%:{curr['osma_norm']:.1f}")

                    if bot_state['last_signal_time'][tf] != curr['time']:
                        buy_sig, sell_sig = evaluate_state_machine(tf, curr)

                        if buy_sig or sell_sig:
                            # Spread check
                            skip = False
                            if bot_state['use_max_spread']:
                                try:
                                    tick = await bot_state['connection_obj'].get_tick(
                                        bot_state['symbol'])
                                    spread = ((tick['ask'] - tick['bid'])
                                              / bot_state['pip_value'])
                                    if spread > bot_state['max_spread_pips']:
                                        skip = True
                                        c_log(f"[{tf}] ⚠️ سبريد مرتفع: {spread:.1f}")
                                except:
                                    pass

                            if not skip:
                                bot_state['last_signal_time'][tf] = curr['time']
                                p   = prev_close['close']
                                m   = 1 if buy_sig else -1
                                t_str = "شراء 🟢 BUY" if buy_sig else "بيع 🔴 SELL"

                                tp_dist = (curr['atr'] * bot_state['atr_mult_tp']
                                           if bot_state['use_atr']
                                           else bot_state['tp_pips'][tf]
                                           * bot_state['pip_value'])
                                sl_dist = (curr['atr'] * bot_state['atr_mult_sl']
                                           if bot_state['use_atr']
                                           else bot_state['sl_pips'][tf]
                                           * bot_state['pip_value'])
                                tp = round(p + (m * tp_dist), 2)
                                sl = round(p - (m * sl_dist), 2)

                                c_log(f"🎯 [{tf}] {t_str} — جاري التنفيذ...")
                                try:
                                    if buy_sig:
                                        await bot_state['connection_obj'].create_market_buy_order(
                                            bot_state['symbol'],
                                            bot_state['lot_size'],
                                            stop_loss=sl, take_profit=tp)
                                    else:
                                        await bot_state['connection_obj'].create_market_sell_order(
                                            bot_state['symbol'],
                                            bot_state['lot_size'],
                                            stop_loss=sl, take_profit=tp)
                                    await send_tg_msg(
                                        f"🚨 <b>تم فتح صفقة!</b>\n"
                                        f"النوع: {t_str}\n"
                                        f"الفريم: {tf} | السماحية: "
                                        f"{bot_state['tolerance_mode']}\n"
                                        f"السعر: {p} | TP: {tp} | SL: {sl}\n"
                                        f"K:{curr['K']:.1f} "
                                        f"MACD%:{curr['macd_norm']:.1f} "
                                        f"OsMA%:{curr['osma_norm']:.1f}"
                                    )
                                except Exception as e:
                                    await send_tg_msg(f"❌ <b>فشل التنفيذ!</b>\n{e}")

            await asyncio.sleep(10)
        except:
            await asyncio.sleep(15)


# =============================================================
# TELEGRAM HANDLER
# =============================================================

async def process_tg_update(update):
    # ── TEXT MESSAGES ──────────────────────────────────────────
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']

        if msg == '/start':
            await send_tg_msg(
                "🤖 <b>مرحباً بك في لوحة التحكم!</b>",
                get_main_keyboard())

        elif msg == '/debug':
            if not bot_state['live_connected']:
                await send_tg_msg("⚠️ البوت غير متصل باللايف.")
            else:
                try:
                    tick = await bot_state['connection_obj'].get_tick(
                        bot_state['symbol'])
                    raw = await bot_state['account_obj'].get_historical_candles(
                        bot_state['symbol'], '5m', limit=500)
                    df   = calculate_indicators(pd.DataFrame(raw))
                    curr = df.iloc[-2]
                    spread = round((tick['ask'] - tick['bid'])
                                   / bot_state['pip_value'], 1)
                    st = bot_state['setup_state']['5m']
                    await send_tg_msg(
                        f"✅ <b>حالة النظام [5m]:</b>\n"
                        f"السعر: {df.iloc[-1]['close']:.2f} | سبريد: {spread}\n"
                        f"K={curr['K']:.1f}\n"
                        f"MACD_norm={curr['macd_norm']:.1f} (GREEN)\n"
                        f"OsMA_norm={curr['osma_norm']:.1f} (RED)\n"
                        f"━━━━━━\n"
                        f"BUY  setup active: {st['buy_active']}\n"
                        f"SELL setup active: {st['sell_active']}\n"
                        f"wait_count: {st['wait_count']}\n"
                        f"Tolerance: {bot_state['tolerance_mode']}"
                    )
                except Exception as e:
                    await send_tg_msg(f"❌ خطأ: {e}")

        elif msg.startswith('/set'):
            p = msg.split()
            if len(p) == 4:
                bot_state[p[2] + '_pips'][p[1]] = int(p[3])
                await send_tg_msg(
                    f"✅ تم تحديث {p[2]} لفريم {p[1]} إلى {p[3]}")

        elif msg.startswith('/backtest'):
            try:
                date_str = msg.split()[1]
                st = datetime.strptime(date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc)
                asyncio.create_task(run_oanda_backtest(st))
                await send_tg_msg(f"✅ باك تيست من: {date_str}")
            except:
                await send_tg_msg("⚠️ استخدم: /backtest YYYY-MM-DD")

    # ── CALLBACK QUERIES ──────────────────────────────────────
    elif 'callback_query' in update:
        q       = update['callback_query']
        d       = q['data']
        chat_id = q['message']['chat']['id']
        msg_id  = q['message']['message_id']
        bot_state['chat_id'] = chat_id

        # Navigation
        if d == "menu_main":
            await edit_tg_msg(chat_id, msg_id,
                              "🏠 القائمة الرئيسية:", get_main_keyboard())

        elif d == "toggle_status":
            bot_state['status'] = (
                'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING')
            await edit_tg_msg(chat_id, msg_id,
                              "🏠 القائمة الرئيسية:", get_main_keyboard())

        # Tolerance toggle (main menu quick toggle)
        elif d == "toggle_tolerance":
            bot_state['tolerance_mode'] = (
                'TIME' if bot_state['tolerance_mode'] == 'LEVEL' else 'LEVEL')
            # Reset all setup states when mode changes
            for tf in _TFS:
                bot_state['setup_state'][tf] = {
                    'buy_active': False, 'sell_active': False, 'wait_count': 0}
            await edit_tg_msg(chat_id, msg_id,
                              "🏠 القائمة الرئيسية:", get_main_keyboard())

        # Live connection
        elif d == "toggle_live_conn":
            if not bot_state['live_connected']:
                await edit_tg_msg(chat_id, msg_id,
                                  "⏳ جاري الاتصال...", get_main_keyboard())
                try:
                    api = MetaApi(METAAPI_TOKEN)
                    bot_state['account_obj'] = (
                        await api.metatrader_account_api.get_account(ACCOUNT_ID))
                    bot_state['connection_obj'] = (
                        bot_state['account_obj'].get_rpc_connection())
                    await bot_state['connection_obj'].connect()
                    await bot_state['connection_obj'].wait_synchronized()
                    bot_state['live_connected'] = True
                    await edit_tg_msg(chat_id, msg_id,
                                      "✅ تم الاتصال!", get_main_keyboard())
                except Exception as e:
                    await edit_tg_msg(chat_id, msg_id,
                                      f"❌ فشل: {e}", get_main_keyboard())
            else:
                bot_state['live_connected'] = False
                bot_state['connection_obj'] = bot_state['account_obj'] = None
                await edit_tg_msg(chat_id, msg_id,
                                  "🔌 تم الفصل.", get_main_keyboard())

        # Filters menu
        elif d == "menu_filters":
            await edit_tg_msg(chat_id, msg_id,
                              "🎛 <b>فلاتر وإعدادات التداول:</b>",
                              get_filters_keyboard())

        elif d == "set_tol_level":
            bot_state['tolerance_mode'] = 'LEVEL'
            for tf in _TFS:
                bot_state['setup_state'][tf] = {
                    'buy_active': False, 'sell_active': False, 'wait_count': 0}
            await edit_tg_msg(chat_id, msg_id,
                              "✅ <b>LEVEL</b> مُفعّل: إلغاء عند خروج Stoch من المنطقة",
                              get_filters_keyboard())

        elif d == "set_tol_time":
            bot_state['tolerance_mode'] = 'TIME'
            for tf in _TFS:
                bot_state['setup_state'][tf] = {
                    'buy_active': False, 'sell_active': False, 'wait_count': 0}
            await edit_tg_msg(chat_id, msg_id,
                              "✅ <b>TIME</b> مُفعّل: إلغاء بعد N شموع",
                              get_filters_keyboard())

        elif d == "inc_tol_cnt":
            bot_state['max_tolerance_candles'] = min(
                bot_state['max_tolerance_candles'] + 1, 20)
            await edit_tg_msg(chat_id, msg_id,
                              "🎛 <b>فلاتر وإعدادات التداول:</b>",
                              get_filters_keyboard())

        elif d == "dec_tol_cnt":
            bot_state['max_tolerance_candles'] = max(
                bot_state['max_tolerance_candles'] - 1, 1)
            await edit_tg_msg(chat_id, msg_id,
                              "🎛 <b>فلاتر وإعدادات التداول:</b>",
                              get_filters_keyboard())

        elif d == "toggle_time":
            bot_state['use_time_filter'] = not bot_state['use_time_filter']
            await edit_tg_msg(chat_id, msg_id,
                              "🎛 <b>فلاتر وإعدادات التداول:</b>",
                              get_filters_keyboard())

        elif d == "toggle_danger":
            bot_state['use_danger_filter'] = not bot_state['use_danger_filter']
            await edit_tg_msg(chat_id, msg_id,
                              "🎛 <b>فلاتر وإعدادات التداول:</b>",
                              get_filters_keyboard())

        # Timeframes menu
        elif d == "menu_tfs":
            await edit_tg_msg(chat_id, msg_id,
                              "⏱ إدارة الفريمات:", get_tf_keyboard())

        elif d.startswith("toggle_tf_"):
            tf = d.split("_")[2]
            bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            # Reset that TF's state machine
            bot_state['setup_state'][tf] = {
                'buy_active': False, 'sell_active': False, 'wait_count': 0}
            await edit_tg_msg(chat_id, msg_id,
                              "⏱ إدارة الفريمات:", get_tf_keyboard())

        # Settings menu
        elif d == "menu_settings":
            await edit_tg_msg(chat_id, msg_id,
                              "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "toggle_be":
            bot_state['use_be'] = not bot_state['use_be']
            await edit_tg_msg(chat_id, msg_id,
                              "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "toggle_atr":
            bot_state['use_atr'] = not bot_state['use_atr']
            await edit_tg_msg(chat_id, msg_id,
                              "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "toggle_spread":
            bot_state['use_max_spread'] = not bot_state['use_max_spread']
            await edit_tg_msg(chat_id, msg_id,
                              "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "inc_lot":
            bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2)
            await edit_tg_msg(chat_id, msg_id,
                              "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "dec_lot":
            bot_state['lot_size'] = max(
                0.01, round(bot_state['lot_size'] - 0.01, 2))
            await edit_tg_msg(chat_id, msg_id,
                              "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "view_tpsl":
            txt = "📖 <b>أهداف الفريمات:</b>\n" + "\n".join(
                f"[{tf}] TP:{bot_state['tp_pips'][tf]} | SL:{bot_state['sl_pips'][tf]}"
                for tf in bot_state['timeframes'])
            await edit_tg_msg(chat_id, msg_id, txt, get_settings_keyboard())

        # Reports
        elif d == "report":
            lines = []
            for tf in bot_state['timeframes']:
                if bot_state['active_tfs'][tf]:
                    st  = bot_state['setup_state'][tf]
                    buy = "🟡" if st['buy_active']  else "⬜"
                    sel = "🟡" if st['sell_active'] else "⬜"
                    lines.append(
                        f"[{tf}] {bot_state['market_data'][tf]}\n"
                        f"       BUY:{buy} SELL:{sel} wait:{st['wait_count']}")
            txt = "📊 <b>حالة السوق الحية:</b>\n" + "\n".join(lines)
            await edit_tg_msg(chat_id, msg_id, txt, get_main_keyboard())

        elif d == "account":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                try:
                    acc = await bot_state['connection_obj'].get_account_information()
                    await edit_tg_msg(
                        chat_id, msg_id,
                        f"💳 <b>الحساب:</b>\n"
                        f"رصيد: {acc['balance']}\n"
                        f"إيكويتي: {acc['equity']}",
                        get_main_keyboard())
                except:
                    pass
            else:
                await send_tg_msg("يجب الاتصال بالسيرفر أولاً!")

        # Backtest menu
        elif d == "menu_backtest":
            kb = {"inline_keyboard": [
                [{"text": "📊 1 يوم",   "callback_data": "bto_1"},
                 {"text": "📊 3 أيام",  "callback_data": "bto_3"},
                 {"text": "📊 7 أيام",  "callback_data": "bto_7"}],
                [{"text": "🔬 Advanced Report (MT5 Style) — 7 أيام",
                  "callback_data": "bto_adv_7"}],
                [{"text": "🔬 Advanced Report — 14 يوم",
                  "callback_data": "bto_adv_14"}],
                [{"text": "🔙 رجوع", "callback_data": "menu_main"}],
            ]}
            await edit_tg_msg(
                chat_id, msg_id,
                "اختر المدة أو أرسل /backtest YYYY-MM-DD:", kb)

        elif d.startswith("bto_adv_"):
            adv_days = int(d.split('_')[2])
            asyncio.create_task(run_advanced_backtest(days=adv_days))

        elif d.startswith("bto_"):
            days = int(d.split('_')[1])
            asyncio.create_task(
                run_oanda_backtest(
                    datetime.now(timezone.utc) - timedelta(days=days)))

        # Close all
        elif d == "close_all":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                async def _close():
                    try:
                        pos = await bot_state['connection_obj'].get_positions()
                        for p in pos:
                            await bot_state['connection_obj'].close_position(p['id'])
                        await send_tg_msg("✅ تم إغلاق جميع الصفقات.")
                    except Exception as e:
                        await send_tg_msg(f"❌ خطأ: {e}")
                asyncio.create_task(_close())

        elif d == "noop":
            pass

        await answer_callback(q['id'])


# =============================================================
# TELEGRAM POLLING
# =============================================================

async def telegram_polling_loop():
    c_log("✅ خدمة التلغرام جاهزة.")
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(url, params={
                    'offset':  bot_state['last_update_id'] + 1,
                    'timeout': 10,
                }) as r:
                    if r.status == 200:
                        for u in (await r.json()).get('result', []):
                            bot_state['last_update_id'] = u['update_id']
                            asyncio.create_task(process_tg_update(u))
            except:
                await asyncio.sleep(2)


# =============================================================
# WEB SERVER  +  MAIN
# =============================================================

async def handle_ping(request):
    return web.Response(text="Gold Scalper Bot v2 is ALIVE!")


async def main():
    app    = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    c_log(f"🚀 وب سيرفر يعمل على بورت {port}")

    tasks = [asyncio.create_task(timeframe_scanner(tf))
             for tf in bot_state['timeframes']]
    tasks.append(asyncio.create_task(telegram_polling_loop()))
    tasks.append(asyncio.create_task(position_monitor()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
