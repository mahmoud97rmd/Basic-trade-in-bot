import asyncio
import aiohttp
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, timezone
from metaapi_cloud_sdk import MetaApi
from aiohttp import web

# =============================================================
# CONFIGURATION
# =============================================================
METAAPI_TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9..NRMo-BO9ezZBEb4XmCQzkMsRN1iAz1rVSk7XWFP-ZGS_AZEyxSfIjnJ5w-r4egazV7tnxNLjjMuAdUb25T3ur3XWKCL4Jo9LFPy9tZzhIMRtlhq8d6YAHK9uxJclqJv5BZQFDeMeiFtyalLNjaE100Lp2zEnGWwlloxF-dpCw5DXvVKeGfMyVx4L2kisshcysDo7OeMkDBU1UB7leHi2eviEl7XQCpmhxdzT4BwMkf8YERx2jouKVu8-koVy00aon0drktGBSlQDOFw2WV0hg-VUfeCBR_Hgw2czqKVJ_lj_ZN3EsjWirirpiuXWbtwdD-VPokjKtX1z3ugcSTS1nd2iFIzauUHdOfb7Jl0R6cm8FosVS-4Iu046DiMsrxiAJ4PBywOXQhsFzZiePqmil1w5HHCxrw_78HNR9XcjBETMpHx9W48llIeUOkBVbsKfBP5iYtGSjS52i0QgpvHkfKrtXfbkMT0_9yJFG2kfZJHwJ5BJzWT4aKXto3l6iGe45xe4ZJhYhZX_RkC6dxR2w84M-uY-wlqiv_sxjHNOguSyOx4lfaeoq5H-LuJiWpHAYxEJUQWoQAQ7PObZOXCDWLRc_vP2gcbv1qYxTjD54FHnqhyf-oTGzAkWG5CVQFKpp9jTHQ3pXEYTSgIUTfHDbtoesAY1HG3nHcHbwujnqo0'
ACCOUNT_ID    = '7d54fa6f-eaf7-4637-92a1-e0356ee729f8'
TG_TOKEN      = '8779425898:AAG2tyWLIasXmvFlTWjf9tqWuHO08QHJvgk'
OANDA_ID      = '101-001-39389982-001'
OANDA_API     = 'd05b25b3f1ce0c8fa105ffefa45efb01-a5c26f544a26a4f810f1809913a2795f'
OANDA_URL     = 'https://api-fxpractice.oanda.com/v3'

_TFS = ['1m', '2m', '3m', '5m', '15m']


def c_log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================
# GLOBAL STATE
# =============================================================
# strategy_mode:
#   'STOCH_NEW'   = K يتجاوز مستوى ثابت (10/15/20)
#   'STOCH_OLD'   = K يتقاطع مع D داخل منطقة التشبع
#   'COMPOSITE'   = الاستراتيجية الجديدة: Stoch + RSI2 + OsMA + MACD (State Machine)
#
# filter_mode (لـ STOCH_NEW و STOCH_OLD فقط):
#   'FULL'   = توازي ema15 > ema50 > ema150
#   'SIMPLE' = توازي ema50 > ema150
#   'NO_MA'  = بلا موفينجات
#
# tolerance_mode (لـ COMPOSITE فقط):
#   'LEVEL'  = إلغاء Setup عند خروج Stoch من المنطقة
#   'TIME'   = إلغاء Setup بعد N شمعة
# =============================================================
bot_state = {
    # ── وضع التشغيل العام ──
    'status':       'RUNNING',
    'symbol':       'XAUUSD@',
    'live_connected': False,
    'timeframes':   _TFS,
    'active_tfs':   {'1m': False, '2m': True, '3m': True, '5m': False, '15m': False},
    'lot_size':     0.05,
    'pip_value':    0.1,
    'spread_pips':  2.2,
    'chat_id':      None,
    'last_update_id': 0,
    'tp_pips':      {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80},
    'sl_pips':      {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},

    # ── اختيار الاستراتيجية ──
    'strategy_mode': 'STOCH_NEW',   # 'STOCH_NEW' | 'STOCH_OLD' | 'COMPOSITE'

    # ── إعدادات STOCH_NEW / STOCH_OLD ──
    'filter_mode':    'NO_MA',       # 'FULL' | 'SIMPLE' | 'NO_MA'
    'stoch_k':        5,
    'stoch_d':        5,
    'stoch_smooth':   5,
    'use_stoch_deep': True,          # 10/90
    'use_stoch_mid':  True,          # 15/85
    'use_stoch_shal': False,         # 20/80
    'use_f_cons':     False,
    'cons_count':     3,

    # ── إعدادات COMPOSITE ──
    # نافذة البحث عن تقاطع K/D حول شمعة العمود
    'comp_lookback':      5,   # شموع للخلف قبل ظهور العمود  (1-10)
    'comp_tolerance_fwd': 5,   # شموع للأمام بعد ظهور العمود (1-10)
    # مناطق Stochastic للتقاطع
    'comp_use_deep': True,   # BUY avg(K,D)<=10  / SELL avg(K,D)>=90
    'comp_use_mid':  True,   # BUY avg(K,D)<=20  / SELL avg(K,D)>=80
    'comp_use_shal': False,  # BUY avg(K,D)<=30  / SELL avg(K,D)>=70
    'setup_state': {
        tf: {
            'buy_macd_wait': 999, 'sell_osma_wait': 999,
            'buy_stoch_wait': 999, 'sell_stoch_wait': 999,
            'buy_macd_val': 0.0,  'sell_osma_val': 0.0,
            'buy_stoch_lbl': '',  'sell_stoch_lbl': ''
        }
        for tf in _TFS
    },

    # ── فلاتر الوقت (مشتركة) ──
    'use_time_filter':   False,
    'use_danger_filter': True,

    # ── إدارة المخاطر (مشتركة) ──
    'use_be':          False,
    'use_atr':         False,
    'use_max_spread':  True,
    'max_spread_pips': 3.0,
    'atr_mult_tp':     1.5,
    'atr_mult_sl':     3.0,
    'tp_tolerance_pips': 5.0,

    # ── بيانات مباشرة ──
    'market_data':      {tf: "⏸ بانتظار الاتصال (Offline)" for tf in _TFS},
    'last_signal_time': {tf: None for tf in _TFS},
    'connection_obj':   None,
    'account_obj':      None,
    'is_backtesting':   False,
}


# =============================================================
# INDICATOR ENGINE — يحسب كل شيء دفعة واحدة
# =============================================================

def _rsi(series: pd.Series, period: int) -> pd.Series:
    """RSI بـ Wilder smoothing."""
    delta  = series.diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    avg_g  = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_l  = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs     = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _osma_col(series: pd.Series, fast: int, slow: int, signal: int) -> pd.Series:
    """OsMA = MACD_line − Signal."""
    macd = _ema(series, fast) - _ema(series, slow)
    return macd - _ema(macd, signal)


def _macd_hist(series: pd.Series, fast: int, slow: int, signal: int) -> pd.Series:
    """MACD histogram = MACD_line − Signal."""
    macd = _ema(series, fast) - _ema(series, slow)
    return macd - _ema(macd, signal)


def _rolling_minmax(series: pd.Series, window: int = 200) -> pd.Series:
    """Scale to [0, 100] with rolling min-max."""
    rmin  = series.rolling(window, min_periods=1).min()
    rmax  = series.rolling(window, min_periods=1).max()
    denom = (rmax - rmin).replace(0, np.nan)
    return (100.0 * (series - rmin) / denom).fillna(50.0)


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    يحسب جميع المؤشرات المطلوبة للاستراتيجيات الثلاث.

    أعمدة مضافة:
        ema15, ema50, ema150        ← فلتر الترند (STOCH_NEW/OLD)
        K, D                        ← Stochastic (10,2,10) مشترك
        rsi2                        ← RSI(2) على Close
        osma_raw / osma_norm        ← OsMA(200,5,200) على RSI2   [RED]
        macd_rsi / osma_macd        ← MACD+OsMA على RSI2     [COMPOSITE]
        atr                         ← ATR(14)
    """
    if df.empty:
        return df

    # ── Moving Averages (STOCH strategies) ──────────────────
    df['ema15']  = _ema(df['close'], 15)
    df['ema50']  = _ema(df['close'], 50)
    df['ema150'] = _ema(df['close'], 150)

    # ── Stochastic (10, 2, 10) — مشترك بين الثلاث ──────────
    stoch_k   = bot_state.get('stoch_k', 10)       # للـ STOCH يستخدم قيمة المستخدم
    stoch_s   = bot_state.get('stoch_smooth', 2)    # للـ COMPOSITE ثابت 2
    stoch_d   = bot_state.get('stoch_d', 10)        # للـ COMPOSITE ثابت 10

    # نحسب نسختين: واحدة للستوكاستيك القابل للتعديل، وواحدة ثابتة للـ COMPOSITE
    low_min   = df['low'].rolling(stoch_k).min()
    high_max  = df['high'].rolling(stoch_k).max()
    denom     = (high_max - low_min).replace(0, 1e-10)
    k_raw     = 100.0 * (df['close'] - low_min) / denom
    df['K']   = k_raw.ewm(span=stoch_s, adjust=False).mean()
    df['D']   = df['K'].ewm(span=stoch_d, adjust=False).mean()

    # Stochastic ثابت للـ COMPOSITE (10,2,10)
    lm10   = df['low'].rolling(10).min()
    hm10   = df['high'].rolling(10).max()
    dn10   = (hm10 - lm10).replace(0, 1e-10)
    kr10   = 100.0 * (df['close'] - lm10) / dn10
    df['K_comp'] = kr10.ewm(span=2, adjust=False).mean()
    df['D_comp'] = df['K_comp'].ewm(span=10, adjust=False).mean()

    # ── Sub-window 2: البنية الصحيحة كما في MT5 ─────────────
    # RSI(2) على Close    → لوحة خلفية فقط [0..100]
    # MACD(1000,5,5) على RSI(2)  → أعمدة خضراء = macd_rsi
    # OsMA(200,5,200) على MACD   → أعمدة حمراء = osma_macd
    # Fire level: 10 خط ثابت
    # BUY:  macd_rsi  >= 10
    # SELL: osma_macd >= 10

    df['rsi2']     = _rsi(df['close'], 2)

    # MACD(1000,5,5) مطبق على rsi2
    _ml_rsi        = _ema(df['rsi2'], 1000) - _ema(df['rsi2'], 5)
    df['macd_rsi'] = _ml_rsi - _ema(_ml_rsi, 5)

    # OsMA(200,5,200) مطبق على macd_rsi
    _ml_osma        = _ema(df['macd_rsi'], 200) - _ema(df['macd_rsi'], 5)
    df['osma_macd'] = _ml_osma - _ema(_ml_osma, 200)

    # aliases للتوافق
    df['macd_raw'] = df['macd_rsi']
    df['osma_raw'] = df['osma_macd']

    # ── ATR(14) ─────────────────────────────────────────────
    tr0       = abs(df['high'] - df['low'])
    tr1       = abs(df['high'] - df['close'].shift())
    tr2       = abs(df['low']  - df['close'].shift())
    df['atr'] = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1).rolling(14).mean().bfill()

    return df


# =============================================================
# STRATEGY A — STOCH_NEW  (K يتجاوز مستوى ثابت)
# STRATEGY B — STOCH_OLD  (K يتقاطع مع D في المنطقة)
# =============================================================

def get_stoch_signals(prev_k, prev_d, curr_k, curr_d):
    """
    يُعيد (buy_sig, sell_sig, b_label, s_label)
    بناءً على strategy_mode (STOCH_NEW أو STOCH_OLD).
    """
    mode = bot_state['strategy_mode']

    if mode == 'STOCH_NEW':
        buy_deep  = (prev_k <= 10) and (curr_k > 10) and bot_state['use_stoch_deep']
        buy_mid   = (10 < prev_k <= 15) and (curr_k > 15) and bot_state['use_stoch_mid']
        buy_shal  = (15 < prev_k <= 20) and (curr_k > 20) and bot_state['use_stoch_shal']
        sell_deep = (prev_k >= 90) and (curr_k < 90) and bot_state['use_stoch_deep']
        sell_mid  = (85 <= prev_k < 90) and (curr_k < 85) and bot_state['use_stoch_mid']
        sell_shal = (80 <= prev_k < 85) and (curr_k < 80) and bot_state['use_stoch_shal']

    else:  # STOCH_OLD — K/D crossover في منطقة التشبع
        k_cross_up   = (prev_k < prev_d) and (curr_k >= curr_d)
        k_cross_down = (prev_k > prev_d) and (curr_k <= curr_d)
        avg_k = (prev_k + curr_k) / 2.0
        buy_deep  = k_cross_up   and (avg_k <= 10) and bot_state['use_stoch_deep']
        buy_mid   = k_cross_up   and (10 < avg_k <= 15) and bot_state['use_stoch_mid']
        buy_shal  = k_cross_up   and (15 < avg_k <= 20) and bot_state['use_stoch_shal']
        sell_deep = k_cross_down and (avg_k >= 90) and bot_state['use_stoch_deep']
        sell_mid  = k_cross_down and (85 <= avg_k < 90) and bot_state['use_stoch_mid']
        sell_shal = k_cross_down and (80 <= avg_k < 85) and bot_state['use_stoch_shal']

    buy_sig  = buy_deep  or buy_mid  or buy_shal
    sell_sig = sell_deep or sell_mid or sell_shal
    b_label  = "DEEP(10)" if buy_deep  else "MID(15)"  if buy_mid  else "SHAL(20)"
    s_label  = "DEEP(90)" if sell_deep else "MID(85)"  if sell_mid else "SHAL(80)"
    return buy_sig, sell_sig, b_label, s_label


def compute_trend_ok(df, i, curr):
    """فلتر الترند للباك تيست (يستخدم loc)."""
    mode = bot_state['filter_mode']
    if mode == 'NO_MA':
        return True, True
    cons  = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema, s_ema = True, True
    for j in range(cons):
        c = df.loc[i - j]
        if not (c['ema50'] > c['ema150']): b_ema = False
        if not (c['ema150'] > c['ema50']): s_ema = False
    if mode == 'SIMPLE':
        return b_ema, s_ema
    ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
    ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
    return (b_ema and ma_buy), (s_ema and ma_sell)


def compute_trend_ok_live(df, curr):
    """فلتر الترند للـ Live Scanner (يستخدم iloc)."""
    mode = bot_state['filter_mode']
    if mode == 'NO_MA':
        return True, True
    cons  = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema, s_ema = True, True
    for j in range(cons):
        cc = df.iloc[(-2) - j]
        if not (cc['ema50'] > cc['ema150']): b_ema = False
        if not (cc['ema150'] > cc['ema50']): s_ema = False
    if mode == 'SIMPLE':
        return b_ema, s_ema
    ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
    ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
    return (b_ema and ma_buy), (s_ema and ma_sell)


# =============================================================
# STRATEGY C — COMPOSITE  (State Machine)
# =============================================================

def _get_comp_zones(bs):
    """
    يُعيد قوائم مستويات الـ trigger والـ fire للـ COMPOSITE.

    BUY  triggers : K_comp <= threshold
    SELL triggers : K_comp >= (100 - threshold)
    Returns:
        buy_zones:  list of (max_level, zone_name)
        sell_zones: list of (min_level, zone_name)
    """
    buy_zones = []
    if bs.get('comp_use_deep'):  buy_zones.append((10, 'DEEP'))
    if bs.get('comp_use_mid'):   buy_zones.append((20, 'MID'))
    if bs.get('comp_use_shal'):  buy_zones.append((30, 'SHAL'))

    sell_zones = []
    if bs.get('comp_use_deep'):  sell_zones.append((90, 'DEEP'))
    if bs.get('comp_use_mid'):   sell_zones.append((80, 'MID'))
    if bs.get('comp_use_shal'):  sell_zones.append((70, 'SHAL'))

    return buy_zones, sell_zones


def _run_composite_state(state: dict, curr: pd.Series, prev: pd.Series,
                          bs: dict, df: pd.DataFrame, idx: int) -> tuple:
    lookback = bs.get('comp_lookback', 5)
    fwd      = bs.get('comp_tolerance_fwd', 5)

    state['buy_macd_wait']  += 1
    state['sell_osma_wait'] += 1
    state['buy_stoch_wait'] += 1
    state['sell_stoch_wait'] += 1

    if curr['macd_rsi'] >= 10:
        state['buy_macd_wait'] = 0
        state['buy_macd_val']  = curr['macd_rsi']

    if curr['osma_macd'] >= 10:
        state['sell_osma_wait'] = 0
        state['sell_osma_val']  = curr['osma_macd']

    pk = prev['K_comp']; pd_ = prev['D_comp']
    ck = curr['K_comp']; cd  = curr['D_comp']
    avg = (ck + cd) / 2.0
    buy_zones, sell_zones = _get_comp_zones(bs)

    if pk < pd_ and ck >= cd:
        for lvl, nm in buy_zones:
            if avg <= lvl:
                state['buy_stoch_wait'] = 0
                state['buy_stoch_lbl']  = f"STOCH {nm}(<={lvl}) K={ck:.1f}"
                break

    if pk > pd_ and ck <= cd:
        for lvl, nm in sell_zones:
            if avg >= lvl:
                state['sell_stoch_wait'] = 0
                state['sell_stoch_lbl']  = f"STOCH {nm}(>={lvl}) K={ck:.1f}"
                break

    buy_sig, sell_sig = False, False
    b_label, s_label  = '', ''

    if state['buy_macd_wait'] == 0 and state['buy_stoch_wait'] <= lookback:
        buy_sig = True
        b_label = f"MACD={state['buy_macd_val']:.1f} + {state['buy_stoch_lbl']} [STOCH First]"
    elif state['buy_stoch_wait'] == 0 and state['buy_macd_wait'] <= fwd:
        buy_sig = True
        b_label = f"MACD={state['buy_macd_val']:.1f} + {state['buy_stoch_lbl']} [MACD First]"

    if state['sell_osma_wait'] == 0 and state['sell_stoch_wait'] <= lookback:
        sell_sig = True
        s_label = f"OsMA={state['sell_osma_val']:.1f} + {state['sell_stoch_lbl']} [STOCH First]"
    elif state['sell_stoch_wait'] == 0 and state['sell_osma_wait'] <= fwd:
        sell_sig = True
        s_label = f"OsMA={state['sell_osma_val']:.1f} + {state['sell_stoch_lbl']} [OsMA First]"

    if buy_sig and sell_sig:
        if curr['macd_rsi'] >= curr['osma_macd']:
            sell_sig = False; s_label = ''
        else:
            buy_sig = False; b_label = ''

    if buy_sig:
        state['buy_macd_wait'] = 999
        state['buy_stoch_wait'] = 999
    if sell_sig:
        state['sell_osma_wait'] = 999
        state['sell_stoch_wait'] = 999

    label = b_label if buy_sig else s_label
    return buy_sig, sell_sig, label


def evaluate_composite_live(tf: str, curr: pd.Series, prev: pd.Series,
                             df: pd.DataFrame, idx: int) -> tuple:
    """Live: يُعدّل bot_state مباشرةً. يُعيد (buy, sell, label)."""
    state = bot_state['setup_state'][tf]
    b, s, lbl = _run_composite_state(state, curr, prev, bot_state, df, idx)
    if b: c_log(f"[{tf}] 🟢 COMPOSITE BUY  [{lbl}]")
    if s: c_log(f"[{tf}] 🔴 COMPOSITE SELL [{lbl}]")
    return b, s, lbl


def evaluate_composite_backtest(state: dict, curr: pd.Series, prev: pd.Series,
                                 bs: dict, df: pd.DataFrame, idx: int) -> tuple:
    """Backtest: dict معزول لكل TF."""
    return _run_composite_state(state, curr, prev, bs, df, idx)


def is_danger_time(dt_utc: datetime) -> bool:
    """حظر 19:00–22:00 بتوقيت دمشق (= 16:00–19:00 UTC صيفاً)."""
    dh = (dt_utc.hour + 3) % 24
    return 19 <= dh <= 21


# =============================================================
# OANDA HELPERS
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
                body = await resp.text()
                if resp.status == 200:
                    import json as _json
                    data = _json.loads(body)
                    return [
                        {'time':  pd.to_datetime(c['time'], utc=True),
                         'open':  float(c['mid']['o']),
                         'high':  float(c['mid']['h']),
                         'low':   float(c['mid']['l']),
                         'close': float(c['mid']['c'])}
                        for c in data.get('candles', []) if c['complete']
                    ]
                else:
                    err_msg = f"OANDA {resp.status}: {body[:300]}"
                    c_log(f"❌ {err_msg}")
                    # أرسل الخطأ لتيليجرام إذا كان chat_id متاحاً
                    if bot_state.get('chat_id'):
                        asyncio.create_task(send_tg_msg(
                            f"❌ <b>OANDA API Error</b>\n"
                            f"Status: {resp.status}\n"
                            f"URL: {instrument}/{granularity}\n"
                            f"<code>{body[:200]}</code>"))
        except Exception as e:
            c_log(f"❌ خطأ Oanda: {e}")
            if bot_state.get('chat_id'):
                asyncio.create_task(send_tg_msg(f"❌ OANDA Exception: {e}"))
    return []


# =============================================================
# TELEGRAM HELPERS
# =============================================================

async def send_tg_msg(text, reply_markup=None):
    if not bot_state['chat_id']: return
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as s:
        try: await s.post(url, json=payload)
        except: pass


async def edit_tg_msg(chat_id, message_id, text, reply_markup=None):
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/editMessageText"
    payload = {'chat_id': chat_id, 'message_id': message_id,
               'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as s:
        try: await s.post(url, json=payload)
        except: pass


async def answer_callback(cbq_id, text=None):
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery"
    payload = {'callback_query_id': cbq_id}
    if text: payload['text'] = text
    async with aiohttp.ClientSession() as s:
        try: await s.post(url, json=payload)
        except: pass


async def send_tg_document(file_path, caption):
    if not bot_state['chat_id']: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
    async with aiohttp.ClientSession() as s:
        try:
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('chat_id',   str(bot_state['chat_id']))
                data.add_field('document',  f)
                data.add_field('caption',   caption)
                await s.post(url, data=data)
        except Exception as e:
            c_log(f"❌ خطأ إرسال الملف: {e}")


# =============================================================
# KEYBOARDS
# =============================================================

def _strat_label():
    m = bot_state['strategy_mode']
    return {"STOCH_NEW": "📈 STOCH-NEW",
            "STOCH_OLD": "📉 STOCH-OLD",
            "COMPOSITE": "🧠 COMPOSITE"}[m]


def get_main_keyboard():
    live_icon = "🟢 متصل"  if bot_state['live_connected'] else "🔴 غير متصل"
    st_icon   = "🟢 RUN"   if bot_state['status'] == 'RUNNING' else "🔴 PAUSE"
    return {"inline_keyboard": [
        [{"text": f"🔌 سيرفر التداول الحي: {live_icon}", "callback_data": "toggle_live_conn"}],
        [{"text": f"Status: {st_icon}",      "callback_data": "toggle_status"},
         {"text": f"Strategy: {_strat_label()}", "callback_data": "cycle_strategy"}],
        [{"text": "🎛 فلاتر وإعدادات",      "callback_data": "menu_filters"},
         {"text": "⏱ فريمات",               "callback_data": "menu_tfs"}],
        [{"text": "📊 Live Report",           "callback_data": "report"},
         {"text": "💳 Account",              "callback_data": "account"}],
        [{"text": "🛠 إعدادات المخاطرة",     "callback_data": "menu_settings"},
         {"text": "🔬 BACKTEST",             "callback_data": "menu_backtest"}],
        [{"text": "🛑 إغلاق جميع الصفقات",  "callback_data": "close_all"}],
    ]}


def get_filters_keyboard():
    """
    يعرض قسمين:
      - إذا STOCH_NEW أو STOCH_OLD: فلتر الترند + مستويات الستوكاستيك
      - إذا COMPOSITE: إعدادات السماحية
      - فلاتر الوقت مشتركة دائماً
    """
    sm  = bot_state['strategy_mode']
    t_i = "🟢" if bot_state['use_time_filter']   else "🔴"
    d_i = "🟢" if bot_state['use_danger_filter'] else "🔴"
    rows = []

    if sm in ('STOCH_NEW', 'STOCH_OLD'):
        fm = bot_state['filter_mode']
        fi = {"FULL": "✅" if fm=='FULL' else "⬜",
              "SIMPLE": "✅" if fm=='SIMPLE' else "⬜",
              "NO_MA":  "✅" if fm=='NO_MA'  else "⬜"}
        dp = "🟢" if bot_state['use_stoch_deep'] else "🔴"
        md = "🟢" if bot_state['use_stoch_mid']  else "🔴"
        sh = "🟢" if bot_state['use_stoch_shal'] else "🔴"
        ci = "🟢" if bot_state['use_f_cons']     else "🔴"
        rows += [
            [{"text": "━━ فلتر الترند (اختر واحداً) ━━", "callback_data": "noop"}],
            [{"text": f"{fi['FULL']} FULL: ema15+ema50+ema150", "callback_data": "set_filter_full"}],
            [{"text": f"{fi['SIMPLE']} SIMPLE: ema50+ema150",   "callback_data": "set_filter_simple"}],
            [{"text": f"{fi['NO_MA']} NO MA: ستوكاستيك فقط",   "callback_data": "set_filter_noma"}],
            [{"text": "━━ مستويات الستوكاستيك ━━", "callback_data": "noop"}],
            [{"text": f"⚙️ Stoch({bot_state['stoch_k']},{bot_state['stoch_smooth']},{bot_state['stoch_d']})",
              "callback_data": "menu_stoch_settings"}],
            [{"text": f"DEEP 10/90: {dp}", "callback_data": "toggle_stoch_deep"},
             {"text": f"MID  15/85: {md}", "callback_data": "toggle_stoch_mid"},
             {"text": f"SHAL 20/80: {sh}", "callback_data": "toggle_stoch_shal"}],
            [{"text": f"ثبات الترند ({bot_state['cons_count']} شموع): {ci}",
              "callback_data": "toggle_f_cons"}],
        ]
    else:  # COMPOSITE
        bs   = bot_state
        cd_i = "🟢" if bot_state['comp_use_deep']  else "🔴"
        cm_i = "🟢" if bot_state['comp_use_mid']   else "🔴"
        cs_i = "🟢" if bot_state['comp_use_shal']  else "🔴"
        rows += [
            [{"text": "━━ منطقة Stochastic للتقاطع ━━", "callback_data": "noop"}],
            [{"text": f"DEEP  BUY avg≤10 / SELL avg≥90: {cd_i}", "callback_data": "toggle_comp_deep"}],
            [{"text": f"MID   BUY avg≤20 / SELL avg≥80: {cm_i}", "callback_data": "toggle_comp_mid"}],
            [{"text": f"SHAL  BUY avg≤30 / SELL avg≥70: {cs_i}", "callback_data": "toggle_comp_shal"}],
            [{"text": "━━ نافذة البحث (Fire=10 ثابت) ━━", "callback_data": "noop"}],
            [{"text": f"↩️ قبل العمود (Lookback) = {bs['comp_lookback']} شموع", "callback_data": "noop"}],
            [{"text": "➖", "callback_data": "dec_lookback"},
             {"text": f"{bs['comp_lookback']}", "callback_data": "noop"},
             {"text": "➕", "callback_data": "inc_lookback"}],
            [{"text": f"↪️ بعد العمود (Tolerance) = {bs['comp_tolerance_fwd']} شموع", "callback_data": "noop"}],
            [{"text": "➖", "callback_data": "dec_fwd"},
             {"text": f"{bs['comp_tolerance_fwd']}", "callback_data": "noop"},
             {"text": "➕", "callback_data": "inc_fwd"}],
        ]

    rows += [
        [{"text": "━━ فلاتر الوقت ━━", "callback_data": "noop"}],
        [{"text": f"Time Filter 08-17 UTC: {t_i}", "callback_data": "toggle_time"},
         {"text": f"حظر 19:00-22:00 دمشق: {d_i}", "callback_data": "toggle_danger"}],
        [{"text": "🔙 القائمة الرئيسية", "callback_data": "menu_main"}],
    ]
    return {"inline_keyboard": rows}


def get_stoch_settings_keyboard():
    k, d, s = bot_state['stoch_k'], bot_state['stoch_d'], bot_state['stoch_smooth']
    return {"inline_keyboard": [
        [{"text": f"الإعداد الحالي: Stoch({k},{s},{d})", "callback_data": "noop"}],
        [{"text": "━━ K Period ━━", "callback_data": "noop"}],
        [{"text": "➖ K", "callback_data": "dec_stoch_k"},
         {"text": f"K = {k}", "callback_data": "noop"},
         {"text": "➕ K", "callback_data": "inc_stoch_k"}],
        [{"text": "━━ Smooth ━━", "callback_data": "noop"}],
        [{"text": "➖ S", "callback_data": "dec_stoch_s"},
         {"text": f"S = {s}", "callback_data": "noop"},
         {"text": "➕ S", "callback_data": "inc_stoch_s"}],
        [{"text": "━━ D Period ━━", "callback_data": "noop"}],
        [{"text": "➖ D", "callback_data": "dec_stoch_d"},
         {"text": f"D = {d}", "callback_data": "noop"},
         {"text": "➕ D", "callback_data": "inc_stoch_d"}],
        [{"text": "5,5,5 (افتراضي)", "callback_data": "reset_stoch"},
         {"text": "14,3,3",          "callback_data": "preset_14_3_3"}],
        [{"text": "🔙 رجوع", "callback_data": "menu_filters"}],
    ]}


def get_tf_keyboard():
    kb, row = [], []
    for tf in bot_state['timeframes']:
        row.append({"text": f"{tf}: {'🟢' if bot_state['active_tfs'][tf] else '🔴'}",
                    "callback_data": f"toggle_tf_{tf}"})
        if len(row) == 2: kb.append(row); row = []
    if row: kb.append(row)
    kb.append([{"text": "🔙 رجوع", "callback_data": "menu_main"}])
    return {"inline_keyboard": kb}


def get_settings_keyboard():
    be_i  = "🟢" if bot_state['use_be']         else "🔴"
    atr_i = "🟢" if bot_state['use_atr']        else "🔴"
    spr_i = "🟢" if bot_state['use_max_spread'] else "🔴"
    return {"inline_keyboard": [
        [{"text": f"تأمين الدخول (BE 20p): {be_i}", "callback_data": "toggle_be"}],
        [{"text": f"أهداف ATR: {atr_i}",            "callback_data": "toggle_atr"}],
        [{"text": f"حماية السبريد: {spr_i}",        "callback_data": "toggle_spread"}],
        [{"text": f"LOT SIZE: {bot_state['lot_size']}", "callback_data": "noop"}],
        [{"text": "➕ Lot", "callback_data": "inc_lot"},
         {"text": "➖ Lot", "callback_data": "dec_lot"}],
        [{"text": "📖 عرض TP/SL", "callback_data": "view_tpsl"}],
        [{"text": "🔙 رجوع",     "callback_data": "menu_main"}],
    ]}


# =============================================================
# SHARED TRADE EXECUTION HELPER
# =============================================================

def _get_signal_for_bar(df, i, curr, prev, tf, bt_composite_state=None):
    """
    يُعيد (buy_sig, sell_sig, label) لشمعة واحدة في الباك تيست.
    bt_composite_state: dict معزول يُمرَّر فقط عند COMPOSITE.
    """
    sm = bot_state['strategy_mode']

    if sm in ('STOCH_NEW', 'STOCH_OLD'):
        trend_buy, trend_sell = compute_trend_ok(df, i, curr)
        raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(
            prev['K'], prev['D'], curr['K'], curr['D'])
        buy_sig  = raw_buy  and trend_buy
        sell_sig = raw_sell and trend_sell
        label    = b_lbl if buy_sig else s_lbl
        return buy_sig, sell_sig, label

    else:  # COMPOSITE
        buy_sig, sell_sig, label = evaluate_composite_backtest(
            bt_composite_state, curr, prev,
            bot_state, df, i)
        # أضف K_comp للـ label إذا كان فارغاً (حالة عدم إطلاق إشارة)
        if not label:
            label = f"K={curr['K_comp']:.0f}"
        return buy_sig, sell_sig, label


# =============================================================
# BACKTEST ENGINE — Standard
# =============================================================

async def run_oanda_backtest(start_dt):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة."); return
    bot_state['is_backtesting'] = True
    c_log("بدء الباك تيست...")

    sm = bot_state['strategy_mode']
    tol_desc = (f"COMPOSITE/LB{bot_state['comp_lookback']}_FWD{bot_state['comp_tolerance_fwd']}"
                if sm == 'COMPOSITE'
                else f"{sm}/{bot_state['filter_mode']}")

    await send_tg_msg(
        f"⏳ <b>بدء الباك تيست</b>\n"
        f"من: {start_dt.strftime('%Y-%m-%d')}\n"
        f"الاستراتيجية: {tol_desc}")

    fname       = f"BT_{datetime.now().strftime('%H%M%S')}.xlsx"
    trade_logs  = []
    blocked_logs= []
    total_prof  = peak_equity = max_dd = 0.0
    total_win   = total_loss = 0.0
    win_count   = loss_count = be_count = 0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            c_log(f"[BT] {tf}: جُلب {len(c_data)} شمعة من OANDA")
            if len(c_data) < 300:
                await send_tg_msg(f"⚠️ [{tf}]: بيانات غير كافية ({len(c_data)} شمعة)")
                continue
            df = calculate_indicators(
                pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))

            # فحص timezone وعدد الصفوف في النطاق
            sample_t      = df['time'].iloc[-1]
            rows_in_range = len(df[df['time'] >= start_dt])
            c_log(f"[BT] {tf}: آخر شمعة={sample_t} | start={start_dt} | صفوف في النطاق={rows_in_range}")
            if rows_in_range == 0:
                await send_tg_msg(
                    f"⚠️ [{tf}]: لا توجد شموع في النطاق!\n"
                    f"آخر شمعة OANDA: {sample_t}\n"
                    f"تاريخ البداية المطلوب: {start_dt}")
                continue

            safe_start = max(10, bot_state['cons_count'])
            bt_cs = {'buy_macd_wait': 999, 'sell_osma_wait': 999, 'buy_stoch_wait': 999, 'sell_stoch_wait': 999, 'buy_macd_val': 0.0, 'sell_osma_val': 0.0, 'buy_stoch_lbl': '', 'sell_stoch_lbl': ''}
            signals_found = 0

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                curr = df.loc[i]
                prev = df.loc[i - 1]

                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                buy_sig, sell_sig, label = _get_signal_for_bar(df, i, curr, prev, tf, bt_cs)

                # تسجيل المرفوضات (للـ STOCH فقط)
                if sm in ('STOCH_NEW', 'STOCH_OLD'):
                    raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(
                        prev['K'], prev['D'], curr['K'], curr['D'])
                    if not buy_sig and raw_buy:
                        blocked_logs.append({
                            'Timeframe': tf, 'Type': f'BUY BLOCKED ({b_lbl})',
                            'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                            'Entry Price': curr['close'],
                            'Reason': f'REJECTED ({bot_state["filter_mode"]})'})
                    if not sell_sig and raw_sell:
                        blocked_logs.append({
                            'Timeframe': tf, 'Type': f'SELL BLOCKED ({s_lbl})',
                            'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                            'Entry Price': curr['close'],
                            'Reason': f'REJECTED ({bot_state["filter_mode"]})'})


                if not (buy_sig or sell_sig): continue
                if i + 1 >= len(df): continue

                next_c  = df.loc[i + 1]
                entry_p = next_c['open']
                entry_t = next_c['time']
                m       = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])

                tp_dist = (curr['atr'] * bot_state['atr_mult_tp'] if bot_state['use_atr']
                           else bot_state['tp_pips'][tf] * bot_state['pip_value'])
                sl_dist = (curr['atr'] * bot_state['atr_mult_sl'] if bot_state['use_atr']
                           else bot_state['sl_pips'][tf] * bot_state['pip_value'])
                tp_p    = round(act_ent + (m * tp_dist), 2)
                sl_p    = round(act_ent - (m * sl_dist), 2)
                tol     = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp  = (tp_p - tol) if buy_sig else (tp_p + tol)

                max_ext = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                val_c   = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                outcome = "EXPIRED"; exit_t = max_ext
                be_act  = False
                be_tgt  = act_ent + (m * 20 * bot_state['pip_value'])

                for vc in [v for v in val_c if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['low']  <= sl_p:  outcome = "BREAK-EVEN" if be_act else "LOSS"; exit_t = vc['time']; break
                        if vc['high'] >= eff_tp: outcome = "WIN"; exit_t = vc['time']; break
                    else:
                        if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['high'] >= sl_p:  outcome = "BREAK-EVEN" if be_act else "LOSS"; exit_t = vc['time']; break
                        if vc['low']  <= eff_tp: outcome = "WIN"; exit_t = vc['time']; break

                if outcome == "BREAK-EVEN":
                    p_usd = 0.0; be_count += 1
                elif outcome in ("WIN", "LOSS"):
                    p_usd = round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p))
                        * 100 * bot_state['lot_size'], 2
                    ) * (1 if outcome == "WIN" else -1)
                    if outcome == "WIN":  total_win  += p_usd; win_count  += 1
                    else:                 total_loss += p_usd; loss_count += 1
                else:
                    p_usd = 0.0

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)

                trade_logs.append({
                    'Timeframe':   tf,
                    'Type':        ("BUY" if buy_sig else "SELL") + f" [{label}]",
                    'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price': round(act_ent, 2),
                    'TP': tp_p, 'SL': sl_p,
                    'Pips': round(abs(act_ent - (tp_p if outcome == "WIN" else sl_p))
                                  / bot_state['pip_value'], 1) if outcome in ("WIN","LOSS") else 0,
                    'Outcome':    outcome,
                    'Profit ($)': p_usd,
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على أي صفقات."); return

        from openpyxl.styles import PatternFill, Font
        df_logs      = pd.DataFrame(trade_logs)
        total_trades = win_count + loss_count
        win_rate     = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct       = round(max_dd / peak_equity * 100, 1) if peak_equity else 0

        # تفاصيل إعدادات COMPOSITE في الملخص
        if sm == 'COMPOSITE':
            trig_zones = '+'.join([z for z,v in [('DEEP',bot_state['comp_use_deep']),
                                                  ('MID', bot_state['comp_use_mid']),
                                                  ('SHAL',bot_state['comp_use_shal'])] if v]) or 'none'
            fire_zones = f"MACD/OsMA≥10 | LB={bot_state['comp_lookback']} FWD={bot_state['comp_tolerance_fwd']}"
        else:
            trig_zones = 'N/A'
            fire_zones = 'N/A'

        summary = {
            'البند':   ['✅ الربح الكلي','❌ الخسارة الكلية','💰 المحصلة',
                        '🎯 نسبة الفوز','📉 أقصى DD','🔄 بريك إيفن',
                        '📌 الاستراتيجية','📌 Trigger Zones','📌 Fire Levels'],
            'القيمة':  [f'{win_count} | +${round(total_win,2)}',
                        f'{loss_count} | -${abs(round(total_loss,2))}',
                        f'${round(total_prof,2)}',
                        f'{win_rate}% ({total_trades} صفقة)',
                        f'${round(max_dd,2)} ({dd_pct}%)',
                        str(be_count),
                        tol_desc,
                        trig_zones,
                        fire_zones],
        }
        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            df_logs.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(summary).to_excel(writer, sheet_name='الملخص', index=False)
            if blocked_logs:
                pd.DataFrame(blocked_logs).to_excel(writer, sheet_name='المرفوضة', index=False)
            _style_sheet(writer.sheets['الصفقات'])

        await send_tg_document(fname,
            f"📊 <b>الباك تيست</b> | {tol_desc}\n"
            f"✅ +${round(total_win,2)} ({win_count}) | ❌ -${abs(round(total_loss,2))} ({loss_count})\n"
            f"💰 ${round(total_prof,2)} | 🎯 {win_rate}% | 📉 DD:{round(max_dd,2)}")
        os.remove(fname)

    except Exception as e:
        c_log(f"❌ خطأ باك تيست: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally:
        bot_state['is_backtesting'] = False


# =============================================================
# BACKTEST ENGINE — Advanced (MT5 Style)
# =============================================================


# =============================================================
# DIAGNOSTIC — يُرسل تقرير تفصيلي عن سبب عدم وجود صفقات
# =============================================================

async def run_diagnostic():
    """
    يُشغّل تشخيصاً شاملاً ويُرسل نتيجة كاملة عبر تيليجرام.
    يتحقق من كل خطوة: جلب البيانات، المؤشرات، الـ K/D، الـ fire.
    """
    sm   = bot_state['strategy_mode']
    lines = [f"🔬 <b>تشخيص COMPOSITE</b>"]

    for tf in bot_state['timeframes']:
        if not bot_state['active_tfs'][tf]:
            lines.append(f"\n[{tf}] ⏭ غير مفعّل")
            continue

        lines.append(f"\n━━ [{tf}] ━━")

        # 1. جلب البيانات
        c_data = await fetch_oanda_candles('XAU_USD', tf, 500)
        lines.append(f"📥 شموع مجلوبة: {len(c_data)}")
        if len(c_data) < 50:
            lines.append("❌ بيانات غير كافية"); continue

        df = calculate_indicators(
            pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))

        # 2. فحص الأعمدة المحسوبة
        last = df.iloc[-2]
        lines.append(
            f"📊 آخر شمعة مغلقة:\n"
            f"  K_comp={last['K_comp']:.1f}  D_comp={last['D_comp']:.1f}\n"
            f"  macd_rsi={last['macd_rsi']:.4f}  [BUY fire إذا ≥10]\n"
            f"  osma_macd={last['osma_macd']:.4f}  [SELL fire إذا ≥10]"
        )

        # 3. فحص مدى القيم التاريخية
        macd_max = df['macd_rsi'].max()
        macd_min = df['macd_rsi'].min()
        osma_max = df['osma_macd'].max()
        osma_min = df['osma_macd'].min()
        macd_above10 = (df['macd_rsi'] >= 10).sum()
        osma_above10 = (df['osma_macd'] >= 10).sum()

        lines.append(
            f"📈 نطاق macd_rsi:  [{macd_min:.2f} .. {macd_max:.2f}]\n"
            f"   شموع macd_rsi≥10: {macd_above10}\n"
            f"📉 نطاق osma_macd: [{osma_min:.2f} .. {osma_max:.2f}]\n"
            f"   شموع osma_macd≥10: {osma_above10}"
        )

        if macd_above10 == 0:
            lines.append("⚠️ macd_rsi لم يصل إلى 10 أبداً — مستوى الـ fire بعيد جداً!")
        if osma_above10 == 0:
            lines.append("⚠️ osma_macd لم يصل إلى 10 أبداً — مستوى الـ fire بعيد جداً!")

        # 4. فحص تقاطعات K/D
        crossups   = 0
        crossdowns = 0
        bs = bot_state
        buy_zones, sell_zones = _get_comp_zones(bs)
        crossups_in_zone   = 0
        crossdowns_in_zone = 0

        for i in range(1, len(df)):
            pk = df.iloc[i-1]['K_comp']; pd_ = df.iloc[i-1]['D_comp']
            ck = df.iloc[i]['K_comp'];   cd  = df.iloc[i]['D_comp']
            ku = (pk < pd_) and (ck >= cd)
            kd = (pk > pd_) and (ck <= cd)
            avg = (ck + cd) / 2.0
            if ku:
                crossups += 1
                for lvl, _ in buy_zones:
                    if avg <= lvl: crossups_in_zone += 1; break
            if kd:
                crossdowns += 1
                for lvl, _ in sell_zones:
                    if avg >= lvl: crossdowns_in_zone += 1; break

        lines.append(
            f"🔀 تقاطعات K/D:\n"
            f"   صاعدة (BUY trigger): {crossups} | داخل المنطقة: {crossups_in_zone}\n"
            f"   هابطة (SELL trigger): {crossdowns} | داخل المنطقة: {crossdowns_in_zone}"
        )

        if crossups_in_zone == 0 and crossdowns_in_zone == 0:
            lines.append(
                f"⚠️ لا تقاطعات داخل المناطق المفعّلة!\n"
                f"   مناطق BUY:  {buy_zones}\n"
                f"   مناطق SELL: {sell_zones}"
            )

        # 5. محاكاة الإشارات على آخر 200 شمعة مع تفصيل
        bt_state = {'buy_macd_wait': 999, 'sell_osma_wait': 999, 'buy_stoch_wait': 999, 'sell_stoch_wait': 999, 'buy_macd_val': 0.0, 'sell_osma_val': 0.0, 'buy_stoch_lbl': '', 'sell_stoch_lbl': ''}
        sigs = 0; buy_sigs = 0; sell_sigs = 0
        setups_opened = 0; setups_fired = 0; setups_cancelled = 0
        sub = df.tail(200).reset_index(drop=True).reset_index(drop=True)
        prev_buy_active = False; prev_sell_active = False
        for i in range(1, len(sub)):
            c = sub.iloc[i]; p = sub.iloc[i-1]
            was_buy = bt_state['buy_active']
            was_sell = bt_state['sell_active']
            b, s, lbl = evaluate_composite_backtest(
                bt_state, c, p,
                bot_state, sub, i)
            if bt_state['buy_active'] and not was_buy:   setups_opened += 1
            if bt_state['sell_active'] and not was_sell: setups_opened += 1
            if b: buy_sigs  += 1; sigs += 1; setups_fired += 1
            if s: sell_sigs += 1; sigs += 1; setups_fired += 1
            if was_buy  and not bt_state['buy_active']  and not b: setups_cancelled += 1
            if was_sell and not bt_state['sell_active'] and not s: setups_cancelled += 1

        lines.append(
            f"🎯 إشارات في آخر 200 شمعة: {sigs} "
            f"(BUY:{buy_sigs} SELL:{sell_sigs})\n"
            f"   Setups مفتوحة: {setups_opened} | "
            f"أُطلقت: {setups_fired} | ألغيت: {setups_cancelled}"
        )
        if sigs == 0 and setups_opened == 0:
            lines.append("❌ لا setups تفتح أصلاً — K/D لا يتقاطعان داخل المنطقة")
        elif sigs == 0 and setups_opened > 0:
            lines.append(
                f"⚠️ {setups_opened} setup تفتح لكن لا إشارات\n"
                f"   macd_rsi >= 10 لا يحدث بعد التقاطع\n"
                f"   أو يحدث قبله فقط"
            )

    await send_tg_msg("\n".join(lines))


async def run_advanced_backtest(days=7):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة."); return
    bot_state['is_backtesting'] = True
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    sm = bot_state['strategy_mode']
    tol_desc = (f"COMPOSITE/LB{bot_state['comp_lookback']}_FWD{bot_state['comp_tolerance_fwd']}"
                if sm == 'COMPOSITE'
                else f"{sm}/{bot_state['filter_mode']}")

    await send_tg_msg(
        f"⏳ <b>Advanced Backtest</b>\n"
        f"من: {start_dt.strftime('%Y-%m-%d')} ({days} أيام)\n"
        f"الاستراتيجية: {tol_desc}")

    trade_logs  = []; blocked_logs = []
    total_prof  = peak_equity = max_dd = 0.0
    total_win   = total_loss = 0.0
    win_count   = loss_count = be_count = 0
    long_win    = long_loss = short_win = short_loss = 0
    all_profits = []
    consec_win  = consec_loss = 0
    max_cw = max_cl = 0
    max_cw_usd = max_cl_usd = cur_w = cur_l = 0.0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300: continue
            df = calculate_indicators(
                pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(10, bot_state['cons_count'])
            bt_cs = {'buy_macd_wait': 999, 'sell_osma_wait': 999, 'buy_stoch_wait': 999, 'sell_stoch_wait': 999, 'buy_macd_val': 0.0, 'sell_osma_val': 0.0, 'buy_stoch_lbl': '', 'sell_stoch_lbl': ''}

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                curr = df.loc[i]; prev = df.loc[i - 1]

                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                buy_sig, sell_sig, label = _get_signal_for_bar(df, i, curr, prev, tf, bt_cs)

                if sm in ('STOCH_NEW','STOCH_OLD'):
                    raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(
                        prev['K'], prev['D'], curr['K'], curr['D'])
                    if not buy_sig  and raw_buy:
                        blocked_logs.append({'Timeframe': tf, 'Type': f'BUY BLOCKED ({b_lbl})',
                            'Entry Time': (curr['time']+timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                            'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'})
                    if not sell_sig and raw_sell:
                        blocked_logs.append({'Timeframe': tf, 'Type': f'SELL BLOCKED ({s_lbl})',
                            'Entry Time': (curr['time']+timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                            'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'})

                if not (buy_sig or sell_sig): continue
                if i + 1 >= len(df): continue

                next_c  = df.loc[i + 1]
                entry_p = next_c['open']; entry_t = next_c['time']
                m       = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])

                tp_dist = (curr['atr'] * bot_state['atr_mult_tp'] if bot_state['use_atr']
                           else bot_state['tp_pips'][tf] * bot_state['pip_value'])
                sl_dist = (curr['atr'] * bot_state['atr_mult_sl'] if bot_state['use_atr']
                           else bot_state['sl_pips'][tf] * bot_state['pip_value'])
                tp_p = round(act_ent + (m * tp_dist), 2)
                sl_p = round(act_ent - (m * sl_dist), 2)
                tol  = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp = (tp_p - tol) if buy_sig else (tp_p + tol)

                max_ext = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                val_c   = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                outcome = "EXPIRED"; exit_t = max_ext
                be_act  = False; be_tgt = act_ent + (m * 20 * bot_state['pip_value'])

                for vc in [v for v in val_c if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['low']  <= sl_p:   outcome = "BREAK-EVEN" if be_act else "LOSS"; exit_t = vc['time']; break
                        if vc['high'] >= eff_tp:  outcome = "WIN"; exit_t = vc['time']; break
                    else:
                        if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['high'] >= sl_p:   outcome = "BREAK-EVEN" if be_act else "LOSS"; exit_t = vc['time']; break
                        if vc['low']  <= eff_tp:  outcome = "WIN"; exit_t = vc['time']; break

                if outcome == "BREAK-EVEN":
                    p_usd = 0.0; be_count += 1
                elif outcome in ("WIN","LOSS"):
                    p_usd = round(abs(act_ent - (tp_p if outcome=="WIN" else sl_p))
                                  * 100 * bot_state['lot_size'], 2) * (1 if outcome=="WIN" else -1)
                else:
                    p_usd = 0.0

                if outcome == "WIN":
                    total_win += p_usd; win_count += 1
                    consec_win += 1; cur_w += p_usd; consec_loss = 0; cur_l = 0.0
                    if consec_win > max_cw: max_cw = consec_win; max_cw_usd = cur_w
                    (long_win if buy_sig else short_win).__class__  # dummy
                    if buy_sig: long_win  += 1
                    else:       short_win += 1
                elif outcome == "LOSS":
                    total_loss += p_usd; loss_count += 1
                    consec_loss += 1; cur_l += p_usd; consec_win = 0; cur_w = 0.0
                    if consec_loss > max_cl: max_cl = consec_loss; max_cl_usd = cur_l
                    if buy_sig: long_loss  += 1
                    else:       short_loss += 1

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)
                all_profits.append(p_usd)
                _dh = (curr['time'].hour + 3) % 24

                trade_logs.append({
                    'Timeframe':   tf,
                    'Type':        ("BUY" if buy_sig else "SELL") + f" [{label}]",
                    'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price': round(act_ent,2), 'TP': tp_p, 'SL': sl_p,
                    'Pips': round(abs(act_ent-(tp_p if outcome=="WIN" else sl_p))
                                  /bot_state['pip_value'],1) if outcome in ("WIN","LOSS") else 0,
                    'Outcome': outcome, 'Profit ($)': p_usd,
                    'Hour_Damascus': _dh, 'Weekday': curr['time'].strftime('%a'),
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على صفقات."); return

        total_trades    = win_count + loss_count
        win_rate        = round(win_count/total_trades*100,1) if total_trades else 0
        dd_pct          = round(max_dd/peak_equity*100,1) if peak_equity else 0
        profit_factor   = round(total_win/abs(total_loss),2) if total_loss else 999
        expected_payoff = round(total_prof/total_trades,2) if total_trades else 0
        recovery_factor = round(total_prof/max_dd,2) if max_dd else 999
        wins_only       = [p for p in all_profits if p > 0]
        losses_only     = [p for p in all_profits if p < 0]
        avg_win         = round(sum(wins_only)/len(wins_only),2) if wins_only else 0
        avg_loss        = round(sum(losses_only)/len(losses_only),2) if losses_only else 0
        largest_win     = round(max(wins_only),2) if wins_only else 0
        largest_loss    = round(min(losses_only),2) if losses_only else 0

        df_t        = pd.DataFrame(trade_logs)
        actv        = df_t[df_t['Outcome'].isin(['WIN','LOSS'])]
        hour_counts = actv.groupby('Hour_Damascus').size()
        day_counts  = actv.groupby('Weekday').size()

        def bar(dd, w=18):
            if not dd: return "(لا بيانات)"
            mx = max(dd.values())
            return "\n".join(f"  {str(k):>4} |{'█'*int(v/mx*w):<{w}}| {v}"
                             for k,v in sorted(dd.items()))

        report = (
            f"📊 <b>Advanced Report — {days} يوم</b>\n"
            f"📌 {tol_desc}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>💰 الأرباح</b>\n"
            f"  صافي:     ${round(total_prof,2)}\n"
            f"  ربح:      +${round(total_win,2)}\n"
            f"  خسارة:    -${abs(round(total_loss,2))}\n"
            f"  PF: {profit_factor} | EP: ${expected_payoff} | RF: {recovery_factor}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📉 Drawdown</b>: ${round(max_dd,2)} ({dd_pct}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📈 الصفقات</b>\n"
            f"  {total_trades} صفقة | فوز: {win_count} ({win_rate}%) | خسارة: {loss_count}\n"
            f"  Long W/L: {long_win}/{long_loss} | Short W/L: {short_win}/{short_loss}\n"
            f"  بريك إيفن: {be_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🔢 إحصاءات</b>\n"
            f"  أكبر ربح: +${largest_win} | أكبر خسارة: ${largest_loss}\n"
            f"  متوسط ربح: +${avg_win} | متوسط خسارة: ${avg_loss}\n"
            f"  سلسلة فوز:   {max_cw} (+${round(max_cw_usd,2)})\n"
            f"  سلسلة خسارة: {max_cl} (-${abs(round(max_cl_usd,2))})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🕐 بالساعة:</b>\n<pre>{bar(hour_counts.to_dict())}</pre>\n"
            f"<b>📅 بالأيام:</b>\n<pre>{bar(day_counts.to_dict())}</pre>"
        )
        await send_tg_msg(report)

        from openpyxl.styles import PatternFill, Font
        xlsx_adv = f"ADV_{datetime.now().strftime('%H%M%S')}.xlsx"
        df_exec  = df_t.drop(columns=['Hour_Damascus','Weekday'], errors='ignore')
        # تفاصيل COMPOSITE للـ Advanced report
        if sm == 'COMPOSITE':
            adv_trig = '+'.join([z for z,v in [('DEEP',bot_state['comp_use_deep']),
                                                ('MID', bot_state['comp_use_mid']),
                                                ('SHAL',bot_state['comp_use_shal'])] if v]) or 'none'
            adv_fire = f"MACD/OsMA≥10 | LB={bot_state['comp_lookback']} FWD={bot_state['comp_tolerance_fwd']}"
        else:
            adv_trig = adv_fire = 'N/A'

        stats = {
            'المقياس': ['صافي الربح','إجمالي الربح','إجمالي الخسارة',
                        'Profit Factor','Expected Payoff','Recovery Factor',
                        'أقصى DD','DD%','إجمالي الصفقات','فوز','خسارة',
                        'نسبة الفوز','بريك إيفن','Long W/L','Short W/L',
                        'أكبر ربح','أكبر خسارة','متوسط ربح','متوسط خسارة',
                        'أكبر سلسلة فوز','أكبر سلسلة خسارة',
                        'الاستراتيجية','Trigger Zones','Fire Levels'],
            'القيمة':  [f'${round(total_prof,2)}',f'+${round(total_win,2)}',
                        f'-${abs(round(total_loss,2))}',profit_factor,
                        expected_payoff,recovery_factor,
                        f'${round(max_dd,2)}',f'{dd_pct}%',
                        total_trades,win_count,loss_count,f'{win_rate}%',be_count,
                        f'{long_win}/{long_loss}',f'{short_win}/{short_loss}',
                        f'+${largest_win}',f'${largest_loss}',
                        f'+${avg_win}',f'${avg_loss}',
                        f'{max_cw}(+${round(max_cw_usd,2)})',
                        f'{max_cl}(-${abs(round(max_cl_usd,2))})',
                        tol_desc, adv_trig, adv_fire],
        }
        with pd.ExcelWriter(xlsx_adv, engine='openpyxl') as writer:
            df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(stats).to_excel(writer, sheet_name='الإحصاءات', index=False)
            if blocked_logs:
                pd.DataFrame(blocked_logs).to_excel(writer, sheet_name='المرفوضة', index=False)
            _style_sheet(writer.sheets['الصفقات'])

        await send_tg_document(xlsx_adv, f"📊 Advanced Report — {days} يوم | {tol_desc}")
        os.remove(xlsx_adv)

    except Exception as e:
        c_log(f"❌ خطأ Advanced BT: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally:
        bot_state['is_backtesting'] = False


def _style_sheet(ws):
    """تنسيق ألوان ورقة الصفقات."""
    from openpyxl.styles import PatternFill, Font
    gf = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
    rf = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
    hf = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')
    for cell in ws[1]:
        cell.fill = hf; cell.font = Font(color='FFFFFF', bold=True)
    oc = next((i+1 for i,c in enumerate(ws[1]) if c.value == 'Outcome'), 9)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        val = str(row[oc-1].value) if len(row) >= oc else ''
        if val == 'WIN':
            for cell in row: cell.fill = gf
        elif val == 'LOSS':
            for cell in row: cell.fill = rf
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = min(
            max((len(str(c.value or '')) for c in col), default=8) + 3, 28)


# =============================================================
# LIVE POSITION MONITOR — Break-Even
# =============================================================

async def position_monitor():
    while True:
        try:
            if bot_state['live_connected'] and bot_state['use_be'] and bot_state['connection_obj']:
                positions = await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] != bot_state['symbol']: continue
                    op, tp, sl, cp = p['openPrice'], p.get('takeProfit'), p.get('stopLoss'), p['currentPrice']
                    if tp and sl != op and abs(cp - op) >= 20 * bot_state['pip_value']:
                        is_buy = tp > op
                        if (is_buy and cp > op) or (not is_buy and cp < op):
                            await bot_state['connection_obj'].modify_position(p['id'], stop_loss=op)
                            await send_tg_msg(f"🛡️ <b>BE</b> تأمين الدخول لصفقة: {p['id']}")
        except: pass
        await asyncio.sleep(5)


# =============================================================
# LIVE TIMEFRAME SCANNER
# =============================================================

async def timeframe_scanner(tf):
    c_log(f"✅ ماسح [{tf}] يعمل.")
    while True:
        try:
            if bot_state['status'] == 'RUNNING' and bot_state['active_tfs'][tf]:
                if not bot_state['live_connected'] or not bot_state['account_obj']:
                    bot_state['market_data'][tf] = "⏸ بانتظار الاتصال (Offline)"
                    await asyncio.sleep(5); continue

                try:
                    raw = await bot_state['account_obj'].get_historical_candles(
                        bot_state['symbol'], tf, limit=500)
                except:
                    await asyncio.sleep(15); continue

                df      = calculate_indicators(pd.DataFrame(raw))
                curr    = df.iloc[-2]   # آخر شمعة مغلقة
                prev    = df.iloc[-3]
                now_utc = datetime.now(timezone.utc)
                sm      = bot_state['strategy_mode']

                danger_now = bot_state['use_danger_filter'] and is_danger_time(now_utc)
                time_block = bot_state['use_time_filter'] and not (8 <= now_utc.hour <= 17)

                # بيانات العرض حسب الاستراتيجية
                if sm == 'COMPOSITE':
                    st = bot_state['setup_state'][tf]
                    bot_state['market_data'][tf] = (
                        f"{df.iloc[-1]['close']:.2f} | "
                        f"K:{curr['K_comp']:.1f} D:{curr['D_comp']:.1f} "
                        f"MACD:{curr['macd_rsi']:.2f} OsMA:{curr['osma_macd']:.2f} "
                        f"{'🟡B' if st['buy_active'] else ''}{'🟡S' if st['sell_active'] else ''}")
                else:
                    bot_state['market_data'][tf] = (
                        f"{df.iloc[-1]['close']:.2f} | K:{curr['K']:.1f} D:{curr['D']:.1f}")

                if time_block or danger_now:
                    bot_state['market_data'][tf] = f"⏸ خمول | {df.iloc[-1]['close']:.2f}"
                elif bot_state['last_signal_time'][tf] != curr['time']:

                    buy_sig = sell_sig = False
                    label   = ""

                    if sm in ('STOCH_NEW', 'STOCH_OLD'):
                        trend_buy, trend_sell = compute_trend_ok_live(df, curr)
                        raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(
                            prev['K'], prev['D'], curr['K'], curr['D'])
                        buy_sig  = raw_buy  and trend_buy
                        sell_sig = raw_sell and trend_sell
                        label    = b_lbl if buy_sig else s_lbl
                        if raw_buy  and not buy_sig:
                            c_log(f"🛑 [{tf}] BUY {b_lbl} مرفوض (فلتر: {bot_state['filter_mode']})")
                        if raw_sell and not sell_sig:
                            c_log(f"🛑 [{tf}] SELL {s_lbl} مرفوض (فلتر: {bot_state['filter_mode']})")
                    else:
                        buy_sig, sell_sig, label = evaluate_composite_live(tf, curr, prev, df, len(df)-2)

                    # فلتر السبريد
                    skip = False
                    if bot_state['use_max_spread']:
                        try:
                            tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                            if ((tick['ask'] - tick['bid']) / bot_state['pip_value']) > bot_state['max_spread_pips']:
                                skip = True; c_log(f"[{tf}] ⚠️ سبريد مرتفع")
                        except: pass

                    if not skip and (buy_sig or sell_sig):
                        bot_state['last_signal_time'][tf] = curr['time']
                        price = df.iloc[-1]['close']
                        m     = 1 if buy_sig else -1
                        t_str = "شراء 🟢 BUY" if buy_sig else "بيع 🔴 SELL"

                        tp_dist = (curr['atr'] * bot_state['atr_mult_tp'] if bot_state['use_atr']
                                   else bot_state['tp_pips'][tf] * bot_state['pip_value'])
                        sl_dist = (curr['atr'] * bot_state['atr_mult_sl'] if bot_state['use_atr']
                                   else bot_state['sl_pips'][tf] * bot_state['pip_value'])
                        tp = round(price + (m * tp_dist), 2)
                        sl = round(price - (m * sl_dist), 2)

                        c_log(f"🎯 [{tf}] {t_str} [{label}] — جاري التنفيذ...")
                        try:
                            if buy_sig:
                                await bot_state['connection_obj'].create_market_buy_order(
                                    bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                            else:
                                await bot_state['connection_obj'].create_market_sell_order(
                                    bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                            await send_tg_msg(
                                f"🚨 <b>تم فتح صفقة!</b>\n"
                                f"النوع: {t_str}\nالفريم: {tf} | {tol_desc if sm=='COMPOSITE' else bot_state['filter_mode']}\n"
                                f"السعر: {price} | TP: {tp} | SL: {sl}\n"
                                f"[{label}]")
                        except Exception as e:
                            await send_tg_msg(f"❌ <b>فشل التنفيذ!</b>\n{e}")

            await asyncio.sleep(10)
        except:
            await asyncio.sleep(15)


# =============================================================
# TELEGRAM HANDLER
# =============================================================

async def process_tg_update(update):
    # ── رسائل نصية ──────────────────────────────────────────
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']

        if msg == '/start':
            await send_tg_msg("🤖 <b>مرحباً! Gold Scalper v3</b>\n"
                              "ثلاث استراتيجيات متكاملة جاهزة.", get_main_keyboard())

        elif msg == '/debug':
            sm = bot_state['strategy_mode']
            if not bot_state['live_connected']:
                await send_tg_msg("⚠️ البوت غير متصل باللايف."); return
            try:
                tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                raw  = await bot_state['account_obj'].get_historical_candles(
                    bot_state['symbol'], '5m', limit=500)
                df   = calculate_indicators(pd.DataFrame(raw))
                curr = df.iloc[-2]; prev = df.iloc[-3]
                spr  = round((tick['ask'] - tick['bid']) / bot_state['pip_value'], 1)
                if sm in ('STOCH_NEW', 'STOCH_OLD'):
                    tb, ts = compute_trend_ok_live(df, curr)
                    rb, rs, bl, sl_l = get_stoch_signals(prev['K'], prev['D'], curr['K'], curr['D'])
                    await send_tg_msg(
                        f"✅ <b>Debug [5m] — {sm}</b>\n"
                        f"K:{curr['K']:.1f} D:{curr['D']:.1f} | سبريد: {spr}\n"
                        f"ema15:{curr['ema15']:.2f} ema50:{curr['ema50']:.2f} ema150:{curr['ema150']:.2f}\n"
                        f"Trend Buy:{('✅' if tb else '❌')} Sell:{('✅' if ts else '❌')}\n"
                        f"Raw BUY:{('✅ '+bl if rb else '❌')} SELL:{('✅ '+sl_l if rs else '❌')}")
                else:
                    st = bot_state['setup_state']['5m']
                    await send_tg_msg(
                        f"✅ <b>Debug [5m] — COMPOSITE</b>\n"
                        f"K_comp:{curr['K_comp']:.1f} | سبريد: {spr}\n"
                        f"MACD_rsi:{curr['macd_rsi']:.3f} (GREEN>=10=BUY)\n"
                        f"OsMA_norm:{curr['osma_norm']:.1f} (RED)\n"
                        f"BUY active:{st['buy_active']} | SELL active:{st['sell_active']}\n"
                        f"B_fire:{st.get('buy_fire_idx',0)} | S_fire:{st.get('sell_fire_idx',0)}")
            except Exception as e:
                await send_tg_msg(f"❌ خطأ: {e}")

        elif msg.startswith('/set'):
            p = msg.split()
            if len(p) == 4:
                bot_state[p[2] + '_pips'][p[1]] = int(p[3])
                await send_tg_msg(f"✅ تم تحديث {p[2]} لفريم {p[1]} إلى {p[3]}")

        elif msg == '/diag':
            asyncio.create_task(run_diagnostic())
            await send_tg_msg("⏳ جاري التشخيص...")

        elif msg.startswith('/backtest'):
            try:
                st = datetime.strptime(msg.split()[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                asyncio.create_task(run_oanda_backtest(st))
                await send_tg_msg(f"✅ باك تيست من: {msg.split()[1]}")
            except:
                await send_tg_msg("⚠️ استخدم: /backtest YYYY-MM-DD")

    # ── Callback Queries ─────────────────────────────────────
    elif 'callback_query' in update:
        q = update['callback_query']
        d, chat_id, msg_id = q['data'], q['message']['chat']['id'], q['message']['message_id']
        bot_state['chat_id'] = chat_id

        def _reset_composite_states():
            for tf in _TFS:
                bot_state['setup_state'][tf] = {
                    'buy_macd_wait': 999, 'sell_osma_wait': 999,
                    'buy_stoch_wait': 999, 'sell_stoch_wait': 999,
                    'buy_macd_val': 0.0,  'sell_osma_val': 0.0,
                    'buy_stoch_lbl': '',  'sell_stoch_lbl': ''}

        # ── Navigation ──
        if d == "menu_main":
            await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())

        elif d == "toggle_status":
            bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'
            await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())

        # ── تبديل الاستراتيجية (دوري) ──
        elif d == "cycle_strategy":
            order = ['STOCH_NEW', 'STOCH_OLD', 'COMPOSITE']
            cur   = bot_state['strategy_mode']
            bot_state['strategy_mode'] = order[(order.index(cur) + 1) % 3]
            _reset_composite_states()
            await edit_tg_msg(chat_id, msg_id,
                f"🏠 القائمة الرئيسية:\n📌 الاستراتيجية: {_strat_label()}",
                get_main_keyboard())

        # ── اتصال Live ──
        elif d == "toggle_live_conn":
            if not bot_state['live_connected']:
                await edit_tg_msg(chat_id, msg_id, "⏳ جاري الاتصال...", get_main_keyboard())
                try:
                    api = MetaApi(METAAPI_TOKEN)
                    bot_state['account_obj']    = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                    bot_state['connection_obj'] = bot_state['account_obj'].get_rpc_connection()
                    await bot_state['connection_obj'].connect()
                    await bot_state['connection_obj'].wait_synchronized()
                    bot_state['live_connected'] = True
                    await edit_tg_msg(chat_id, msg_id, "✅ تم الاتصال!", get_main_keyboard())
                except Exception as e:
                    await edit_tg_msg(chat_id, msg_id, f"❌ فشل: {e}", get_main_keyboard())
            else:
                bot_state['live_connected'] = False
                bot_state['connection_obj'] = bot_state['account_obj'] = None
                await edit_tg_msg(chat_id, msg_id, "🔌 تم الفصل.", get_main_keyboard())

        # ── Filters ──
        elif d == "menu_filters":
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        # فلاتر STOCH
        elif d == "set_filter_full":
            bot_state['filter_mode'] = 'FULL'
            await edit_tg_msg(chat_id, msg_id, "✅ FULL مُفعّل", get_filters_keyboard())
        elif d == "set_filter_simple":
            bot_state['filter_mode'] = 'SIMPLE'
            await edit_tg_msg(chat_id, msg_id, "✅ SIMPLE مُفعّل", get_filters_keyboard())
        elif d == "set_filter_noma":
            bot_state['filter_mode'] = 'NO_MA'
            await edit_tg_msg(chat_id, msg_id, "✅ NO MA مُفعّل", get_filters_keyboard())
        elif d == "toggle_stoch_deep":
            bot_state['use_stoch_deep'] = not bot_state['use_stoch_deep']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر:</b>", get_filters_keyboard())
        elif d == "toggle_stoch_mid":
            bot_state['use_stoch_mid']  = not bot_state['use_stoch_mid']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر:</b>", get_filters_keyboard())
        elif d == "toggle_stoch_shal":
            bot_state['use_stoch_shal'] = not bot_state['use_stoch_shal']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر:</b>", get_filters_keyboard())
        elif d == "toggle_f_cons":
            bot_state['use_f_cons'] = not bot_state['use_f_cons']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر:</b>", get_filters_keyboard())

        # فلاتر COMPOSITE — مناطق الـ trigger
        elif d == "toggle_comp_deep":
            bot_state['comp_use_deep']  = not bot_state['comp_use_deep'];  _reset_composite_states()
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())
        elif d == "toggle_comp_mid":
            bot_state['comp_use_mid']   = not bot_state['comp_use_mid'];   _reset_composite_states()
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())
        elif d == "toggle_comp_shal":
            bot_state['comp_use_shal']  = not bot_state['comp_use_shal'];  _reset_composite_states()
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())
        # مستويات RSI2 للـ fire

        # نافذة البحث
        elif d == "inc_lookback":
            bot_state['comp_lookback'] = min(bot_state['comp_lookback']+1, 10)
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())
        elif d == "dec_lookback":
            bot_state['comp_lookback'] = max(bot_state['comp_lookback']-1, 1)
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())
        elif d == "inc_fwd":
            bot_state['comp_tolerance_fwd'] = min(bot_state['comp_tolerance_fwd']+1, 10)
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())
        elif d == "dec_fwd":
            bot_state['comp_tolerance_fwd'] = max(bot_state['comp_tolerance_fwd']-1, 1)
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر COMPOSITE:</b>", get_filters_keyboard())

        # فلاتر الوقت
        elif d == "toggle_time":
            bot_state['use_time_filter']   = not bot_state['use_time_filter']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر:</b>", get_filters_keyboard())
        elif d == "toggle_danger":
            bot_state['use_danger_filter'] = not bot_state['use_danger_filter']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر:</b>", get_filters_keyboard())

        # ── Stoch Settings ──
        elif d == "menu_stoch_settings":
            await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "inc_stoch_k": bot_state['stoch_k'] = min(bot_state['stoch_k']+1,50); await edit_tg_msg(chat_id,msg_id,"⚙️ <b>إعدادات الستوكاستيك:</b>",get_stoch_settings_keyboard())
        elif d == "dec_stoch_k": bot_state['stoch_k'] = max(bot_state['stoch_k']-1,1);  await edit_tg_msg(chat_id,msg_id,"⚙️ <b>إعدادات الستوكاستيك:</b>",get_stoch_settings_keyboard())
        elif d == "inc_stoch_s": bot_state['stoch_smooth'] = min(bot_state['stoch_smooth']+1,50); await edit_tg_msg(chat_id,msg_id,"⚙️ <b>إعدادات الستوكاستيك:</b>",get_stoch_settings_keyboard())
        elif d == "dec_stoch_s": bot_state['stoch_smooth'] = max(bot_state['stoch_smooth']-1,1);  await edit_tg_msg(chat_id,msg_id,"⚙️ <b>إعدادات الستوكاستيك:</b>",get_stoch_settings_keyboard())
        elif d == "inc_stoch_d": bot_state['stoch_d'] = min(bot_state['stoch_d']+1,50); await edit_tg_msg(chat_id,msg_id,"⚙️ <b>إعدادات الستوكاستيك:</b>",get_stoch_settings_keyboard())
        elif d == "dec_stoch_d": bot_state['stoch_d'] = max(bot_state['stoch_d']-1,1);  await edit_tg_msg(chat_id,msg_id,"⚙️ <b>إعدادات الستوكاستيك:</b>",get_stoch_settings_keyboard())
        elif d == "reset_stoch":
            bot_state.update({'stoch_k':5,'stoch_d':5,'stoch_smooth':5})
            await edit_tg_msg(chat_id,msg_id,"✅ تم إعادة الضبط لـ 5,5,5",get_stoch_settings_keyboard())
        elif d == "preset_14_3_3":
            bot_state.update({'stoch_k':14,'stoch_d':3,'stoch_smooth':3})
            await edit_tg_msg(chat_id,msg_id,"✅ تم ضبط 14,3,3",get_stoch_settings_keyboard())

        # ── Timeframes ──
        elif d == "menu_tfs":
            await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())
        elif d.startswith("toggle_tf_"):
            tf = d.split("_")[2]
            bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            bot_state['setup_state'][tf] = {'buy_macd_wait': 999, 'sell_osma_wait': 999, 'buy_stoch_wait': 999, 'sell_stoch_wait': 999, 'buy_macd_val': 0.0, 'sell_osma_val': 0.0, 'buy_stoch_lbl': '', 'sell_stoch_lbl': ''}
            await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())

        # ── Settings ──
        elif d == "menu_settings":
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_be":
            bot_state['use_be'] = not bot_state['use_be']
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_atr":
            bot_state['use_atr'] = not bot_state['use_atr']
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_spread":
            bot_state['use_max_spread'] = not bot_state['use_max_spread']
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "inc_lot":
            bot_state['lot_size'] = round(bot_state['lot_size']+0.01, 2)
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "dec_lot":
            bot_state['lot_size'] = max(0.01, round(bot_state['lot_size']-0.01, 2))
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "view_tpsl":
            txt = "📖 <b>أهداف الفريمات:</b>\n" + "\n".join(
                f"[{tf}] TP:{bot_state['tp_pips'][tf]} | SL:{bot_state['sl_pips'][tf]}"
                for tf in bot_state['timeframes'])
            await edit_tg_msg(chat_id, msg_id, txt, get_settings_keyboard())

        # ── Reports ──
        elif d == "report":
            sm   = bot_state['strategy_mode']
            lines = []
            for tf in bot_state['timeframes']:
                if not bot_state['active_tfs'][tf]: continue
                md = bot_state['market_data'][tf]
                if sm == 'COMPOSITE':
                    st = bot_state['setup_state'][tf]
                    lines.append(f"[{tf}] {md}\n"
                                 f"       B:{('🟡' if st['buy_active'] else '⬜')} "
                                 f"S:{('🟡' if st['sell_active'] else '⬜')} "
                                 f"B_fire:{st.get('buy_fire_idx',0)} S_fire:{st.get('sell_fire_idx',0)}")
                else:
                    lines.append(f"[{tf}] {md}")
            txt = f"📊 <b>حالة السوق الحية — {_strat_label()}</b>\n" + "\n".join(lines)
            await edit_tg_msg(chat_id, msg_id, txt, get_main_keyboard())

        elif d == "account":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                try:
                    acc = await bot_state['connection_obj'].get_account_information()
                    await edit_tg_msg(chat_id, msg_id,
                        f"💳 <b>الحساب:</b>\nرصيد: {acc['balance']}\nإيكويتي: {acc['equity']}",
                        get_main_keyboard())
                except: pass
            else:
                await send_tg_msg("يجب الاتصال بالسيرفر أولاً!")

        # ── Backtest ──
        elif d == "menu_backtest":
            kb = {"inline_keyboard": [
                [{"text": "📊 1 يوم",   "callback_data": "bto_1"},
                 {"text": "📊 3 أيام",  "callback_data": "bto_3"},
                 {"text": "📊 7 أيام",  "callback_data": "bto_7"}],
                [{"text": "🔬 Advanced — 7 أيام",  "callback_data": "bto_adv_7"}],
                [{"text": "🔬 Advanced — 14 يوم",  "callback_data": "bto_adv_14"}],
                [{"text": "🔙 رجوع", "callback_data": "menu_main"}],
            ]}
            sm = bot_state['strategy_mode']
            await edit_tg_msg(chat_id, msg_id,
                f"🔬 <b>Backtest</b> — الاستراتيجية: {_strat_label()}\n"
                f"اختر المدة أو أرسل /backtest YYYY-MM-DD:", kb)

        elif d.startswith("bto_adv_"):
            asyncio.create_task(run_advanced_backtest(days=int(d.split('_')[2])))
        elif d.startswith("bto_"):
            asyncio.create_task(run_oanda_backtest(
                datetime.now(timezone.utc) - timedelta(days=int(d.split('_')[1]))))

        # ── Close All ──
        elif d == "close_all":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                async def _close():
                    try:
                        pos = await bot_state['connection_obj'].get_positions()
                        for p in pos: await bot_state['connection_obj'].close_position(p['id'])
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
                    'offset': bot_state['last_update_id'] + 1, 'timeout': 10
                }) as r:
                    if r.status == 200:
                        for u in (await r.json()).get('result', []):
                            bot_state['last_update_id'] = u['update_id']
                            asyncio.create_task(process_tg_update(u))
            except:
                await asyncio.sleep(2)


# =============================================================
# WEB SERVER + MAIN
# =============================================================

async def handle_ping(request):
    return web.Response(text="Gold Scalper Bot v3 — ALIVE!")


async def main():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    c_log(f"🚀 وب سيرفر على بورت {port}")

    tasks  = [asyncio.create_task(timeframe_scanner(tf)) for tf in bot_state['timeframes']]
    tasks += [asyncio.create_task(telegram_polling_loop()),
              asyncio.create_task(position_monitor())]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
