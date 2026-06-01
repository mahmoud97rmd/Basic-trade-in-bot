"""
Gold Scalper Bot — v4
Strategies : STOCH-NEW  |  STOCH-OLD
"""

import asyncio
import aiohttp
import json
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from metaapi_cloud_sdk import MetaApi
from aiohttp import web

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
METAAPI_TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9..NRMo-BO9ezZBEb4XmCQzkMsRN1iAz1rVSk7XWFP-ZGS_AZEyxSfIjnJ5w-r4egazV7tnxNLjjMuAdUb25T3ur3XWKCL4Jo9LFPy9tZzhIMRtlhq8d6YAHK9uxJclqJv5BZQFDeMeiFtyalLNjaE100Lp2zEnGWwlloxF-dpCw5DXvVKeGfMyVx4L2kisshcysDo7OeMkDBU1UB7leHi2eviEl7XQCpmhxdzT4BwMkf8YERx2jouKVu8-koVy00aon0drktGBSlQDOFw2WV0hg-VUfeCBR_Hgw2czqKVJ_lj_ZN3EsjWirirpiuXWbtwdD-VPokjKtX1z3ugcSTS1nd2iFIzauUHdOfb7Jl0R6cm8FosVS-4Iu046DiMsrxiAJ4PBywOXQhsFzZiePqmil1w5HHCxrw_78HNR9XcjBETMpHx9W48llIeUOkBVbsKfBP5iYtGSjS52i0QgpvHkfKrtXfbkMT0_9yJFG2kfZJHwJ5BJzWT4aKXto3l6iGe45xe4ZJhYhZX_RkC6dxR2w84M-uY-wlqiv_sxjHNOguSyOx4lfaeoq5H-LuJiWpHAYxEJUQWoQAQ7PObZOXCDWLRc_vP2gcbv1qYxTjD54FHnqhyf-oTGzAkWG5CVQFKpp9jTHQ3pXEYTSgIUTfHDbtoesAY1HG3nHcHbwujnqo0'
ACCOUNT_ID    = '7d54fa6f-eaf7-4637-92a1-e0356ee729f8'
TG_TOKEN      = '8779425898:AAHqcPePBlu1XGpIrdQ2Jz63wWemwpyooCI'
OANDA_API     = 'd05b25b3f1ce0c8fa105ffefa45efb01-a5c26f544a26a4f810f1809913a2795f'
OANDA_URL     = 'https://api-fxpractice.oanda.com/v3'

_TFS = ['1m', '2m', '3m', '5m', '15m']

# ─────────────────────────────────────────────────────────────
# GLOBAL SHARED HTTP SESSION
# One session for the entire process lifetime — avoids the
# overhead of creating/tearing down a connector per request and
# prevents "connector closed" errors during high-frequency use.
# ─────────────────────────────────────────────────────────────
_http: aiohttp.ClientSession | None = None


def get_http() -> aiohttp.ClientSession:
    """Return (or lazily create) the process-wide HTTP session."""
    global _http
    if _http is None or _http.closed:
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        timeout   = aiohttp.ClientTimeout(total=30, connect=10)
        _http     = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _http


def c_log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────
bot_state: dict = {
    # Runtime
    'status':            'RUNNING',
    'symbol':            'XAUUSD@',
    'live_connected':    False,
    'timeframes':        _TFS,
    'active_tfs':        {'1m': False, '2m': True, '3m': True, '5m': False, '15m': False},
    # Trade sizing
    'lot_size':          0.05,
    'pip_value':         0.1,
    'spread_pips':       2.2,
    # Telegram
    'chat_id':           None,
    'last_update_id':    0,
    # TP / SL (pips)
    'tp_pips':           {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80},
    'sl_pips':           {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},
    # Strategy
    'strategy_mode':     'STOCH_OLD',   # 'STOCH_NEW' | 'STOCH_OLD'
    'filter_mode':       'NO_MA',       # 'NO_MA' | 'SIMPLE' | 'FULL'
    # Stochastic parameters
    'stoch_k':           5,
    'stoch_smooth':      5,
    'stoch_d':           5,
    # Stochastic level gates
    'use_stoch_deep':    True,
    'use_stoch_mid':     True,
    'use_stoch_shal':    False,
    # MA consistency filter
    'use_f_cons':        False,
    'cons_count':        3,
    # Time guards
    'use_time_filter':   False,
    'use_danger_filter': True,
    # Risk features
    'use_be':            False,
    'use_atr':           False,
    'use_max_spread':    True,
    'max_spread_pips':   3.0,
    'atr_mult_tp':       1.5,
    'atr_mult_sl':       3.0,
    'tp_tolerance_pips': 5.0,
    # Live data
    'market_data':       {tf: '⏸ بانتظار الاتصال (Offline)' for tf in _TFS},
    'last_signal_time':  {tf: None for tf in _TFS},
    # MetaApi handles
    'connection_obj':    None,
    'account_obj':       None,
    # Flags
    'is_backtesting':    False,
}


# ─────────────────────────────────────────────────────────────
# INDICATOR ENGINE
# ─────────────────────────────────────────────────────────────
def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Moving averages
    df['ema15']  = _ema(df['close'], 15)
    df['ema50']  = _ema(df['close'], 50)
    df['ema150'] = _ema(df['close'], 150)

    # Stochastic  %K (smoothed) → %D
    k_period = bot_state['stoch_k']
    smooth   = bot_state['stoch_smooth']
    d_period = bot_state['stoch_d']

    low_min  = df['low'].rolling(k_period).min()
    high_max = df['high'].rolling(k_period).max()
    denom    = (high_max - low_min).replace(0, 1e-10)
    df['K']  = (100.0 * (df['close'] - low_min) / denom).ewm(span=smooth, adjust=False).mean()
    df['D']  = df['K'].ewm(span=d_period, adjust=False).mean()

    # ATR (14-period)
    tr = pd.concat([
        (df['high'] - df['low']).abs(),
        (df['high'] - df['close'].shift()).abs(),
        (df['low']  - df['close'].shift()).abs(),
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean().bfill()

    return df


# ─────────────────────────────────────────────────────────────
# SIGNAL GENERATION
# ─────────────────────────────────────────────────────────────
def get_stoch_signals(
    prev_k: float, prev_d: float,
    curr_k: float, curr_d: float,
) -> tuple:
    """
    Returns (buy_signal, sell_signal, buy_label, sell_label).

    STOCH_NEW : price crosses a fixed K level (no D crossover needed).
    STOCH_OLD : K crosses D while both are in an extreme zone.
    """
    mode = bot_state['strategy_mode']

    if mode == 'STOCH_NEW':
        buy_deep  = (prev_k <= 10)          and (curr_k > 10)  and bot_state['use_stoch_deep']
        buy_mid   = (10 < prev_k <= 15)     and (curr_k > 15)  and bot_state['use_stoch_mid']
        buy_shal  = (15 < prev_k <= 20)     and (curr_k > 20)  and bot_state['use_stoch_shal']
        sell_deep = (prev_k >= 90)          and (curr_k < 90)  and bot_state['use_stoch_deep']
        sell_mid  = (85 <= prev_k < 90)     and (curr_k < 85)  and bot_state['use_stoch_mid']
        sell_shal = (80 <= prev_k < 85)     and (curr_k < 80)  and bot_state['use_stoch_shal']

    else:  # STOCH_OLD — K/D crossover inside an extreme zone
        k_cross_up   = (prev_k < prev_d) and (curr_k >= curr_d)
        k_cross_down = (prev_k > prev_d) and (curr_k <= curr_d)
        avg_k = (prev_k + curr_k) / 2.0
        buy_deep  = k_cross_up   and (avg_k <= 10)        and bot_state['use_stoch_deep']
        buy_mid   = k_cross_up   and (10 < avg_k <= 15)   and bot_state['use_stoch_mid']
        buy_shal  = k_cross_up   and (15 < avg_k <= 20)   and bot_state['use_stoch_shal']
        sell_deep = k_cross_down and (avg_k >= 90)        and bot_state['use_stoch_deep']
        sell_mid  = k_cross_down and (85 <= avg_k < 90)   and bot_state['use_stoch_mid']
        sell_shal = k_cross_down and (80 <= avg_k < 85)   and bot_state['use_stoch_shal']

    buy_sig  = buy_deep  or buy_mid  or buy_shal
    sell_sig = sell_deep or sell_mid or sell_shal

    b_label = 'DEEP(10)' if buy_deep  else 'MID(15)'  if buy_mid  else 'SHAL(20)'
    s_label = 'DEEP(90)' if sell_deep else 'MID(85)'  if sell_mid else 'SHAL(80)'

    return buy_sig, sell_sig, b_label, s_label


def compute_trend_ok(df: pd.DataFrame, i: int, curr: pd.Series) -> tuple:
    """MA trend filter for a backtest bar at positional index *i*."""
    mode = bot_state['filter_mode']
    if mode == 'NO_MA':
        return True, True

    cons  = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema = s_ema = True
    for j in range(cons):
        idx = i - j
        if idx not in df.index:
            return False, False
        c = df.loc[idx]
        if not (c['ema50'] > c['ema150']):  b_ema = False
        if not (c['ema150'] > c['ema50']):  s_ema = False

    if mode == 'SIMPLE':
        return b_ema, s_ema

    # FULL: strict alignment ema15 > ema50 > ema150
    ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
    ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
    return (b_ema and ma_buy), (s_ema and ma_sell)


def compute_trend_ok_live(df: pd.DataFrame, curr: pd.Series) -> tuple:
    """MA trend filter for the most-recent closed bar in live mode."""
    mode = bot_state['filter_mode']
    if mode == 'NO_MA':
        return True, True

    cons  = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema = s_ema = True
    for j in range(cons):
        c = df.iloc[(-2) - j]
        if not (c['ema50'] > c['ema150']):  b_ema = False
        if not (c['ema150'] > c['ema50']):  s_ema = False

    if mode == 'SIMPLE':
        return b_ema, s_ema

    ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
    ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
    return (b_ema and ma_buy), (s_ema and ma_sell)


def is_danger_time(dt_utc: datetime) -> bool:
    """Block 19:00–21:59 Damascus time (UTC+3)."""
    return 19 <= (dt_utc.hour + 3) % 24 <= 21


def _get_signal_for_bar(
    df: pd.DataFrame, i: int,
    curr: pd.Series, prev: pd.Series,
) -> tuple:
    """Evaluate stochastic signal for a single backtest candle."""
    trend_buy, trend_sell = compute_trend_ok(df, i, curr)
    raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(
        prev['K'], prev['D'], curr['K'], curr['D']
    )
    buy_sig  = raw_buy  and trend_buy
    sell_sig = raw_sell and trend_sell
    label    = b_lbl if buy_sig else s_lbl
    return buy_sig, sell_sig, label


# ─────────────────────────────────────────────────────────────
# OANDA REST HELPER
# ─────────────────────────────────────────────────────────────
_TF_MAP = {
    's5': 'S5', '1m': 'M1', '2m': 'M2', '3m': 'M3',
    '5m': 'M5', '15m': 'M15', '1h': 'H1',
}

# Semaphore: created lazily inside the running event loop.
_oanda_sem: asyncio.Semaphore | None = None

def _get_oanda_sem() -> asyncio.Semaphore:
    global _oanda_sem
    if _oanda_sem is None:
        _oanda_sem = asyncio.Semaphore(3)
    return _oanda_sem


async def fetch_oanda_candles(
    instrument: str,
    granularity: str,
    count: int = 5000,
    end_time: datetime = None,
) -> list:
    url     = f'{OANDA_URL}/instruments/{instrument}/candles'
    headers = {'Authorization': f'Bearer {OANDA_API}'}
    params  = {'granularity': _TF_MAP.get(granularity, 'M5'), 'count': count, 'price': 'M'}
    if end_time:
        params['to'] = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    async with _get_oanda_sem():
        for attempt in range(3):          # up to 3 retries on transient errors
            try:
                async with get_http().get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = json.loads(await resp.text())
                        return [
                            {
                                'time':  pd.to_datetime(c['time'], utc=True),
                                'open':  float(c['mid']['o']),
                                'high':  float(c['mid']['h']),
                                'low':   float(c['mid']['l']),
                                'close': float(c['mid']['c']),
                            }
                            for c in data.get('candles', []) if c['complete']
                        ]
                    elif resp.status == 429:          # rate limited
                        await asyncio.sleep(2 ** attempt)
                    else:
                        c_log(f'OANDA {resp.status} on attempt {attempt+1}')
                        await asyncio.sleep(1)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                c_log(f'OANDA fetch error (attempt {attempt+1}): {e}')
                await asyncio.sleep(1)
    return []


# ─────────────────────────────────────────────────────────────
# TELEGRAM HELPERS  (all use the shared session)
# ─────────────────────────────────────────────────────────────
async def send_tg_msg(text: str, reply_markup: dict = None) -> None:
    if not bot_state['chat_id']:
        return
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup:
        payload['reply_markup'] = reply_markup
    try:
        async with get_http().post(
            f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', json=payload
        ) as resp:
            if resp.status not in (200, 400):   # 400 = message too long / duplicate etc.
                c_log(f'TG send status {resp.status}')
    except Exception as e:
        c_log(f'TG send error: {e}')


async def edit_tg_msg(chat_id: int, message_id: int, text: str, reply_markup: dict = None) -> None:
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
    if reply_markup:
        payload['reply_markup'] = reply_markup
    try:
        async with get_http().post(
            f'https://api.telegram.org/bot{TG_TOKEN}/editMessageText', json=payload
        ) as resp:
            if resp.status not in (200, 400):   # 400 = "not modified" or bad request — non-fatal
                body = await resp.text()
                c_log(f'TG edit status {resp.status}: {body[:120]}')
    except Exception as e:
        c_log(f'TG edit error: {e}')


async def answer_callback(cbq_id: str, text: str = None) -> None:
    payload = {'callback_query_id': cbq_id}
    if text:
        payload['text'] = text
    try:
        async with get_http().post(
            f'https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery', json=payload
        ) as _:
            pass
    except Exception as e:
        c_log(f'TG callback error: {e}')


async def send_tg_document(file_path: str, caption: str) -> None:
    if not bot_state['chat_id']:
        return
    try:
        with open(file_path, 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('chat_id',  str(bot_state['chat_id']))
            data.add_field('document', f, filename=os.path.basename(file_path))
            data.add_field('caption',  caption)
            async with get_http().post(
                f'https://api.telegram.org/bot{TG_TOKEN}/sendDocument', data=data
            ) as resp:
                if resp.status != 200:
                    c_log(f'TG document status {resp.status}')
    except Exception as e:
        c_log(f'TG document error: {e}')


# ─────────────────────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────────────────────
def _strat_label() -> str:
    return {'STOCH_NEW': '📈 STOCH-NEW', 'STOCH_OLD': '📉 STOCH-OLD'}[bot_state['strategy_mode']]


def get_main_keyboard() -> dict:
    live = '🟢 متصل'  if bot_state['live_connected']    else '🔴 غير متصل'
    st   = '🟢 RUN'   if bot_state['status'] == 'RUNNING' else '🔴 PAUSE'
    return {'inline_keyboard': [
        [{'text': f'🔌 سيرفر التداول الحي: {live}', 'callback_data': 'toggle_live_conn'}],
        [
            {'text': f'حالة البوت: {st}',              'callback_data': 'toggle_status'},
            {'text': f'الاستراتيجية: {_strat_label()}', 'callback_data': 'cycle_strategy'},
        ],
        [
            {'text': '🎛 فلاتر وإعدادات', 'callback_data': 'menu_filters'},
            {'text': '⏱ الفريمات',        'callback_data': 'menu_tfs'},
        ],
        [
            {'text': '📊 تقرير السوق',    'callback_data': 'report'},
            {'text': '💳 الحساب',         'callback_data': 'account'},
        ],
        [
            {'text': '🛠 إعدادات المخاطرة', 'callback_data': 'menu_settings'},
            {'text': '🔬 باك تيست',         'callback_data': 'menu_backtest'},
        ],
        [{'text': '🛑 إغلاق جميع الصفقات', 'callback_data': 'close_all'}],
    ]}


def get_filters_keyboard() -> dict:
    fm  = bot_state['filter_mode']
    fi  = {k: '✅' if fm == k else '⬜' for k in ('FULL', 'SIMPLE', 'NO_MA')}
    dp  = '🟢' if bot_state['use_stoch_deep'] else '🔴'
    md  = '🟢' if bot_state['use_stoch_mid']  else '🔴'
    sh  = '🟢' if bot_state['use_stoch_shal'] else '🔴'
    ci  = '🟢' if bot_state['use_f_cons']     else '🔴'
    t_i = '🟢' if bot_state['use_time_filter']   else '🔴'
    d_i = '🟢' if bot_state['use_danger_filter'] else '🔴'
    k, s, d = bot_state['stoch_k'], bot_state['stoch_smooth'], bot_state['stoch_d']

    return {'inline_keyboard': [
        [{'text': '━━ فلتر الترند (اختر واحداً) ━━', 'callback_data': 'noop'}],
        [{'text': f"{fi['FULL']} FULL: ema15 + ema50 + ema150", 'callback_data': 'set_filter_full'}],
        [{'text': f"{fi['SIMPLE']} SIMPLE: ema50 + ema150",     'callback_data': 'set_filter_simple'}],
        [{'text': f"{fi['NO_MA']} NO MA: ستوكاستيك فقط",        'callback_data': 'set_filter_noma'}],
        [{'text': '━━ مستويات الستوكاستيك ━━', 'callback_data': 'noop'}],
        [{'text': f'⚙️ Stoch({k}, {s}, {d})  — اضغط للضبط', 'callback_data': 'menu_stoch_settings'}],
        [
            {'text': f'DEEP 10/90: {dp}', 'callback_data': 'toggle_stoch_deep'},
            {'text': f'MID  15/85: {md}', 'callback_data': 'toggle_stoch_mid'},
            {'text': f'SHAL 20/80: {sh}', 'callback_data': 'toggle_stoch_shal'},
        ],
        [{'text': f'ثبات الترند ({bot_state["cons_count"]} شموع): {ci}', 'callback_data': 'toggle_f_cons'}],
        [{'text': '━━ فلاتر الوقت ━━', 'callback_data': 'noop'}],
        [
            {'text': f'Time Filter 08-17 UTC: {t_i}', 'callback_data': 'toggle_time'},
            {'text': f'حظر 19-22 دمشق: {d_i}',        'callback_data': 'toggle_danger'},
        ],
        [{'text': '🔙 القائمة الرئيسية', 'callback_data': 'menu_main'}],
    ]}


def get_stoch_settings_keyboard() -> dict:
    k = bot_state['stoch_k']
    s = bot_state['stoch_smooth']
    d = bot_state['stoch_d']
    return {'inline_keyboard': [
        [{'text': f'الإعداد الحالي: Stoch({k}, {s}, {d})', 'callback_data': 'noop'}],
        [{'text': '📝 أرسل نصياً: /stoch K S D   (مثال: /stoch 14 3 3)', 'callback_data': 'noop'}],
        [{'text': '━━ K Period ━━', 'callback_data': 'noop'}],
        [
            {'text': '➖', 'callback_data': 'dec_stoch_k'},
            {'text': f'K = {k}', 'callback_data': 'noop'},
            {'text': '➕', 'callback_data': 'inc_stoch_k'},
        ],
        [{'text': '━━ Smooth ━━', 'callback_data': 'noop'}],
        [
            {'text': '➖', 'callback_data': 'dec_stoch_s'},
            {'text': f'S = {s}', 'callback_data': 'noop'},
            {'text': '➕', 'callback_data': 'inc_stoch_s'},
        ],
        [{'text': '━━ D Period ━━', 'callback_data': 'noop'}],
        [
            {'text': '➖', 'callback_data': 'dec_stoch_d'},
            {'text': f'D = {d}', 'callback_data': 'noop'},
            {'text': '➕', 'callback_data': 'inc_stoch_d'},
        ],
        [
            {'text': '5, 5, 5 (افتراضي)',  'callback_data': 'preset_5_5_5'},
            {'text': '14, 3, 3',            'callback_data': 'preset_14_3_3'},
            {'text': '10, 3, 3',            'callback_data': 'preset_10_3_3'},
        ],
        [{'text': '🔙 رجوع للفلاتر', 'callback_data': 'menu_filters'}],
    ]}


def get_tf_keyboard() -> dict:
    rows, row = [], []
    for tf in bot_state['timeframes']:
        icon = '🟢' if bot_state['active_tfs'][tf] else '🔴'
        row.append({'text': f'{tf}: {icon}', 'callback_data': f'toggle_tf_{tf}'})
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([{'text': '🔙 رجوع', 'callback_data': 'menu_main'}])
    return {'inline_keyboard': rows}


def get_settings_keyboard() -> dict:
    be_i  = '🟢' if bot_state['use_be']         else '🔴'
    atr_i = '🟢' if bot_state['use_atr']        else '🔴'
    spr_i = '🟢' if bot_state['use_max_spread'] else '🔴'
    return {'inline_keyboard': [
        [{'text': f'تأمين الدخول (BE 20p): {be_i}',                          'callback_data': 'toggle_be'}],
        [{'text': f'أهداف ATR: {atr_i}',                                      'callback_data': 'toggle_atr'}],
        [{'text': f'حماية السبريد ≤{bot_state["max_spread_pips"]}p: {spr_i}', 'callback_data': 'toggle_spread'}],
        [{'text': f'حجم اللوت: {bot_state["lot_size"]:.2f}',                  'callback_data': 'noop'}],
        [
            {'text': '➕ Lot', 'callback_data': 'inc_lot'},
            {'text': '➖ Lot', 'callback_data': 'dec_lot'},
        ],
        [{'text': '🎯 تعديل TP / SL لكل فريم', 'callback_data': 'view_tpsl'}],
        [{'text': '🔙 رجوع',                    'callback_data': 'menu_main'}],
    ]}


def get_tpsl_overview_keyboard() -> dict:
    """
    Overview screen: shows current TP/SL for every timeframe,
    with a dedicated [تعديل] button per row that opens the per-TF editor.
    Also shows the /set command hint.
    """
    rows = [
        [{'text': '━━ اضغط على فريم لتعديله ━━', 'callback_data': 'noop'}],
    ]
    for tf in bot_state['timeframes']:
        icon = '🟢' if bot_state['active_tfs'][tf] else '🔴'
        tp   = bot_state['tp_pips'][tf]
        sl   = bot_state['sl_pips'][tf]
        rows.append([
            {'text': f'{icon} [{tf}]  TP:{tp}p  SL:{sl}p', 'callback_data': 'noop'},
            {'text': '✏️ تعديل', 'callback_data': f'tpsl_edit_{tf}'},
        ])
    rows.append([{'text': '📝 نصياً: /set 1m sl 75', 'callback_data': 'noop'}])
    rows.append([{'text': '🔙 رجوع للمخاطرة', 'callback_data': 'menu_settings'}])
    return {'inline_keyboard': rows}


def get_tpsl_edit_keyboard(tf: str) -> dict:
    """Per-timeframe TP/SL editor — max 3 buttons per row for mobile."""
    tp = bot_state['tp_pips'][tf]
    sl = bot_state['sl_pips'][tf]
    rr = round(tp / sl, 2) if sl else '∞'
    return {'inline_keyboard': [
        [{'text': f'[{tf}]  TP: {tp}p  |  SL: {sl}p  |  R:R 1:{rr}', 'callback_data': 'noop'}],
        # ── Take Profit ──
        [{'text': '🎯 Take Profit', 'callback_data': 'noop'}],
        [
            {'text': f'➖10  ({tp-10})', 'callback_data': f'dec_tp10_{tf}'},
            {'text': f'TP={tp}p',        'callback_data': 'noop'},
            {'text': f'➕10  ({tp+10})', 'callback_data': f'inc_tp10_{tf}'},
        ],
        [
            {'text': f'➖5  ({tp-5})', 'callback_data': f'dec_tp5_{tf}'},
            {'text': '─',              'callback_data': 'noop'},
            {'text': f'➕5  ({tp+5})', 'callback_data': f'inc_tp5_{tf}'},
        ],
        # ── Stop Loss ──
        [{'text': '🛑 Stop Loss', 'callback_data': 'noop'}],
        [
            {'text': f'➖10  ({sl-10})', 'callback_data': f'dec_sl10_{tf}'},
            {'text': f'SL={sl}p',        'callback_data': 'noop'},
            {'text': f'➕10  ({sl+10})', 'callback_data': f'inc_sl10_{tf}'},
        ],
        [
            {'text': f'➖5  ({sl-5})', 'callback_data': f'dec_sl5_{tf}'},
            {'text': '─',              'callback_data': 'noop'},
            {'text': f'➕5  ({sl+5})', 'callback_data': f'inc_sl5_{tf}'},
        ],
        [{'text': f'📝 /set {tf} tp|sl قيمة', 'callback_data': 'noop'}],
        [{'text': '🔙 رجوع للقائمة', 'callback_data': 'view_tpsl'}],
    ]}


def get_backtest_keyboard() -> dict:
    return {'inline_keyboard': [
        [
            {'text': '📊 1 يوم',  'callback_data': 'bto_1'},
            {'text': '📊 3 أيام', 'callback_data': 'bto_3'},
            {'text': '📊 7 أيام', 'callback_data': 'bto_7'},
        ],
        [{'text': '🔬 Advanced — 7 أيام', 'callback_data': 'bto_adv_7'}],
        [{'text': '🔙 رجوع', 'callback_data': 'menu_main'}],
    ]}


# ─────────────────────────────────────────────────────────────
# BACKTEST — SHARED HELPERS
# ─────────────────────────────────────────────────────────────
def _build_trade_row(
    tf: str, is_buy: bool, label: str,
    entry_t: datetime, exit_t: datetime,
    act_ent: float, tp_p: float, sl_p: float,
    outcome: str, p_usd: float,
) -> dict:
    pv = bot_state['pip_value']
    return {
        'Timeframe':   tf,
        'Type':        ('BUY' if is_buy else 'SELL') + f' [{label}]',
        'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
        'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
        'Entry Price': round(act_ent, 2),
        'TP':          tp_p,
        'SL':          sl_p,
        'Pips': (
            round(abs(act_ent - (tp_p if outcome == 'WIN' else sl_p)) / pv, 1)
            if outcome in ('WIN', 'LOSS') else 0
        ),
        'Outcome':    outcome,
        'Profit ($)': p_usd,
    }


def _simulate_trade_in_memory(
    is_buy: bool, act_ent: float,
    tp_p: float, sl_p: float, eff_tp: float,
    entry_t: datetime,
    minute_candles: list,          # pre-fetched 1-m candles for the whole backtest window
) -> tuple:
    """
    Walk pre-fetched 1-minute candles to determine trade outcome.
    Pure CPU — no I/O.  Returns (outcome, exit_time, final_sl).

    The candles list must be sorted ascending by time and already
    cover the full simulation window (entry_t … entry_t + 72 h).
    """
    pv     = bot_state['pip_value']
    be_act = False
    be_tgt = act_ent + (1 if is_buy else -1) * 20 * pv
    max_ext = entry_t + timedelta(hours=72)
    outcome = 'EXPIRED'
    exit_t  = max_ext

    for vc in minute_candles:
        t = vc['time']
        if t < entry_t:
            continue
        if t > max_ext:
            break

        if is_buy:
            if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                sl_p   = act_ent
                be_act = True
            if vc['low'] <= sl_p:
                outcome = 'BREAK-EVEN' if be_act else 'LOSS'
                exit_t  = t; break
            if vc['high'] >= eff_tp:
                outcome = 'WIN'
                exit_t  = t; break
        else:
            if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                sl_p   = act_ent
                be_act = True
            if vc['high'] >= sl_p:
                outcome = 'BREAK-EVEN' if be_act else 'LOSS'
                exit_t  = t; break
            if vc['low'] <= eff_tp:
                outcome = 'WIN'
                exit_t  = t; break

    return outcome, exit_t, sl_p


def _calc_pnl(outcome: str, act_ent: float, tp_p: float, sl_p: float) -> float:
    if outcome == 'BREAK-EVEN':
        return 0.0
    if outcome in ('WIN', 'LOSS'):
        exit_p = tp_p if outcome == 'WIN' else sl_p
        raw    = abs(act_ent - exit_p) * 100 * bot_state['lot_size']
        return round(raw, 2) * (1 if outcome == 'WIN' else -1)
    return 0.0  # EXPIRED


def _entry_params(curr: pd.Series, is_buy: bool, tf: str) -> tuple:
    """Return (act_ent, tp_p, sl_p, eff_tp) for a given signal bar."""
    m       = 1 if is_buy else -1
    act_ent = curr['open'] + m * bot_state['spread_pips'] * bot_state['pip_value']
    tp_dist = (curr['atr'] * bot_state['atr_mult_tp']
               if bot_state['use_atr']
               else bot_state['tp_pips'][tf] * bot_state['pip_value'])
    sl_dist = (curr['atr'] * bot_state['atr_mult_sl']
               if bot_state['use_atr']
               else bot_state['sl_pips'][tf] * bot_state['pip_value'])
    tp_p   = round(act_ent + m * tp_dist, 2)
    sl_p   = round(act_ent - m * sl_dist, 2)
    tol    = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
    eff_tp = (tp_p - tol) if is_buy else (tp_p + tol)
    return act_ent, tp_p, sl_p, eff_tp


# ─────────────────────────────────────────────────────────────
# STANDARD BACKTEST
# ─────────────────────────────────────────────────────────────
async def run_oanda_backtest(start_dt: datetime) -> None:
    if bot_state['is_backtesting']:
        await send_tg_msg('⚠️ يوجد باك تيست قيد المعالجة.'); return

    bot_state['is_backtesting'] = True
    desc  = f"{bot_state['strategy_mode']} / {bot_state['filter_mode']}"
    fname = f"BT_{datetime.now().strftime('%H%M%S')}.xlsx"

    await send_tg_msg(
        f'⏳ <b>بدء الباك تيست</b>\n'
        f'من: {start_dt.strftime("%Y-%m-%d")}\n'
        f'الاستراتيجية: {desc}'
    )

    trade_logs, blocked_logs = [], []
    total_prof = peak_equity = max_dd = 0.0
    total_win  = total_loss  = 0.0
    win_count  = loss_count  = be_count = 0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            await asyncio.sleep(0)      # yield to event loop between TFs

            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300:
                c_log(f'[{tf}] not enough candles ({len(c_data)}), skipping'); continue

            df         = calculate_indicators(pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(10, bot_state['cons_count'])

            # ── Pre-fetch 1-minute candles ONCE for the whole TF window ──
            # We need up to (last_entry + 72 h) worth of 1m bars.
            # OANDA allows max 5000 per call; for 7 days that is 10 080 bars.
            # We make at most 2 sequential fetches if needed.
            tf_end    = datetime.now(timezone.utc)
            total_min = int((tf_end - start_dt).total_seconds() / 60) + 72 * 60 + 60
            chunk     = min(total_min, 5000)
            m1_raw    = await fetch_oanda_candles('XAU_USD', '1m', chunk, tf_end)
            if total_min > 5000 and m1_raw:
                older_end = m1_raw[0]['time']
                m1_extra  = await fetch_oanda_candles('XAU_USD', '1m', 5000, older_end)
                m1_raw    = m1_extra + m1_raw
            minute_candles = sorted(m1_raw, key=lambda x: x['time'])
            c_log(f'[{tf}] fetched {len(minute_candles)} 1m candles for simulation')

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                await asyncio.sleep(0)  # yield every bar — keeps polling alive

                curr = df.loc[i]; prev = df.loc[i - 1]
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                buy_sig, sell_sig, label = _get_signal_for_bar(df, i, curr, prev)

                raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(prev['K'], prev['D'], curr['K'], curr['D'])
                ts = (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M')
                if not buy_sig  and raw_buy:
                    blocked_logs.append({'Timeframe': tf, 'Type': f'BUY BLOCKED ({b_lbl})', 'Entry Time': ts, 'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'})
                if not sell_sig and raw_sell:
                    blocked_logs.append({'Timeframe': tf, 'Type': f'SELL BLOCKED ({s_lbl})', 'Entry Time': ts, 'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'})

                if not (buy_sig or sell_sig) or i + 1 >= len(df): continue

                next_c  = df.loc[i + 1]
                entry_t = next_c['time']
                is_buy  = bool(buy_sig)
                signal_bar         = next_c.copy()
                signal_bar['atr']  = curr['atr']
                act_ent, tp_p, sl_p, eff_tp = _entry_params(signal_bar, is_buy, tf)

                outcome, exit_t, sl_p = _simulate_trade_in_memory(
                    is_buy, act_ent, tp_p, sl_p, eff_tp, entry_t, minute_candles
                )
                p_usd = _calc_pnl(outcome, act_ent, tp_p, sl_p)

                if outcome == 'BREAK-EVEN':   be_count   += 1
                elif outcome == 'WIN':         total_win  += p_usd; win_count  += 1
                elif outcome == 'LOSS':        total_loss += p_usd; loss_count += 1

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)
                trade_logs.append(_build_trade_row(tf, is_buy, label, entry_t, exit_t, act_ent, tp_p, sl_p, outcome, p_usd))

        if not trade_logs:
            await send_tg_msg('⚠️ لم يتم العثور على أي صفقات.'); return

        total_trades = win_count + loss_count
        win_rate     = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct       = round(max_dd / peak_equity * 100, 1) if peak_equity else 0

        summary = {
            'البند': ['✅ الربح الكلي', '❌ الخسارة الكلية', '💰 المحصلة', '🎯 نسبة الفوز', '📉 أقصى DD', '🔄 بريك إيفن', '📌 الاستراتيجية'],
            'القيمة': [
                f'{win_count} | +${round(total_win, 2)}',
                f'{loss_count} | -${abs(round(total_loss, 2))}',
                f'${round(total_prof, 2)}',
                f'{win_rate}% ({total_trades} صفقة)',
                f'${round(max_dd, 2)} ({dd_pct}%)',
                str(be_count),
                desc,
            ],
        }

        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            pd.DataFrame(trade_logs).to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(summary).to_excel(writer, sheet_name='الملخص', index=False)
            if blocked_logs:
                pd.DataFrame(blocked_logs).to_excel(writer, sheet_name='المرفوضة', index=False)
            _style_sheet(writer.sheets['الصفقات'])

        await send_tg_document(
            fname,
            f'📊 <b>الباك تيست</b> | {desc}\n'
            f'✅ +${round(total_win, 2)} ({win_count}) | '
            f'❌ -${abs(round(total_loss, 2))} ({loss_count})\n'
            f'💰 ${round(total_prof, 2)} | 🎯 {win_rate}% | 📉 DD:{round(max_dd, 2)}'
        )
        os.remove(fname)

    except Exception as e:
        await send_tg_msg(f'❌ خطأ في الباك تيست: {e}')
        c_log(f'Backtest error: {e}')
    finally:
        bot_state['is_backtesting'] = False


# ─────────────────────────────────────────────────────────────
# ADVANCED BACKTEST
# ─────────────────────────────────────────────────────────────
async def run_advanced_backtest(days: int = 7) -> None:
    if bot_state['is_backtesting']:
        await send_tg_msg('⚠️ يوجد باك تيست قيد المعالجة.'); return

    bot_state['is_backtesting'] = True
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    desc     = f"{bot_state['strategy_mode']} / {bot_state['filter_mode']}"

    await send_tg_msg(
        f'⏳ <b>Advanced Backtest</b>\n'
        f'من: {start_dt.strftime("%Y-%m-%d")} ({days} أيام)\n'
        f'الاستراتيجية: {desc}'
    )

    trade_logs, blocked_logs = [], []
    total_prof  = peak_equity = max_dd = 0.0
    total_win   = total_loss  = 0.0
    win_count   = loss_count  = be_count = 0
    long_win    = long_loss   = short_win = short_loss = 0
    all_profits = []
    consec_win  = consec_loss = max_cw = max_cl = 0
    max_cw_usd  = max_cl_usd = cur_w = cur_l = 0.0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            await asyncio.sleep(0)      # yield to event loop between TFs

            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300: continue

            df         = calculate_indicators(pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(10, bot_state['cons_count'])

            # Pre-fetch 1m candles once for the whole TF window
            tf_end    = datetime.now(timezone.utc)
            total_min = int((tf_end - start_dt).total_seconds() / 60) + 72 * 60 + 60
            chunk     = min(total_min, 5000)
            m1_raw    = await fetch_oanda_candles('XAU_USD', '1m', chunk, tf_end)
            if total_min > 5000 and m1_raw:
                older_end = m1_raw[0]['time']
                m1_extra  = await fetch_oanda_candles('XAU_USD', '1m', 5000, older_end)
                m1_raw    = m1_extra + m1_raw
            minute_candles = sorted(m1_raw, key=lambda x: x['time'])
            c_log(f'[ADV/{tf}] fetched {len(minute_candles)} 1m candles for simulation')

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                await asyncio.sleep(0)  # yield every bar

                curr = df.loc[i]; prev = df.loc[i - 1]
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                buy_sig, sell_sig, label = _get_signal_for_bar(df, i, curr, prev)

                raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(prev['K'], prev['D'], curr['K'], curr['D'])
                ts = (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M')
                if not buy_sig  and raw_buy:
                    blocked_logs.append({'Timeframe': tf, 'Type': f'BUY BLOCKED ({b_lbl})', 'Entry Time': ts, 'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'})
                if not sell_sig and raw_sell:
                    blocked_logs.append({'Timeframe': tf, 'Type': f'SELL BLOCKED ({s_lbl})', 'Entry Time': ts, 'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'})

                if not (buy_sig or sell_sig) or i + 1 >= len(df): continue

                next_c  = df.loc[i + 1]
                entry_t = next_c['time']
                is_buy  = bool(buy_sig)
                signal_bar         = next_c.copy()
                signal_bar['atr']  = curr['atr']
                act_ent, tp_p, sl_p, eff_tp = _entry_params(signal_bar, is_buy, tf)

                outcome, exit_t, sl_p = _simulate_trade_in_memory(
                    is_buy, act_ent, tp_p, sl_p, eff_tp, entry_t, minute_candles
                )
                p_usd = _calc_pnl(outcome, act_ent, tp_p, sl_p)

                if outcome == 'WIN':
                    total_win  += p_usd; win_count  += 1
                    consec_win += 1; cur_w += p_usd; consec_loss = 0; cur_l = 0.0
                    if consec_win > max_cw: max_cw = consec_win; max_cw_usd = cur_w
                    if is_buy: long_win  += 1
                    else:      short_win += 1
                elif outcome == 'LOSS':
                    total_loss  += p_usd; loss_count  += 1
                    consec_loss += 1; cur_l += p_usd; consec_win = 0; cur_w = 0.0
                    if consec_loss > max_cl: max_cl = consec_loss; max_cl_usd = cur_l
                    if is_buy: long_loss  += 1
                    else:      short_loss += 1
                elif outcome == 'BREAK-EVEN':
                    be_count += 1

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)
                all_profits.append(p_usd)

                row = _build_trade_row(tf, is_buy, label, entry_t, exit_t, act_ent, tp_p, sl_p, outcome, p_usd)
                row['Hour_Damascus'] = (curr['time'].hour + 3) % 24
                row['Weekday']       = curr['time'].strftime('%a')
                trade_logs.append(row)

        if not trade_logs:
            await send_tg_msg('⚠️ لم يتم العثور على صفقات.'); return

        total_trades    = win_count + loss_count
        win_rate        = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct          = round(max_dd / peak_equity * 100, 1)     if peak_equity  else 0
        profit_factor   = round(total_win / abs(total_loss), 2)    if total_loss   else 999.0
        expected_payoff = round(total_prof / total_trades, 2)       if total_trades else 0
        recovery_factor = round(total_prof / max_dd, 2)             if max_dd       else 999.0

        wins_only    = [p for p in all_profits if p > 0]
        losses_only  = [p for p in all_profits if p < 0]
        avg_win      = round(sum(wins_only)   / len(wins_only),   2) if wins_only   else 0
        avg_loss     = round(sum(losses_only) / len(losses_only), 2) if losses_only else 0
        largest_win  = round(max(wins_only),  2) if wins_only   else 0
        largest_loss = round(min(losses_only), 2) if losses_only else 0

        df_t        = pd.DataFrame(trade_logs)
        actv        = df_t[df_t['Outcome'].isin(['WIN', 'LOSS'])]
        hour_counts = actv.groupby('Hour_Damascus').size()
        day_counts  = actv.groupby('Weekday').size()

        def bar(dd: dict, width: int = 18) -> str:
            if not dd: return '(لا بيانات)'
            mx = max(dd.values())
            return '\n'.join(
                f'  {str(k):>4} |{"█" * int(v / mx * width):<{width}}| {v}'
                for k, v in sorted(dd.items())
            )

        report = (
            f'📊 <b>Advanced Report — {days} يوم</b>\n📌 {desc}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>💰 الأرباح</b>\n'
            f'  صافي:  ${round(total_prof, 2)}\n'
            f'  ربح:   +${round(total_win, 2)}\n'
            f'  خسارة: -${abs(round(total_loss, 2))}\n'
            f'  PF: {profit_factor} | EP: ${expected_payoff} | RF: {recovery_factor}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>📉 Drawdown</b>: ${round(max_dd, 2)} ({dd_pct}%)\n'
            f'━━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>📈 الصفقات</b>\n'
            f'  {total_trades} صفقة | فوز: {win_count} ({win_rate}%) | خسارة: {loss_count}\n'
            f'  Long W/L: {long_win}/{long_loss} | Short W/L: {short_win}/{short_loss}\n'
            f'  بريك إيفن: {be_count}\n'
            f'━━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>🔢 إحصاءات</b>\n'
            f'  أكبر ربح: +${largest_win} | أكبر خسارة: ${largest_loss}\n'
            f'  متوسط ربح: +${avg_win} | متوسط خسارة: ${avg_loss}\n'
            f'  سلسلة فوز:   {max_cw} (+${round(max_cw_usd, 2)})\n'
            f'  سلسلة خسارة: {max_cl} (-${abs(round(max_cl_usd, 2))})\n'
            f'━━━━━━━━━━━━━━━━━━━━━━━\n'
            f'<b>🕐 بالساعة:</b>\n<pre>{bar(hour_counts.to_dict())}</pre>\n'
            f'<b>📅 بالأيام:</b>\n<pre>{bar(day_counts.to_dict())}</pre>'
        )
        await send_tg_msg(report)

        xlsx_adv = f"ADV_{datetime.now().strftime('%H%M%S')}.xlsx"
        df_exec  = df_t.drop(columns=['Hour_Damascus', 'Weekday'], errors='ignore')
        stats = {
            'المقياس': ['صافي الربح', 'إجمالي الربح', 'إجمالي الخسارة', 'Profit Factor', 'Expected Payoff', 'Recovery Factor', 'أقصى DD', 'DD%', 'إجمالي الصفقات', 'فوز', 'خسارة', 'نسبة الفوز', 'بريك إيفن', 'Long W/L', 'Short W/L', 'أكبر ربح', 'أكبر خسارة', 'متوسط ربح', 'متوسط خسارة', 'أكبر سلسلة فوز', 'أكبر سلسلة خسارة', 'الاستراتيجية'],
            'القيمة':  [f'${round(total_prof,2)}', f'+${round(total_win,2)}', f'-${abs(round(total_loss,2))}', profit_factor, expected_payoff, recovery_factor, f'${round(max_dd,2)}', f'{dd_pct}%', total_trades, win_count, loss_count, f'{win_rate}%', be_count, f'{long_win}/{long_loss}', f'{short_win}/{short_loss}', f'+${largest_win}', f'${largest_loss}', f'+${avg_win}', f'${avg_loss}', f'{max_cw}(+${round(max_cw_usd,2)})', f'{max_cl}(-${abs(round(max_cl_usd,2))})', desc],
        }
        with pd.ExcelWriter(xlsx_adv, engine='openpyxl') as writer:
            df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(stats).to_excel(writer, sheet_name='الإحصاءات', index=False)
            if blocked_logs:
                pd.DataFrame(blocked_logs).to_excel(writer, sheet_name='المرفوضة', index=False)
            _style_sheet(writer.sheets['الصفقات'])

        await send_tg_document(xlsx_adv, f'📊 Advanced Report — {days} يوم | {desc}')
        os.remove(xlsx_adv)

    except Exception as e:
        await send_tg_msg(f'❌ خطأ: {e}')
        c_log(f'Advanced backtest error: {e}')
    finally:
        bot_state['is_backtesting'] = False


# ─────────────────────────────────────────────────────────────
# EXCEL STYLING
# ─────────────────────────────────────────────────────────────
def _style_sheet(ws) -> None:
    from openpyxl.styles import PatternFill, Font
    green  = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
    red    = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
    header = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')

    for cell in ws[1]:
        cell.fill = header
        cell.font = Font(color='FFFFFF', bold=True)

    outcome_col = next((i + 1 for i, c in enumerate(ws[1]) if c.value == 'Outcome'), 9)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        val = str(row[outcome_col - 1].value) if len(row) >= outcome_col else ''
        if val == 'WIN':
            for cell in row: cell.fill = green
        elif val == 'LOSS':
            for cell in row: cell.fill = red

    for col in ws.columns:
        max_len = max((len(str(c.value or '')) for c in col), default=8)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 3, 28)


# ─────────────────────────────────────────────────────────────
# LIVE MONITORS
# ─────────────────────────────────────────────────────────────
async def position_monitor() -> None:
    """BE monitor: moves SL to break-even when trade is +20 pips in profit."""
    while True:
        try:
            if bot_state['live_connected'] and bot_state['use_be'] and bot_state['connection_obj']:
                pv        = bot_state['pip_value']
                positions = await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] != bot_state['symbol']: continue
                    op, tp, sl, cp = p['openPrice'], p.get('takeProfit'), p.get('stopLoss'), p['currentPrice']
                    if tp and sl != op and abs(cp - op) >= 20 * pv:
                        is_buy = tp > op
                        if (is_buy and cp > op) or (not is_buy and cp < op):
                            await bot_state['connection_obj'].modify_position(p['id'], stop_loss=op)
                            await send_tg_msg(f"🛡️ <b>BE</b> تأمين الدخول — صفقة: {p['id']}")
        except Exception as e:
            c_log(f'Position monitor error: {e}')
        await asyncio.sleep(5)


async def timeframe_scanner(tf: str) -> None:
    c_log(f'✅ ماسح [{tf}] يعمل.')
    while True:
        try:
            if not (bot_state['status'] == 'RUNNING' and bot_state['active_tfs'][tf]):
                await asyncio.sleep(10); continue

            if not (bot_state['live_connected'] and bot_state['account_obj']):
                bot_state['market_data'][tf] = '⏸ بانتظار الاتصال (Offline)'
                await asyncio.sleep(5); continue

            try:
                raw = await bot_state['account_obj'].get_historical_candles(bot_state['symbol'], tf, limit=500)
            except Exception:
                await asyncio.sleep(15); continue

            df      = calculate_indicators(pd.DataFrame(raw))
            curr    = df.iloc[-2]
            prev    = df.iloc[-3]
            now_utc = datetime.now(timezone.utc)

            bot_state['market_data'][tf] = f"{df.iloc[-1]['close']:.2f} | K:{curr['K']:.1f}  D:{curr['D']:.1f}"

            danger_now = bot_state['use_danger_filter'] and is_danger_time(now_utc)
            time_block = bot_state['use_time_filter'] and not (8 <= now_utc.hour <= 17)

            if time_block or danger_now:
                bot_state['market_data'][tf] = f"⏸ خمول | {df.iloc[-1]['close']:.2f}"
                await asyncio.sleep(10); continue

            if bot_state['last_signal_time'][tf] == curr['time']:
                await asyncio.sleep(10); continue

            # Evaluate signal
            trend_buy, trend_sell = compute_trend_ok_live(df, curr)
            raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(prev['K'], prev['D'], curr['K'], curr['D'])
            buy_sig  = raw_buy  and trend_buy
            sell_sig = raw_sell and trend_sell
            label    = b_lbl if buy_sig else s_lbl

            # Spread guard
            if bot_state['use_max_spread'] and (buy_sig or sell_sig):
                try:
                    tick        = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                    spread_pips = (tick['ask'] - tick['bid']) / bot_state['pip_value']
                    if spread_pips > bot_state['max_spread_pips']:
                        c_log(f'[{tf}] spread {spread_pips:.1f}p > max, skipping')
                        buy_sig = sell_sig = False
                except Exception:
                    pass

            if not (buy_sig or sell_sig):
                await asyncio.sleep(10); continue

            bot_state['last_signal_time'][tf] = curr['time']
            price  = df.iloc[-1]['close']
            m      = 1 if buy_sig else -1
            t_str  = 'شراء 🟢 BUY' if buy_sig else 'بيع 🔴 SELL'

            tp_dist = (curr['atr'] * bot_state['atr_mult_tp']
                       if bot_state['use_atr'] else
                       bot_state['tp_pips'][tf] * bot_state['pip_value'])
            sl_dist = (curr['atr'] * bot_state['atr_mult_sl']
                       if bot_state['use_atr'] else
                       bot_state['sl_pips'][tf] * bot_state['pip_value'])
            tp = round(price + m * tp_dist, 2)
            sl = round(price - m * sl_dist, 2)

            try:
                if buy_sig:
                    await bot_state['connection_obj'].create_market_buy_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                else:
                    await bot_state['connection_obj'].create_market_sell_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                await send_tg_msg(
                    f'🚨 <b>تم فتح صفقة!</b>\n'
                    f'النوع:  {t_str}\n'
                    f'الفريم: {tf}\n'
                    f'السعر:  {price:.2f} | TP: {tp} | SL: {sl}\n'
                    f'[{label}]'
                )
            except Exception as e:
                await send_tg_msg(f'❌ <b>فشل التنفيذ [{tf}]:</b>\n{e}')

        except Exception as e:
            c_log(f'Scanner [{tf}] error: {e}')

        await asyncio.sleep(10)


# ─────────────────────────────────────────────────────────────
# COMMAND PARSERS
# ─────────────────────────────────────────────────────────────
def _parse_stoch_cmd(msg: str):
    """
    Parse `/stoch K S D` and return (k, s, d) or None.
    All three values must be integers in [1, 100].
    """
    parts = msg.strip().split()
    if len(parts) != 4:
        return None
    try:
        k, s, d = int(parts[1]), int(parts[2]), int(parts[3])
        if all(1 <= v <= 100 for v in (k, s, d)):
            return k, s, d
    except ValueError:
        pass
    return None


def _parse_set_cmd(msg: str):
    """
    Parse `/set {tf} {tp|sl} {value}` and return (tf, key, value) or None.

    Examples:
        /set 1m sl 75      → ('1m', 'sl', 75)
        /set 15m tp 120    → ('15m', 'tp', 120)
        /set 2m TP 50      → ('2m', 'tp', 50)   # case-insensitive
    """
    parts = msg.strip().split()
    if len(parts) != 4:
        return None
    _, tf, key, val = parts
    tf  = tf.lower()
    key = key.lower()
    if tf not in _TFS:
        return None
    if key not in ('tp', 'sl'):
        return None
    try:
        value = int(val)
        if value < 1:
            return None
        return tf, key, value
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────
# TELEGRAM UPDATE HANDLER
# ─────────────────────────────────────────────────────────────
async def process_tg_update(update: dict) -> None:

    # ── Text / Command messages ───────────────────────────────────
    if 'message' in update and 'text' in update['message']:
        msg  = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']

        # /start
        if msg == '/start':
            await send_tg_msg(
                '🤖 <b>Gold Scalper Bot v4</b>\n'
                'الاستراتيجيات المتاحة: STOCH-NEW | STOCH-OLD',
                get_main_keyboard(),
            )

        # /stoch K S D  — set stochastic parameters via text
        elif msg.lower().startswith('/stoch'):
            result = _parse_stoch_cmd(msg)
            if result:
                k, s, d = result
                bot_state['stoch_k']      = k
                bot_state['stoch_smooth'] = s
                bot_state['stoch_d']      = d
                await send_tg_msg(
                    f'✅ <b>تم تحديث الستوكاستيك</b>\n'
                    f'K = {k} | Smooth = {s} | D = {d}\n'
                    f'الإعداد الجديد: <code>Stoch({k}, {s}, {d})</code>'
                )
            else:
                await send_tg_msg(
                    '⚠️ <b>صيغة خاطئة</b>\n'
                    'الاستخدام: <code>/stoch K S D</code>\n'
                    'مثال:     <code>/stoch 14 3 3</code>\n'
                    'القيم يجب أن تكون بين 1 و 100'
                )

        # /set {tf} {tp|sl} {value}  — set TP or SL pips for a timeframe
        elif msg.lower().startswith('/set'):
            result = _parse_set_cmd(msg)
            if result:
                tf, key, value = result
                target_dict = 'tp_pips' if key == 'tp' else 'sl_pips'
                bot_state[target_dict][tf] = value
                tp = bot_state['tp_pips'][tf]
                sl = bot_state['sl_pips'][tf]
                rr = round(tp / sl, 2) if sl else '∞'
                await send_tg_msg(
                    f'✅ <b>تم التحديث</b>\n'
                    f'الفريم: [{tf}]\n'
                    f'TP = {tp}p  |  SL = {sl}p\n'
                    f'R:R = 1:{rr}\n\n'
                    f'💡 لتعديل فريم آخر:\n'
                    f'<code>/set 2m sl 100</code>\n'
                    f'<code>/set 15m tp 80</code>'
                )
            else:
                tfs_str = ' | '.join(_TFS)
                await send_tg_msg(
                    f'⚠️ <b>صيغة خاطئة</b>\n'
                    f'الاستخدام: <code>/set فريم tp|sl قيمة</code>\n\n'
                    f'الفريمات المتاحة: <code>{tfs_str}</code>\n\n'
                    f'أمثلة:\n'
                    f'  <code>/set 1m  sl 75</code>\n'
                    f'  <code>/set 2m  tp 40</code>\n'
                    f'  <code>/set 15m sl 150</code>'
                )

        # /backtest YYYY-MM-DD
        elif msg.startswith('/backtest'):
            try:
                start_dt = datetime.strptime(msg.split()[1], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                asyncio.create_task(run_oanda_backtest(start_dt))
            except (IndexError, ValueError):
                await send_tg_msg('⚠️ الاستخدام: <code>/backtest YYYY-MM-DD</code>')

        # /debug — live indicator snapshot
        elif msg == '/debug':
            if not bot_state['account_obj']:
                await send_tg_msg('⚠️ غير متصل بالسيرفر.'); return
            try:
                raw  = await bot_state['account_obj'].get_historical_candles(bot_state['symbol'], '5m', limit=5)
                df   = calculate_indicators(pd.DataFrame(raw))
                curr = df.iloc[-2]
                await send_tg_msg(
                    f'✅ <b>Debug [5m — آخر شمعة مغلقة]</b>\n'
                    f'K: {curr["K"]:.2f}  |  D: {curr["D"]:.2f}\n'
                    f'EMA15:  {curr["ema15"]:.2f}\n'
                    f'EMA50:  {curr["ema50"]:.2f}\n'
                    f'EMA150: {curr["ema150"]:.2f}\n'
                    f'ATR:    {curr["atr"]:.2f}'
                )
            except Exception as e:
                await send_tg_msg(f'❌ خطأ: {e}')

        return  # end text-message handling

    # ── Callback queries ──────────────────────────────────────────
    if 'callback_query' not in update:
        return

    q       = update['callback_query']
    d       = q['data']
    chat_id = q['message']['chat']['id']
    msg_id  = q['message']['message_id']
    bot_state['chat_id'] = chat_id
    c_log(f'CB: {d}')   # visible in Render logs — helps debug silently-failing buttons

    try:
        # ── No-op (label-only buttons) ────────────────────────────────
        if d == 'noop':
            pass

    # ── Navigation ────────────────────────────────────────────────
        elif d == 'menu_main':
            await edit_tg_msg(chat_id, msg_id, '🏠 القائمة الرئيسية:', get_main_keyboard())

        elif d == 'menu_filters':
            await edit_tg_msg(chat_id, msg_id, '🎛 <b>فلاتر وإعدادات التداول:</b>', get_filters_keyboard())

        elif d == 'menu_stoch_settings':
            await edit_tg_msg(chat_id, msg_id, '⚙️ <b>إعدادات الستوكاستيك:</b>', get_stoch_settings_keyboard())

        elif d == 'menu_tfs':
            await edit_tg_msg(chat_id, msg_id, '⏱ إدارة الفريمات الزمنية:', get_tf_keyboard())

        elif d == 'menu_settings':
            await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'menu_backtest':
            await edit_tg_msg(chat_id, msg_id, f'🔬 <b>باك تيست</b> — {_strat_label()}', get_backtest_keyboard())

        # ── Bot status ────────────────────────────────────────────────
        elif d == 'toggle_status':
            bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'
            await edit_tg_msg(chat_id, msg_id, '🏠 القائمة الرئيسية:', get_main_keyboard())

        elif d == 'cycle_strategy':
            modes = ['STOCH_NEW', 'STOCH_OLD']
            bot_state['strategy_mode'] = modes[(modes.index(bot_state['strategy_mode']) + 1) % len(modes)]
            await edit_tg_msg(chat_id, msg_id, f'🏠 القائمة الرئيسية:\n📌 الاستراتيجية: {_strat_label()}', get_main_keyboard())

        # ── Live connection ───────────────────────────────────────────
        elif d == 'toggle_live_conn':
            if not bot_state['live_connected']:
                await edit_tg_msg(chat_id, msg_id, '⏳ جاري الاتصال بالسيرفر...', get_main_keyboard())
                try:
                    api = MetaApi(METAAPI_TOKEN)
                    bot_state['account_obj']    = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                    bot_state['connection_obj'] = bot_state['account_obj'].get_rpc_connection()
                    await bot_state['connection_obj'].connect()
                    await bot_state['connection_obj'].wait_synchronized()
                    bot_state['live_connected'] = True
                    await edit_tg_msg(chat_id, msg_id, '✅ تم الاتصال بالسيرفر بنجاح!', get_main_keyboard())
                except Exception as e:
                    await edit_tg_msg(chat_id, msg_id, f'❌ فشل الاتصال:\n{e}', get_main_keyboard())
            else:
                bot_state['live_connected'] = False
                bot_state['connection_obj'] = None
                bot_state['account_obj']    = None
                await edit_tg_msg(chat_id, msg_id, '🔌 تم قطع الاتصال عن السيرفر.', get_main_keyboard())

        # ── MA Filters ────────────────────────────────────────────────
        elif d == 'set_filter_full':
            bot_state['filter_mode'] = 'FULL'
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d == 'set_filter_simple':
            bot_state['filter_mode'] = 'SIMPLE'
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d == 'set_filter_noma':
            bot_state['filter_mode'] = 'NO_MA'
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        # ── Stochastic level toggles (ALL THREE fixed) ────────────────
        elif d == 'toggle_stoch_deep':
            bot_state['use_stoch_deep'] = not bot_state['use_stoch_deep']
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d == 'toggle_stoch_mid':
            bot_state['use_stoch_mid'] = not bot_state['use_stoch_mid']
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d == 'toggle_stoch_shal':                             # ← bug fix
            bot_state['use_stoch_shal'] = not bot_state['use_stoch_shal']
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d == 'toggle_f_cons':
            bot_state['use_f_cons'] = not bot_state['use_f_cons']
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        # ── Stochastic K parameter ────────────────────────────────────
        elif d == 'inc_stoch_k':
            bot_state['stoch_k'] = min(bot_state['stoch_k'] + 1, 100)
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        elif d == 'dec_stoch_k':
            bot_state['stoch_k'] = max(bot_state['stoch_k'] - 1, 1)
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        # ── Stochastic Smooth parameter ───────────────────────────────
        elif d == 'inc_stoch_s':
            bot_state['stoch_smooth'] = min(bot_state['stoch_smooth'] + 1, 100)
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        elif d == 'dec_stoch_s':
            bot_state['stoch_smooth'] = max(bot_state['stoch_smooth'] - 1, 1)
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        # ── Stochastic D parameter ────────────────────────────────────
        elif d == 'inc_stoch_d':
            bot_state['stoch_d'] = min(bot_state['stoch_d'] + 1, 100)
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        elif d == 'dec_stoch_d':
            bot_state['stoch_d'] = max(bot_state['stoch_d'] - 1, 1)
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        # ── Stochastic presets ────────────────────────────────────────
        elif d == 'preset_5_5_5':
            bot_state['stoch_k'] = bot_state['stoch_smooth'] = bot_state['stoch_d'] = 5
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        elif d == 'preset_14_3_3':
            bot_state['stoch_k'] = 14; bot_state['stoch_smooth'] = 3; bot_state['stoch_d'] = 3
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        elif d == 'preset_10_3_3':
            bot_state['stoch_k'] = 10; bot_state['stoch_smooth'] = 3; bot_state['stoch_d'] = 3
            await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        # ── Time filters ──────────────────────────────────────────────
        elif d == 'toggle_time':
            bot_state['use_time_filter'] = not bot_state['use_time_filter']
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d == 'toggle_danger':
            bot_state['use_danger_filter'] = not bot_state['use_danger_filter']
            await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        # ── Timeframe toggles ─────────────────────────────────────────
        elif d.startswith('toggle_tf_'):
            tf = d.split('_')[2]
            if tf in bot_state['active_tfs']:
                bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            await edit_tg_msg(chat_id, msg_id, '⏱ إدارة الفريمات:', get_tf_keyboard())

        # ── Risk settings ─────────────────────────────────────────────
        elif d == 'toggle_be':
            bot_state['use_be'] = not bot_state['use_be']
            await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'toggle_atr':
            bot_state['use_atr'] = not bot_state['use_atr']
            await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'toggle_spread':
            bot_state['use_max_spread'] = not bot_state['use_max_spread']
            await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'inc_lot':
            bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2)
            await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'dec_lot':
            bot_state['lot_size'] = max(0.01, round(bot_state['lot_size'] - 0.01, 2))
            await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'view_tpsl':
            await edit_tg_msg(chat_id, msg_id, '🎯 <b>تعديل TP / SL لكل فريم:</b>', get_tpsl_overview_keyboard())

        # ── TP/SL per-TF editor ───────────────────────────────────────
        elif d.startswith('tpsl_edit_'):
            tf = d[len('tpsl_edit_'):]
            if tf in _TFS:
                await edit_tg_msg(chat_id, msg_id, f'✏️ <b>تعديل [{tf}]:</b>', get_tpsl_edit_keyboard(tf))

        # ── TP adjustments (±5 and ±10) ──────────────────────────────
        elif d.startswith('inc_tp5_') or d.startswith('inc_tp10_') \
          or d.startswith('dec_tp5_') or d.startswith('dec_tp10_'):
            # parse: action_typeSTEP_tf  e.g. inc_tp5_2m  dec_tp10_15m
            parts = d.split('_')          # ['inc','tp5','2m'] or ['dec','tp10','15m']
            direction = parts[0]          # 'inc' | 'dec'
            step_str  = parts[1]          # 'tp5' | 'tp10'
            tf        = '_'.join(parts[2:])   # handles '15m' correctly
            step = 5 if step_str == 'tp5' else 10
            if tf in _TFS:
                current = bot_state['tp_pips'][tf]
                new_val = current + step if direction == 'inc' else max(5, current - step)
                bot_state['tp_pips'][tf] = new_val
                await edit_tg_msg(chat_id, msg_id, f'✏️ <b>تعديل [{tf}]:</b>', get_tpsl_edit_keyboard(tf))

        # ── SL adjustments (±5 and ±10) ──────────────────────────────
        elif d.startswith('inc_sl5_') or d.startswith('inc_sl10_') \
          or d.startswith('dec_sl5_') or d.startswith('dec_sl10_'):
            parts     = d.split('_')
            direction = parts[0]
            step_str  = parts[1]          # 'sl5' | 'sl10'
            tf        = '_'.join(parts[2:])
            step = 5 if step_str == 'sl5' else 10
            if tf in _TFS:
                current = bot_state['sl_pips'][tf]
                new_val = current + step if direction == 'inc' else max(5, current - step)
                bot_state['sl_pips'][tf] = new_val
                await edit_tg_msg(chat_id, msg_id, f'✏️ <b>تعديل [{tf}]:</b>', get_tpsl_edit_keyboard(tf))

        # ── Reports ───────────────────────────────────────────────────
        elif d == 'report':
            lines = [f'📊 <b>حالة السوق الحية — {_strat_label()}</b>']
            for tf in bot_state['timeframes']:
                if bot_state['active_tfs'][tf]:
                    lines.append(f'[{tf}] {bot_state["market_data"][tf]}')
            await edit_tg_msg(chat_id, msg_id, '\n'.join(lines), get_main_keyboard())

        elif d == 'account':
            if not (bot_state['live_connected'] and bot_state['connection_obj']):
                await edit_tg_msg(chat_id, msg_id, '⚠️ غير متصل بالسيرفر.', get_main_keyboard())
            else:
                try:
                    info = await bot_state['connection_obj'].get_account_information()
                    pos  = await bot_state['connection_obj'].get_positions()
                    text = (
                        f'💳 <b>معلومات الحساب</b>\n'
                        f'الرصيد:           ${info.get("balance",    "N/A")}\n'
                        f'الإكويتي:         ${info.get("equity",     "N/A")}\n'
                        f'الهامش الحر:      ${info.get("freeMargin", "N/A")}\n'
                        f'الصفقات المفتوحة: {len(pos)}'
                    )
                    await edit_tg_msg(chat_id, msg_id, text, get_main_keyboard())
                except Exception as e:
                    await edit_tg_msg(chat_id, msg_id, f'❌ خطأ في جلب البيانات:\n{e}', get_main_keyboard())

        # ── Backtest triggers ─────────────────────────────────────────
            elif d.startswith('bto_adv_'):
            days = int(d.split('_')[2])
            asyncio.create_task(run_advanced_backtest(days=days))

            elif d.startswith('bto_'):
            days  = int(d.split('_')[1])
            start = datetime.now(timezone.utc) - timedelta(days=days)
            asyncio.create_task(run_oanda_backtest(start))

            elif d == 'close_all':
            if not (bot_state['live_connected'] and bot_state['connection_obj']):
                await send_tg_msg('⚠️ غير متصل بالسيرفر.')
            else:
                try:
                    positions = await bot_state['connection_obj'].get_positions()
                    if not positions:
                        await send_tg_msg('ℹ️ لا توجد صفقات مفتوحة حالياً.')
                    else:
                        for p in positions:
                            await bot_state['connection_obj'].close_position(p['id'])
                        await send_tg_msg(f'✅ تم إغلاق {len(positions)} صفقة.')
                    except Exception as e:
                        await send_tg_msg(f'❌ خطأ في الإغلاق: {e}')

            else:
                c_log(f'CB unhandled: {d!r}')

    except Exception as e:
        c_log(f'CB handler error [{d!r}]: {e}')
        try:
            await send_tg_msg(f'❌ خطأ داخلي: {e}')
        except Exception:
            pass

    finally:
        # ALWAYS answer the callback — prevents the Telegram spinner from
        # staying active and makes the button feel responsive even on error.
        await answer_callback(q['id'])


# ─────────────────────────────────────────────────────────────
# TELEGRAM LONG-POLLING LOOP
# Uses the shared session.  A dedicated per-poll timeout prevents
# the long-poll from blocking the session during backtest.
# ─────────────────────────────────────────────────────────────
async def telegram_polling_loop() -> None:
    c_log('✅ خدمة التلغرام جاهزة.')
    url        = f'https://api.telegram.org/bot{TG_TOKEN}/getUpdates'
    poll_to    = aiohttp.ClientTimeout(total=20, sock_read=15)   # independent timeout
    backoff    = 1   # seconds; doubles on repeated errors, resets on success

    while True:
        try:
            async with get_http().get(
                url,
                params={'offset': bot_state['last_update_id'] + 1, 'timeout': 10},
                timeout=poll_to,
            ) as resp:
                if resp.status == 200:
                    backoff = 1   # reset on success
                    for upd in (await resp.json()).get('result', []):
                        bot_state['last_update_id'] = upd['update_id']
                        asyncio.create_task(process_tg_update(upd))
                elif resp.status == 429:
                    retry_after = int(resp.headers.get('Retry-After', 5))
                    c_log(f'TG rate-limited, sleeping {retry_after}s')
                    await asyncio.sleep(retry_after)
                else:
                    c_log(f'TG polling status {resp.status}')
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            c_log(f'Polling error: {e}')
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ─────────────────────────────────────────────────────────────
# TASK SUPERVISOR
# Restarts any crashed task automatically so the bot never dies
# silently.
# ─────────────────────────────────────────────────────────────
async def supervised(coro_fn, *args, label: str = ''):
    """Run coro_fn(*args) forever, restarting on crash."""
    while True:
        try:
            await coro_fn(*args)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            c_log(f'💥 Task "{label or coro_fn.__name__}" crashed: {e} — restarting in 5s')
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# WEB SERVER  (keep-alive ping + uptime info)
# ─────────────────────────────────────────────────────────────
_start_time = datetime.now(timezone.utc)


async def handle_ping(request: web.Request) -> web.Response:
    uptime = str(datetime.now(timezone.utc) - _start_time).split('.')[0]
    bt     = '🔴 جاري' if bot_state['is_backtesting'] else '✅ خامل'
    live   = '🟢 متصل' if bot_state['live_connected'] else '🔴 غير متصل'
    return web.Response(
        text=f'Gold Scalper Bot v4 — OK\nUptime: {uptime}\nLive: {live}\nBacktest: {bt}',
        content_type='text/plain',
    )


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
async def main() -> None:
    # Initialise the shared HTTP session before any task starts
    get_http()

    app    = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port   = int(os.environ.get('PORT', 10000))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    c_log(f'🚀 Web server running on port {port}')

    tasks = [
        asyncio.create_task(supervised(timeframe_scanner, tf, label=f'scanner_{tf}'))
        for tf in bot_state['timeframes']
    ]
    tasks += [
        asyncio.create_task(supervised(telegram_polling_loop, label='polling')),
        asyncio.create_task(supervised(position_monitor,      label='position_monitor')),
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        # Close the shared HTTP session on exit
        if _http and not _http.closed:
            await _http.close()
        c_log('👋 Bot shut down.')


if __name__ == '__main__':
    asyncio.run(main())
