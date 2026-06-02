"""
Gold Scalper Bot — v5.5 (Bugfix: File Upload Parse_Mode & Fail-Safe)
Strategies : STOCH-NEW  |  STOCH-OLD  |  RSI-REVERSAL
"""

import asyncio
import aiohttp
import json
import os
import traceback
import zipfile
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
TG_TOKEN      = '8779425898:AAGJFaArYBeUGP7WfL24U-keKmqHLeqRRvg'
OANDA_API     = 'd05b25b3f1ce0c8fa105ffefa45efb01-a5c26f544a26a4f810f1809913a2795f'
OANDA_URL     = 'https://api-fxpractice.oanda.com/v3'

_TFS = ['1m', '2m', '3m', '5m', '15m']

# ─────────────────────────────────────────────────────────────
# PROGRESS BAR & CLOUD HELPERS
# ─────────────────────────────────────────────────────────────
def make_progress_bar(pct: int, length: int = 12) -> str:
    filled = int(length * (pct / 100))
    return '█' * filled + '░' * (length - filled)

async def upload_to_fileio(filepath: str) -> str:
    try:
        with open(filepath, 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=os.path.basename(filepath))
            async with get_http().post('https://file.io/?expires=1w', data=data) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    return res.get('link', '')
    except Exception as e:
        c_log(f'File.io Upload Error: {e}')
    return ''

# ─────────────────────────────────────────────────────────────
# GLOBAL SHARED HTTP SESSION
# ─────────────────────────────────────────────────────────────
_http: aiohttp.ClientSession | None = None

def get_http() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)
        timeout   = aiohttp.ClientTimeout(total=30, connect=10)
        _http     = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _http

def c_log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ─────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────
bot_state: dict = {
    'status':            'RUNNING',
    'symbol':            'XAUUSD@',
    'live_connected':    False,
    'timeframes':        _TFS,
    'active_tfs':        {'1m': False, '2m': True, '3m': True, '5m': False, '15m': False},
    'lot_size':          0.05,
    'pip_value':         0.1,
    'spread_pips':       2.2,
    'chat_id':           None,
    'last_update_id':    0,
    'tp_pips':           {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80},
    'sl_pips':           {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},
    'strategy_mode':     'STOCH_OLD',
    'filter_mode':       'NO_MA',
    'stoch_k':           5,
    'stoch_smooth':      5,
    'stoch_d':           5,
    'use_stoch_deep':    True,
    'use_stoch_mid':     True,
    'use_stoch_shal':    False,
    'rsi_period':        14,
    'use_rsi_18':        True,
    'use_rsi_25':        False,
    'use_rsi_77':        False,
    'use_rsi_83':        True,
    'use_f_cons':        False,
    'cons_count':        3,
    'use_time_filter':   False,
    'use_danger_filter': True,
    'use_be':            False,
    'use_atr':           False,
    'use_max_spread':    True,
    'max_spread_pips':   3.0,
    'atr_mult_tp':       1.5,
    'atr_mult_sl':       3.0,
    'tp_tolerance_pips': 5.0,
    'market_data':       {tf: '⏸ بانتظار الاتصال (Offline)' for tf in _TFS},
    'last_signal_time':  {tf: None for tf in _TFS},
    'connection_obj':    None,
    'account_obj':       None,
    'is_backtesting':    False,
}

# ─────────────────────────────────────────────────────────────
# INDICATOR ENGINE
# ─────────────────────────────────────────────────────────────
def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    
    df['ema15']  = _ema(df['close'], 15)
    df['ema50']  = _ema(df['close'], 50)
    df['ema150'] = _ema(df['close'], 150)

    k_period, smooth, d_period = bot_state['stoch_k'], bot_state['stoch_smooth'], bot_state['stoch_d']
    low_min  = df['low'].rolling(k_period).min()
    high_max = df['high'].rolling(k_period).max()
    denom    = (high_max - low_min).replace(0, 1e-10)
    df['K']  = (100.0 * (df['close'] - low_min) / denom).ewm(span=smooth, adjust=False).mean()
    df['D']  = df['K'].ewm(span=d_period, adjust=False).mean()

    rsi_p = bot_state['rsi_period']
    delta = df['close'].diff()
    up    = delta.clip(lower=0)
    down  = -1 * delta.clip(upper=0)
    ema_up   = up.ewm(com=rsi_p - 1, adjust=False).mean()
    ema_down = down.ewm(com=rsi_p - 1, adjust=False).mean()
    rs = ema_up / ema_down
    df['rsi'] = 100 - (100 / (1 + rs))

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
def get_rsi_signals(prev2_rsi: float, prev_rsi: float, curr_rsi: float) -> tuple:
    buy_sig, sell_sig, b_label, s_label = False, False, "", ""
    if prev2_rsi >= prev_rsi and curr_rsi > prev_rsi:
        if bot_state['use_rsi_18'] and prev_rsi <= 18: buy_sig, b_label = True, "RSI(18-REV)"
        elif bot_state['use_rsi_25'] and prev_rsi <= 25: buy_sig, b_label = True, "RSI(25-REV)"
    if prev2_rsi <= prev_rsi and curr_rsi < prev_rsi:
        if bot_state['use_rsi_83'] and prev_rsi >= 83: sell_sig, s_label = True, "RSI(83-REV)"
        elif bot_state['use_rsi_77'] and prev_rsi >= 77: sell_sig, s_label = True, "RSI(77-REV)"
    return buy_sig, sell_sig, b_label, s_label

def get_stoch_signals(prev_k: float, prev_d: float, curr_k: float, curr_d: float) -> tuple:
    mode = bot_state['strategy_mode']
    if mode == 'STOCH_NEW':
        buy_deep  = (prev_k <= 10)      and (curr_k > 10)  and bot_state['use_stoch_deep']
        buy_mid   = (10 < prev_k <= 15) and (curr_k > 15)  and bot_state['use_stoch_mid']
        buy_shal  = (15 < prev_k <= 20) and (curr_k > 20)  and bot_state['use_stoch_shal']
        sell_deep = (prev_k >= 90)      and (curr_k < 90)  and bot_state['use_stoch_deep']
        sell_mid  = (85 <= prev_k < 90) and (curr_k < 85)  and bot_state['use_stoch_mid']
        sell_shal = (80 <= prev_k < 85) and (curr_k < 80)  and bot_state['use_stoch_shal']
    else:
        k_cross_up   = (prev_k < prev_d) and (curr_k >= curr_d)
        k_cross_down = (prev_k > prev_d) and (curr_k <= curr_d)
        avg_k = (prev_k + curr_k) / 2.0
        buy_deep  = k_cross_up   and (avg_k <= 10)      and bot_state['use_stoch_deep']
        buy_mid   = k_cross_up   and (10 < avg_k <= 15) and bot_state['use_stoch_mid']
        buy_shal  = k_cross_up   and (15 < avg_k <= 20) and bot_state['use_stoch_shal']
        sell_deep = k_cross_down and (avg_k >= 90)      and bot_state['use_stoch_deep']
        sell_mid  = k_cross_down and (85 <= avg_k < 90) and bot_state['use_stoch_mid']
        sell_shal = k_cross_down and (80 <= avg_k < 85) and bot_state['use_stoch_shal']

    buy_sig  = buy_deep  or buy_mid  or buy_shal
    sell_sig = sell_deep or sell_mid or sell_shal
    b_label  = 'DEEP(10)' if buy_deep  else 'MID(15)'  if buy_mid  else 'SHAL(20)'
    s_label  = 'DEEP(90)' if sell_deep else 'MID(85)'  if sell_mid else 'SHAL(80)'
    return buy_sig, sell_sig, b_label, s_label

def compute_trend_ok(df: pd.DataFrame, i: int, curr: pd.Series) -> tuple:
    mode = bot_state['filter_mode']
    if mode == 'NO_MA': return True, True
    cons  = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema = s_ema = True
    for j in range(cons):
        idx = i - j
        if idx not in df.index: return False, False
        c = df.loc[idx]
        if not (c['ema50'] > c['ema150']): b_ema = False
        if not (c['ema150'] > c['ema50']): s_ema = False
    if mode == 'SIMPLE': return b_ema, s_ema
    ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
    ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
    return (b_ema and ma_buy), (s_ema and ma_sell)

def compute_trend_ok_live(df: pd.DataFrame, curr: pd.Series) -> tuple:
    mode = bot_state['filter_mode']
    if mode == 'NO_MA': return True, True
    cons  = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema = s_ema = True
    for j in range(cons):
        c = df.iloc[(-2) - j]
        if not (c['ema50'] > c['ema150']): b_ema = False
        if not (c['ema150'] > c['ema50']): s_ema = False
    if mode == 'SIMPLE': return b_ema, s_ema
    ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
    ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
    return (b_ema and ma_buy), (s_ema and ma_sell)

def is_danger_time(dt_utc: datetime) -> bool:
    return 19 <= (dt_utc.hour + 3) % 24 <= 21

def _get_signal_for_bar(df: pd.DataFrame, i: int, curr: pd.Series, prev: pd.Series, prev2: pd.Series) -> tuple:
    trend_buy, trend_sell = compute_trend_ok(df, i, curr)
    if bot_state['strategy_mode'] == 'RSI_REV':
        raw_buy, raw_sell, b_lbl, s_lbl = get_rsi_signals(prev2['rsi'], prev['rsi'], curr['rsi'])
    else:
        raw_buy, raw_sell, b_lbl, s_lbl = get_stoch_signals(prev['K'], prev['D'], curr['K'], curr['D'])
        
    buy_sig  = raw_buy  and trend_buy
    sell_sig = raw_sell and trend_sell
    label    = b_lbl if buy_sig else s_lbl
    return buy_sig, sell_sig, label

# ─────────────────────────────────────────────────────────────
# OANDA REST HELPER
# ─────────────────────────────────────────────────────────────
_TF_MAP = {'s5': 'S5', '1m': 'M1', '2m': 'M2', '3m': 'M3', '5m': 'M5', '15m': 'M15', '1h': 'H1'}
_oanda_sem: asyncio.Semaphore | None = None

def _get_oanda_sem() -> asyncio.Semaphore:
    global _oanda_sem
    if _oanda_sem is None: _oanda_sem = asyncio.Semaphore(3)
    return _oanda_sem

async def fetch_oanda_candles(instrument: str, granularity: str, count: int = 5000, end_time: datetime = None) -> list:
    url     = f'{OANDA_URL}/instruments/{instrument}/candles'
    headers = {'Authorization': f'Bearer {OANDA_API}'}
    params  = {'granularity': _TF_MAP.get(granularity, 'M5'), 'count': count, 'price': 'M'}
    if end_time:
        params['to'] = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    async with _get_oanda_sem():
        for attempt in range(3):
            try:
                async with get_http().get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
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
                    elif resp.status == 429: await asyncio.sleep(2 ** attempt)
                    else: await asyncio.sleep(1)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(1)
    return []

# ─────────────────────────────────────────────────────────────
# TELEGRAM HELPERS (BUGFIX APPLIED HERE)
# ─────────────────────────────────────────────────────────────
async def send_tg_msg(text: str, reply_markup: dict = None) -> int | None:
    if not bot_state['chat_id']: return None
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    try:
        async with get_http().post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', json=payload) as resp:
            data = await resp.json()
            if data.get('ok'):
                return data['result']['message_id']
    except Exception as e:
        c_log(f'TG send error: {e}')
    return None

async def edit_tg_msg(chat_id: int, message_id: int, text: str, reply_markup: dict = None) -> None:
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    try:
        async with get_http().post(f'https://api.telegram.org/bot{TG_TOKEN}/editMessageText', json=payload) as resp:
            pass
    except Exception as e:
        c_log(f'TG edit error: {e}')

async def answer_callback(cbq_id: str, text: str = None, show_alert: bool = False) -> None:
    payload = {'callback_query_id': cbq_id}
    if text:
        payload['text'] = text
        if show_alert: payload['show_alert'] = True
    try:
        async with get_http().post(f'https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery', json=payload) as _:
            pass
    except Exception:
        pass

async def send_tg_document(file_path: str, caption: str) -> bool:
    if not bot_state['chat_id']: return False
    try:
        with open(file_path, 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('chat_id', str(bot_state['chat_id']))
            data.add_field('document', f, filename=os.path.basename(file_path))
            data.add_field('caption', caption)
            # الحل الجذري للانهيار الصامت: تمرير التنسيق كحقل منفصل وليس كخيار مدمج
            data.add_field('parse_mode', 'HTML')
            
            timeout = aiohttp.ClientTimeout(total=60)
            async with get_http().post(f'https://api.telegram.org/bot{TG_TOKEN}/sendDocument', data=data, timeout=timeout) as resp:
                if resp.status == 200: return True
                else:
                    c_log(f'TG doc upload failed: {resp.status} - {await resp.text()}')
                    return False
    except Exception as e:
        c_log(f'TG doc error: {e}')
        return False

# ─────────────────────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────────────────────
def _strat_label() -> str:
    labels = {'STOCH_NEW': '📈 STOCH-NEW', 'STOCH_OLD': '📉 STOCH-OLD', 'RSI_REV': '📊 RSI-REVERSAL'}
    return labels[bot_state['strategy_mode']]

def get_main_keyboard() -> dict:
    live = '🟢 متصل' if bot_state['live_connected'] else '🔴 غير متصل'
    st   = '🟢 RUN' if bot_state['status'] == 'RUNNING' else '🔴 PAUSE'
    return {'inline_keyboard': [
        [{'text': f'🔌 سيرفر التداول الحي: {live}', 'callback_data': 'toggle_live_conn'}],
        [{'text': f'حالة البوت: {st}', 'callback_data': 'toggle_status'}, {'text': f'الاستراتيجية: {_strat_label()}', 'callback_data': 'cycle_strategy'}],
        [{'text': '🎛 فلاتر وإعدادات', 'callback_data': 'menu_filters'}, {'text': '⏱ الفريمات', 'callback_data': 'menu_tfs'}],
        [{'text': '📊 تقرير السوق', 'callback_data': 'report'}, {'text': '💳 الحساب', 'callback_data': 'account'}],
        [{'text': '🛠 إعدادات المخاطرة', 'callback_data': 'menu_settings'}, {'text': '🔬 باك تيست', 'callback_data': 'menu_backtest'}],
        [{'text': '🛑 إغلاق جميع الصفقات', 'callback_data': 'close_all'}],
    ]}

def get_filters_keyboard() -> dict:
    fm = bot_state['filter_mode']
    fi = {k: '✅' if fm == k else '⬜' for k in ('FULL', 'SIMPLE', 'NO_MA')}
    ci = '🟢' if bot_state['use_f_cons'] else '🔴'
    t_i = '🟢' if bot_state['use_time_filter'] else '🔴'
    d_i = '🟢' if bot_state['use_danger_filter'] else '🔴'
    return {'inline_keyboard': [
        [{'text': '━━ فلتر الترند (مطبق على كل الاستراتيجيات) ━━', 'callback_data': 'noop'}],
        [{'text': f"{fi['FULL']} FULL: ema15 + ema50 + ema150", 'callback_data': 'set_filter_full'}],
        [{'text': f"{fi['SIMPLE']} SIMPLE: ema50 + ema150", 'callback_data': 'set_filter_simple'}],
        [{'text': f"{fi['NO_MA']} NO MA: بدون فلاتر ترند", 'callback_data': 'set_filter_noma'}],
        [{'text': f'ثبات الترند ({bot_state["cons_count"]} شموع): {ci}', 'callback_data': 'toggle_f_cons'}],
        [{'text': '━━ إعدادات الاستراتيجيات والمؤشرات ━━', 'callback_data': 'noop'}],
        [{'text': f'⚙️ Stoch({bot_state["stoch_k"]}, {bot_state["stoch_smooth"]}, {bot_state["stoch_d"]}) — المستويات', 'callback_data': 'menu_stoch_settings'}],
        [{'text': f'📈 RSI({bot_state["rsi_period"]}) — المستويات والانعكاس', 'callback_data': 'menu_rsi_settings'}],
        [{'text': '━━ فلاتر الوقت ━━', 'callback_data': 'noop'}],
        [{'text': f'Time Filter 08-17 UTC: {t_i}', 'callback_data': 'toggle_time'}, {'text': f'حظر 19-22 دمشق: {d_i}', 'callback_data': 'toggle_danger'}],
        [{'text': '🔙 القائمة الرئيسية', 'callback_data': 'menu_main'}],
    ]}

def get_stoch_settings_keyboard() -> dict:
    k, s, d = bot_state['stoch_k'], bot_state['stoch_smooth'], bot_state['stoch_d']
    dp = '🟢' if bot_state['use_stoch_deep'] else '🔴'
    md = '🟢' if bot_state['use_stoch_mid'] else '🔴'
    sh = '🟢' if bot_state['use_stoch_shal'] else '🔴'
    return {'inline_keyboard': [
        [{'text': f'الإعداد الحالي: Stoch({k}, {s}, {d})', 'callback_data': 'noop'}],
        [{'text': '━━ مستويات الستوكاستيك ━━', 'callback_data': 'noop'}],
        [{'text': f'DEEP 10/90: {dp}', 'callback_data': 'toggle_stoch_deep'}, {'text': f'MID  15/85: {md}', 'callback_data': 'toggle_stoch_mid'}, {'text': f'SHAL 20/80: {sh}', 'callback_data': 'toggle_stoch_shal'}],
        [{'text': '━━ K Period ━━', 'callback_data': 'noop'}],
        [{'text': '➖', 'callback_data': 'dec_stoch_k'}, {'text': f'K = {k}', 'callback_data': 'noop'}, {'text': '➕', 'callback_data': 'inc_stoch_k'}],
        [{'text': '━━ Smooth ━━', 'callback_data': 'noop'}],
        [{'text': '➖', 'callback_data': 'dec_stoch_s'}, {'text': f'S = {s}', 'callback_data': 'noop'}, {'text': '➕', 'callback_data': 'inc_stoch_s'}],
        [{'text': '━━ D Period ━━', 'callback_data': 'noop'}],
        [{'text': '➖', 'callback_data': 'dec_stoch_d'}, {'text': f'D = {d}', 'callback_data': 'noop'}, {'text': '➕', 'callback_data': 'inc_stoch_d'}],
        [{'text': '🔙 رجوع للفلاتر', 'callback_data': 'menu_filters'}],
    ]}

def get_rsi_settings_keyboard() -> dict:
    r18 = '🟢' if bot_state['use_rsi_18'] else '🔴'
    r25 = '🟢' if bot_state['use_rsi_25'] else '🔴'
    r77 = '🟢' if bot_state['use_rsi_77'] else '🔴'
    r83 = '🟢' if bot_state['use_rsi_83'] else '🔴'
    p   = bot_state['rsi_period']
    return {'inline_keyboard': [
        [{'text': f'⚙️ إعدادات RSI — Period: {p}', 'callback_data': 'noop'}],
        [{'text': 'المنطق: يلامس المستوى ثم ينعكس للجهة المعاكسة', 'callback_data': 'noop'}],
        [{'text': '━━ مستويات الشراء (دعوم) ━━', 'callback_data': 'noop'}],
        [
            {'text': f'مستوى 18: {r18}', 'callback_data': 'toggle_rsi_18'},
            {'text': f'مستوى 25: {r25}', 'callback_data': 'toggle_rsi_25'},
        ],
        [{'text': '━━ مستويات البيع (مقاومات) ━━', 'callback_data': 'noop'}],
        [
            {'text': f'مستوى 77: {r77}', 'callback_data': 'toggle_rsi_77'},
            {'text': f'مستوى 83: {r83}', 'callback_data': 'toggle_rsi_83'},
        ],
        [{'text': '🔙 رجوع للفلاتر', 'callback_data': 'menu_filters'}],
    ]}

def get_tf_keyboard() -> dict:
    rows, row = [], []
    for tf in bot_state['timeframes']:
        icon = '🟢' if bot_state['active_tfs'][tf] else '🔴'
        row.append({'text': f'{tf}: {icon}', 'callback_data': f'toggle_tf_{tf}'})
        if len(row) == 2: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([{'text': '🔙 رجوع', 'callback_data': 'menu_main'}])
    return {'inline_keyboard': rows}

def get_settings_keyboard() -> dict:
    be_i  = '🟢' if bot_state['use_be'] else '🔴'
    atr_i = '🟢' if bot_state['use_atr'] else '🔴'
    spr_i = '🟢' if bot_state['use_max_spread'] else '🔴'
    return {'inline_keyboard': [
        [{'text': f'تأمين الدخول (BE 20p): {be_i}', 'callback_data': 'toggle_be'}],
        [{'text': f'أهداف ATR: {atr_i}', 'callback_data': 'toggle_atr'}],
        [{'text': f'حماية السبريد ≤{bot_state["max_spread_pips"]}p: {spr_i}', 'callback_data': 'toggle_spread'}],
        [{'text': f'حجم اللوت: {bot_state["lot_size"]:.2f}', 'callback_data': 'noop'}],
        [{'text': '➕ Lot', 'callback_data': 'inc_lot'}, {'text': '➖ Lot', 'callback_data': 'dec_lot'}],
        [{'text': '🎯 تعديل TP / SL لكل فريم', 'callback_data': 'view_tpsl'}],
        [{'text': '🔙 رجوع', 'callback_data': 'menu_main'}],
    ]}

def get_tpsl_overview_keyboard() -> dict:
    rows = [[{'text': '━━ اضغط على فريم لتعديله ━━', 'callback_data': 'noop'}]]
    for tf in bot_state['timeframes']:
        icon = '🟢' if bot_state['active_tfs'][tf] else '🔴'
        tp, sl = bot_state['tp_pips'][tf], bot_state['sl_pips'][tf]
        rows.append([{'text': f'{icon} [{tf}]  TP:{tp}p  SL:{sl}p', 'callback_data': 'noop'}, {'text': '✏️ تعديل', 'callback_data': f'tpsl_edit_{tf}'}])
    rows.append([{'text': '📝 نصياً: /set 1m sl 75', 'callback_data': 'noop'}])
    rows.append([{'text': '🔙 رجوع للمخاطرة', 'callback_data': 'menu_settings'}])
    return {'inline_keyboard': rows}

def get_tpsl_edit_keyboard(tf: str) -> dict:
    tp, sl = bot_state['tp_pips'][tf], bot_state['sl_pips'][tf]
    rr = round(tp / sl, 2) if sl else '∞'
    return {'inline_keyboard': [
        [{'text': f'[{tf}]  TP: {tp}p  |  SL: {sl}p  |  R:R 1:{rr}', 'callback_data': 'noop'}],
        [{'text': '🎯 Take Profit', 'callback_data': 'noop'}],
        [{'text': f'➖10  ({max(5, tp-10)})', 'callback_data': f'dec_tp10_{tf}'}, {'text': f'TP={tp}p', 'callback_data': 'noop'}, {'text': f'➕10  ({tp+10})', 'callback_data': f'inc_tp10_{tf}'}],
        [{'text': f'➖5  ({max(5, tp-5)})', 'callback_data': f'dec_tp5_{tf}'}, {'text': '─', 'callback_data': 'noop'}, {'text': f'➕5  ({tp+5})', 'callback_data': f'inc_tp5_{tf}'}],
        [{'text': '🛑 Stop Loss', 'callback_data': 'noop'}],
        [{'text': f'➖10  ({max(5, sl-10)})', 'callback_data': f'dec_sl10_{tf}'}, {'text': f'SL={sl}p', 'callback_data': 'noop'}, {'text': f'➕10  ({sl+10})', 'callback_data': f'inc_sl10_{tf}'}],
        [{'text': f'➖5  ({max(5, sl-5)})', 'callback_data': f'dec_sl5_{tf}'}, {'text': '─', 'callback_data': 'noop'}, {'text': f'➕5  ({sl+5})', 'callback_data': f'inc_sl5_{tf}'}],
        [{'text': '🔙 رجوع للقائمة', 'callback_data': 'view_tpsl'}],
    ]}

def get_backtest_keyboard() -> dict:
    return {'inline_keyboard': [
        [{'text': '📊 1 يوم', 'callback_data': 'bto_1'}, {'text': '📊 3 أيام', 'callback_data': 'bto_3'}, {'text': '📊 7 أيام', 'callback_data': 'bto_7'}],
        [{'text': '🔬 Advanced — 7 أيام', 'callback_data': 'bto_adv_7'}],
        [{'text': '🔙 رجوع', 'callback_data': 'menu_main'}],
    ]}

# ─────────────────────────────────────────────────────────────
# BACKTEST — SHARED HELPERS
# ─────────────────────────────────────────────────────────────
def _build_trade_row(tf: str, is_buy: bool, label: str, entry_t: datetime, exit_t: datetime, act_ent: float, tp_p: float, sl_p: float, outcome: str, p_usd: float) -> dict:
    pv = bot_state['pip_value']
    return {
        'Timeframe':   tf,
        'Type':        ('BUY' if is_buy else 'SELL') + f' [{label}]',
        'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
        'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
        'Entry Price': round(act_ent, 2),
        'TP':          tp_p,
        'SL':          sl_p,
        'Pips':        round(abs(act_ent - (tp_p if outcome == 'WIN' else sl_p)) / pv, 1) if outcome in ('WIN', 'LOSS') else 0,
        'Outcome':     outcome,
        'Profit ($)':  p_usd,
    }

def _simulate_trade_in_memory(is_buy: bool, act_ent: float, tp_p: float, sl_p: float, eff_tp: float, entry_t: datetime, minute_candles: list) -> tuple:
    pv, be_act = bot_state['pip_value'], False
    be_tgt  = act_ent + (1 if is_buy else -1) * 20 * pv
    max_ext = entry_t + timedelta(hours=72)
    outcome = 'EXPIRED'
    exit_t  = max_ext

    for vc in minute_candles:
        t = vc['time']
        if t < entry_t: continue
        if t > max_ext: break
        if is_buy:
            if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                sl_p, be_act = act_ent, True
            if vc['low'] <= sl_p:
                outcome, exit_t = ('BREAK-EVEN' if be_act else 'LOSS'), t; break
            if vc['high'] >= eff_tp:
                outcome, exit_t = 'WIN', t; break
        else:
            if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                sl_p, be_act = act_ent, True
            if vc['high'] >= sl_p:
                outcome, exit_t = ('BREAK-EVEN' if be_act else 'LOSS'), t; break
            if vc['low'] <= eff_tp:
                outcome, exit_t = 'WIN', t; break
    return outcome, exit_t, sl_p

def _calc_pnl(outcome: str, act_ent: float, tp_p: float, sl_p: float) -> float:
    if outcome == 'BREAK-EVEN': return 0.0
    if outcome in ('WIN', 'LOSS'):
        exit_p = tp_p if outcome == 'WIN' else sl_p
        raw = abs(act_ent - exit_p) * 100 * bot_state['lot_size']
        return round(raw, 2) * (1 if outcome == 'WIN' else -1)
    return 0.0

def _entry_params(curr: pd.Series, is_buy: bool, tf: str) -> tuple:
    m       = 1 if is_buy else -1
    act_ent = curr['open'] + m * bot_state['spread_pips'] * bot_state['pip_value']
    tp_dist = curr['atr'] * bot_state['atr_mult_tp'] if bot_state['use_atr'] else bot_state['tp_pips'][tf] * bot_state['pip_value']
    sl_dist = curr['atr'] * bot_state['atr_mult_sl'] if bot_state['use_atr'] else bot_state['sl_pips'][tf] * bot_state['pip_value']
    tp_p    = round(act_ent + m * tp_dist, 2)
    sl_p    = round(act_ent - m * sl_dist, 2)
    tol     = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
    eff_tp  = (tp_p - tol) if is_buy else (tp_p + tol)
    return act_ent, tp_p, sl_p, eff_tp

# ─────────────────────────────────────────────────────────────
# STANDARD BACKTEST
# ─────────────────────────────────────────────────────────────
async def run_oanda_backtest(start_dt: datetime) -> None:
    desc  = f"{bot_state['strategy_mode']} / {bot_state['filter_mode']}"
    fname = f"BT_{datetime.now().strftime('%H%M%S')}.xlsx"
    
    status_text = f'⏳ <b>بدء الباك تيست</b>\nمن: {start_dt.strftime("%Y-%m-%d")}\nالاستراتيجية: {desc}'
    msg_id = await send_tg_msg(status_text)

    async def update_status(extra: str):
        if msg_id: await edit_tg_msg(bot_state['chat_id'], msg_id, f'{status_text}\n\n{extra}')
        else: await send_tg_msg(extra)

    trade_logs = []
    total_prof = peak_equity = max_dd = total_win = total_loss = 0.0
    win_count = loss_count = be_count = 0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            
            await update_status(f'🔄 <b>[{tf}]</b>: جاري جلب الشموع الأساسية للمحاكاة...')
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300: 
                await asyncio.sleep(2); continue

            df = calculate_indicators(pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(10, bot_state['cons_count'])

            tf_end    = datetime.now(timezone.utc)
            total_min = int((tf_end - start_dt).total_seconds() / 60) + 72 * 60
            
            await update_status(f'📥 <b>[{tf}]</b>: جاري جلب شموع دقيقة (1m)...')
            m1_raw, current_end, fetched = [], tf_end, 0
            while fetched < total_min:
                chunk = min(total_min - fetched, 5000)
                cndls = await fetch_oanda_candles('XAU_USD', '1m', chunk, current_end)
                if not cndls: break
                m1_raw = cndls + m1_raw
                new_end = cndls[0]['time']
                if new_end >= current_end: break 
                current_end = new_end
                fetched += len(cndls)
                if len(cndls) < chunk: break
                await asyncio.sleep(0.5)

            minute_candles = sorted(m1_raw, key=lambda x: x['time'])
            valid_indices = df[df['time'] >= start_dt].index
            total_candles = len(valid_indices)
            if total_candles == 0: continue
            
            step = max(1, total_candles // 10)
            for idx, i in enumerate(valid_indices):
                if i < safe_start: continue
                if idx % 50 == 0: await asyncio.sleep(0)

                if idx % step == 0 or idx == total_candles - 1:
                    pct = int((idx / total_candles) * 100)
                    bar = make_progress_bar(pct)
                    await update_status(f'⚙️ <b>[{tf}]</b>: جاري فحص الإشارات...\n<code>{bar} {pct}%</code>\n(شمعة {idx}/{total_candles})')

                curr, prev, prev2 = df.loc[i], df.loc[i - 1], df.loc[i - 2]
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                buy_sig, sell_sig, label = _get_signal_for_bar(df, i, curr, prev, prev2)
                if not (buy_sig or sell_sig) or i + 1 >= len(df): continue

                next_c = df.loc[i + 1]
                is_buy = bool(buy_sig)
                signal_bar = next_c.copy()
                signal_bar['atr'] = curr['atr']
                act_ent, tp_p, sl_p, eff_tp = _entry_params(signal_bar, is_buy, tf)

                outcome, exit_t, sl_p = _simulate_trade_in_memory(is_buy, act_ent, tp_p, sl_p, eff_tp, next_c['time'], minute_candles)
                p_usd = _calc_pnl(outcome, act_ent, tp_p, sl_p)

                if outcome == 'BREAK-EVEN': be_count += 1
                elif outcome == 'WIN':      total_win += p_usd; win_count += 1
                elif outcome == 'LOSS':     total_loss += p_usd; loss_count += 1

                total_prof += p_usd
                peak_equity = max(peak_equity, total_prof)
                max_dd      = max(max_dd, peak_equity - total_prof)
                trade_logs.append(_build_trade_row(tf, is_buy, label, next_c['time'], exit_t, act_ent, tp_p, sl_p, outcome, p_usd))

        if not trade_logs:
            await update_status('⚠️ لم يتم العثور على أي صفقات خلال هذه الفترة.')
            return

        await update_status('✅ <b>اكتمل الفحص!</b>\nجاري تجهيز الخيارات (1: CSV، 2: ZIP، 4: Cloud)...')
        total_trades = win_count + loss_count
        win_rate     = round(win_count / total_trades * 100, 1) if total_trades else 0
        summary = {
            'البند': ['✅ الربح الكلي', '❌ الخسارة الكلية', '💰 المحصلة', '🎯 نسبة الفوز', '📉 أقصى DD', '🔄 بريك إيفن', '📌 الاستراتيجية'],
            'القيمة': [f'{win_count} | +${round(total_win, 2)}', f'{loss_count} | -${abs(round(total_loss, 2))}', f'${round(total_prof, 2)}', f'{win_rate}% ({total_trades} صفقة)', f'${round(max_dd, 2)}', str(be_count), desc],
        }

        df_exec = pd.DataFrame(trade_logs)
        
        # إنشاء الـ CSV
        csv_fname = fname.replace('.xlsx', '.csv')
        df_exec.to_csv(csv_fname, index=False)

        # إنشاء الـ ZIP مع Excel
        zip_fname = fname.replace('.xlsx', '.zip')
        try:
            with pd.ExcelWriter(fname, engine='openpyxl') as writer:
                df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
                pd.DataFrame(summary).to_excel(writer, sheet_name='الملخص', index=False)
                _style_sheet(writer.sheets['الصفقات'])
            
            with zipfile.ZipFile(zip_fname, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(fname, os.path.basename(fname))
        except Exception as excel_err:
            await update_status(f'❌ فشل في بناء وتجهيز الملفات: {excel_err}')
            return

        await update_status('☁️ <b>جاري الرفع إلى السحابة لتوفير رابط مباشر...</b>')
        cloud_link = await upload_to_fileio(zip_fname)

        await update_status('📤 <b>جاري إرسال الملفات إلى تيليجرام...</b>')
        
        msg_text = f'📊 <b>الباك تيست</b> | {desc}\n✅ +${round(total_win, 2)} | ❌ -${abs(round(total_loss, 2))}\n💰 ${round(total_prof, 2)} | 🎯 {win_rate}%\n'
        if cloud_link:
            msg_text += f'\n🔗 <b>رابط التحميل السحابي (يحذف بعد أسبوع):</b>\n{cloud_link}'

        # خطة الطوارئ (Fail-Safe) - تأكد من الإرسال
        s1 = await send_tg_document(csv_fname, f'📄 <b>[خيار 1] ملف CSV خفيف</b>\n{msg_text}')
        s2 = await send_tg_document(zip_fname, f'🗜️ <b>[خيار 2] ملف Excel مضغوط</b>\n{msg_text}')
        
        if not s1 and not s2:
            # إذا فشل إرسال الملفات، نرسل النتائج كرسالة نصية مع الرابط السحابي
            await update_status(f'⚠️ <b>فشل رفع الملفات لتيليجرام!</b> (حجم كبير أو مشكلة اتصال)\nلكن لا تقلق، إليك التقرير والرابط:\n\n{msg_text}')
        else:
            await update_status('🎉 <b>تم الانتهاء بنجاح! جرب الملفات وأخبرني أي خيار أثبت كفاءته أكثر.</b>')
            
        for f in [fname, csv_fname, zip_fname]:
            if os.path.exists(f): os.remove(f)

    except Exception as e:
        err_msg = traceback.format_exc()
        c_log(f'Backtest Error: {err_msg}')
        await update_status(f'❌ <b>حدث خطأ غير متوقع:</b>\n<code>{e}</code>')
    finally:
        bot_state['is_backtesting'] = False

# ─────────────────────────────────────────────────────────────
# ADVANCED BACKTEST
# ─────────────────────────────────────────────────────────────
async def run_advanced_backtest(days: int = 7) -> None:
    desc     = f"{bot_state['strategy_mode']} / {bot_state['filter_mode']}"
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    fname    = f"ADV_{datetime.now().strftime('%H%M%S')}.xlsx"
    
    status_text = f'⏳ <b>Advanced Backtest</b>\nمن: {start_dt.strftime("%Y-%m-%d")}\nالاستراتيجية: {desc}'
    msg_id = await send_tg_msg(status_text)

    async def update_status(extra: str):
        if msg_id: await edit_tg_msg(bot_state['chat_id'], msg_id, f'{status_text}\n\n{extra}')
        else: await send_tg_msg(extra)

    trade_logs = []
    total_prof = peak_equity = max_dd = total_win = total_loss = 0.0
    win_count = loss_count = be_count = 0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            
            await update_status(f'🔄 <b>[{tf}]</b>: جاري جلب الشموع الأساسية للمحاكاة...')
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 300: 
                await asyncio.sleep(2); continue

            df = calculate_indicators(pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(10, bot_state['cons_count'])

            tf_end    = datetime.now(timezone.utc)
            total_min = int((tf_end - start_dt).total_seconds() / 60) + 72 * 60
            
            await update_status(f'📥 <b>[{tf}]</b>: جاري جلب شموع دقيقة (1m)...')
            m1_raw, current_end, fetched = [], tf_end, 0
            while fetched < total_min:
                chunk = min(total_min - fetched, 5000)
                cndls = await fetch_oanda_candles('XAU_USD', '1m', chunk, current_end)
                if not cndls: break
                m1_raw = cndls + m1_raw
                new_end = cndls[0]['time']
                if new_end >= current_end: break 
                current_end = new_end
                fetched += len(cndls)
                if len(cndls) < chunk: break
                await asyncio.sleep(0.5)

            minute_candles = sorted(m1_raw, key=lambda x: x['time'])
            valid_indices = df[df['time'] >= start_dt].index
            total_candles = len(valid_indices)
            if total_candles == 0: continue
            
            step = max(1, total_candles // 10)
            for idx, i in enumerate(valid_indices):
                if i < safe_start: continue
                if idx % 50 == 0: await asyncio.sleep(0)

                if idx % step == 0 or idx == total_candles - 1:
                    pct = int((idx / total_candles) * 100)
                    bar = make_progress_bar(pct)
                    await update_status(f'⚙️ <b>[{tf}]</b>: جاري فحص الإشارات...\n<code>{bar} {pct}%</code>\n(شمعة {idx}/{total_candles})')

                curr, prev, prev2 = df.loc[i], df.loc[i - 1], df.loc[i - 2]
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                buy_sig, sell_sig, label = _get_signal_for_bar(df, i, curr, prev, prev2)
                if not (buy_sig or sell_sig) or i + 1 >= len(df): continue

                next_c = df.loc[i + 1]
                is_buy = bool(buy_sig)
                signal_bar = next_c.copy()
                signal_bar['atr'] = curr['atr']
                act_ent, tp_p, sl_p, eff_tp = _entry_params(signal_bar, is_buy, tf)

                outcome, exit_t, sl_p = _simulate_trade_in_memory(is_buy, act_ent, tp_p, sl_p, eff_tp, next_c['time'], minute_candles)
                p_usd = _calc_pnl(outcome, act_ent, tp_p, sl_p)

                if outcome == 'WIN':        total_win += p_usd; win_count += 1
                elif outcome == 'LOSS':     total_loss += p_usd; loss_count += 1
                elif outcome == 'BREAK-EVEN': be_count += 1

                total_prof += p_usd
                peak_equity = max(peak_equity, total_prof)
                max_dd      = max(max_dd, peak_equity - total_prof)
                trade_logs.append(_build_trade_row(tf, is_buy, label, next_c['time'], exit_t, act_ent, tp_p, sl_p, outcome, p_usd))

        if not trade_logs:
            await update_status('⚠️ لم يتم العثور على صفقات خلال هذه الفترة.')
            return

        await update_status('✅ <b>اكتمل الفحص!</b>\nجاري تجهيز الخيارات (1: CSV، 2: ZIP، 4: Cloud)...')
        total_trades = win_count + loss_count
        win_rate     = round(win_count / total_trades * 100, 1) if total_trades else 0
        pf           = round(total_win / abs(total_loss), 2) if total_loss else 999.0

        df_exec = pd.DataFrame(trade_logs)
        
        csv_fname = fname.replace('.xlsx', '.csv')
        df_exec.to_csv(csv_fname, index=False)

        zip_fname = fname.replace('.xlsx', '.zip')
        try:
            with pd.ExcelWriter(fname, engine='openpyxl') as writer:
                df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
                _style_sheet(writer.sheets['الصفقات'])
            
            with zipfile.ZipFile(zip_fname, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(fname, os.path.basename(fname))
        except Exception as excel_err:
            await update_status(f'❌ فشل في بناء الملفات: {excel_err}')
            return

        await update_status('☁️ <b>جاري الرفع إلى السحابة لتوفير رابط مباشر...</b>')
        cloud_link = await upload_to_fileio(zip_fname)

        await update_status('📤 <b>جاري إرسال التقرير النهائي إلى تيليجرام...</b>')
        
        msg_text = (
            f'📊 <b>Advanced Report — {days} يوم</b>\n'
            f'صافي: ${round(total_prof, 2)} | ربح: +${round(total_win, 2)} | خسارة: -${abs(round(total_loss, 2))}\n'
            f'صفقات: {total_trades} | فوز: {win_rate}% | PF: {pf}\n'
            f'📉 Drawdown: ${round(max_dd, 2)}'
        )
        if cloud_link:
            msg_text += f'\n\n🔗 <b>رابط التحميل السحابي (يحذف بعد أسبوع):</b>\n{cloud_link}'

        s1 = await send_tg_document(csv_fname, f'📄 <b>[خيار 1] ملف CSV خفيف</b>\n{msg_text}')
        s2 = await send_tg_document(zip_fname, f'🗜️ <b>[خيار 2] ملف Excel مضغوط</b>\n{msg_text}')
        
        if not s1 and not s2:
            await update_status(f'⚠️ <b>فشل رفع الملفات لتيليجرام!</b> (حجم كبير أو مشكلة اتصال)\nلكن لا تقلق، إليك التقرير والرابط:\n\n{msg_text}')
        else:
            await update_status('🎉 <b>تم الانتهاء بنجاح! جرب الملفات لاختيار الأنسب.</b>')
            
        for f in [fname, csv_fname, zip_fname]:
            if os.path.exists(f): os.remove(f)

    except Exception as e:
        err_msg = traceback.format_exc()
        c_log(f'Advanced Backtest Error: {err_msg}')
        await update_status(f'❌ <b>حدث خطأ غير متوقع:</b>\n<code>{e}</code>')
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

    for cell in ws[1]: cell.fill = header; cell.font = Font(color='FFFFFF', bold=True)
    outcome_col = next((i + 1 for i, c in enumerate(ws[1]) if c.value == 'Outcome'), 9)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        val = str(row[outcome_col - 1].value) if len(row) >= outcome_col else ''
        if val == 'WIN':
            for cell in row: cell.fill = green
        elif val == 'LOSS':
            for cell in row: cell.fill = red
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = min(max((len(str(c.value or '')) for c in col), default=8) + 3, 28)

# ─────────────────────────────────────────────────────────────
# LIVE MONITORS
# ─────────────────────────────────────────────────────────────
async def position_monitor() -> None:
    while True:
        try:
            if bot_state['live_connected'] and bot_state['use_be'] and bot_state['connection_obj']:
                pv, positions = bot_state['pip_value'], await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] != bot_state['symbol']: continue
                    op, tp, sl, cp = p['openPrice'], p.get('takeProfit'), p.get('stopLoss'), p['currentPrice']
                    if tp and sl != op and abs(cp - op) >= 20 * pv:
                        is_buy = tp > op
                        if (is_buy and cp > op) or (not is_buy and cp < op):
                            await bot_state['connection_obj'].modify_position(p['id'], stop_loss=op)
                            await send_tg_msg(f"🛡️ <b>BE</b> تأمين الدخول — صفقة: {p['id']}")
        except Exception: pass
        await asyncio.sleep(5)

async def timeframe_scanner(tf: str) -> None:
    while True:
        try:
            if not (bot_state['status'] == 'RUNNING' and bot_state['active_tfs'][tf]): await asyncio.sleep(10); continue
            if not (bot_state['live_connected'] and bot_state['account_obj']):
                bot_state['market_data'][tf] = '⏸ بانتظار الاتصال (Offline)'; await asyncio.sleep(5); continue

            try: raw = await bot_state['account_obj'].get_historical_candles(bot_state['symbol'], tf, limit=500)
            except Exception: await asyncio.sleep(15); continue

            df, now_utc = calculate_indicators(pd.DataFrame(raw)), datetime.now(timezone.utc)
            curr, prev, prev2  = df.iloc[-2], df.iloc[-3], df.iloc[-4]
            
            if bot_state['strategy_mode'] == 'RSI_REV':
                bot_state['market_data'][tf] = f"{df.iloc[-1]['close']:.2f} | RSI:{curr['rsi']:.1f}"
            else:
                bot_state['market_data'][tf] = f"{df.iloc[-1]['close']:.2f} | K:{curr['K']:.1f}  D:{curr['D']:.1f}"

            if (bot_state['use_danger_filter'] and is_danger_time(now_utc)) or (bot_state['use_time_filter'] and not (8 <= now_utc.hour <= 17)):
                bot_state['market_data'][tf] = f"⏸ خمول | {df.iloc[-1]['close']:.2f}"; await asyncio.sleep(10); continue

            if bot_state['last_signal_time'][tf] == curr['time']: await asyncio.sleep(10); continue

            buy_sig, sell_sig, label = _get_signal_for_bar(df, df.index[-2], curr, prev, prev2)

            if bot_state['use_max_spread'] and (buy_sig or sell_sig):
                try:
                    tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                    if (tick['ask'] - tick['bid']) / bot_state['pip_value'] > bot_state['max_spread_pips']: buy_sig = sell_sig = False
                except Exception: pass

            if not (buy_sig or sell_sig): await asyncio.sleep(10); continue
            bot_state['last_signal_time'][tf] = curr['time']

            price, m = df.iloc[-1]['close'], 1 if buy_sig else -1
            tp_dist = curr['atr'] * bot_state['atr_mult_tp'] if bot_state['use_atr'] else bot_state['tp_pips'][tf] * bot_state['pip_value']
            sl_dist = curr['atr'] * bot_state['atr_mult_sl'] if bot_state['use_atr'] else bot_state['sl_pips'][tf] * bot_state['pip_value']
            tp, sl  = round(price + m * tp_dist, 2), round(price - m * sl_dist, 2)

            try:
                if buy_sig: await bot_state['connection_obj'].create_market_buy_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                else:       await bot_state['connection_obj'].create_market_sell_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                await send_tg_msg(f'🚨 <b>تم فتح صفقة!</b>\nالنوع:  {"شراء 🟢" if buy_sig else "بيع 🔴"}\nالفريم: {tf}\nالسعر: {price:.2f} | TP: {tp} | SL: {sl}\n[{label}]')
            except Exception as e: await send_tg_msg(f'❌ <b>فشل التنفيذ [{tf}]:</b>\n{e}')

        except Exception: pass
        await asyncio.sleep(10)

# ─────────────────────────────────────────────────────────────
# TELEGRAM UPDATE HANDLER
# ─────────────────────────────────────────────────────────────
async def process_tg_update(update: dict) -> None:
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']

        if msg == '/start':
            await send_tg_msg('🤖 <b>Gold Scalper Bot v5.5</b>\nالاستراتيجيات المتاحة: STOCH-NEW | STOCH-OLD | RSI-REVERSAL', get_main_keyboard())
        elif msg.startswith('/backtest'):
            if bot_state['is_backtesting']:
                await send_tg_msg('⚠️ <b>عذراً:</b> البوت يقوم بباك تيست حالياً. الرجاء الانتظار حتى ينتهي.')
            else:
                try: 
                    start_dt = datetime.strptime(msg.split()[1], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    bot_state['is_backtesting'] = True
                    asyncio.create_task(run_oanda_backtest(start_dt))
                except: 
                    await send_tg_msg('⚠️ الاستخدام: <code>/backtest YYYY-MM-DD</code>')
        return

    if 'callback_query' not in update: return
    q, d, chat_id, msg_id = update['callback_query'], update['callback_query']['data'], update['callback_query']['message']['chat']['id'], update['callback_query']['message']['message_id']
    bot_state['chat_id'] = chat_id

    try:
        if d == 'noop': pass
        elif d == 'menu_main': await edit_tg_msg(chat_id, msg_id, '🏠 القائمة الرئيسية:', get_main_keyboard())
        elif d == 'menu_filters': await edit_tg_msg(chat_id, msg_id, '🎛 <b>فلاتر وإعدادات التداول:</b>', get_filters_keyboard())
        elif d == 'menu_stoch_settings': await edit_tg_msg(chat_id, msg_id, '⚙️ <b>إعدادات الستوكاستيك:</b>', get_stoch_settings_keyboard())
        elif d == 'menu_rsi_settings': await edit_tg_msg(chat_id, msg_id, '📈 <b>إعدادات استراتيجية RSI:</b>', get_rsi_settings_keyboard())
        elif d == 'menu_tfs': await edit_tg_msg(chat_id, msg_id, '⏱ إدارة الفريمات الزمنية:', get_tf_keyboard())
        elif d == 'menu_settings': await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())
        elif d == 'menu_backtest': await edit_tg_msg(chat_id, msg_id, f'🔬 <b>باك تيست</b> — {_strat_label()}', get_backtest_keyboard())
        
        elif d == 'toggle_status':
            bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'
            await edit_tg_msg(chat_id, msg_id, '🏠 القائمة الرئيسية:', get_main_keyboard())
        elif d == 'cycle_strategy':
            modes = ['STOCH_NEW', 'STOCH_OLD', 'RSI_REV']
            bot_state['strategy_mode'] = modes[(modes.index(bot_state['strategy_mode']) + 1) % len(modes)]
            await edit_tg_msg(chat_id, msg_id, '🏠 القائمة الرئيسية:', get_main_keyboard())

        elif d == 'toggle_time':
            bot_state['use_time_filter'] = not bot_state['use_time_filter']; await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())
        elif d == 'toggle_danger':
            bot_state['use_danger_filter'] = not bot_state['use_danger_filter']; await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())

        elif d.startswith('toggle_tf_'):
            tf = d.split('_')[2]
            if tf in bot_state['active_tfs']: bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            await edit_tg_msg(chat_id, msg_id, '⏱ إدارة الفريمات:', get_tf_keyboard())

        elif d == 'set_filter_full': bot_state['filter_mode'] = 'FULL'; await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())
        elif d == 'set_filter_simple': bot_state['filter_mode'] = 'SIMPLE'; await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())
        elif d == 'set_filter_noma': bot_state['filter_mode'] = 'NO_MA'; await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())
        
        elif d == 'toggle_stoch_deep': bot_state['use_stoch_deep'] = not bot_state['use_stoch_deep']; await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'toggle_stoch_mid': bot_state['use_stoch_mid'] = not bot_state['use_stoch_mid']; await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'toggle_stoch_shal': bot_state['use_stoch_shal'] = not bot_state['use_stoch_shal']; await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'toggle_f_cons': bot_state['use_f_cons'] = not bot_state['use_f_cons']; await edit_tg_msg(chat_id, msg_id, '🎛 الفلاتر:', get_filters_keyboard())
        elif d == 'inc_stoch_k': bot_state['stoch_k'] = min(bot_state['stoch_k'] + 1, 100); await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'dec_stoch_k': bot_state['stoch_k'] = max(bot_state['stoch_k'] - 1, 1); await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'inc_stoch_s': bot_state['stoch_smooth'] = min(bot_state['stoch_smooth'] + 1, 100); await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'dec_stoch_s': bot_state['stoch_smooth'] = max(bot_state['stoch_smooth'] - 1, 1); await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'inc_stoch_d': bot_state['stoch_d'] = min(bot_state['stoch_d'] + 1, 100); await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())
        elif d == 'dec_stoch_d': bot_state['stoch_d'] = max(bot_state['stoch_d'] - 1, 1); await edit_tg_msg(chat_id, msg_id, '⚙️ إعدادات الستوكاستيك:', get_stoch_settings_keyboard())

        elif d.startswith('toggle_rsi_'):
            level = d.split('_')[2]
            key = f'use_rsi_{level}'
            bot_state[key] = not bot_state[key]
            await edit_tg_msg(chat_id, msg_id, '📈 <b>إعدادات استراتيجية RSI:</b>', get_rsi_settings_keyboard())

        elif d == 'toggle_be': bot_state['use_be'] = not bot_state['use_be']; await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())
        elif d == 'toggle_atr': bot_state['use_atr'] = not bot_state['use_atr']; await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())
        elif d == 'toggle_spread': bot_state['use_max_spread'] = not bot_state['use_max_spread']; await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())
        elif d == 'inc_lot': bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2); await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())
        elif d == 'dec_lot': bot_state['lot_size'] = max(0.01, round(bot_state['lot_size'] - 0.01, 2)); await edit_tg_msg(chat_id, msg_id, '🛠 إعدادات المخاطرة:', get_settings_keyboard())

        elif d == 'view_tpsl': await edit_tg_msg(chat_id, msg_id, '🎯 <b>تعديل TP / SL:</b>', get_tpsl_overview_keyboard())
        elif d.startswith('tpsl_edit_'): await edit_tg_msg(chat_id, msg_id, f'✏️ <b>تعديل [{d[10:]}]:</b>', get_tpsl_edit_keyboard(d[10:]))
        elif any(d.startswith(prefix) for prefix in ('inc_tp', 'dec_tp', 'inc_sl', 'dec_sl')):
            p = d.split('_'); t_key = 'tp_pips' if 'tp' in p[1] else 'sl_pips'; tf = '_'.join(p[2:]); step = 5 if '5' in p[1] else 10
            if tf in _TFS:
                bot_state[t_key][tf] = bot_state[t_key][tf] + step if p[0] == 'inc' else max(5, bot_state[t_key][tf] - step)
                await edit_tg_msg(chat_id, msg_id, f'✏️ <b>تعديل [{tf}]:</b>', get_tpsl_edit_keyboard(tf))
        
        elif d.startswith('bto_adv_'):
            if bot_state['is_backtesting']: await answer_callback(q['id'], '⚠️ البوت يقوم بباك تيست حالياً! الرجاء الانتظار.', show_alert=True)
            else:
                bot_state['is_backtesting'] = True
                asyncio.create_task(run_advanced_backtest(days=int(d.split('_')[2])))
                
        elif d.startswith('bto_'):
            if bot_state['is_backtesting']: await answer_callback(q['id'], '⚠️ البوت يقوم بباك تيست حالياً! الرجاء الانتظار.', show_alert=True)
            else:
                bot_state['is_backtesting'] = True
                asyncio.create_task(run_oanda_backtest(datetime.now(timezone.utc) - timedelta(days=int(d.split('_')[1]))))
        
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
                
        elif d == 'report':
            lines = [f'📊 <b>حالة السوق الحية — {_strat_label()}</b>']
            for tf in bot_state['timeframes']:
                if bot_state['active_tfs'][tf]: lines.append(f'[{tf}] {bot_state["market_data"][tf]}')
            await edit_tg_msg(chat_id, msg_id, '\n'.join(lines), get_main_keyboard())
            
        elif d == 'account':
            if not (bot_state['live_connected'] and bot_state['connection_obj']): await edit_tg_msg(chat_id, msg_id, '⚠️ غير متصل بالسيرفر.', get_main_keyboard())
            else:
                try:
                    info, pos = await bot_state['connection_obj'].get_account_information(), await bot_state['connection_obj'].get_positions()
                    await edit_tg_msg(chat_id, msg_id, f'💳 <b>الحساب</b>\nالرصيد: ${info.get("balance", "N/A")}\nالصفقات المفتوحة: {len(pos)}', get_main_keyboard())
                except: pass
        elif d == 'close_all':
            if not (bot_state['live_connected'] and bot_state['connection_obj']): await send_tg_msg('⚠️ غير متصل بالسيرفر.')
            else:
                try:
                    pos = await bot_state['connection_obj'].get_positions()
                    for p in pos: await bot_state['connection_obj'].close_position(p['id'])
                    await send_tg_msg(f'✅ تم إغلاق {len(pos)} صفقة.')
                except Exception as e: await send_tg_msg(f'❌ خطأ: {e}')
                
    except Exception as e: c_log(f'Callback Error: {e}')
    finally: await answer_callback(q['id'])

# ─────────────────────────────────────────────────────────────
# CORE LOOPS
# ─────────────────────────────────────────────────────────────
async def telegram_polling_loop() -> None:
    url, backoff = f'https://api.telegram.org/bot{TG_TOKEN}/getUpdates', 1
    while True:
        try:
            async with get_http().get(url, params={'offset': bot_state['last_update_id'] + 1, 'timeout': 10}, timeout=20) as resp:
                if resp.status == 200:
                    backoff = 1
                    for upd in (await resp.json()).get('result', []):
                        bot_state['last_update_id'] = upd['update_id']
                        asyncio.create_task(process_tg_update(upd))
                else: await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)
        except Exception: await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

async def supervised(coro_fn, *args):
    while True:
        try: await coro_fn(*args)
        except Exception as e: c_log(f'Task {coro_fn.__name__} crashed: {e}'); await asyncio.sleep(5)

_start_time = datetime.now(timezone.utc)
async def handle_ping(request: web.Request) -> web.Response:
    return web.Response(text=f'Gold Scalper Bot v5.5 — OK\nUptime: {datetime.now(timezone.utc) - _start_time}', content_type='text/plain')

async def main() -> None:
    get_http()
    app = web.Application(); app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.environ.get('PORT', 10000))).start()

    tasks = [asyncio.create_task(supervised(timeframe_scanner, tf)) for tf in bot_state['timeframes']]
    tasks += [asyncio.create_task(supervised(telegram_polling_loop)), asyncio.create_task(supervised(position_monitor))]
    
    try: await asyncio.gather(*tasks)
    finally:
        if _http and not _http.closed: await _http.close()

if __name__ == '__main__':
    asyncio.run(main())
