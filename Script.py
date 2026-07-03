import json
"""
Gold Scalper Bot — v8.9 (Triple Filter, Fan Labels & Smart Break-Even)
Strategy : Gann Levels + Fan Angles + Break-Even Triggered by Noise Levels
"""

import asyncio
import aiohttp
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from aiohttp import web
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from metaapi_cloud_sdk import MetaApi

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
METAAPI_TOKEN = os.environ.get('METAAPI_TOKEN', 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiJjODRlZDU2MmMyOTE3ZDgxNTU1ZWQ0NDhlNzc2MzNkMCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOjQ5MTg2OWUyLTM2MDQtNDQ2Mi1hN2FhLWJlMmVlYzJkMDU0ZCJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6NDkxODY5ZTItMzYwNC00NDYyLWE3YWEtYmUyZWVjMmQwNTRkIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDo0OTE4NjllMi0zNjA0LTQ0NjItYTdhYS1iZTJlZWMyZDA1NGQiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDo0OTE4NjllMi0zNjA0LTQ0NjItYTdhYS1iZTJlZWMyZDA1NGQiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6NDkxODY5ZTItMzYwNC00NDYyLWE3YWEtYmUyZWVjMmQwNTRkIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOjQ5MTg2OWUyLTM2MDQtNDQ2Mi1hN2FhLWJlMmVlYzJkMDU0ZCJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiYzg0ZWQ1NjJjMjkxN2Q4MTU1NWVkNDQ4ZTc3NjMzZDAiLCJpYXQiOjE3ODI5OTgwODIsImV4cCI6MTc5MDc3NDA4Mn0.nRXX-qibBjuhCG60jZMQ0qZJmfFlNI8dSK3SMapdM4QDjDgrcT4dbJ1EGE-9cnYp2l01ySQa97wOmpjf2M7Qn_1DFYAp34WaVgk9JMLK2aOMt2sFny2uSSd5QmNZcYGiFhynTmaEFl0cNCVds3Z-HBsnAL6HHGkc1d6miYH336gNaWM1npJj08Gl2_d3hApUrRiK6m3ot1dXt4ccfuYTjfczA79zfCLQG0s9Zc27STRsNauPIlGCPU8kxDS6sk5Gdg3seBvl2lK6PqP28PcZHLbZ1u29GAP9b1AWF-F0pVRjid8rUEE2o3h9X_IdWOZ7m-HmO9Dz8EBqAWefLtfpZhog_DGUcU32jGNxDM8JElpGmuhhRrlmptBxDv7HF23CcGSzqRShm1s7NB9SZDtJEblNlaQRdHeXYxQoHewwcvPCS-ssoaM6iTnBl1jvsjZ7MXzuvpisgQSoy5DR4VCXzgABAYVd2L98Q1wzduOaR8BjdLeKSYE767fr-xiE2JYvTCCjIHBFGq9wHiX-7TvYz_9Nm0YCDYj08ljCN49FfkWx11eqr7ECqEvTximOSJicA0E4wxSOT4NvJA34NhWzslb5plMigadWtstS5BTq1yui2jx5pN-r5NusD8i6B4Xw9RbmKbPwlvlpDIBiw9n-BjVQMqM-Sm6tvZ176z8Irh8')
ACCOUNT_ID    = os.environ.get('ACCOUNT_ID',    '491869e2-3604-4462-a7aa-be2eec2d054d')
TG_TOKEN      = os.environ.get('TG_TOKEN',      '8779425898:AAFQgqay6IO89I2Sf98PigL28v9AHCcZPMw')

OANDA_ACCOUNT  = os.environ.get('OANDA_ACCOUNT', '101-004-28533521-003')
OANDA_TOKEN    = os.environ.get('OANDA_TOKEN',   '0e282d5a3e65ad6fdd809e2c195bb1cd-9e2158e12fa13840e030ee3081b36fab')
AVAILABLE_SYMBOLS = ['XAU_USD', 'XAU_EUR', 'XAG_USD', 'EUR_USD', 'GBP_JPY', 'GBP_AUD', 'GBP_NZD', 'AUD_JPY', 'NZD_JPY']
SYMBOL_INFO = {
    'XAU_USD': {'pip_value': 0.1,     'contract_size': 100,    'prec': 2, 'name': 'Gold (USD)'},
    'XAU_EUR': {'pip_value': 0.1,     'contract_size': 100,    'prec': 2, 'name': 'Gold (EUR)'},
    'XAG_USD': {'pip_value': 0.001,   'contract_size': 5000,   'prec': 3, 'name': 'Silver'},
    'EUR_USD': {'pip_value': 0.00001, 'contract_size': 100000, 'prec': 5, 'name': 'EUR/USD'},
    'GBP_JPY': {'pip_value': 0.01,    'contract_size': 100000, 'prec': 3, 'name': 'GBP/JPY'},
    'GBP_AUD': {'pip_value': 0.00001, 'contract_size': 100000, 'prec': 5, 'name': 'GBP/AUD'},
    'GBP_NZD': {'pip_value': 0.00001, 'contract_size': 100000, 'prec': 5, 'name': 'GBP/NZD'},
    'AUD_JPY': {'pip_value': 0.01,    'contract_size': 100000, 'prec': 3, 'name': 'AUD/JPY'},
    'NZD_JPY': {'pip_value': 0.01,    'contract_size': 100000, 'prec': 3, 'name': 'NZD/JPY'},
}
OANDA_BASE_URL = 'https://api-fxpractice.oanda.com/v3'  

_TFS = ['1m', '2m', '3m', '4m', '5m', '6m', '10m', '15m', '20m', '30m', '1h', '2h']

_http: aiohttp.ClientSession | None = None

def get_http() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        timeout   = aiohttp.ClientTimeout(total=30, connect=10)
        _http     = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _http

def c_log(msg: str) -> None:
    dam = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime('%H:%M:%S')
    print(f"[{dam} DAM] {msg}", flush=True)

# ─────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────
bot_state: dict = {
    'status':           'RUNNING',
    'symbol':           'XAUUSDm',
    'live_connected':   False,
    'connection_obj':   None,
    'chat_id':          None,
    'last_update_id':   0,
    'is_backtesting':   False,
    'timeframes':       _TFS,

    
    'menu_button_map': {},
    'last_poll_ok':     0.0,
    'live_daily_realized': 0.0,
    'live_daily_date': None,
    'live_daily_hit': False,

    # ── Gann Levels Engine ──
    'active_symbols': {s: (s == 'XAU_USD') for s in AVAILABLE_SYMBOLS},
    'ui_selected_symbol': 'XAU_USD',
    'symbol_state': {s: {
        'gann_levels': [],
        'gann_level_status': {},
        'gann_close_used': None,
        'gann_last_h1_time': None,
        'gann_cycle_active': False,
        'gann_cycle_started_at': None,
        'gann_open_trades': {},
        'auto_trade': False,
        'lot_size': 0.05,
        'gann_cycle_hours': 1,
        'gann_zone_filter': 'star',  
        'gann_entry_mode': 'touch_trend', 
        'trend_filter_type': 'ema',     
        'trend_vwap_period': 100,
        'trend_ema_period': 60,
        'trend_timeframe': '1h',    
        'break_even_enabled': False,
        'gann_monitor_tfs': {tf: (tf in ['5m', '10m', '15m', '20m', '30m', '1h', '4m', '6m', '2h', '1m', '2m', '3m']) for tf in _TFS},
        'gann_touch_margin_pts': 5,       
        'gann_tpsl_mode': 'fixed', 
        'gann_tp_points': 70,
        'gann_sl_points': 110,
        'gann_tp_per_tf': {tf: 0 for tf in _TFS},
        'gann_sl_per_tf': {tf: 0 for tf in _TFS},
        'gann_atr_period': 14,
        'gann_atr_sl_mult': 1.5,
        'gann_atr_tp_mult': 2,
    } for s in AVAILABLE_SYMBOLS},
    
# ── Filter Type (star, star_fan, all) ──
    
    
    
    # ── Trend Filters ──
    
    
    
    
    
    # ── Trade Management ──
    
    'prot_daily_dd_usd':      200,
    'prot_daily_profit_usd':  150,
    'prot_allow_multi_tf':    True,

    
    
    
    
    
    
    
    
    
    
    
    
}


DAM_OFF = timedelta(hours=3)
def _utc_to_dam(dt) -> datetime:
    if isinstance(dt, pd.Timestamp): dt = dt.to_pydatetime()
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt + DAM_OFF

# ─────────────────────────────────────────────────────────────
# TELEGRAM HELPERS
# ─────────────────────────────────────────────────────────────
async def _tg_post(url: str, **kwargs) -> bool:
    try:
        sess = get_http()
        async with sess.post(url, **kwargs) as resp: return resp.status == 200
    except Exception: return False

def _to_reply_kbd(inline_kbd: dict):
    rows = []; bmap = {}
    for row in inline_kbd.get('inline_keyboard', []):
        new_row = []
        for btn in row:
            text = btn['text']; cb = btn.get('callback_data', 'noop')
            new_row.append({'text': text}); bmap[text] = cb
        rows.append(new_row)
    return {'keyboard': rows, 'resize_keyboard': True, 'is_persistent': True, 'input_field_placeholder': 'اختر من القائمة...'}, bmap

async def send_tg_msg(text: str, reply_markup: dict = None) -> None:
    if not bot_state['chat_id']: return
    if reply_markup and 'inline_keyboard' in reply_markup:
        reply_markup, bmap = _to_reply_kbd(reply_markup); bot_state['menu_button_map'] = bmap
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', json=payload)

async def edit_tg_msg(chat_id, message_id, text, reply_markup=None) -> None:
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/editMessageText', json=payload)

async def _show(chat_id, msg_id, text: str, reply_markup: dict = None) -> None:
    if msg_id: await edit_tg_msg(chat_id, msg_id, text, reply_markup)
    else: await send_tg_msg(text, reply_markup)

async def answer_callback(cbq_id: str) -> None:
    await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery', json={'callback_query_id': cbq_id})

async def send_tg_document(file_path: str, caption: str) -> None:
    if not bot_state['chat_id']: return
    try:
        with open(file_path, 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('chat_id',  str(bot_state['chat_id']))
            data.add_field('document', f, filename=os.path.basename(file_path))
            data.add_field('caption',  caption)
            await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/sendDocument', data=data)
    except Exception: pass

# ─────────────────────────────────────────────────────────────
# OANDA FETCHER 
# ─────────────────────────────────────────────────────────────
_OANDA_GRAN = {'1m':'M1','2m':'M2','3m':'M3','4m':'M4','5m':'M5','6m':'M6','10m':'M10','15m':'M15','20m':'M20','30m':'M30','1h':'H1','2h':'H2'}
_oanda_sem: asyncio.Semaphore | None = None
def _get_oanda_sem() -> asyncio.Semaphore:
    global _oanda_sem
    if _oanda_sem is None: _oanda_sem = asyncio.Semaphore(3)
    return _oanda_sem

async def fetch_candles(symbol: str, granularity_str: str, count: int = 5000, end_time: datetime = None) -> list:
    gran_str = _OANDA_GRAN.get(granularity_str, 'M1'); fetch_count = min(count, 120000)  
    collected = []; remaining = fetch_count
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}', 'Content-Type':  'application/json'}
    url = f'{OANDA_BASE_URL}/instruments/{symbol}/candles'
    current_end = end_time if end_time else datetime.now(timezone.utc)

    sem = _get_oanda_sem()
    async with sem:
        while remaining > 0:
            chunk = min(remaining, 5000)
            params = {'granularity': gran_str, 'count': chunk, 'to': current_end.strftime('%Y-%m-%dT%H:%M:%S.000000000Z'), 'price': 'M'}
            candles = []
            for attempt in range(3):
                try:
                    async with get_http().get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status != 200: break
                        data = await resp.json(); candles = data.get('candles', []); break
                except Exception: await asyncio.sleep(1)

            if not candles: break
            complete = [c for c in candles if c.get('complete', True)]
            if not complete: break

            formatted = [{'time': pd.Timestamp(c['time']).tz_convert('UTC'), 
                          'open': float(c['mid']['o']), 'high': float(c['mid']['h']), 
                          'low': float(c['mid']['l']), 'close': float(c['mid']['c']),
                          'volume': float(c.get('volume', 1.0))} for c in complete]
                          
            collected = formatted + collected; remaining -= len(complete)
            earliest = pd.Timestamp(complete[0]['time']).tz_convert('UTC')
            current_end = earliest.to_pydatetime() - timedelta(seconds=1)
            if len(complete) < chunk: break
            await asyncio.sleep(0.2)
    return collected

# ─────────────────────────────────────────────────────────────
# GANN LEVELS & FAN ENGINE (⭐ & ⭐🌀)
# ─────────────────────────────────────────────────────────────
GANN_TFC_H1 = 0.02

# تصنيف دقيق للمستويات:
# star = المستويات الأصلية القوية التي حققت لك 100%
# fan  = المستويات الموازية للمروحة
GANN_COEFS = [
    {'c': 0.0208, 'star': False, 'fan': False},
    {'c': 0.0417, 'star': False, 'fan': False},
    {'c': 0.0625, 'star': False, 'fan': False},
    {'c': 0.0833, 'star': True,  'fan': False}, 
    {'c': 0.125,  'star': False, 'fan': True},  # 8x1
    {'c': 0.25,   'star': False, 'fan': True},  # 4x1
    {'c': 0.333,  'star': False, 'fan': True},  # 3x1
    {'c': 0.5,    'star': True,  'fan': True},  # 2x1 (أيضاً يعتبر أصلي)
    {'c': 1.0,    'star': True,  'fan': True},  # 1x1 (أيضاً يعتبر أصلي)
    {'c': 2.0,    'star': False, 'fan': True},  # 1x2
    {'c': 3.0,    'star': False, 'fan': True},  # 1x3
    {'c': 4.0,    'star': False, 'fan': True},  # 1x4
    {'c': 8.0,    'star': False, 'fan': True},  # 1x8
]

def gann_calc_levels(symbol: str, close: float) -> list[dict]:
    levels = []
    for i, item in enumerate(GANN_COEFS):
        offset = close * item['c'] * GANN_TFC_H1
        prec = SYMBOL_INFO[symbol]['prec']
        up = round(close + offset, prec)
        dn = round(close - offset, prec)
        
        # التسميات الدقيقة
        up_lbl = "مقاومة"
        dn_lbl = "دعم"
        if item['star'] and not item['fan']:
            up_lbl = "مقاومة ⭐"
            dn_lbl = "دعم ⭐"
        elif item['star'] and item['fan']:
            up_lbl = "مقاومة ⭐"
            dn_lbl = "دعم ⭐"
        elif item['fan']:
            up_lbl = "مقاومة موازية للمروحة 🌀"
            dn_lbl = "دعم موازي للمروحة 🌀"
            
        levels.append({'key': f'up_{i}', 'price': up, 'dir': 'up', 'star': item['star'], 'fan': item['fan'], 'label': up_lbl})
        if dn > 0:
            levels.append({'key': f'dn_{i}', 'price': dn, 'dir': 'dn', 'star': item['star'], 'fan': item['fan'], 'label': dn_lbl})
            
    levels.append({'key': 'ref', 'price': round(close, SYMBOL_INFO[symbol]['prec']), 'dir': 'ref', 'star': False, 'fan': False, 'label': 'إغلاق H1'})
    levels.sort(key=lambda x: x['price'], reverse=True)
    return levels

def gann_active_levels(symbol: str) -> list[dict]:
    sym_state = bot_state['symbol_state'][symbol]
    lv = [l for l in bot_state['symbol_state'][symbol]['gann_levels'] if l['dir'] != 'ref']
    f = sym_state['gann_zone_filter']
    if f == 'star': return [l for l in lv if l['star']]
    elif f == 'star_fan': return [l for l in lv if l['star'] or l['fan']]
    return lv

def _gann_tf_tp(symbol: str, tf: str) -> int:
    sym_state = bot_state['symbol_state'][symbol]
    v = sym_state['gann_tp_per_tf'].get(tf, 0)
    return v if v > 0 else sym_state['gann_tp_points']

def _gann_tf_sl(symbol: str, tf: str) -> int:
    sym_state = bot_state['symbol_state'][symbol]
    v = sym_state['gann_sl_per_tf'].get(tf, 0)
    return v if v > 0 else sym_state['gann_sl_points']

def _gann_atr(candles: list, period: int) -> float | None:
    if len(candles) < period + 1: return None
    df = pd.DataFrame(candles[-(period + 50):])
    df['prev_close'] = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['prev_close']).abs(),
        (df['low']  - df['prev_close']).abs(),
    ], axis=1).max(axis=1)
    val = tr.rolling(period).mean().iloc[-1]
    return float(val) if not pd.isna(val) else None

def _gann_calc_tpsl(symbol: str, entry: float, is_buy: bool, candles: list, tf: str = '') -> tuple[float, float]:
    sym_state = bot_state['symbol_state'][symbol]
    pv = SYMBOL_INFO[symbol]['pip_value']
    prec = SYMBOL_INFO[symbol]['prec']
    if sym_state['gann_tpsl_mode'] == 'atr':
        atr = _gann_atr(candles, sym_state['gann_atr_period'])
        if not atr: atr = _gann_tf_sl(symbol, tf) * pv
        sl_dist = atr * sym_state['gann_atr_sl_mult']
        tp_dist = atr * sym_state['gann_atr_tp_mult']
    else:
        sl_dist = _gann_tf_sl(symbol, tf) * pv
        tp_dist = _gann_tf_tp(symbol, tf) * pv
    if is_buy: return round(entry + tp_dist, prec), round(entry - sl_dist, prec)
    return round(entry - tp_dist, prec), round(entry + sl_dist, prec)

async def _gann_fetch_last_closed_h1(symbol: str) -> dict | None:
    candles = await fetch_candles(symbol, '1h', count=2)
    if not candles: return None
    candles = sorted(candles, key=lambda c: c['time'])
    # fetch_candles already filters out incomplete (currently forming) candles!
    # So candles[-1] is exactly the LAST CLOSED candle.
    return candles[-1]

def _gann_fmt_levels_msg(symbol: str, close: float) -> str:
    sym_state = bot_state['symbol_state'][symbol]
    lines = []
    for l in bot_state['symbol_state'][symbol]['gann_levels']:
        if l['dir'] == 'ref':
            lines.append(f"➖ <b>{l['price']:.2f}</b>  (إغلاق H1)")
            continue
        
        icon = '🔴' if l['dir'] == 'up' else '🟢'
        lines.append(f"{icon} {l['price']:.2f}  {l['label']}")
        
    f_mode = sym_state['gann_zone_filter']
    if f_mode == 'star': filt = '⭐ المستويات الأصلية القوية فقط'
    elif f_mode == 'star_fan': filt = '⭐🌀 القوية + الموازية للمروحة'
    else: filt = '📋 كل المستويات (مخاطرة)'
    
    flt_trend = sym_state['trend_filter_type'].upper()
    if flt_trend == 'BOTH': flt_trend = 'VWAP + EMA'
    
    mode = f'لمس مباشر + فلتر ({flt_trend}_{sym_state["trend_timeframe"].upper()})' if sym_state['gann_entry_mode'] == 'touch_trend' else 'لمس أعمى (بدون فلتر)'
    return (f"📐 <b>سلّم جان (المروحة) — دورة جديدة</b>\n"
            f"إغلاق H1: <b>{close:.2f}</b>\n\n"
            f"مدة المراقبة: {sym_state['gann_cycle_hours']}س  |  فلتر: {filt}\nالدخول: {mode}\n\n\n"
            + '\n'.join(lines))

async def _gann_open_trade(symbol: str, is_buy: bool, level: dict, candles: list, reason: str, tf: str) -> None:
    sym_state = bot_state['symbol_state'][symbol]
    try:
        price = float(candles[-1]['close'])
        tp, sl = _gann_calc_tpsl(symbol, price, is_buy, candles, tf=tf)
        lot = sym_state['lot_size']; side = 'BUY' if is_buy else 'SELL'
        tp_pts = _gann_tf_tp(symbol, tf); sl_pts = _gann_tf_sl(symbol, tf)
        
        tpsl_lbl = (f"ATR({sym_state['gann_atr_period']})×{sym_state['gann_atr_sl_mult']}/{sym_state['gann_atr_tp_mult']}\n"
                    if sym_state['gann_tpsl_mode'] == 'atr' else f"SL:{sl_pts}p TP:{tp_pts}p")
        
        be_lbl = " | 🛡️ BE Active" if sym_state['break_even_enabled'] else ""
        
        is_real = sym_state.get('auto_trade', False)
        trade_id = f"sim_{int(datetime.now().timestamp())}_{tf}"
        real_msg = ""
        
        if is_real:
            if not METAAPI_TOKEN or METAAPI_TOKEN == 'YOUR_METAAPI_TOKEN' or not ACCOUNT_ID or ACCOUNT_ID == 'YOUR_ACCOUNT_ID':
                real_msg = "\n⚠️ التداول الآلي مفعل ولكن بيانات MetaAPI غير صحيحة! تم تسجيلها وهمياً."
                is_real = False
            else:
                try:
                    api = MetaApi(METAAPI_TOKEN)
                    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                    conn = account.get_rpc_connection()
                    await conn.connect()
                    await conn.wait_synchronized()
                    
                    # MetaTrader symbols rarely have underscores, and user configured bot_state['symbol'] as XAUUSDm
                    mt4_symbol = bot_state.get('symbol', symbol.replace('_', ''))
                    if is_buy:
                        res = await conn.create_market_buy_order(mt4_symbol, lot, stop_loss=sl, take_profit=tp)
                    else:
                        res = await conn.create_market_sell_order(mt4_symbol, lot, stop_loss=sl, take_profit=tp)
                        
                    trade_id = str(res.get('orderId', res.get('positionId', trade_id)))
                    real_msg = "\n🚀 <b>تم فتح الصفقة حقيقياً على حسابك!</b>"
                except Exception as ex:
                    real_msg = f"\n❌ <b>فشل فتح الصفقة حقيقياً:</b> {ex}"
                    is_real = False
        
        bot_state['symbol_state'][symbol]['gann_open_trades'][trade_id] = {
            'tf': tf, 'is_buy': is_buy, 'entry': price, 'is_real': is_real, 'sl': sl, 'tp': tp,
            'be_trigger': (price + (tp - price)/2) if is_buy else (price - (price - tp)/2) # simplified BE trigger
        }
        bot_state['symbol_state'][symbol]['gann_level_status'][level['key']] = 'used'
        
        await send_tg_msg(
            f"<b>✅ {'BUY 📈' if is_buy else 'SELL 📉'} [{symbol} - جان {tf}]</b>  {reason}\n\n"
            f"المستوى: {level['price']:.2f}  |  الدخول: {price:.2f}\n\n"
            f"TP: {tp}  SL: {sl}  |  {tpsl_lbl}{be_lbl}\n"
            f"إغلاق H1: {bot_state['symbol_state'][symbol]['gann_close_used']:.5f}\n"
            f"{real_msg}"
        )
    except Exception as e:
        bot_state['symbol_state'][symbol]['gann_level_status'][level['key']] = 'used'
        await send_tg_msg(f"<b>❌ فشل تنفيذ الصفقة [{symbol} - جان {tf}]</b>\nالمستوى: {level['price']:.5f}\n{e}")

# ─────────────────────────────────────────────────────────────
# BACKTEST PROGRESS TRACKER
# ─────────────────────────────────────────────────────────────
class BtProgress:
    BAR_LEN = 14; HEARTBEAT = 15
    def __init__(self, label: str, active_tfs: list):
        self.label = label; self.active_tfs = active_tfs; self.cancelled = False; self.phase = 'Initialising...'
        self.tf_done = 0; self.tf_total = len(active_tfs); self.current_tf = ''
        self.bars_done = 0; self.bars_total = 0; self.win = 0; self.loss = 0; self.be = 0; self.profit = 0.0
        self.chat_id = None; self.msg_id = None; self._last_edit = 0.0; self._lock = asyncio.Lock(); self._hb_task = None; self._start_ts = 0.0

    def _bar(self, done: int, total: int) -> str:
        if total == 0: return chr(9617) * self.BAR_LEN
        filled = round(done / total * self.BAR_LEN)
        return chr(9608) * filled + chr(9617) * (self.BAR_LEN - filled)

    def _elapsed(self) -> str:
        secs = int(datetime.now(timezone.utc).timestamp() - self._start_ts); m, s = divmod(secs, 60); return f'{m}m {s:02d}s'

    def _build_text(self) -> str:
        total = self.win + self.loss; wr = f'{round(self.win / total * 100)}%' if total else '-'
        pnl = f'+${round(self.profit,2)}' if self.profit >= 0 else f'-${abs(round(self.profit,2))}'; icon = '▲' if self.profit >= 0 else '▼'
        overall = (self.tf_done + self.bars_done / self.bars_total) / max(self.tf_total, 1) if self.bars_total else self.tf_done / max(self.tf_total, 1)
        ov_bar = self._bar(round(overall * 100), 100); ov_pct = f'{round(overall * 100)}%'
        tf_bar = self._bar(self.bars_done, self.bars_total) if self.bars_total else chr(9617) * self.BAR_LEN
        tf_pct = f'{round(self.bars_done / self.bars_total * 100)}%' if self.bars_total else '-'
        lines = [f'Backtest — <b>{self.label}</b>', f'<b>Phase:</b> {self.phase}', '', f'<b>Overall</b>  {ov_pct}', f'<code>[{ov_bar}]</code>']
        if self.current_tf: lines += ['', f'<b>TF:</b> {self.current_tf}  ({self.tf_done}/{self.tf_total})', f'<code>[{tf_bar}] {tf_pct}</code>', f'Bars: {self.bars_done}/{self.bars_total}']
        lines += ['', f'W:{self.win}  L:{self.loss}  BE:{self.be}', f'{icon} {pnl}  WR:{wr}', '', f'Elapsed: {self._elapsed()}']
        if self.cancelled: lines.append('<b>CANCELLED</b>')
        return '\n'.join(lines)

    async def start(self, chat_id: int) -> None:
        self.chat_id = chat_id; self._start_ts = datetime.now(timezone.utc).timestamp(); self._last_edit = self._start_ts
        payload = {'chat_id': chat_id, 'text': self._build_text(), 'parse_mode': 'HTML', 'reply_markup': {'inline_keyboard': [[{'text': '⏹ Cancel', 'callback_data': 'cancel_bt'}]]}}
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', json=payload) as resp:
                    if resp.status == 200: self.msg_id = (await resp.json())['result']['message_id']
        except Exception: pass
        self._hb_task = asyncio.create_task(self._heartbeat())

    async def _heartbeat(self) -> None:
        while not self.cancelled: await asyncio.sleep(self.HEARTBEAT); await self._edit(force=True)

    async def _edit(self, force: bool = False) -> None:
        now = datetime.now(timezone.utc).timestamp()
        if not force and (now - self._last_edit) < 3: return
        if not self.msg_id or not self.chat_id: return
        async with self._lock:
            self._last_edit = now; payload = {'chat_id': self.chat_id, 'message_id': self.msg_id, 'text': self._build_text(), 'parse_mode': 'HTML'}
            if not self.cancelled: payload['reply_markup'] = {'inline_keyboard': [[{'text': '⏹ Cancel', 'callback_data': 'cancel_bt'}]]}
            try: await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/editMessageText', json=payload)
            except Exception: pass

    async def set_phase(self, phase: str) -> None: self.phase = phase; await self._edit()
    async def set_tf(self, tf: str, bars_total: int) -> None: self.current_tf = tf; self.bars_done = 0; self.bars_total = bars_total; await self._edit(force=True)
    async def tick(self, bar_n: int, win: int, loss: int, be: int, profit: float) -> None: self.bars_done = bar_n; self.win = win; self.loss = loss; self.be = be; self.profit = profit; await self._edit()
    async def done(self, final_text: str) -> None:
        if self._hb_task: self._hb_task.cancel()
        if not self.msg_id or not self.chat_id: return
        try: await edit_tg_msg(self.chat_id, self.msg_id, final_text)
        except Exception: pass
    async def cancel(self) -> None:
        self.cancelled = True; self.phase = 'Cancelling...'
        if self._hb_task: self._hb_task.cancel()
        await self._edit(force=True)

_bt_progress: BtProgress | None = None

# ─────────────────────────────────────────────────────────────
# KEYBOARDS 
# ─────────────────────────────────────────────────────────────
def get_main_keyboard() -> dict:
    return {'inline_keyboard': [
        [{'text': '🔌 فحص حالة حساب MetaAPI', 'callback_data': 'check_metaapi_status'}],
        [{'text': '📐 محرك جان (الاستراتيجية)', 'callback_data': 'menu_gann'}],
        [{'text': '🛡️ إعدادات الحماية', 'callback_data': 'menu_protection'}],
        [{'text': '💾 إدارة الإعدادات (Presets)', 'callback_data': 'menu_presets'}],
        [{'text': '📊 بدء الباكتيست', 'callback_data': 'menu_gann_bt'}],
    ]}


def get_protection_keyboard() -> dict:
    dd = bot_state['prot_daily_dd_usd']
    profit = bot_state['prot_daily_profit_usd']
    multi_tf = '✅ مسموح' if bot_state['prot_allow_multi_tf'] else '❌ ممنوع'
    
    rows = [
        [{'text': '── الحماية وإدارة المخاطر ──', 'callback_data': 'noop'}],
        [{'text': f'تكرار الصفقات (Multi-TF): {multi_tf}', 'callback_data': 'prot_toggle_multitf'}],
        [{'text': f'📉 أقصى تراجع يومي: ${dd}', 'callback_data': 'noop'}],
        [
            {'text': '➖ $50', 'callback_data': 'prot_dec_dd'},
            {'text': '➕ $50', 'callback_data': 'prot_inc_dd'}
        ],
        [{'text': f'💰 هدف الربح اليومي: ${profit}', 'callback_data': 'noop'}],
        [
            {'text': '➖ $100', 'callback_data': 'prot_dec_profit'},
            {'text': '➕ $100', 'callback_data': 'prot_inc_profit'}
        ],
        [{'text': '🔙 رجوع', 'callback_data': 'menu_main'}]
    ]
    return {'inline_keyboard': rows}

def get_gann_keyboard() -> dict:
    sym = bot_state['ui_selected_symbol']
    sym_state = bot_state['symbol_state'][sym]
    zf   = sym_state['gann_zone_filter']
    em   = sym_state['gann_entry_mode']
    mg   = sym_state['gann_touch_margin_pts']
    tpsm = sym_state['gann_tpsl_mode']
    hrs  = sym_state['gann_cycle_hours']
    cyc  = '🟢 نشطة' if sym_state['gann_cycle_active'] else '⚫ غير نشطة'
    open_n = len(bot_state['symbol_state'][sym]['gann_open_trades'])
    
    flt_type = sym_state['trend_filter_type']
    
    # تحديث التسميات لزر الفلتر
    if zf == 'star': zf_lbl = '⭐ المستويات الأصلية القوية فقط'
    elif zf == 'star_fan': zf_lbl = '⭐🌀 القوية + موازية للمروحة'
    else: zf_lbl = '📋 كل المستويات (للتجارب)'
    
    if flt_type == 'ema':
        filt_btn_lbl = "📉 الفلتر المعتمد: (EMA الشامل)"
        flt_name = 'EMA'
    else:
        filt_btn_lbl = "🌊 الفلتر المعتمد: (VWAP الشامل)"
        flt_name = 'VWAP'
        
    ttf_lbl = sym_state['trend_timeframe'].upper()
    em_lbl  = f'⚡ لمس + فلتر ({flt_name}_{ttf_lbl})' if em == 'touch_trend' else '⚡ لمس أعمى (بدون فلتر)'
    tps_lbl = f'🎯 TP/SL: {"نقاط ثابتة" if tpsm == "fixed" else "حسب ATR"}'

    tp = sym_state['gann_tp_points']; sl = sym_state['gann_sl_points']
    atp = sym_state['gann_atr_tp_mult']; asp = sym_state['gann_atr_sl_mult']
    ap  = sym_state['gann_atr_period']
    be_lbl = "🟢 مفعل" if sym_state['break_even_enabled'] else "⚫ معطل"
    
    auto_t = '🟢 مفعل' if sym_state.get('auto_trade', False) else '🔴 معطل'
    
    rows = [
        [{'text': f'🤖 التداول الآلي (MetaAPI): {auto_t}', 'callback_data': 'gann_toggle_auto_trade'}],
        [{'text': f'📐 {sym} — دورة: {cyc}  |  صفقات: {open_n}', 'callback_data': 'noop'}],
        [{'text': '🔄 عرض الدعوم والمقاومات الحالية', 'callback_data': 'gann_show_levels'}],
    ]
    
    rows.append([{'text': '── أزواج التداول والباكتيست ──', 'callback_data': 'noop'}])
    pair_row = []
    for p in AVAILABLE_SYMBOLS:
        icon = '✅' if bot_state['active_symbols'][p] else '⬜'
        pair_row.append({'text': f'{icon} {p}', 'callback_data': f'gann_toggle_pair_{p}'})
        if len(pair_row) == 2:
            rows.append(pair_row)
            pair_row = []
    if pair_row: rows.append(pair_row)

    rows.append([{'text': '── تخصيص إعدادات الزوج ──', 'callback_data': 'noop'}])
    sel_row = []
    for p in AVAILABLE_SYMBOLS:
        sel = '📌 ' if p == sym else ''
        sel_row.append({'text': f'{sel}{p}', 'callback_data': f'gann_sel_pair_{p}'})
        if len(sel_row) == 2:
            rows.append(sel_row)
            sel_row = []
    if sel_row: rows.append(sel_row)
    
    rows += [
        [{'text': '── الاستراتيجية والفلتر ──', 'callback_data': 'noop'}],
        [{'text': f'الاستراتيجية: {em_lbl}', 'callback_data': 'gann_toggle_entry'}],
        [{'text': f'فلتر الدخول: {zf_lbl}', 'callback_data': 'gann_toggle_filter'}],
        [{'text': filt_btn_lbl, 'callback_data': 'gann_toggle_filter_type'}],
        [{'text': f'⏱️ فريم الترند: {ttf_lbl}', 'callback_data': 'gann_toggle_ttf'}],
        [{'text': f'🛡️ صمام الأمان (Break-Even): {be_lbl}', 'callback_data': 'gann_toggle_be'}],
    ]
    
    if flt_type == 'vwap':
        vwap_val = sym_state['trend_vwap_period']
        rows.append([{'text': 'VWAP −10', 'callback_data': 'gann_dec_vwap'}, 
                     {'text': f'قيمة {ttf_lbl} VWAP: {vwap_val}', 'callback_data': 'noop'}, 
                     {'text': 'VWAP +10', 'callback_data': 'gann_inc_vwap'}])
                     
    if flt_type == 'ema':
        ema_val = sym_state['trend_ema_period']
        rows.append([{'text': 'EMA −10', 'callback_data': 'gann_dec_ema'}, 
                     {'text': f'قيمة {ttf_lbl} EMA: {ema_val}', 'callback_data': 'noop'}, 
                     {'text': 'EMA +10', 'callback_data': 'gann_inc_ema'}])
        
    rows += [
        [{'text': '📝 مساعدة: تغيير القيم الخاصة بالأوامر', 'callback_data': 'gann_filter_help'}],
        [{'text': '── فريمات التنفيذ ──', 'callback_data': 'noop'}],
    ]
    
    tf_items = list(sym_state['gann_monitor_tfs'].items())
    for i in range(0, len(tf_items), 4):
        rows.append([{'text': ('✅' if on else '⬜') + f' {tfk}', 'callback_data': f'gann_tf_{tfk}'} for tfk, on in tf_items[i:i+4]])
        
    rows += [
        [{'text': '── إعدادات عامة ──', 'callback_data': 'noop'}],
        [{'text': '−ساعة', 'callback_data': 'gann_dec_hours'}, {'text': f'مدة تجميد السلّم: {hrs} ساعة', 'callback_data': 'noop'}, {'text': '+ساعة', 'callback_data': 'gann_inc_hours'}],
        [{'text': 'Lot −0.01', 'callback_data': 'gann_dec_lot'}, {'text': f'حجم اللوت: {sym_state["lot_size"]}', 'callback_data': 'noop'}, {'text': 'Lot +0.01', 'callback_data': 'gann_inc_lot'}],
        [{'text': 'Margin −1', 'callback_data': 'gann_dec_margin'}, {'text': f'هامش اللمس {mg}p', 'callback_data': 'noop'}, {'text': 'Margin +1', 'callback_data': 'gann_inc_margin'}],
        [{'text': '── TP / SL ──', 'callback_data': 'noop'}],
        [{'text': tps_lbl, 'callback_data': 'gann_toggle_tpsl'}],
    ]

    if tpsm == 'fixed':
        rows += [
            [{'text': 'TP  −10', 'callback_data': 'gann_dec_tp10'}, {'text': f'TP={tp}p', 'callback_data': 'noop'}, {'text': 'TP  +10', 'callback_data': 'gann_inc_tp10'}],
            [{'text': 'SL  −10', 'callback_data': 'gann_dec_sl10'}, {'text': f'SL={sl}p', 'callback_data': 'noop'}, {'text': 'SL  +10', 'callback_data': 'gann_inc_sl10'}],
        ]
    else:
        rows += [
            [{'text': 'ATR Period −', 'callback_data': 'gann_dec_atrp'}, {'text': f'Period={ap}', 'callback_data': 'noop'}, {'text': 'ATR Period +', 'callback_data': 'gann_inc_atrp'}],
            [{'text': 'SL mult −0.5', 'callback_data': 'gann_dec_atrsl'}, {'text': f'SL×{asp}', 'callback_data': 'noop'}, {'text': 'SL mult +0.5', 'callback_data': 'gann_inc_atrsl'}],
            [{'text': 'TP mult −0.5', 'callback_data': 'gann_dec_atrtp'}, {'text': f'TP×{atp}', 'callback_data': 'noop'}, {'text': 'TP mult +0.5', 'callback_data': 'gann_inc_atrtp'}],
        ]

    rows += [
        [{'text': '⚙️ TP/SL مخصص لكل فريم', 'callback_data': 'gann_tpsl_tf'}],
        [{'text': '📊 بدء الباكتيست', 'callback_data': 'menu_gann_bt'}],
        [{'text': '← رجوع', 'callback_data': 'menu_main'}],
    ]
    return {'inline_keyboard': rows}

def get_gann_tpsl_tf_keyboard(sel_tf: str = '') -> dict:
    sym_state = bot_state['symbol_state'][bot_state['ui_selected_symbol']]
    rows = [[{'text': '⚙️ TP/SL مخصص لكل فريم', 'callback_data': 'noop'}],
            [{'text': '(0 = يرجع للقيمة العامة)', 'callback_data': 'noop'}]]
    tfs_list = list(sym_state['gann_monitor_tfs'].keys())
    tf_row = []
    for tfk in tfs_list:
        icon = '👉' if tfk == sel_tf else ''
        tf_row.append({'text': f'{icon}{tfk}', 'callback_data': f'gann_tptf_sel_{tfk}'})
        if len(tf_row) == 4: rows.append(tf_row); tf_row = []
    if tf_row: rows.append(tf_row)
    if sel_tf:
        tp_v = sym_state['gann_tp_per_tf'].get(sel_tf, 0); sl_v = sym_state['gann_sl_per_tf'].get(sel_tf, 0)
        eff_tp = tp_v if tp_v > 0 else sym_state['gann_tp_points']
        eff_sl = sl_v if sl_v > 0 else sym_state['gann_sl_points']
        rows += [
            [{'text': f'── [{sel_tf}] ──', 'callback_data': 'noop'}],
            [{'text': f'TP فعلي: {eff_tp}p {"(مخصص)" if tp_v>0 else "(عام)"}', 'callback_data': 'noop'}],
            [{'text': 'TP −10', 'callback_data': f'gann_tptf_dtp_{sel_tf}'}, {'text': f'TP={tp_v}', 'callback_data': 'noop'}, {'text': 'TP +10', 'callback_data': f'gann_tptf_itp_{sel_tf}'}],
            [{'text': f'SL فعلي: {eff_sl}p {"(مخصص)" if sl_v>0 else "(عام)"}', 'callback_data': 'noop'}],
            [{'text': 'SL −10', 'callback_data': f'gann_tptf_dsl_{sel_tf}'}, {'text': f'SL={sl_v}', 'callback_data': 'noop'}, {'text': 'SL +10', 'callback_data': f'gann_tptf_isl_{sel_tf}'}],
            [{'text': '↺ إعادة ضبط', 'callback_data': f'gann_tptf_rst_{sel_tf}'}],
        ]
    rows.append([{'text': '← رجوع', 'callback_data': 'menu_gann'}])
    return {'inline_keyboard': rows}

def get_gann_bt_keyboard() -> dict:
    if bot_state['is_backtesting']:
        return {'inline_keyboard': [[{'text': '⏳ الباكتيست يعمل...', 'callback_data': 'noop'}], [{'text': '⏹ إلغاء', 'callback_data': 'cancel_bt'}]]}
    return {'inline_keyboard': [
        [{'text': 'يوم واحد', 'callback_data': 'gbt_1'}, {'text': 'يومين', 'callback_data': 'gbt_2'}],
        [{'text': 'ثلاثة أيام', 'callback_data': 'gbt_3'}, {'text': 'أسبوع', 'callback_data': 'gbt_7'}],
        [{'text': 'شهر كامل', 'callback_data': 'gbt_30'}],
        [{'text': 'أو أرسل: /backtest YYYY-MM-DD', 'callback_data': 'noop'}],
        [{'text': '← رجوع', 'callback_data': 'menu_gann'}],
    ]}

# ─────────────────────────────────────────────────────────────
# LIVE SCANNER (VWAP / EMA / BOTH Macro)
# ─────────────────────────────────────────────────────────────
async def gann_monitor_scanner() -> None:
    c_log('Gann live scanner started.')
    while True:
        try:
            if bot_state['status'] != 'RUNNING':
                await asyncio.sleep(10); continue

            now_dt = datetime.now(timezone.utc)
            today_date = now_dt.date()
            if bot_state.get('live_daily_date') != today_date:
                bot_state['live_daily_date'] = today_date
                bot_state['live_daily_realized'] = 0.0
                bot_state['live_daily_hit'] = False
                c_log(f"Daily limits reset for {today_date}")
                
            if bot_state.get('live_daily_hit'):
                await asyncio.sleep(60)
                continue
                
            active_symbols = [s for s, on in bot_state['active_symbols'].items() if on]
            
            total_floating = 0.0
            total_realized = bot_state.get('live_daily_realized', 0.0)
            
            # --- First pass: track open trades and calculate floating ---
            for symbol in active_symbols:
                sym_state = bot_state['symbol_state'][symbol]
                if sym_state['gann_open_trades']:
                    mc = await fetch_candles(symbol, '1m', count=2)
                    if mc:
                        live_px = float(mc[-1]['close'])
                        closed_ids = []
                        for tid, tr in sym_state['gann_open_trades'].items():
                            is_buy = tr.get('is_buy')
                            tp = tr.get('tp')
                            sl = tr.get('sl')
                            entry = tr.get('entry')
                            tf = tr.get('tf')
                            
                            diff = (live_px - entry) if is_buy else (entry - live_px)
                            cs = SYMBOL_INFO[symbol]['contract_size']
                            quote_conv = 1.0
                            trade_pl = round(diff * sym_state['lot_size'] * cs * quote_conv, 2)
                            
                            outcome = None
                            if is_buy:
                                if live_px >= tp: outcome = 'WIN ✅'
                                elif live_px <= sl: outcome = 'LOSS ❌'
                            else:
                                if live_px <= tp: outcome = 'WIN ✅'
                                elif live_px >= sl: outcome = 'LOSS ❌'
                                
                            if outcome:
                                closed_ids.append(tid)
                                bot_state['live_daily_realized'] += trade_pl
                                msg = f"🔔 <b>تحديث صفقة [{symbol} - جان {tf}]</b>\n\nالنتيجة: {outcome} ({trade_pl}$)\nسعر الإغلاق: {live_px:.2f}"
                                if tr.get('is_real'): msg += "\n\n(الصفقة حقيقية، تم إغلاقها تلقائياً على منصتك بناءً على TP/SL)"
                                await send_tg_msg(msg)
                            else:
                                total_floating += trade_pl
                                
                        for tid in closed_ids:
                            del sym_state['gann_open_trades'][tid]

            # --- Evaluate Daily Limits ---
            total_daily = bot_state['live_daily_realized'] + total_floating
            dd_limit = -float(bot_state.get('prot_daily_dd_usd', 220))
            profit_limit = float(bot_state.get('prot_daily_profit_usd', 150))
            
            if (dd_limit < 0 and total_daily <= dd_limit) or (profit_limit > 0 and total_daily >= profit_limit):
                bot_state['live_daily_hit'] = True
                limit_type = '🛑 تراجع عائم' if total_daily <= dd_limit else '✅ هدف يومي عائم'
                await send_tg_msg(f"{limit_type} تم الوصول إليه! ({total_daily:.2f}$)\nسيتم إغلاق جميع الصفقات المفتوحة وإيقاف التداول لهذا اليوم.")
                
                # Close all open trades internally AND via MetaAPI if real
                for symbol in active_symbols:
                    sym_state = bot_state['symbol_state'][symbol]
                    for tid, tr in list(sym_state['gann_open_trades'].items()):
                        if tr.get('is_real'):
                            try:
                                api = MetaApi(METAAPI_TOKEN)
                                account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                                conn = account.get_rpc_connection()
                                await conn.connect()
                                await conn.wait_synchronized()
                                await conn.close_position(tid)
                                await send_tg_msg(f"✅ <b>تم إغلاق صفقة {symbol} (حقيقية) آلياً لحماية الحساب!</b>")
                            except Exception as e:
                                await send_tg_msg(f"⚠️ <b>تنبيه عاجل:</b> فشل البوت في إغلاق صفقة {symbol} آلياً ({e}). <b>يرجى إغلاقها يدوياً من منصتك فوراً!</b>")
                        del sym_state['gann_open_trades'][tid]
                        
                continue # Skip new trades
                
            for symbol in active_symbols:
                sym_state = bot_state['symbol_state'][symbol]

                flt_type = sym_state['trend_filter_type']
                ttf = sym_state['trend_timeframe']
                enabled_tfs = [tf for tf, on in sym_state['gann_monitor_tfs'].items() if on]
                if not sym_state['gann_cycle_active'] or not sym_state['gann_levels']:
                    continue

                macro_trend_up = None
                if sym_state['gann_entry_mode'] == 'touch_trend':
                    p_vwap = sym_state['trend_vwap_period'] if flt_type == 'vwap' else 0
                    p_ema  = sym_state['trend_ema_period'] if flt_type == 'ema' else 0
                    max_period = max(p_vwap, p_ema, 100)
                    
                    trend_candles = await fetch_candles(symbol, ttf, count=max(max_period+10, 120))
                    if trend_candles:
                        df_trend = pd.DataFrame(trend_candles)
                        current_trend_close = float(trend_candles[-1]['close'])
                        
                        if flt_type == 'vwap':
                            df_trend['Typical_Price'] = (df_trend['high'] + df_trend['low'] + df_trend['close']) / 3
                            df_trend['VWAP'] = (df_trend['Typical_Price'] * df_trend['volume']).rolling(window=p_vwap).sum() / df_trend['volume'].rolling(window=p_vwap).sum()
                            current_vwap = df_trend.iloc[-1]['VWAP']
                            if pd.isna(current_vwap): current_vwap = current_trend_close
                            
                        if flt_type == 'ema':
                            df_trend['EMA'] = df_trend['close'].ewm(span=p_ema, adjust=False).mean()
                            current_ema = df_trend.iloc[-1]['EMA']

                        if flt_type == 'vwap':
                            macro_trend_up = (current_trend_close > current_vwap)
                        elif flt_type == 'ema':
                            macro_trend_up = (current_trend_close > current_ema)

                levels      = gann_active_levels(symbol)
                margin      = sym_state['gann_touch_margin_pts'] * SYMBOL_INFO[symbol]['pip_value']

                for tf in enabled_tfs:
                    if any(isinstance(v, dict) and v.get('tf') == tf for v in sym_state['gann_open_trades'].values()) or tf in sym_state['gann_open_trades'].values(): continue 

                    need = sym_state['gann_atr_period'] + 50
                    candles = await fetch_candles(symbol, tf, count=need)
                    if not candles or len(candles) < 3: continue
                    close_px = float(candles[-1]['close'])
                    live_px  = close_px 

                    trend_up = True
                    if sym_state['gann_entry_mode'] == 'touch_trend':
                        if macro_trend_up is None: continue 
                        trend_up = macro_trend_up

                    for lv in levels:
                        k = lv['key']; dir = lv['dir']
                        combo_key = f"{k}_{tf}" if bot_state['prot_allow_multi_tf'] else k
                        status = sym_state['gann_level_status'].get(combo_key)
                        if status == 'used': continue

                        is_buy = (dir == 'dn')
                        
                        if sym_state['gann_entry_mode'] == 'touch_trend':
                            if is_buy and not trend_up: continue
                            if not is_buy and trend_up: continue

                        if abs(live_px - lv['price']) <= margin:
                            if flt_type == 'vwap': flt_label = f"VWAP={sym_state['trend_vwap_period']}\n"
                            elif flt_type == 'ema': flt_label = f"EMA={sym_state['trend_ema_period']}\n"
                            else: flt_label = f"VWAP+EMA"
                            
                            reason = f"لمس دعم 🟢 (مع {flt_label}_{ttf.upper()})" if is_buy else f"لمس مقاومة 🔴 (مع {flt_label}_{ttf.upper()})\n"
                            await _gann_open_trade(symbol, is_buy, lv, candles, reason=reason, tf=tf)
                            break
                            
        except Exception as e: c_log(f'Gann monitor scanner error: {e}')
        await asyncio.sleep(15)

# ─────────────────────────────────────────────────────────────
# PRO BACKTEST ENGINE (Macro Trend & Smart Break-Even)
# ─────────────────────────────────────────────────────────────

async def gann_cycle_manager() -> None:
    c_log('Gann cycle manager started.')
    while True:
        try:
            if bot_state['status'] != 'RUNNING':
                await asyncio.sleep(60); continue

            now_utc = datetime.now(timezone.utc)
            active_symbols = [s for s, on in bot_state['active_symbols'].items() if on]
            for symbol in active_symbols:
                sym_state = bot_state['symbol_state'][symbol]
                if not sym_state['gann_cycle_active']:
                    continue
                    
                cycle_h = sym_state['gann_cycle_hours']
                last_h1 = await _gann_fetch_last_closed_h1(symbol)
                
                if last_h1:
                    h1_time = last_h1['time']
                    # Check if this new H1 candle is newer than our currently tracked one
                    if not sym_state['gann_last_h1_time'] or h1_time > sym_state['gann_last_h1_time']:
                        # Only trigger if the difference in hours is >= cycle_h
                        if not sym_state['gann_last_h1_time'] or (h1_time - sym_state['gann_last_h1_time']).total_seconds() / 3600.0 >= cycle_h:
                            h1_close = float(last_h1['close'])
                            sym_state['gann_levels'] = gann_calc_levels(symbol, h1_close)
                            sym_state['gann_close_used'] = h1_close
                            sym_state['gann_last_h1_time'] = h1_time
                            sym_state['gann_cycle_started_at'] = now_utc
                            
                            # Clear used levels so we can take trades again!
                            sym_state['gann_level_status'] = {}
                            
                            c_log(f'[{symbol}] New {cycle_h}h cycle started at {h1_close}')
                            await send_tg_msg(f"🔄 <b>تحديث دورة جان ({cycle_h}h)</b>\nالزوج: {symbol}\nإغلاق H1: {h1_close:.5f}\nتم تصفير المستويات لتبدأ من جديد!")
                            
        except Exception as e:
            c_log(f'Cycle manager error: {e}')
        
        await asyncio.sleep(60)

async def run_gann_backtest(start_dt: datetime, end_dt: datetime) -> None:
    global _bt_progress
    bot_state['is_backtesting'] = True
    fname = f"GannBT_{datetime.now(timezone.utc).strftime('%H%M%S')}.xlsx"
    
    active_symbols = [s for s, on in bot_state['active_symbols'].items() if on]
    if not active_symbols:
        bot_state['is_backtesting'] = False
        return
        
    first_sym_state = bot_state['symbol_state'][active_symbols[0]]
    
    enabled_tfs = [tf for tf, on in first_sym_state['gann_monitor_tfs'].items() if on] or ['5m']
    flt_type = first_sym_state['trend_filter_type']
    ttf = first_sym_state['trend_timeframe']
    desc_ttf = ttf.upper()
    
    if first_sym_state['gann_entry_mode'] == 'touch_trend':
        if flt_type == 'vwap': desc_mode = f"Touch(VWAP{first_sym_state['trend_vwap_period']}_{desc_ttf})\n"
        elif flt_type == 'ema': desc_mode = f"Touch(EMA{first_sym_state['trend_ema_period']}_{desc_ttf})\n"
        else: desc_mode = f"Touch(VWAP+EMA_{desc_ttf})\n"
    else:
        desc_mode = "Pure Touch"
        
    desc_be = " | 🛡️ BE" if first_sym_state['break_even_enabled'] else ""
    
    zf = first_sym_state['gann_zone_filter']
    if zf == 'star': desc_star = "⭐ الأصلية"
    elif zf == 'star_fan': desc_star = "⭐🌀 الأصلية والمروحة"
    else: desc_star = "📋 الكل"
        
    desc_tfs = "+".join(enabled_tfs)
    syms_label = "+".join(active_symbols)
    
    prog = BtProgress(label=f"{syms_label} جان H1→[{desc_tfs}] | {desc_mode} | {desc_star}{desc_be}", active_tfs=['H1']); _bt_progress = prog
    await prog.start(bot_state['chat_id'])

    res = {'win': 0, 'loss': 0, 'be': 0, 'total_prof': 0.0, 'total_win_usd': 0.0, 'total_loss_usd': 0.0, 'peak_equity': 0.0, 'max_dd': 0.0, 'trade_logs': [], 'cycle_logs': []}
    
    try:
        delta_hours = int((end_dt - start_dt).total_seconds() / 3600)
        
        # PHASE 1: Data Gathering & Signal Generation
        all_signals = []
        all_candles_events = []
        
        for symbol in active_symbols:
            sym_state = bot_state['symbol_state'][symbol]
            cycle_h = sym_state['gann_cycle_hours']; tpsl_mode = sym_state['gann_tpsl_mode']
            pv  = SYMBOL_INFO[symbol]['pip_value']; lot = sym_state['lot_size']; margin = sym_state['gann_touch_margin_pts'] * pv
            cs  = SYMBOL_INFO[symbol]['contract_size'];
            prec = SYMBOL_INFO[symbol]['prec'];
            
            quote = symbol.split('_')[1] if '_' in symbol else 'USD'
            quote_conv = {'USD': 1.0, 'JPY': 1/150.0, 'AUD': 0.66, 'NZD': 0.61, 'EUR': 1.08, 'GBP': 1.27, 'CAD': 0.73, 'CHF': 1.11}.get(quote, 1.0)

            await prog.set_phase(f'جلب بيانات الترند ({desc_ttf})...')
            max_period = max(sym_state['trend_vwap_period'], sym_state['trend_ema_period'], 100)
            trend_count = (delta_hours * (2 if ttf == '30m' else 1)) + max_period + 10
            candles_trend = await fetch_candles(symbol, ttf, count=trend_count, end_time=end_dt)
            if not candles_trend: continue

            df_trend = pd.DataFrame(candles_trend)
            if flt_type == 'vwap':
                p_vwap = sym_state['trend_vwap_period']
                df_trend['Typical_Price'] = (df_trend['high'] + df_trend['low'] + df_trend['close']) / 3
                df_trend['VWAP'] = (df_trend['Typical_Price'] * df_trend['volume']).rolling(window=p_vwap).sum() / df_trend['volume'].rolling(window=p_vwap).sum()
            if flt_type == 'ema':
                p_ema = sym_state['trend_ema_period']
                df_trend['EMA'] = df_trend['close'].ewm(span=p_ema, adjust=False).mean()

            df_trend.set_index('time', inplace=True)
            if flt_type == 'vwap': df_trend['macro_trend_up'] = df_trend['close'] > df_trend['VWAP']
            elif flt_type == 'ema': df_trend['macro_trend_up'] = df_trend['close'] > df_trend['EMA']
            elif flt_type == 'both':
                c1_up = df_trend['close'] > df_trend['VWAP']; c2_up = df_trend['close'] > df_trend['EMA']
                c1_dn = df_trend['close'] < df_trend['VWAP']; c2_dn = df_trend['close'] < df_trend['EMA']
                df_trend['macro_trend_up'] = np.where(c1_up & c2_up, True, np.where(c1_dn & c2_dn, False, None))

            await prog.set_phase('جلب بيانات H1...')
            candles_h1 = await fetch_candles(symbol, '1h', count=delta_hours + 10, end_time=end_dt)
            if not candles_h1: continue

            await prog.set_phase('جلب شموع الفريمات الصغيرة...')
            monitor_tfs_data = {}
            days_diff = (end_dt - start_dt).days or 1
            # Always fetch 1m for high-resolution price tracking during simulation
            need_1m = days_diff * 24 * 60 + 300
            mc_1m = await fetch_candles(symbol, '1m', count=need_1m, end_time=end_dt)
            if mc_1m:
                for c in mc_1m:
                    all_candles_events.append({'time': c['time'], 'symbol': symbol, 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'tf': '1m_track'})

            for btf in enabled_tfs:
                bmin = int(''.join(filter(str.isdigit, btf)))
                if 'h' in btf: bmin *= 60
                need_m = days_diff * 24 * (60 // max(bmin, 1)) + 300
                mc = await fetch_candles(symbol, btf, count=need_m, end_time=end_dt)
                if mc: 
                    monitor_tfs_data[btf] = sorted(mc, key=lambda c: c['time'])
                    # We only need to add these to events if they might trigger signals at times not covered by 1m
                    # But 1m covers everything. Still, let's keep them in events for safety.
                    for c in mc:
                        all_candles_events.append({'time': c['time'], 'symbol': symbol, 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close']), 'tf': btf})

            start_ts = start_dt.timestamp(); end_ts = end_dt.timestamp()
            valid_h1 = [c for c in candles_h1 if start_ts <= (c['time'].timestamp() + 3600) <= end_ts]
            
            trend_freq = '30min' if ttf == '30m' else '1h'

            for idx, h1 in enumerate(valid_h1):
                if prog.cancelled: return
                await asyncio.sleep(0)
                t_start = h1['time'] + timedelta(hours=1)
                t_end   = t_start + timedelta(hours=cycle_h)
                close   = float(h1['close'])
                levels = gann_calc_levels(symbol, close)
                f_mode = sym_state['gann_zone_filter']
                active_lv = [l for l in levels if l['dir'] != 'ref' and (f_mode == 'all' or (f_mode == 'star' and l['star']) or (f_mode == 'star_fan' and (l['star'] or l['fan'])))]
                
                res['cycle_logs'].append({'symbol': symbol, 'time_ts': h1['time'].timestamp(), 'time_dt': h1['time'], 'close': close, 'levels': len(active_lv)})
                
                level_used = set()
                for btf, candles_m in monitor_tfs_data.items():
                    m_window = [c for c in candles_m if t_start <= c['time'] < t_end]
                    m_before = [c for c in candles_m if c['time'] < t_start]
                    atr_val  = _gann_atr(m_before, sym_state['gann_atr_period']) if tpsl_mode == 'atr' else None

                    for bar in m_window:
                        bar_close = float(bar['close']); bar_time = bar['time']
                        trend_up = True
                        if sym_state['gann_entry_mode'] == 'touch_trend':
                            trend_time = bar_time.floor(trend_freq)
                            if trend_time in df_trend.index:
                                val = df_trend.loc[trend_time, 'macro_trend_up']
                                if isinstance(val, pd.Series): val = val.iloc[-1]
                                macro_trend_up = None if pd.isna(val) else bool(val)
                            else: macro_trend_up = None
                            if macro_trend_up is None: continue
                            trend_up = macro_trend_up

                        for lv in active_lv:
                            k = lv['key']; dir = lv['dir']; combo_key = f'{k}_{btf}' if bot_state['prot_allow_multi_tf'] else k
                            if combo_key in level_used: continue
                            is_buy = (dir == 'dn')
                            if sym_state['gann_entry_mode'] == 'touch_trend':
                                if is_buy and not trend_up: continue
                                if not is_buy and trend_up: continue
                            if abs(bar_close - lv['price']) > margin: continue
                            
                            entry = lv['price']
                            be_trigger_px = None
                            if sym_state['break_even_enabled']:
                                if is_buy:
                                    hl = [l['price'] for l in levels if l['price'] > entry]
                                    if hl: be_trigger_px = min(hl)
                                else:
                                    ll = [l['price'] for l in levels if l['price'] < entry]
                                    if ll: be_trigger_px = max(ll)

                            tf_tp = _gann_tf_tp(symbol, btf); tf_sl = _gann_tf_sl(symbol, btf)
                            if tpsl_mode == 'atr' and atr_val:
                                sl_d = atr_val * sym_state['gann_atr_sl_mult']
                                tp_d = atr_val * sym_state['gann_atr_tp_mult']
                            else:
                                sl_d = tf_sl * pv; tp_d = tf_tp * pv

                            tp_px = entry + tp_d if is_buy else entry - tp_d
                            sl_px = entry - sl_d if is_buy else entry + sl_d
                            
                            all_signals.append({
                                'time': bar_time, 'symbol': symbol, 'is_buy': is_buy, 'entry': entry,
                                'tp_px': tp_px, 'sl_px': sl_px, 'sl_d': sl_d, 'tp_d': tp_d, 'be_trigger_px': be_trigger_px,
                                'lot': lot, 'cs': cs, 'quote_conv': quote_conv, 'tf': btf, 'combo_key': combo_key,
                                'cycle_time': h1['time'], 'cycle_close': close, 'level_key': k
                            })
                            level_used.add(combo_key)
        
        # PHASE 2: Chronological Event-Driven Simulation
        await prog.set_phase('محاكاة الصفقات الزمنية (تقييم الأرباح العائمة)...')
        c_log(f'BT: Sorting {len(all_signals)} signals')
        all_signals.sort(key=lambda x: x['time'])
        c_log(f'BT: Sorting {len(all_candles_events)} events')
        all_candles_events.sort(key=lambda x: x['time'])
        
        open_trades = []
        closed_trades = []
        suspended_days = {}
        daily_pl = 0.0
        current_day = None
        latest_price = {}
        
        signal_idx = 0
        total_signals = len(all_signals)
        
        dd_limit = - float(bot_state['prot_daily_dd_usd'])
        profit_limit = float(bot_state['prot_daily_profit_usd'])
        
        total_events = len(all_candles_events)
        await prog.set_tf('محاكاة عائمة', total_events)
        
        for i, event in enumerate(all_candles_events):
            if i % 5000 == 0:
                await asyncio.sleep(0)
            if prog.cancelled: break
            t = event['time']; sym = event['symbol']; h = event['high']; l = event['low']; c = event['close']
            day_str = _utc_to_dam(t).strftime('%Y-%m-%d')
            latest_price[sym] = c
            
            if day_str != current_day:
                current_day = day_str
                daily_pl = 0.0
            
            # Check floating PnL against limits
            if current_day not in suspended_days:
                floating_pl = 0.0
                for tr in open_trades:
                    lp = latest_price.get(tr['symbol'], tr['entry'])
                    diff = (lp - tr['entry']) if tr['is_buy'] else (tr['entry'] - lp)
                    floating_pl += round(diff * tr['lot'] * tr['cs'] * tr['quote_conv'], 2)
                    
                total_daily = daily_pl + floating_pl
                if dd_limit < 0 and total_daily <= dd_limit:
                    suspended_days[current_day] = f'🛑 تراجع عائم ({round(total_daily, 2)}$)'
                elif profit_limit > 0 and total_daily >= profit_limit:
                    suspended_days[current_day] = f'✅ هدف عائم ({round(total_daily, 2)}$)'
                    
                if current_day in suspended_days:
                    # Close all open trades at current market price!
                    for tr in open_trades:
                        lp = latest_price.get(tr['symbol'], tr['entry'])
                        diff = (lp - tr['entry']) if tr['is_buy'] else (tr['entry'] - lp)
                        p_usd = round(diff * tr['lot'] * tr['cs'] * tr['quote_conv'], 2)
                        tr['outcome'] = 'DAILY_LIMIT'
                        tr['p_usd'] = p_usd
                        tr['close_time'] = t
                        closed_trades.append(tr)
                        daily_pl += p_usd
                    open_trades.clear()
            
            # Process Exits for open trades (if not suspended)
            if current_day not in suspended_days:
                surviving_trades = []
                for tr in open_trades:
                    if tr['symbol'] != sym:
                        surviving_trades.append(tr)
                        continue
                        
                    is_buy = tr['is_buy']; sl_current = tr['sl_current']; entry = tr['entry']
                    be_trigger_px = tr['be_trigger_px']; tp_px = tr['tp_px']; sl_d = tr['sl_d']
                    lot = tr['lot']; cs = tr['cs']; quote_conv = tr['quote_conv']
                    
                    closed = False
                    if is_buy:
                        if l <= sl_current:
                            tr['outcome'] = 'BREAK_EVEN' if sl_current == entry else 'LOSS'
                            tr['p_usd'] = 0.0 if sl_current == entry else -round(sl_d * lot * cs * quote_conv, 2)
                            closed = True
                        elif not tr['be_activated'] and be_trigger_px is not None and h >= be_trigger_px:
                            tr['sl_current'] = entry
                            tr['be_activated'] = True
                        elif h >= tp_px:
                            tr['outcome'] = 'WIN'
                            tr['p_usd'] = round(tr['tp_d'] * lot * cs * quote_conv, 2)
                            closed = True
                    else:
                        if h >= sl_current:
                            tr['outcome'] = 'BREAK_EVEN' if sl_current == entry else 'LOSS'
                            tr['p_usd'] = 0.0 if sl_current == entry else -round(sl_d * lot * cs * quote_conv, 2)
                            closed = True
                        elif not tr['be_activated'] and be_trigger_px is not None and l <= be_trigger_px:
                            tr['sl_current'] = entry
                            tr['be_activated'] = True
                        elif l <= tp_px:
                            tr['outcome'] = 'WIN'
                            tr['p_usd'] = round(tr['tp_d'] * lot * cs * quote_conv, 2)
                            closed = True
                            
                    if closed:
                        tr['close_time'] = t
                        daily_pl += tr['p_usd']
                        closed_trades.append(tr)
                    else:
                        surviving_trades.append(tr)
                open_trades = surviving_trades
            
            # Process Entries
            while signal_idx < total_signals and all_signals[signal_idx]['time'] <= t:
                sig = all_signals[signal_idx]
                signal_idx += 1
                if current_day not in suspended_days:
                    sig['sl_current'] = sig['sl_px']
                    sig['be_activated'] = False
                    open_trades.append(sig)
                    
            await prog.tick(i, res['win'], res['loss'], res['be'], res['total_prof'])
            
        # Post-process closed trades to match old format
        c_log(f'BT: Post-processing {len(closed_trades)} closed trades')
        for tr in closed_trades:
            if tr['outcome'] == 'WIN' or (tr['outcome'] == 'DAILY_LIMIT' and tr['p_usd'] > 0): 
                res['win'] += 1; res['total_win_usd'] += tr['p_usd']
            elif tr['outcome'] == 'LOSS' or (tr['outcome'] == 'DAILY_LIMIT' and tr['p_usd'] < 0): 
                res['loss'] += 1; res['total_loss_usd'] += abs(tr['p_usd'])
            elif tr['outcome'] == 'BREAK_EVEN' or (tr['outcome'] == 'DAILY_LIMIT' and tr['p_usd'] == 0): 
                res['be'] += 1
            
            res['total_prof'] += tr['p_usd']
            dir_str = 'BUY 📈' if tr['is_buy'] else 'SELL 📉'
            res['trade_logs'].append({
                'الزوج': tr['symbol'], 
                'وقت الصفقة (DAM)': _utc_to_dam(tr['time']).strftime('%Y-%m-%d %H:%M'),
                'TF': tr['tf'], 
                'اتجاه': dir_str, 
                'المستوى (الدخول)': f"{tr['entry']:.2f} ({tr['level_key']})",
                'الهدف (TP)': round(tr['tp_px'], 2),
                'الوقف (SL)': round(tr['sl_px'], 2),
                'النتيجة': tr['outcome'], 
                'ربح ($)': tr['p_usd'],
                'cycle_ts': tr['cycle_time'].timestamp(),
                'cycle_time_str': _utc_to_dam(tr['cycle_time']).strftime('%Y-%m-%d %H:%M'),
                'cycle_close': tr['cycle_close']
            })
            
        res['trade_logs'].sort(key=lambda x: x['وقت الصفقة (DAM)'])
        
        running_eq = 5000.0
        peak_eq = 5000.0
        max_dd = 0.0
        for t_log in res['trade_logs']:
            running_eq += t_log['ربح ($)']
            t_log['رصيد تراكمي ($)'] = round(running_eq, 2)
            if running_eq > peak_eq: peak_eq = running_eq
            dd = peak_eq - running_eq
            if dd > max_dd: max_dd = dd
            
        res['peak_equity'] = peak_eq
        res['max_dd'] = max_dd

        if not res['trade_logs']:
            await prog.done('<b>باكتيست اكتمل ✅</b>\nلا توجد صفقات في هذا النطاق.')
            bot_state['is_backtesting'] = False; return

        await prog.set_phase('إنشاء ملف Excel المنسق...')
        
        c_log('BT: Generating Excel')
        
        sum_text = (
            f"جان {syms_label} | {desc_mode} | {desc_star}\n"
            f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}\n\n"
            f"Net: {'PROFIT ▲' if res['total_prof']>=0 else 'LOSS ▼'} ${round(res['total_prof'], 2)}\n"
            f"Win:  +${round(res['total_win_usd'], 2)} ({res['win']})\n"
            f"Loss: -${round(res['total_loss_usd'], 2)} ({res['loss']})\n"
            f"Break-Even: $0 ({res['be']})\n"
            f"WR: {round(res['win']/max(1, res['win']+res['loss'])*100)}% ({len(res['trade_logs'])} صفقة)\n"
            f"Max DD: ${round(res['max_dd'],2)} ({round((res['max_dd']/max(1,res['peak_equity']))*100)}%)\n"
        )
        
        if suspended_days:
            sum_text += "\nالتعليق بسبب حماية رأس المال:\n"
            for d_str, rsn in suspended_days.items():
                sum_text += f"- {d_str}: {rsn}\n"
                
        sum_text += f"\nدورات H1: {len(res['cycle_logs'])}  |  TP/SL: {str('ATR' if tpsl_mode=='atr' else 'نقاط ثابتة')} | Lot: {lot}"

        wb = openpyxl.Workbook()
        ws_trades = wb.active
        ws_trades.title = "الصفقات"
        
        headers = ["الزوج", "وقت الصفقة (DAM)", "TF", "اتجاه", "المستوى (الدخول)", "الهدف (TP)", "الوقف (SL)", "النتيجة", "ربح ($)", "رصيد تراكمي ($)"]
        ws_trades.append(headers)
        
        gray_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
        header_fill = PatternFill(start_color="E2E3E5", end_color="E2E3E5", fill_type="solid")
        red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        be_fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
        
        for cell in ws_trades[1]:
            cell.fill = gray_fill
            cell.font = Font(bold=True)
            
        current_cycle = None
        for tr in res['trade_logs']:
            if tr['cycle_ts'] != current_cycle:
                current_cycle = tr['cycle_ts']
                ws_trades.append([f"دورة H1: {tr['cycle_time_str']}  |  إغلاق H1: {tr['cycle_close']:.2f}"] + [""]*9)
                row_idx = ws_trades.max_row
                ws_trades.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=10)
                ws_trades.cell(row=row_idx, column=1).fill = header_fill
                ws_trades.cell(row=row_idx, column=1).font = Font(bold=True)
                
            row_data = [
                tr['الزوج'], tr['وقت الصفقة (DAM)'], tr['TF'], tr['اتجاه'], tr['المستوى (الدخول)'],
                tr['الهدف (TP)'], tr['الوقف (SL)'], tr['النتيجة'], tr['ربح ($)'], tr['رصيد تراكمي ($)']
            ]
            ws_trades.append(row_data)
            row_idx = ws_trades.max_row
            
            fill = None
            if tr['النتيجة'] == 'WIN': fill = green_fill
            elif tr['النتيجة'] == 'LOSS': fill = red_fill
            elif tr['النتيجة'] == 'BREAK_EVEN': fill = be_fill
            
            if fill:
                for col in range(1, 11):
                    ws_trades.cell(row=row_idx, column=col).fill = fill

        ws_cycles = wb.create_sheet("دورات H1")
        ws_cycles.append(["الزوج", "الدورة (DAM)", "إغلاق H1", "عدد الصفقات", "ملاحظة"])
        for cell in ws_cycles[1]: cell.fill = gray_fill; cell.font = Font(bold=True)
        
        for cycle in res['cycle_logs']:
            num_trades = len([t for t in res['trade_logs'] if t['cycle_ts'] == cycle['time_ts']])
            note = f"تم تنفيذ {num_trades} صفقة" if num_trades > 0 else "لم يلمس السعر أي مستوى"
            ws_cycles.append([cycle['symbol'], _utc_to_dam(cycle['time_dt']).strftime('%Y-%m-%d %H:%M'), cycle['close'], num_trades, note])
            
        ws_susp = wb.create_sheet("أيام الإيقاف")
        ws_susp.append(["التاريخ", "السبب (النتيجة)"])
        for cell in ws_susp[1]: cell.fill = gray_fill; cell.font = Font(bold=True)
        for d_str, rsn in suspended_days.items():
            ws_susp.append([d_str, rsn])
            

        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
        center_align = Alignment(horizontal='center', vertical='center')

        for ws in [ws_trades, ws_cycles, ws_susp]:
            for row in ws.iter_rows():
                for cell in row:
                    cell.border = thin_border
                    cell.alignment = center_align

        from openpyxl.utils import get_column_letter
        for i in range(1, 11): ws_trades.column_dimensions[get_column_letter(i)].width = 22.0
        for i in range(1, 6): ws_cycles.column_dimensions[get_column_letter(i)].width = 22.0
        for i in range(1, 3): ws_susp.column_dimensions[get_column_letter(i)].width = 25.0
        
        wb.save(fname)
        
        await prog.done(f'<b>باكتيست جان اكتمل ✅</b>\n{syms_label} H1→[{desc_tfs}] | {desc_mode} | {desc_star}{desc_be}\n{start_dt.strftime("%Y-%m-%d")} → {end_dt.strftime("%Y-%m-%d")}\n\nNet: {"PROFIT ▲" if res["total_prof"]>=0 else "LOSS ▼"} ${round(res["total_prof"], 2)}\nWin:  +${round(res["total_win_usd"], 2)} ({res["win"]})\nLoss: -${round(res["total_loss_usd"], 2)} ({res["loss"]})\nBreak-Even: $0.0 ({res["be"]})\nWR: {round(res["win"]/max(1, res["win"]+res["loss"])*100)}% ({len(res["trade_logs"])} صفقة)\nMax DD: ${round(res["max_dd"],2)} ({round((res["max_dd"]/max(1,res["peak_equity"]))*100)}%)\nدورات H1: {len(res["cycle_logs"])}  |  TP/SL: {"ATR" if tpsl_mode=="atr" else "نقاط ثابتة"} | Lot: {lot}\n\nإرسال ملف Excel...')
        await send_tg_document(fname, sum_text)
        os.remove(fname)

    except Exception as e:
        c_log(f'BT Error: {e}'); bot_state['is_backtesting'] = False
        if _bt_progress:
            import html
            try: await _bt_progress.done(f'❌ خطأ داخلي في الباكتيست:\n{html.escape(str(e))}')
            except: pass
    finally:
        bot_state['is_backtesting'] = False

async def check_metaapi_status_command(chat_id: int):
    if not METAAPI_TOKEN or METAAPI_TOKEN == 'YOUR_METAAPI_TOKEN':
        await send_tg_msg("❌ MetaAPI Token غير مهيأ.")
        return
    if not ACCOUNT_ID or ACCOUNT_ID == 'YOUR_ACCOUNT_ID':
        await send_tg_msg("❌ Account ID غير مهيأ.")
        return
        
    await send_tg_msg("⏳ جاري فحص حالة الحساب من MetaAPI...")
    api = MetaApi(METAAPI_TOKEN)
    try:
        account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
        state = account.state
        conn_status = account.connection_status
        
        msg = f"<b>حالة الحساب (MetaAPI)</b>\n"
        msg += f"الاسم: {account.name}\n"
        msg += f"الحالة: {state}\n"
        msg += f"الاتصال: {conn_status}\n\n"
        
        if state == 'DEPLOYED' and conn_status == 'CONNECTED':
            conn = account.get_rpc_connection()
            await conn.connect()
            await conn.wait_synchronized()
            
            acc_info = await conn.get_account_information()
            msg += f"<b>الرصيد:</b> {acc_info.get('balance', 0):.2f}\n"
            msg += f"<b>الاكويتي:</b> {acc_info.get('equity', 0):.2f}\n"
            msg += f"<b>الهامش المتاح:</b> {acc_info.get('freeMargin', 0):.2f}\n\n"
            
            positions = await conn.get_positions()
            msg += f"<b>الصفقات المفتوحة:</b> {len(positions)}\n"
            for p in positions:
                msg += f"🔸 {p['symbol']} | {p['type']} | {p['volume']} | Profit: {p.get('profit', 0):.2f}\n"
                
        else:
            msg += "⚠️ الحساب غير متصل حالياً لجلب تفاصيل الرصيد والصفقات."
            
        await send_tg_msg(msg)
    except Exception as e:
        import html
        await send_tg_msg(f"❌ خطأ في الاتصال بـ MetaAPI:\n{html.escape(str(e))}")

async def _handle_callback(d: str, chat_id: int, msg_id: int) -> None:
    if d == 'check_metaapi_status':
        asyncio.create_task(check_metaapi_status_command(chat_id))
        return

    sym = bot_state['ui_selected_symbol']
    sym_state = bot_state['symbol_state'][sym]
    if d == 'menu_main': await _show(chat_id, msg_id, 'القائمة الرئيسية:', get_main_keyboard())
    elif d == 'menu_main':
        await _show(chat_id, msg_id, '<b>مرحباً بك في Gold Scalper Bot v8.9</b>', get_main_keyboard())

    elif d == 'menu_presets':
        kbd = {'inline_keyboard': [
            [{'text': '💾 حفظ كـ Preset 1', 'callback_data': 'save_preset_1'}, {'text': '📂 تحميل Preset 1', 'callback_data': 'load_preset_1'}],
            [{'text': '💾 حفظ كـ Preset 2', 'callback_data': 'save_preset_2'}, {'text': '📂 تحميل Preset 2', 'callback_data': 'load_preset_2'}],
            [{'text': '💾 حفظ كـ Preset 3', 'callback_data': 'save_preset_3'}, {'text': '📂 تحميل Preset 3', 'callback_data': 'load_preset_3'}],
            [{'text': '🔙 رجوع', 'callback_data': 'menu_main'}]
        ]}
        await _show(chat_id, msg_id, '<b>إدارة الإعدادات (Presets):</b>\nهنا يمكنك حفظ إعدادات جميع الأزواج واستعادتها لاحقاً.', kbd)
    elif d.startswith('save_preset_'):
        p_num = d.split('_')[-1]
        try:
            with open('presets.json', 'r') as f: data = json.load(f)
        except: data = {}
        data[f'preset_{p_num}'] = bot_state['symbol_state']
        with open('presets.json', 'w') as f: json.dump(data, f)
        await send_tg_msg(f"✅ تم حفظ الإعدادات الحالية في Preset {p_num}")
    elif d.startswith('load_preset_'):
        p_num = d.split('_')[-1]
        try:
            with open('presets.json', 'r') as f: data = json.load(f)
            if f'preset_{p_num}' in data:
                # Load settings, but keep live data like open_trades and gann_levels untouched
                for s_name, s_data in data[f'preset_{p_num}'].items():
                    if s_name in bot_state['symbol_state']:
                        for k, v in s_data.items():
                            if k not in ['gann_levels', 'gann_level_status', 'gann_cycle_active', 'gann_open_trades', 'gann_last_h1_time', 'gann_cycle_started_at', 'auto_trade']:
                                bot_state['symbol_state'][s_name][k] = v
                await send_tg_msg(f"✅ تم تحميل الإعدادات من Preset {p_num} بنجاح!")
            else:
                await send_tg_msg("❌ لا يوجد إعدادات محفوظة في هذا الـ Preset.")
        except Exception as e:
            await send_tg_msg("❌ حدث خطأ أثناء التحميل.")

    elif d == 'menu_protection':
        await _show(chat_id, msg_id, 'إعدادات الحماية:', get_protection_keyboard())
    elif d == 'prot_toggle_multitf':
        bot_state['prot_allow_multi_tf'] = not bot_state['prot_allow_multi_tf']
        await _show(chat_id, msg_id, 'إعدادات الحماية:', get_protection_keyboard())
    elif d == 'prot_dec_dd':
        bot_state['prot_daily_dd_usd'] = max(50, bot_state['prot_daily_dd_usd'] - 50)
        await _show(chat_id, msg_id, 'إعدادات الحماية:', get_protection_keyboard())
    elif d == 'prot_inc_dd':
        bot_state['prot_daily_dd_usd'] = min(5000, bot_state['prot_daily_dd_usd'] + 50)
        await _show(chat_id, msg_id, 'إعدادات الحماية:', get_protection_keyboard())
    elif d == 'prot_dec_profit':
        bot_state['prot_daily_profit_usd'] = max(0, bot_state['prot_daily_profit_usd'] - 100)
        await _show(chat_id, msg_id, 'إعدادات الحماية:', get_protection_keyboard())
    elif d == 'prot_inc_profit':
        bot_state['prot_daily_profit_usd'] = min(10000, bot_state['prot_daily_profit_usd'] + 100)
        await _show(chat_id, msg_id, 'إعدادات الحماية:', get_protection_keyboard())
    elif d == 'menu_gann': await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_show_levels':
        sym = bot_state['ui_selected_symbol']
        if not bot_state['symbol_state'][sym]['gann_levels'] or not bot_state['symbol_state'][sym]['gann_close_used']:
            await send_tg_msg(f'⏳ لا يوجد سلّم نشط لـ {sym}، جاري جلب آخر شمعة H1...')
            last_h1 = await _gann_fetch_last_closed_h1(sym)
            if last_h1:
                h1_close = float(last_h1['close'])
                bot_state['symbol_state'][sym]['gann_levels']          = gann_calc_levels(sym, h1_close)
                bot_state['symbol_state'][sym]['gann_close_used']       = h1_close
                bot_state['symbol_state'][sym]['gann_last_h1_time']     = last_h1['time']
                bot_state['symbol_state'][sym]['gann_cycle_started_at'] = datetime.now(timezone.utc)
                bot_state['symbol_state'][sym]['gann_cycle_active']     = True
            else:
                await send_tg_msg('❌ تعذّر جلب البيانات.'); return
        await send_tg_msg(_gann_fmt_levels_msg(sym, bot_state['symbol_state'][sym]['gann_close_used']))
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_entry':
        sym_state['gann_entry_mode'] = 'pure_touch' if sym_state['gann_entry_mode'] == 'touch_trend' else 'touch_trend'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_filter':
        current = sym_state['gann_zone_filter']
        if current == 'star': sym_state['gann_zone_filter'] = 'star_fan'
        elif current == 'star_fan': sym_state['gann_zone_filter'] = 'all'
        else: sym_state['gann_zone_filter'] = 'star'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_filter_type':
        current = sym_state['trend_filter_type']
        if current == 'vwap': sym_state['trend_filter_type'] = 'ema'
        else: sym_state['trend_filter_type'] = 'vwap'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_auto_trade':
        sym_state['auto_trade'] = not sym_state.get('auto_trade', False)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_ttf':
        sym_state['trend_timeframe'] = '30m' if sym_state['trend_timeframe'] == '1h' else '1h'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_be':
        sym_state['break_even_enabled'] = not sym_state['break_even_enabled']
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_vwap': 
        sym_state['trend_vwap_period'] = max(10, sym_state['trend_vwap_period'] - 10)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_vwap': 
        sym_state['trend_vwap_period'] = min(500, sym_state['trend_vwap_period'] + 10)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_ema': 
        sym_state['trend_ema_period'] = max(10, sym_state['trend_ema_period'] - 10)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_ema': 
        sym_state['trend_ema_period'] = min(500, sym_state['trend_ema_period'] + 10)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_filter_help':
        help_txt = ("<b>⚙️ دليل تخصيص القيم:</b>\n\n"
                    "أرسل أمراً مباشراً في الدردشة لتغيير أي قيمة بالصيغة التالية:\n\n"
                    "<b>تغيير فلاتر الترند الشامل:</b>\n"
                    "<code>/set ema 50</code>\n"
                    "<code>/set vwap 100</code>\n\n"
                    "<b>تخصيص الأهداف والوقف لكل فريم:</b>\n"
                    "<code>/set 5m tp 40</code>\n"
                    "<code>/set 15m sl 25</code>\n\n"
                    "سيتم حفظ القيمة وتطبيقها فوراً.")
        await _show(chat_id, msg_id, help_txt, get_gann_keyboard())
    elif d == 'gann_dec_margin': 
        sym_state['gann_touch_margin_pts'] = max(1, sym_state['gann_touch_margin_pts'] - 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_margin': 
        sym_state['gann_touch_margin_pts'] = min(50, sym_state['gann_touch_margin_pts'] + 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_hours': 
        sym_state['gann_cycle_hours'] = max(1, sym_state['gann_cycle_hours'] - 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_hours': 
        sym_state['gann_cycle_hours'] = min(24, sym_state['gann_cycle_hours'] + 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_tpsl':
        sym_state['gann_tpsl_mode'] = 'atr' if sym_state['gann_tpsl_mode'] == 'fixed' else 'fixed'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_tp10': sym_state['gann_tp_points'] = max(10, sym_state['gann_tp_points'] - 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_tp10': sym_state['gann_tp_points'] = min(1000, sym_state['gann_tp_points'] + 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_sl10': sym_state['gann_sl_points'] = max(10, sym_state['gann_sl_points'] - 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_sl10': sym_state['gann_sl_points'] = min(1000, sym_state['gann_sl_points'] + 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_atrp':  sym_state['gann_atr_period'] = max(5,   sym_state['gann_atr_period'] - 1); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_atrp':  sym_state['gann_atr_period'] = min(50,  sym_state['gann_atr_period'] + 1); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_atrsl': sym_state['gann_atr_sl_mult'] = max(0.5, round(sym_state['gann_atr_sl_mult'] - 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_atrsl': sym_state['gann_atr_sl_mult'] = min(5.0, round(sym_state['gann_atr_sl_mult'] + 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_atrtp': sym_state['gann_atr_tp_mult'] = max(0.5, round(sym_state['gann_atr_tp_mult'] - 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_atrtp': sym_state['gann_atr_tp_mult'] = min(8.0, round(sym_state['gann_atr_tp_mult'] + 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d.startswith('gann_toggle_pair_'):
        pair = d[len('gann_toggle_pair_'):]
        bot_state['active_symbols'][pair] = not bot_state['active_symbols'][pair]
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d.startswith('gann_sel_pair_'):
        pair = d[len('gann_sel_pair_'):]
        bot_state['ui_selected_symbol'] = pair
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d.startswith('gann_tf_'):
        tfk = d[len('gann_tf_'):]
        if tfk in sym_state['gann_monitor_tfs']: sym_state['gann_monitor_tfs'][tfk] = not sym_state['gann_monitor_tfs'][tfk]
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_tpsl_tf': await _show(chat_id, msg_id, '⚙️ TP/SL مخصص لكل فريم:', get_gann_tpsl_tf_keyboard())
    elif d.startswith('gann_tptf_sel_'):
        sel_tf = d[len('gann_tptf_sel_'):]; await _show(chat_id, msg_id, f'⚙️ TP/SL [{sel_tf}]:', get_gann_tpsl_tf_keyboard(sel_tf))
    elif d.startswith('gann_tptf_itp_'):
        tf = d[len('gann_tptf_itp_'):]; sym_state['gann_tp_per_tf'][tf] = sym_state['gann_tp_per_tf'].get(tf, 0) + 10; await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_dtp_'):
        tf = d[len('gann_tptf_dtp_'):]; sym_state['gann_tp_per_tf'][tf] = max(0, sym_state['gann_tp_per_tf'].get(tf, 0) - 10); await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_isl_'):
        tf = d[len('gann_tptf_isl_'):]; sym_state['gann_sl_per_tf'][tf] = sym_state['gann_sl_per_tf'].get(tf, 0) + 10; await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_dsl_'):
        tf = d[len('gann_tptf_dsl_'):]; sym_state['gann_sl_per_tf'][tf] = max(0, sym_state['gann_sl_per_tf'].get(tf, 0) - 10); await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_rst_'):
        tf = d[len('gann_tptf_rst_'):]; sym_state['gann_tp_per_tf'][tf] = 0; sym_state['gann_sl_per_tf'][tf] = 0; await _show(chat_id, msg_id, f'⚙️ تمت إعادة الضبط:', get_gann_tpsl_tf_keyboard(tf))
    elif d == 'menu_gann_bt':
        await _show(chat_id, msg_id, 'اختر مدة الباكتيست:', get_gann_bt_keyboard())
    elif d.startswith('gbt_'):
        days = int(d.split('_')[1])
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        if not bot_state['is_backtesting']: asyncio.create_task(run_gann_backtest(start_dt, end_dt))
        await _show(chat_id, msg_id, f'⏳ باكتيست يعمل...', get_gann_bt_keyboard())
    elif d == 'cancel_bt':
        global _bt_progress
        if _bt_progress and bot_state['is_backtesting']: await _bt_progress.cancel()
        bot_state['is_backtesting'] = False
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    else: c_log(f'Unhandled callback: {d}')

# ─────────────────────────────────────────────────────────────
# TELEGRAM POLLING & WATCHDOG
# ─────────────────────────────────────────────────────────────
async def process_tg_update(update: dict) -> None:
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip(); bot_state['chat_id'] = update['message']['chat']['id']
        
        parts = msg.lower().split()
        if parts[0] == '/set':
            sym_state = bot_state['symbol_state'][bot_state['ui_selected_symbol']]
            if len(parts) == 3 and parts[1] in ['ema', 'vwap'] and parts[2].isdigit():
                val = int(parts[2])
                if parts[1] == 'ema': sym_state['trend_ema_period'] = val
                elif parts[1] == 'vwap': sym_state['trend_vwap_period'] = val
                await send_tg_msg(f"✅ <b>تم التحديث بنجاح!</b>\n⚙️ {parts[1].upper()} الشامل: {val}")
                return
            elif len(parts) == 4:
                _, tf, param, val = parts
                if tf in _TFS and param in ['tp', 'sl'] and val.isdigit():
                    val = int(val)
                    if param == 'tp': sym_state['gann_tp_per_tf'][tf] = val
                    elif param == 'sl': sym_state['gann_sl_per_tf'][tf] = val
                    await send_tg_msg(f"✅ <b>تم التحديث بنجاح!</b>\n📌 الفريم: {tf}\n⚙️ {param.upper()}: {val}")
                    return
            await send_tg_msg("❌ <b>صيغة خاطئة!</b>\n<b>أمثلة صحيحة:</b>\n<code>/set ema 50</code>\n<code>/set vwap 100</code>\n<code>/set 5m tp 40</code>\n<code>/set 15m sl 25</code>")
            return

        if parts[0] == '/backtest':
            try:
                if len(parts) == 2:
                    dt = datetime.strptime(parts[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if not bot_state['is_backtesting']: asyncio.create_task(run_gann_backtest(dt, dt + timedelta(days=1)))
                    await send_tg_msg(f"⏳ جاري باكتيست ليوم {parts[1]}...")
                    return
                elif len(parts) == 3:
                    dt1 = datetime.strptime(parts[1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    dt2 = datetime.strptime(parts[2], "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
                    if not bot_state['is_backtesting']: asyncio.create_task(run_gann_backtest(dt1, dt2))
                    await send_tg_msg(f"⏳ جاري باكتيست من {parts[1]} إلى {parts[2]}...")
                    return
            except Exception:
                await send_tg_msg("❌ <b>خطأ في التاريخ!</b>\nالصيغة: <code>/backtest 2026-06-24</code>\nأو <code>/backtest 2026-06-24 2026-06-26</code>")
                return

        if not msg.startswith('/') and msg in bot_state.get('menu_button_map', {}):
            cb = bot_state['menu_button_map'][msg]
            if cb != 'noop': await _handle_callback(cb, bot_state['chat_id'], None)
            return

        if msg.startswith('/setsymbol '):
            new_sym = msg.split(' ')[1].strip()
            bot_state['symbol'] = new_sym
            await send_tg_msg(f"✅ تم تغيير الرمز الخاص بـ MetaTrader إلى: <b>{new_sym}</b>")
        elif msg == '/start': await send_tg_msg('<b>مرحباً بك في Gold Scalper Bot v8.9</b>', get_main_keyboard())
        return

    if 'callback_query' not in update: return
    q = update['callback_query']; d = q['data']; chat_id = q['message']['chat']['id']; msg_id = q['message']['message_id']
    bot_state['chat_id'] = chat_id
    asyncio.create_task(answer_callback(q['id']))
    try: await _handle_callback(d, chat_id, msg_id)
    except Exception as e: c_log(f'CB error [{d}]: {e}')

_poll_task: asyncio.Task | None = None

async def telegram_polling_loop() -> None:
    c_log('Telegram polling started.'); url = f'https://api.telegram.org/bot{TG_TOKEN}/getUpdates'
    backoff = 1
    while True:
        connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300, force_close=True)
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=28)
        sess = aiohttp.ClientSession(connector=connector, timeout=timeout)
        try:
            while True:
                try:
                    async with sess.get(url, params={'offset': bot_state['last_update_id'] + 1, 'timeout': 20}) as resp:
                        if resp.status == 200:
                            backoff = 1; bot_state['last_poll_ok'] = datetime.now(timezone.utc).timestamp()
                            data = await resp.json()
                            for upd in data.get('result', []):
                                bot_state['last_update_id'] = upd['update_id']
                                asyncio.create_task(process_tg_update(upd))
                        else: await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)
                except Exception: await asyncio.sleep(backoff); backoff = min(backoff * 2, 30); break
        except asyncio.CancelledError: await sess.close(); raise
        finally: await sess.close()
        await asyncio.sleep(1)

async def telegram_watchdog() -> None:
    global _poll_task
    await asyncio.sleep(30)
    while True:
        await asyncio.sleep(20)
        last = bot_state.get('last_poll_ok', 0.0); age = datetime.now(timezone.utc).timestamp() - last
        if age > 60 and _poll_task is not None and not _poll_task.done(): _poll_task.cancel()

async def supervised(coro_fn, *args, label: str = '') -> None:
    global _poll_task
    while True:
        try:
            task = asyncio.current_task()
            if label == 'tg_polling': _poll_task = task
            await coro_fn(*args)
        except asyncio.CancelledError: await asyncio.sleep(2)   
        except Exception as e: c_log(f'Task "{label}" crashed: {e}'); await asyncio.sleep(5)

# ─────────────────────────────────────────────────────────────
# ENTRY POINT & WEB SERVER
# ─────────────────────────────────────────────────────────────
async def handle_ping(request: web.Request) -> web.Response:
    return web.Response(text="Bot is running smoothly!")

async def main() -> None:
    get_http()
    app = web.Application()
    app.router.add_get('/', handle_ping)
    
    runner = web.AppRunner(app); await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    c_log(f'Web server started on port {port}')

    bot_state['last_poll_ok'] = datetime.now(timezone.utc).timestamp()

    tasks = [
        asyncio.create_task(supervised(telegram_polling_loop, label='tg_polling')),
        asyncio.create_task(supervised(telegram_watchdog,     label='tg_watchdog')),
        asyncio.create_task(supervised(gann_monitor_scanner,  label='gann_monitor')),
        asyncio.create_task(supervised(gann_cycle_manager,    label='gann_cycle')),
    ]
    
    c_log('Gold Scalper Bot v8.9 started successfully.')
    try: await asyncio.gather(*tasks)
    finally:
        if _http and not _http.closed: await _http.close()

if __name__ == '__main__':
    asyncio.run(main())
