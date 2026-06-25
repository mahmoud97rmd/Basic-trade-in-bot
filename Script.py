"""
Gold Scalper Bot — v5.3 (Gann Levels Engine)
Strategy : Gann H1 Support / Resistance — Touch & Breakout+Retest
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
METAAPI_TOKEN = os.environ.get('METAAPI_TOKEN', 'YOUR_METAAPI_TOKEN')
ACCOUNT_ID    = os.environ.get('ACCOUNT_ID',    'YOUR_ACCOUNT_ID')
TG_TOKEN      = os.environ.get('TG_TOKEN',      '8876071259:AAEfZ0Cw4zpMgUp35ob8CKeCnmySe4ALRq8')

# OANDA REST API
OANDA_ACCOUNT  = os.environ.get('OANDA_ACCOUNT', '101-004-28533521-003')
OANDA_TOKEN    = os.environ.get('OANDA_TOKEN',   '0e282d5a3e65ad6fdd809e2c195bb1cd-9e2158e12fa13840e030ee3081b36fab')
OANDA_SYMBOL   = 'XAU_USD'
OANDA_BASE_URL = 'https://api-fxpractice.oanda.com/v3'  

DD_LIMIT_PCT = 0.03

# ─────────────────────────────────────────────────────────────
# GLOBAL HTTP SESSION
# ─────────────────────────────────────────────────────────────
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
    'chat_id':          None,
    'last_update_id':   0,
    'is_backtesting':   False,
    'connection_obj':   None,
    'account_obj':      None,

    # ── Position tracking ──
    'consecutive_losses': 0,
    'tracked_positions':  {},

    # ── Risk ──
    'lot_size':         0.05,
    'pip_value':        0.1,    # gold: $0.1 per point
    'contract_size':    100,    # oz per lot (XAUUSD)
    'use_max_spread':   True,
    'max_spread_pips':  3.0,

    # ── Break-even ──
    'use_be':           False,
    'be_points':        20,

    # ── Trailing stop ──
    'use_trailing':     False,
    'trail_points':     200,
    'trail_offset':     400,

    # ── Daily P&L ──
    'sod_balance':              None,
    'sod_date':                 None,
    'dd_triggered':             False,
    'daily_target_enabled':     False,
    'daily_target_usd':         100.0,
    'profit_target_triggered':  False,
    'daily_loss_enabled':       False,
    'daily_loss_usd':           100.0,
    'loss_limit_triggered':     False,
    'last_poll_ok':             0.0,

    # ── Gann Levels Engine ──
    'gann_levels':           [],
    'gann_level_status':     {},   # {level_key: 'used'|'broken_up'|'broken_dn'}
    'gann_close_used':       None,
    'gann_last_h1_time':     None,
    'gann_cycle_active':     False,
    'gann_cycle_started_at': None,
    'gann_cycle_hours':      1,
    'gann_cycle_end_flag':   None,
    'gann_open_trades':      {},   # {position_id: tf}

    # ── Entry settings ──
    'gann_zone_filter':      'star',    # 'star' | 'all'
    'gann_entry_mode':       'touch',   # 'touch' | 'breakout_retest'
    'gann_anti_spam':        True,      # True = one trade per level total | False = one per level per TF
    'gann_monitor_tfs': {
        '1m': False, '2m': False, '3m': False, '4m': False, '5m': True,
        '6m': False, '10m': False, '12m': False, '15m': True,
        '20m': False, '30m': False, '60m': False, '120m': False,
    },
    'gann_touch_margin_pts': 5,

    # ── TP/SL ──
    'gann_tpsl_mode':   'fixed',
    'gann_tp_points':   180,
    'gann_sl_points':   100,
    'gann_tp_per_tf':   {tf: 0 for tf in ['1m','2m','3m','4m','5m','6m','10m','12m','15m','20m','30m','60m','120m']},
    'gann_sl_per_tf':   {tf: 0 for tf in ['1m','2m','3m','4m','5m','6m','10m','12m','15m','20m','30m','60m','120m']},
    'gann_atr_period':  14,
    'gann_atr_sl_mult': 1.5,
    'gann_atr_tp_mult': 2.5,

    # ── Trend filter ──
    'gann_trend_filter': 'off',    # 'off' | 'ema200' | 'ema_dual'
    'gann_trend_tf':     '60m',    # TF used to compute EMA
    'gann_ema_single':   200,
    'gann_ema_fast':     50,
    'gann_ema_slow':     150,
}

# ─────────────────────────────────────────────────────────────
# TREND FILTER — EMA (computed from cached candles)
# ─────────────────────────────────────────────────────────────
def _ema(s: 'pd.Series', n: int) -> 'pd.Series':
    return s.ewm(span=n, adjust=False).mean()

async def _gann_trend_direction() -> str | None:
    """Returns 'bull', 'bear', or None if filter is off/failed."""
    mode = bot_state['gann_trend_filter']
    if mode == 'off': return None
    try:
        tf   = bot_state['gann_trend_tf']
        need = max(bot_state['gann_ema_slow'], bot_state['gann_ema_single']) + 50
        candles = await fetch_candles(tf, count=need)
        if not candles or len(candles) < 30: return None
        close = pd.Series([float(c['close']) for c in sorted(candles, key=lambda x: x['time'])])
        if mode == 'ema200':
            ema = _ema(close, bot_state['gann_ema_single'])
            return 'bull' if float(close.iloc[-1]) > float(ema.iloc[-1]) else 'bear'
        elif mode == 'ema_dual':
            fast = _ema(close, bot_state['gann_ema_fast'])
            slow = _ema(close, bot_state['gann_ema_slow'])
            return 'bull' if float(fast.iloc[-1]) > float(slow.iloc[-1]) else 'bear'
    except Exception as e:
        c_log(f'Trend filter error: {e}')
    return None

# ─────────────────────────────────────────────────────────────
# DAILY DRAWDOWN PROTECTOR
# ─────────────────────────────────────────────────────────────
async def _capture_sod_balance() -> None:
    if not (bot_state['live_connected'] and bot_state['connection_obj']): return
    try:
        info  = await bot_state['connection_obj'].get_account_information()
        bal   = float(info.get('balance', 0))
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # --- تصفير يومي ---
        if bot_state.get('sod_date') and bot_state['sod_date'] != today:
            bot_state['consecutive_losses'] = 0
            await send_tg_msg(f'🌅 <b>يوم تداول جديد!</b> تم تصفير عداد الخسائر.')
        # ------------------
        
        bot_state['sod_balance']           = bal
        bot_state['sod_date']              = today
        bot_state['dd_triggered']          = False
        bot_state['profit_target_triggered'] = False
        bot_state['loss_limit_triggered']    = False
        target_line = f'\n🎯 Daily Target: <b>${bot_state["daily_target_usd"]:.2f}</b>\nBot pauses at equity: <b>${bal + bot_state["daily_target_usd"]:.2f}</b>' if bot_state['daily_target_enabled'] else ''
        loss_line = f'\n🛑 Daily Loss Limit: <b>${bot_state["daily_loss_usd"]:.2f}</b>\nBot pauses at equity: <b>${bal - bot_state["daily_loss_usd"]:.2f}</b>' if bot_state['daily_loss_enabled'] else ''
        await send_tg_msg(f'📅 <b>New Day — SOD Snapshot</b>\nOpening Balance: <b>${bal:.2f}</b>\nDaily Loss Limit (3%): <b>${bal * DD_LIMIT_PCT:.2f}</b>\nBot pauses at equity: <b>${bal * (1 - DD_LIMIT_PCT):.2f}</b>{target_line}{loss_line}')
    except Exception as e: c_log(f'DD capture SOD error: {e}')

async def daily_drawdown_monitor() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            if not (bot_state['live_connected'] and bot_state['connection_obj']): continue
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            if bot_state['sod_date'] != today or bot_state['sod_balance'] is None:
                await _capture_sod_balance(); continue
            if bot_state['dd_triggered']: continue
            info     = await bot_state['connection_obj'].get_account_information()
            equity   = float(info.get('equity', 0)); sod = bot_state['sod_balance']
            limit    = sod * (1 - DD_LIMIT_PCT); used_pct = round((sod - equity) / sod * 100, 2) if sod else 0
            if equity <= limit:
                bot_state['status'] = 'PAUSED'; bot_state['dd_triggered'] = True; closed = 0
                try:
                    positions = await bot_state['connection_obj'].get_positions()
                    for p in positions: await bot_state['connection_obj'].close_position(p['id']); closed += 1
                except Exception as ce: c_log(f'DD close error: {ce}')
                await send_tg_msg(f'🚨 <b>BOT AUTO-PAUSED — DD LIMIT REACHED</b> 🚨\n\n💰 SOD:    <b>${sod:.2f}</b>\n📉 Equity: <b>${equity:.2f}</b>\n📊 Loss:   <b>${sod-equity:.2f} ({used_pct}%)</b>\n\n{"Closed " + str(closed) + " position(s)." if closed else "No open positions."}\nPress <b>RUN ▶</b> to resume tomorrow.', get_main_keyboard())
        except Exception as e: c_log(f'DD monitor error: {e}')

# ─────────────────────────────────────────────────────────────
# DAILY PROFIT TARGET
# ─────────────────────────────────────────────────────────────
async def _close_all_positions() -> int:
    closed = 0
    try:
        positions = await bot_state['connection_obj'].get_positions()
        for p in positions: await bot_state['connection_obj'].close_position(p['id']); closed += 1
    except Exception as ce: c_log(f'close_all_positions error: {ce}')
    return closed

async def daily_profit_target_monitor() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            if not (bot_state['daily_target_enabled'] or bot_state['daily_loss_enabled']): continue
            if not (bot_state['live_connected'] and bot_state['connection_obj']): continue
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            if bot_state['sod_date'] != today or bot_state['sod_balance'] is None: continue

            info   = await bot_state['connection_obj'].get_account_information()
            equity = float(info.get('equity', 0)); sod = bot_state['sod_balance']; pnl = equity - sod

            if bot_state['daily_target_enabled'] and not bot_state['profit_target_triggered']:
                target = bot_state['daily_target_usd']
                if pnl >= target:
                    bot_state['status'] = 'PAUSED'; bot_state['profit_target_triggered'] = True; closed = await _close_all_positions()
                    await send_tg_msg(f'🎯🎯🎯 <b>DAILY TARGET REACHED!</b> 🎯🎯🎯\n\n💰 SOD:    <b>${sod:.2f}</b>\n📈 Equity: <b>${equity:.2f}</b>\n✅ Profit: <b>${pnl:.2f}</b>  (target: ${target:.2f})\n\n{"Closed " + str(closed) + " position(s) — profit locked in." if closed else "No open positions."}\n\nBot is now <b>PAUSED</b>.\nPress <b>RUN ▶</b> to resume (target resets tomorrow).', get_main_keyboard())
                    continue

            if bot_state['daily_loss_enabled'] and not bot_state['loss_limit_triggered']:
                limit = bot_state['daily_loss_usd']
                if pnl <= -limit:
                    bot_state['status'] = 'PAUSED'; bot_state['loss_limit_triggered'] = True; closed = await _close_all_positions()
                    await send_tg_msg(f'🛑🛑🛑 <b>DAILY LOSS LIMIT HIT!</b> 🛑🛑🛑\n\n💰 SOD:    <b>${sod:.2f}</b>\n📉 Equity: <b>${equity:.2f}</b>\n❌ Loss:   <b>${pnl:.2f}</b>  (limit: -${limit:.2f})\n\n{"Closed " + str(closed) + " position(s) — losses stopped." if closed else "No open positions."}\n\nBot is now <b>PAUSED</b>.\nPress <b>RUN ▶</b> to resume (limit resets tomorrow).', get_main_keyboard())
        except Exception as e: c_log(f'Profit/Loss target monitor error: {e}')

# ─────────────────────────────────────────────────────────────
# TIME UTILITIES  (Damascus UTC+3)
# ─────────────────────────────────────────────────────────────
DAM_OFF = timedelta(hours=3)
def _now_dam() -> datetime: return datetime.now(timezone.utc) + DAM_OFF
def _utc_to_dam(dt) -> datetime:
    if isinstance(dt, pd.Timestamp): dt = dt.to_pydatetime()
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return dt + DAM_OFF
def _dam_to_utc(dt) -> datetime:
    if isinstance(dt, str): dt = datetime.strptime(dt, '%Y-%m-%d %H:%M')
    if dt.tzinfo is not None: return dt - DAM_OFF
    return (dt - DAM_OFF).replace(tzinfo=timezone.utc)
def _fmt_dam(t) -> str:
    if t is None: return '-'
    dam = _utc_to_dam(t) if not isinstance(t, str) else t
    if hasattr(dam, 'strftime'): return dam.strftime('%Y-%m-%d %H:%M')
    return str(dam)
def _fmt_utc(t) -> str:
    if hasattr(t, 'strftime'): return t.strftime('%Y-%m-%d %H:%M')
    return str(t)

# ─────────────────────────────────────────────────────────────
# DERIV WEBSOCKET FETCHER (OANDA)
# ─────────────────────────────────────────────────────────────
_OANDA_GRAN = {
    '1m': 'M1',  '2m': 'M2',  '3m': 'M3',  '4m': 'M4', '5m': 'M5',  '6m': 'M6',  '8m': 'M8',  '10m': 'M10',
    '12m': 'M12','15m': 'M15','20m': 'M20','30m': 'M30', '45m': 'M45','48m': 'M30',  
    '1h': 'H1',  '2h': 'H2',  '3h': 'H3',  '4h': 'H4', '6h': 'H6',  '8h': 'H8',  '12h': 'H12','1d': 'D', '90m': 'H1',   
}
_oanda_sem: asyncio.Semaphore | None = None
def _get_oanda_sem() -> asyncio.Semaphore:
    global _oanda_sem
    if _oanda_sem is None: _oanda_sem = asyncio.Semaphore(3)
    return _oanda_sem

async def fetch_oanda_candles(granularity_str: str, count: int = 5000, end_time: datetime = None) -> list:
    gran_str = _OANDA_GRAN.get(granularity_str, 'M1'); fetch_count = min(count, 120000)  
    collected = []; remaining = fetch_count
    headers = {'Authorization': f'Bearer {OANDA_TOKEN}', 'Content-Type':  'application/json'}
    url = f'{OANDA_BASE_URL}/instruments/{OANDA_SYMBOL}/candles'
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

            formatted = [{'time': pd.Timestamp(c['time']).tz_convert('UTC'), 'open': float(c['mid']['o']), 'high': float(c['mid']['h']), 'low': float(c['mid']['l']), 'close': float(c['mid']['c'])} for c in complete]
            collected = formatted + collected; remaining -= len(complete)
            earliest = pd.Timestamp(complete[0]['time']).tz_convert('UTC')
            current_end = earliest.to_pydatetime() - timedelta(seconds=1)
            if len(complete) < chunk: break
            await asyncio.sleep(0.2)
    return collected

fetch_candles = fetch_oanda_candles

# ─────────────────────────────────────────────────────────────
# GANN LEVELS ENGINE  (replaces TEMA/MTF strategy engine)
# ─────────────────────────────────────────────────────────────
GANN_ACOEF  = [0.0208, 0.0417, 0.0625, 0.0833, 0.125, 0.25, 0.333, 0.5, 1, 2, 4]
GANN_AIMP   = [False,  False,  False,  True,   False, False, False, True, True, False, False]
GANN_TFC_H1 = 0.02   # نفس معامل الفريم الساعي من حاسبة index.html (TFC['H1'])

def gann_calc_levels(close: float) -> list[dict]:
    """يولّد سلّم الدعوم/المقاومات حول إغلاق H1 — منفذ مباشر لمعادلة index.html (ACOEF × TFC)."""
    levels = []
    for i, coef in enumerate(GANN_ACOEF):
        offset = close * coef * GANN_TFC_H1
        up = round(close + offset, 2)
        dn = round(close - offset, 2)
        star = GANN_AIMP[i]
        levels.append({'key': f'up_{i}', 'price': up, 'dir': 'up', 'star': star})
        if dn > 0:
            levels.append({'key': f'dn_{i}', 'price': dn, 'dir': 'dn', 'star': star})
    levels.append({'key': 'ref', 'price': round(close, 2), 'dir': 'ref', 'star': False})
    levels.sort(key=lambda x: x['price'], reverse=True)
    return levels

def gann_active_levels() -> list[dict]:
    """يرجع المستويات القابلة للتداول حسب فلتر القوة الحالي (⭐ فقط أو الكل)، بدون خط الإغلاق المرجعي."""
    lv = [l for l in bot_state['gann_levels'] if l['dir'] != 'ref']
    if bot_state['gann_zone_filter'] == 'star':
        return [l for l in lv if l['star']]
    return lv

def _gann_fmt_levels_msg(close: float) -> str:
    lines = []
    for l in bot_state['gann_levels']:
        if l['dir'] == 'ref':
            lines.append(f"➖ <b>{l['price']:.2f}</b>  (إغلاق H1)")
            continue
        role = 'مقاومة' if l['dir'] == 'up' else 'دعم'
        star = ' ⭐' if l['star'] else ''
        icon = '🔴' if l['dir'] == 'up' else '🟢'
        lines.append(f"{icon} {l['price']:.2f}  {role}{star}")
    filt = '⭐ القوية فقط' if bot_state['gann_zone_filter'] == 'star' else 'كل المستويات'
    mode = 'لمس (ارتداد)' if bot_state['gann_entry_mode'] == 'touch' else 'كسر + ريتيست (استمرار)'
    return (f"📐 <b>سلّم جان — دورة جديدة</b>\n"
            f"إغلاق H1: <b>{close:.2f}</b>\n"
            f"مدة المراقبة: {bot_state['gann_cycle_hours']}س  |  فلتر: {filt}  |  وضع الدخول: {mode}\n\n"
            + '\n'.join(lines))

async def _gann_fetch_last_closed_h1() -> dict | None:
    candles = await fetch_candles('1h', count=2)
    if not candles: return None
    candles = sorted(candles, key=lambda c: c['time'])
    return candles[-1]   # fetch_oanda_candles يُرجع شموع complete=True فقط، فهذه آخر شمعة H1 مُغلقة فعلياً

async def gann_cycle_manager() -> None:
    """
    يدير دورة حياة سلّم جان:
    - يولّد سلّماً جديداً فقط عند رصد إغلاق H1 جديد (مختلف عن آخر إغلاق استُخدم).
    - بعد التوليد، يبقى السلّم "مجمَّداً" ونشطاً حتى:
        (1) صفقة رابحة تُغلق على هذا السلّم → ينهي الدورة فوراً (الإنهاء يتم خارجياً من محرك التنفيذ
            في Phase 2 بضبط gann_cycle_active=False و gann_cycle_end_flag='win').
        (2) انتهاء مهلة المراقبة (gann_cycle_hours) دون إغلاق صفقة رابحة → يُهجر السلّم هنا مباشرة.
      في حالة الخسارة فقط، السلّم يبقى نشطاً (لا أحد يُغيّر gann_cycle_active) ويستمر رصد بقية
      المستويات حتى تنفد المهلة نفسها.
    """
    c_log('Gann cycle manager started.')
    while True:
        try:
            if bot_state['status'] != 'RUNNING' or not bot_state['live_connected']:
                await asyncio.sleep(10); continue

            last_h1 = await _gann_fetch_last_closed_h1()
            if not last_h1:
                await asyncio.sleep(15); continue

            h1_time   = last_h1['time']
            h1_close  = float(last_h1['close'])
            # أول تشغيل (gann_last_h1_time = None) → ولّد فوراً بدون انتظار شمعة جديدة
            is_new_h1 = (h1_time != bot_state['gann_last_h1_time'])

            if bot_state['gann_cycle_active']:
                started = bot_state['gann_cycle_started_at']
                hours_passed = (datetime.now(timezone.utc) - started).total_seconds() / 3600
                if hours_passed >= bot_state['gann_cycle_hours']:
                    bot_state['gann_cycle_active']   = False
                    bot_state['gann_cycle_end_flag'] = 'timeout'
                    await send_tg_msg(
                        f"⌛ <b>انتهت مهلة المراقبة ({bot_state['gann_cycle_hours']}س)</b>\n"
                        f"تم هجر السلّم الحالي (إغلاق: {bot_state['gann_close_used']:.2f}).\n"
                        f"بانتظار إغلاق شمعة H1 التالية لتوليد سلّم جديد..."
                    )

            if (not bot_state['gann_cycle_active']) and is_new_h1:
                bot_state['gann_levels']           = gann_calc_levels(h1_close)
                bot_state['gann_close_used']        = h1_close
                bot_state['gann_last_h1_time']      = h1_time
                bot_state['gann_cycle_started_at']  = datetime.now(timezone.utc)
                bot_state['gann_cycle_active']      = True
                bot_state['gann_cycle_end_flag']    = None
                bot_state['gann_level_status']     = {}
                bot_state['gann_open_trades']       = {}
                await send_tg_msg(_gann_fmt_levels_msg(h1_close))

        except Exception as e:
            c_log(f'Gann cycle manager error: {e}')
        await asyncio.sleep(30)

# ─────────────────────────────────────────────────────────────
# GANN ENGINE — ATR helper + TP/SL + trade execution
# ─────────────────────────────────────────────────────────────
def _gann_tf_tp(tf: str) -> int:
    """يرجع TP بالنقاط للفريم المحدد — القيمة الخاصة إذا ضُبطت، وإلا القيمة العامة."""
    v = bot_state['gann_tp_per_tf'].get(tf, 0)
    return v if v > 0 else bot_state['gann_tp_points']

def _gann_tf_sl(tf: str) -> int:
    """يرجع SL بالنقاط للفريم المحدد — القيمة الخاصة إذا ضُبطت، وإلا القيمة العامة."""
    v = bot_state['gann_sl_per_tf'].get(tf, 0)
    return v if v > 0 else bot_state['gann_sl_points']

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

def _gann_calc_tpsl(entry: float, is_buy: bool, candles: list, tf: str = '') -> tuple[float, float]:
    """يرجع (tp, sl) — يستخدم القيم الخاصة بالفريم إذا وُجدت."""
    pv = bot_state['pip_value']
    if bot_state['gann_tpsl_mode'] == 'atr':
        atr = _gann_atr(candles, bot_state['gann_atr_period'])
        if not atr:
            atr = _gann_tf_sl(tf) * pv
        sl_dist = atr * bot_state['gann_atr_sl_mult']
        tp_dist = atr * bot_state['gann_atr_tp_mult']
    else:
        sl_dist = _gann_tf_sl(tf) * pv
        tp_dist = _gann_tf_tp(tf) * pv
    if is_buy:
        return round(entry + tp_dist, 2), round(entry - sl_dist, 2)
    return round(entry - tp_dist, 2), round(entry + sl_dist, 2)

async def _gann_open_trade(is_buy: bool, level: dict, candles: list, reason: str, tf: str) -> None:
    try:
        tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
        price = float(tick['ask'] if is_buy else tick['bid'])
    except Exception:
        price = float(candles[-1]['close'])

    tp, sl = _gann_calc_tpsl(price, is_buy, candles, tf=tf)
    lot = bot_state['lot_size']; side = 'BUY' if is_buy else 'SELL'
    tp_pts = _gann_tf_tp(tf); sl_pts = _gann_tf_sl(tf)
    tpsl_lbl = (f"ATR({bot_state['gann_atr_period']})×{bot_state['gann_atr_sl_mult']}/{bot_state['gann_atr_tp_mult']}"
                if bot_state['gann_tpsl_mode'] == 'atr'
                else f"SL:{sl_pts}p TP:{tp_pts}p")
    try:
        if is_buy: res = await bot_state['connection_obj'].create_market_buy_order(bot_state['symbol'], lot, stop_loss=sl, take_profit=tp)
        else:      res = await bot_state['connection_obj'].create_market_sell_order(bot_state['symbol'], lot, stop_loss=sl, take_profit=tp)
        trade_id = str(res.get('positionId') or res.get('orderId'))
        bot_state['gann_open_trades'][trade_id]          = tf
        bot_state['gann_level_status'][level['key']]     = 'used'
        await send_tg_msg(
            f"<b>✅ {'BUY 📈' if is_buy else 'SELL 📉'} [جان {tf}]</b>  {reason}\n"
            f"المستوى: {level['price']:.2f}  |  الدخول: {price:.2f}\n"
            f"TP: {tp} ({tp_pts}p)  SL: {sl} ({sl_pts}p)  Lot: {lot}\n"
            f"إغلاق H1: {bot_state['gann_close_used']:.2f}"
        )
    except Exception as e:
        bot_state['gann_level_status'][level['key']] = 'used'
        await send_tg_msg(f"<b>❌ فشل تنفيذ {side} [جان {tf}]</b>\nالمستوى: {level['price']:.2f}\n{e}")

# ─────────────────────────────────────────────────────────────
# GANN ENGINE — MONITOR / ENTRY SCANNER  (touch | breakout_retest)
# ─────────────────────────────────────────────────────────────
async def gann_monitor_scanner() -> None:
    c_log('Gann monitor scanner started.')
    while True:
        try:
            if not (bot_state['status'] == 'RUNNING' and bot_state['live_connected'] and bot_state['account_obj']):
                await asyncio.sleep(10); continue
            if bot_state['dd_triggered']:
                await asyncio.sleep(20); continue
            if not bot_state['gann_cycle_active'] or not bot_state['gann_levels']:
                await asyncio.sleep(10); continue

            enabled_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on]
            levels      = gann_active_levels()
            margin      = bot_state['gann_touch_margin_pts'] * bot_state['pip_value']
            trend_dir   = await _gann_trend_direction()   # None='off', 'bull', 'bear'

            for tf in enabled_tfs:
                tf_already_open = tf in bot_state['gann_open_trades'].values()
                if tf_already_open: continue

                need    = max(bot_state['gann_atr_period'] + 30, 60)
                candles = await fetch_candles(tf, count=need)
                if not candles or len(candles) < 3: continue
                candles = sorted(candles, key=lambda c: c['time'])

                try:
                    tick    = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                    live_px = float(tick['bid'])
                except Exception:
                    live_px = float(candles[-1]['close'])

                close_px = float(candles[-1]['close'])

                if bot_state['gann_entry_mode'] == 'touch':
                    for lv in levels:
                        # Anti-spam: True → блок на уровне, False → блок на уровне+TF
                        used_key = lv['key'] if bot_state['gann_anti_spam'] else f"{lv['key']}_{tf}"
                        if bot_state['gann_level_status'].get(lv['key']) == 'used' and bot_state['gann_anti_spam']: continue
                        if not bot_state['gann_anti_spam'] and bot_state['gann_level_status'].get(used_key) == 'used': continue
                        if abs(live_px - lv['price']) > margin: continue
                        is_buy = (lv['dir'] == 'dn')
                        # فلتر الاتجاه
                        if trend_dir == 'bull' and not is_buy: continue
                        if trend_dir == 'bear' and is_buy: continue
                        await _gann_open_trade(is_buy, lv, candles,
                                               reason=f"لمس {'دعم 🟢' if is_buy else 'مقاومة 🔴'}", tf=tf)
                        break

                else:  # breakout_retest
                    for lv in levels:
                        if bot_state['gann_anti_spam'] and bot_state['gann_level_status'].get(lv['key']) == 'used': continue
                        status = bot_state['gann_level_status'].get(lv['key'])
                        if not status:
                            if lv['dir'] == 'up' and close_px > lv['price']:
                                bot_state['gann_level_status'][lv['key']] = 'broken_up'
                            elif lv['dir'] == 'dn' and close_px < lv['price']:
                                bot_state['gann_level_status'][lv['key']] = 'broken_dn'
                        elif status in ('broken_up', 'broken_dn'):
                            if abs(live_px - lv['price']) <= margin:
                                is_buy = (status == 'broken_up')
                                if trend_dir == 'bull' and not is_buy: continue
                                if trend_dir == 'bear' and is_buy: continue
                                await _gann_open_trade(is_buy, lv, candles,
                                                       reason=f"كسر+ريتيست {'مقاومة↑ 🟢' if is_buy else 'دعم↓ 🔴'}", tf=tf)
                                break

        except Exception as e:
            c_log(f'Gann monitor scanner error: {e}')
        await asyncio.sleep(15)

# ─────────────────────────────────────────────────────────────
# BACKTEST PROGRESS TRACKER
# ─────────────────────────────────────────────────────────────
class BtProgress:
    BAR_LEN = 14; HEARTBEAT = 15
    def __init__(self, label: str):
        self.label = label; self.cancelled = False; self.phase = 'Initialising...'
        self.tf_done = 0; self.tf_total = 1; self.current_tf = ''
        self.bars_done = 0; self.bars_total = 0; self.win = 0; self.loss = 0; self.be = 0; self.profit = 0.0
        self.chat_id = None; self.msg_id = None; self._last_edit = 0.0; self._lock = asyncio.Lock(); self._hb_task = None; self._start_ts = 0.0

    def _bar(self, done: int, total: int) -> str:
        if total == 0: return chr(9617) * self.BAR_LEN
        filled = round(done / total * self.BAR_LEN)
        return chr(9608) * filled + chr(9617) * (self.BAR_LEN - filled)

    def _elapsed(self) -> str:
        secs = int(datetime.now(timezone.utc).timestamp() - self._start_ts); m, s = divmod(secs, 60); return f'{m}m {s:02d}s'

    @property
    def overall_progress(self) -> float:
        overall = (self.tf_done + self.bars_done / self.bars_total) / max(self.tf_total, 1) if self.bars_total else self.tf_done / max(self.tf_total, 1)
        return overall * 100.0

    def _build_text(self) -> str:
        total = self.win + self.loss + self.be; wr = f'{round(self.win / total * 100)}%' if total else '-'
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

    def _cancel_kbd(self) -> dict: return {'inline_keyboard': [[{'text': '⏹ Cancel', 'callback_data': 'cancel_bt'}]]}

    async def start(self, chat_id: int) -> None:
        self.chat_id = chat_id; self._start_ts = datetime.now(timezone.utc).timestamp(); self._last_edit = self._start_ts
        payload = {'chat_id': chat_id, 'text': self._build_text(), 'parse_mode': 'HTML', 'reply_markup': self._cancel_kbd()}
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True), timeout=aiohttp.ClientTimeout(total=12, connect=5)) as sess:
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
            if not self.cancelled: payload['reply_markup'] = self._cancel_kbd()
            try: await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/editMessageText', json=payload)
            except Exception: pass

    async def set_phase(self, phase: str) -> None: self.phase = phase; await self._edit()
    async def set_tf(self, tf: str, bars_total: int) -> None: self.current_tf = tf; self.bars_done = 0; self.bars_total = bars_total; self.phase = f'Scanning [{tf}]'; await self._edit(force=True)
    async def tick(self, bar_n: int, win: int, loss: int, be: int, profit: float) -> None: self.bars_done = bar_n; self.win = win; self.loss = loss; self.be = be; self.profit = profit; await self._edit()
    async def update(self, done: int, total: int) -> None:
        """Simple progress update used by Gann backtest (bars_done/bars_total only)."""
        self.bars_done = done; self.bars_total = total; self.win = 0; self.loss = 0; await self._edit()
    async def finish_tf(self) -> None: self.tf_done += 1; self.bars_done = self.bars_total; await self._edit(force=True)
    async def done(self, final_text: str) -> None:
        if self._hb_task: self._hb_task.cancel()
        if not self.msg_id or not self.chat_id: return
        try: await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/editMessageText', json={'chat_id': self.chat_id, 'message_id': self.msg_id, 'text': final_text, 'parse_mode': 'HTML'})
        except Exception: pass
    async def cancel(self) -> None:
        self.cancelled = True; self.phase = 'Cancelling...'
        if self._hb_task: self._hb_task.cancel()
        await self._edit(force=True)

_bt_progress: BtProgress | None = None

# ─────────────────────────────────────────────────────────────
# TELEGRAM HELPERS
# ─────────────────────────────────────────────────────────────
async def _tg_post(url: str, **kwargs) -> bool:
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True), timeout=aiohttp.ClientTimeout(total=12, connect=5)) as sess:
            async with sess.post(url, **kwargs) as resp: return resp.status == 200
    except Exception as e: return False

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

async def answer_callback(cbq_id: str, text: str = None) -> None:
    payload = {'callback_query_id': cbq_id}
    if text: payload['text'] = text
    await _tg_post(f'https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery', json=payload)

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
# KEYBOARDS
# ─────────────────────────────────────────────────────────────
def _dd_status_line() -> str:
    if not bot_state['live_connected']: return 'DD: offline'
    sod = bot_state['sod_balance']
    if sod is None: return 'DD: monitoring...'
    lim = sod * (1 - DD_LIMIT_PCT)
    return (f'🔴 DD TRIGGERED ${lim:.0f}' if bot_state['dd_triggered'] else f'🟢 DD OK  ${lim:.0f}')

def get_main_keyboard() -> dict:
    live = '🟢 connected' if bot_state['live_connected'] else '🔴 disconnected'
    st   = '▶ RUNNING'   if bot_state['status'] == 'RUNNING' else '⏸ PAUSED'
    bt   = '⏳ BT Running' if bot_state['is_backtesting'] else '📊 Backtest'
    return {'inline_keyboard': [
        [{'text': f'Server: {live}',    'callback_data': 'toggle_live_conn'}],
        [{'text': f'Bot: {st}',         'callback_data': 'toggle_status'}, {'text': '❌ Close All',       'callback_data': 'close_all'}],
        [{'text': '📐 جان — الدعوم والمقاومات', 'callback_data': 'menu_gann'}, {'text': '💰 Risk',   'callback_data': 'menu_risk'}],
        [{'text': '📈 Market Report',   'callback_data': 'report'}, {'text': '💼 Account',         'callback_data': 'account'}],
        [{'text': '❌ إخفاء اللوحة',    'callback_data': 'hide_keyboard'}],
        [{'text': _dd_status_line(),    'callback_data': 'dd_status'}],
    ]}

def get_risk_keyboard() -> dict:
    be_i  = '✅' if bot_state['use_be']        else '⬜'
    tr_i  = '✅' if bot_state['use_trailing']  else '⬜'
    spr_i = '✅' if bot_state['use_max_spread'] else '⬜'
    tgt_i = '✅' if bot_state['daily_target_enabled'] else '⬜'
    los_i = '✅' if bot_state['daily_loss_enabled']   else '⬜'
    be_p  = bot_state['be_points']
    tr_p  = bot_state['trail_points']; tr_o = bot_state['trail_offset']
    return {'inline_keyboard': [
        [{'text': f'🔒 Break-Even: {be_i}  ({be_p}p)', 'callback_data': 'toggle_be'},
         {'text': f'📡 SpreadGuard: {spr_i}', 'callback_data': 'toggle_spread'}],
        [{'text': 'BE −10', 'callback_data': 'dec_be'}, {'text': f'BE={be_p}p', 'callback_data': 'noop'}, {'text': 'BE +10', 'callback_data': 'inc_be'}],
        [{'text': f'🔄 Trailing: {tr_i}', 'callback_data': 'toggle_trailing'}],
        [{'text': f'Trail Pts −', 'callback_data': 'dec_trl'}, {'text': f'Pts={tr_p}', 'callback_data': 'noop'}, {'text': 'Trail Pts +', 'callback_data': 'inc_trl'}],
        [{'text': f'Trail Off −', 'callback_data': 'dec_tro'}, {'text': f'Off={tr_o}', 'callback_data': 'noop'}, {'text': 'Trail Off +', 'callback_data': 'inc_tro'}],
        [{'text': '─' * 20, 'callback_data': 'noop'}],
        [{'text': '−', 'callback_data': 'dec_lot'}, {'text': f'Lot: {bot_state["lot_size"]:.2f}', 'callback_data': 'noop'}, {'text': '+', 'callback_data': 'inc_lot'}],
        [{'text': f'🎯 Target: {tgt_i}  (${bot_state["daily_target_usd"]:.0f})', 'callback_data': 'toggle_daily_target'}],
        [{'text': f'🛑 Loss Limit: {los_i}  (${bot_state["daily_loss_usd"]:.0f})', 'callback_data': 'toggle_daily_loss'}],
        [{'text': '/target N  |  /loss_limit N', 'callback_data': 'noop'}],
        [{'text': '← رجوع', 'callback_data': 'menu_main'}],
    ]}

def get_gann_tpsl_tf_keyboard(sel_tf: str = '') -> dict:
    """قائمة تعديل TP/SL لكل فريم بشكل مستقل."""
    rows = [[{'text': '⚙️ TP/SL مخصص لكل فريم', 'callback_data': 'noop'}],
            [{'text': '(0 = يرجع للقيمة العامة)', 'callback_data': 'noop'}]]
    tfs_list = list(bot_state['gann_monitor_tfs'].keys())
    # صف اختيار الفريم
    tf_row = []
    for tfk in tfs_list:
        icon = '👉' if tfk == sel_tf else ''
        tf_row.append({'text': f'{icon}{tfk}', 'callback_data': f'gann_tptf_sel_{tfk}'})
        if len(tf_row) == 4: rows.append(tf_row); tf_row = []
    if tf_row: rows.append(tf_row)
    if sel_tf:
        tp_v = bot_state['gann_tp_per_tf'].get(sel_tf, 0)
        sl_v = bot_state['gann_sl_per_tf'].get(sel_tf, 0)
        eff_tp = tp_v if tp_v > 0 else bot_state['gann_tp_points']
        eff_sl = sl_v if sl_v > 0 else bot_state['gann_sl_points']
        rows += [
            [{'text': f'── [{sel_tf}] ──', 'callback_data': 'noop'}],
            [{'text': f'TP فعلي: {eff_tp}p {"(مخصص)" if tp_v>0 else "(عام)"}', 'callback_data': 'noop'}],
            [{'text': 'TP −10', 'callback_data': f'gann_tptf_dtp_{sel_tf}'}, {'text': f'TP={tp_v}', 'callback_data': 'noop'}, {'text': 'TP +10', 'callback_data': f'gann_tptf_itp_{sel_tf}'}],
            [{'text': f'SL فعلي: {eff_sl}p {"(مخصص)" if sl_v>0 else "(عام)"}', 'callback_data': 'noop'}],
            [{'text': 'SL −10', 'callback_data': f'gann_tptf_dsl_{sel_tf}'}, {'text': f'SL={sl_v}', 'callback_data': 'noop'}, {'text': 'SL +10', 'callback_data': f'gann_tptf_isl_{sel_tf}'}],
            [{'text': '↺ إعادة ضبط (رجوع للعام)', 'callback_data': f'gann_tptf_rst_{sel_tf}'}],
        ]
    rows.append([{'text': '← رجوع', 'callback_data': 'menu_gann'}])
    return {'inline_keyboard': rows}

def get_trend_filter_keyboard() -> dict:
    m  = bot_state['gann_trend_filter']
    tf = bot_state['gann_trend_tf']
    s  = bot_state['gann_ema_single']
    f  = bot_state['gann_ema_fast']; sl = bot_state['gann_ema_slow']
    m200_i = '✅' if m == 'ema200' else '⬜'
    dual_i = '✅' if m == 'ema_dual' else '⬜'
    off_i  = '✅' if m == 'off' else '⬜'
    return {'inline_keyboard': [
        [{'text': '🔍 فلتر الاتجاه (حماية)', 'callback_data': 'noop'}],
        [{'text': f'{off_i} إيقاف الفلتر', 'callback_data': 'trend_off'},
         {'text': f'{m200_i} EMA واحد', 'callback_data': 'trend_ema200'},
         {'text': f'{dual_i} EMA مزدوج', 'callback_data': 'trend_dual'}],
        [{'text': '── TF حساب EMA ──', 'callback_data': 'noop'}],
        [{'text': tf, 'callback_data': 'trend_cycle_tf'}],
        [{'text': '── EMA واحد ──', 'callback_data': 'noop'}],
        [{'text': 'EMA −10', 'callback_data': 'trend_dec_s'}, {'text': f'EMA={s}', 'callback_data': 'noop'}, {'text': 'EMA +10', 'callback_data': 'trend_inc_s'}],
        [{'text': '── EMA مزدوج (Fast / Slow) ──', 'callback_data': 'noop'}],
        [{'text': 'Fast −5', 'callback_data': 'trend_dec_f'}, {'text': f'Fast={f}', 'callback_data': 'noop'}, {'text': 'Fast +5', 'callback_data': 'trend_inc_f'}],
        [{'text': 'Slow −5', 'callback_data': 'trend_dec_sl'}, {'text': f'Slow={sl}', 'callback_data': 'noop'}, {'text': 'Slow +5', 'callback_data': 'trend_inc_sl'}],
        [{'text': '← رجوع', 'callback_data': 'menu_gann'}],
    ]}

def get_gann_keyboard() -> dict:
    zf   = bot_state['gann_zone_filter']
    em   = bot_state['gann_entry_mode']
    tpsm = bot_state['gann_tpsl_mode']
    hrs  = bot_state['gann_cycle_hours']
    mg   = bot_state['gann_touch_margin_pts']
    cyc  = '🟢 نشطة' if bot_state['gann_cycle_active'] else '⚫ غير نشطة'
    active_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on]
    open_n = len(bot_state.get('gann_open_trades', {}))
    zf_lbl  = '⭐ القوية فقط' if zf == 'star' else '📋 كل المستويات'
    em_lbl  = '🔁 لمس (ارتداد)' if em == 'touch' else '💥 كسر + ريتيست'
    tps_lbl = f'🎯 TP/SL: {"نقاط ثابتة" if tpsm == "fixed" else "ATR"}'
    tp = bot_state['gann_tp_points']; sl = bot_state['gann_sl_points']
    atp = bot_state['gann_atr_tp_mult']; asp = bot_state['gann_atr_sl_mult']
    ap  = bot_state['gann_atr_period']
    spam_lbl  = '🔇 Anti-Spam: ON (مستوى واحد لكل TF)' if bot_state['gann_anti_spam'] else '🔊 Anti-Spam: OFF (عدة TF على نفس المستوى)'
    trend_lbl = {'off':'🔍 فلتر الاتجاه: OFF','ema200':f'🔍 EMA {bot_state["gann_ema_single"]}','ema_dual':f'🔍 EMA {bot_state["gann_ema_fast"]}/{bot_state["gann_ema_slow"]}'}[bot_state['gann_trend_filter']]
    rows = [
        [{'text': f'📐 محرك جان  — دورة: {cyc}  |  صفقات: {open_n}', 'callback_data': 'noop'}],
        [{'text': '🔄 عرض الدعوم والمقاومات الآن', 'callback_data': 'gann_show_levels'}],
        [{'text': spam_lbl, 'callback_data': 'gann_toggle_spam'}],
        [{'text': trend_lbl, 'callback_data': 'menu_trend_filter'}],
        [{'text': '── الفلتر ──', 'callback_data': 'noop'}],
        [{'text': zf_lbl, 'callback_data': 'gann_toggle_filter'}],
        [{'text': '── وضع الدخول ──', 'callback_data': 'noop'}],
        [{'text': em_lbl, 'callback_data': 'gann_toggle_entry'}],
        [{'text': '── فريمات المراقبة ──', 'callback_data': 'noop'}],
    ]
    # صف الفريمات: 4 في كل سطر
    tf_items = list(bot_state['gann_monitor_tfs'].items())
    for i in range(0, len(tf_items), 4):
        chunk = tf_items[i:i+4]
        rows.append([{'text': ('✅' if on else '⬜') + f' {tfk}', 'callback_data': f'gann_tf_{tfk}'} for tfk, on in chunk])
    rows += [
        [{'text': '── مدة تجميد السلّم ──', 'callback_data': 'noop'}],
        [{'text': '−ساعة', 'callback_data': 'gann_dec_hours'}, {'text': f'{hrs} ساعة', 'callback_data': 'noop'}, {'text': '+ساعة', 'callback_data': 'gann_inc_hours'}],
        [{'text': '── هامش اللمس (نقاط) ──', 'callback_data': 'noop'}],
        [{'text': '−', 'callback_data': 'gann_dec_margin'}, {'text': f'{mg} نقطة', 'callback_data': 'noop'}, {'text': '+', 'callback_data': 'gann_inc_margin'}],
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
        [{'text': '📊 باكتيست جان', 'callback_data': 'menu_gann_bt'}],
        [{'text': '⚙️ TP/SL مخصص لكل فريم', 'callback_data': 'gann_tpsl_tf'}],
        [{'text': '← رجوع', 'callback_data': 'menu_main'}],
    ]
    return {'inline_keyboard': rows}

def get_gann_bt_keyboard() -> dict:
    if bot_state['is_backtesting']:
        return {'inline_keyboard': [[{'text': '⏳ BT يعمل...', 'callback_data': 'bt_show_progress'}],
                                     [{'text': '⏹ إلغاء', 'callback_data': 'cancel_bt'}],
                                     [{'text': '← رجوع', 'callback_data': 'menu_gann'}]]}
    return {'inline_keyboard': [
        [{'text': '1 يوم', 'callback_data': 'gbt_1'}, {'text': '3 أيام', 'callback_data': 'gbt_3'}, {'text': '7 أيام', 'callback_data': 'gbt_7'}],
        [{'text': '14 يوم', 'callback_data': 'gbt_14'}, {'text': '30 يوم', 'callback_data': 'gbt_30'}],
        [{'text': '← رجوع', 'callback_data': 'menu_gann'}],
    ]}

# ─────────────────────────────────────────────────────────────
# GANN BACKTEST ENGINE
# Simulates gann_calc_levels on each historical H1 candle, then
# watches subsequent M5 candles for touch/breakout-retest signals,
# applies the same TP/SL logic as live execution.
# ─────────────────────────────────────────────────────────────
def _style_sheet(ws) -> None:
    """تنسيق ورقة Excel: ترويسة ملوّنة + عرض أعمدة تلقائي."""
    from openpyxl.styles import PatternFill, Font, Alignment
    header_fill = PatternFill('solid', fgColor='2E4057')
    header_font = Font(bold=True, color='FFFFFF')
    for cell in ws[1]:
        cell.fill   = header_fill
        cell.font   = header_font
        cell.alignment = Alignment(horizontal='center')
    win_fill  = PatternFill('solid', fgColor='C8E6C9')
    loss_fill = PatternFill('solid', fgColor='FFCDD2')
    outcome_col = None
    for idx, cell in enumerate(ws[1], 1):
        if cell.value == 'Outcome':
            outcome_col = idx; break
    for row in ws.iter_rows(min_row=2):
        if outcome_col:
            ov = row[outcome_col - 1].value
            fill = win_fill if ov == 'WIN' else (loss_fill if ov == 'LOSS' else None)
            if fill:
                for cell in row: cell.fill = fill
    for col in ws.columns:
        max_len = max((len(str(c.value)) if c.value else 0) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 30)

async def run_gann_backtest(days: int, start_dt: datetime = None, end_dt: datetime = None) -> None:
    global _bt_progress
    if bot_state['is_backtesting']: return
    bot_state['is_backtesting'] = True

    if end_dt is None:   end_dt   = datetime.now(timezone.utc)
    if start_dt is None: start_dt = end_dt - timedelta(days=days)
    fname    = f"GannBT_{datetime.now(timezone.utc).strftime('%H%M%S')}.xlsx"
    enabled_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on] or ['5m']
    tfs_label   = '+'.join(enabled_tfs)
    desc     = f"جان H1→[{tfs_label}] | {bot_state['gann_entry_mode']} | {'⭐' if bot_state['gann_zone_filter']=='star' else 'كل المستويات'}"
    prog     = BtProgress(label=desc, active_tfs=['H1']); _bt_progress = prog
    await prog.start(bot_state['chat_id'])

    res = {'win': 0, 'loss': 0, 'be': 0,
           'total_win_usd': 0.0, 'total_loss_usd': 0.0,
           'total_prof': 0.0, 'peak_equity': 0.0, 'max_dd': 0.0,
           'trade_logs': []}
    pv   = bot_state['pip_value']
    lot  = bot_state['lot_size']
    margin_pts = bot_state['gann_touch_margin_pts']
    margin = margin_pts * pv
    cycle_h = bot_state['gann_cycle_hours']
    tpsl_mode  = bot_state['gann_tpsl_mode']

    try:
        await prog.set_phase('Fetching H1 candles...')
        h1_warmup = 5
        total_h1_need = days * 24 + h1_warmup
        candles_h1 = await fetch_candles('1h', count=total_h1_need, end_time=end_dt)
        if not candles_h1: await prog.done('❌ لا توجد بيانات H1.'); return
        candles_h1 = sorted(candles_h1, key=lambda c: c['time'])

        await prog.set_phase(f'Fetching monitor candles [{tfs_label}]...')
        monitor_tfs_data = {}
        for btf in enabled_tfs:
            bmin = {'1m':1,'3m':3,'5m':5,'10m':10,'15m':15,'30m':30,'60m':60,'120m':120}.get(btf, 5)
            need_m = days * 24 * (60 // max(bmin, 1)) + 300
            mc = await fetch_candles(btf, count=need_m, end_time=end_dt)
            if mc: monitor_tfs_data[btf] = sorted(mc, key=lambda c: c['time'])

        await prog.set_phase('تشغيل المحاكاة...')
        start_ts  = start_dt.timestamp()
        end_ts    = end_dt.timestamp()
        # فلترة: نأخذ الشموع التي أُغلقت ضمن الفترة المطلوبة
        # وقت الإغلاق = وقت الفتح + 1 ساعة
        h1_in_range = [c for c in candles_h1
                       if (c['time'].timestamp() + 3600) >= start_ts
                       and c['time'].timestamp() + 3600 <= end_ts]
        total_h1 = len(h1_in_range)
        await prog.set_tf('H1', total_h1)

        cycle_logs   = []
        cs           = bot_state['contract_size']

        for idx, h1 in enumerate(h1_in_range):
            if prog.cancelled: break
            if idx % 5 == 0: await asyncio.sleep(0)

            close    = float(h1['close'])
            # ← الإصلاح الأساسي: نبدأ المراقبة من وقت إغلاق H1 (وقت الفتح + 1 ساعة)
            t_start  = h1['time'] + timedelta(hours=1)
            t_end    = t_start + timedelta(hours=cycle_h)
            levels   = gann_calc_levels(close)
            active_lv = [l for l in levels if l['dir'] != 'ref'
                         and (bot_state['gann_zone_filter'] != 'star' or l['star'])]

            cycle_trades  = 0
            cycle_min_dist = None  # أقرب مسافة من السعر لأي مستوى (لتشخيص عدم الوصول)

            # level_status منفصل لكل فريم (مهم في وضع الكسر+ريتيست)
            level_used: set[str] = set()
            tf_level_status: dict[str, dict] = {btf: {} for btf in monitor_tfs_data}

            for btf, candles_m in monitor_tfs_data.items():
                m_window = [c for c in candles_m if t_start <= c['time'] < t_end]
                if not m_window: continue
                m_before = [c for c in candles_m if c['time'] < t_start]
                atr_val  = _gann_atr(m_before, bot_state['gann_atr_period']) if tpsl_mode == 'atr' else None
                tf_status = tf_level_status[btf]

                for bar in m_window:
                    bar_close = float(bar['close'])
                    bar_time  = bar['time']
                    # ← الإصلاح: البحث عن TP/SL في كل الشموع اللاحقة — وليس فقط ضمن النافذة
                    remaining_bars = [b for b in candles_m if b['time'] > bar_time]

                    # تتبع أقرب مسافة من السعر لأي مستوى (للتشخيص)
                    for lv in active_lv:
                        d = abs(bar_close - lv['price'])
                        if cycle_min_dist is None or d < cycle_min_dist:
                            cycle_min_dist = d

                    for lv in active_lv:
                        k = lv['key']
                        # Anti-spam ON: un seul trade par niveau toutes TF confondues
                        # Anti-spam OFF: un trade par niveau ET par TF
                        if bot_state['gann_anti_spam']:
                            if k in level_used: continue
                        else:
                            if f'{k}_{btf}' in level_used: continue

                        if bot_state['gann_entry_mode'] == 'touch':
                            if abs(bar_close - lv['price']) > margin: continue
                            is_buy = (lv['dir'] == 'dn')
                        else:
                            cur_status = tf_status.get(k)
                            if not cur_status:
                                if lv['dir'] == 'up' and bar_close > lv['price']: tf_status[k] = 'broken_up'
                                elif lv['dir'] == 'dn' and bar_close < lv['price']: tf_status[k] = 'broken_dn'
                                continue
                            if abs(bar_close - lv['price']) > margin: continue
                            is_buy = (cur_status == 'broken_up')

                        entry  = lv['price']
                        tf_tp  = _gann_tf_tp(btf)
                        tf_sl  = _gann_tf_sl(btf)
                        if tpsl_mode == 'atr' and atr_val:
                            sl_d = atr_val * bot_state['gann_atr_sl_mult']
                            tp_d = atr_val * bot_state['gann_atr_tp_mult']
                        else:
                            sl_d = tf_sl * pv
                            tp_d = tf_tp * pv
                        tp_px  = entry + tp_d if is_buy else entry - tp_d
                        sl_px  = entry - sl_d if is_buy else entry + sl_d
                        tp_pts = round(tp_d / pv)
                        sl_pts = round(sl_d / pv)

                        outcome = 'OPEN'; p_usd = 0.0
                        for fb in remaining_bars:
                            fh = float(fb['high']); fl = float(fb['low'])
                            if is_buy:
                                if fh >= tp_px: outcome = 'WIN';  p_usd =  round(tp_d * lot * cs, 2); break
                                if fl <= sl_px: outcome = 'LOSS'; p_usd = -round(sl_d * lot * cs, 2); break
                            else:
                                if fl <= tp_px: outcome = 'WIN';  p_usd =  round(tp_d * lot * cs, 2); break
                                if fh >= sl_px: outcome = 'LOSS'; p_usd = -round(sl_d * lot * cs, 2); break

                        if outcome == 'OPEN': continue   # لا بيانات لاحقة كافية — نتجاوز

                        level_used.add(k if bot_state['gann_anti_spam'] else f'{k}_{btf}')
                        cycle_trades += 1
                        if outcome == 'WIN':  res['win']  += 1; res['total_win_usd']  += p_usd
                        else:                 res['loss'] += 1; res['total_loss_usd'] += abs(p_usd)
                        res['total_prof'] += p_usd
                        res['peak_equity'] = max(res['peak_equity'], res['total_prof'])
                        res['max_dd']      = max(res['max_dd'], res['peak_equity'] - res['total_prof'])

                        dam_bar = _utc_to_dam(bar_time)
                        res['trade_logs'].append({
                            '_cycle_idx':           idx,
                            'وقت الصفقة (DAM)':    dam_bar.strftime('%Y-%m-%d %H:%M'),
                            'TF':                   btf,
                            'اتجاه':                'BUY 📈' if is_buy else 'SELL 📉',
                            'إغلاق H1 (المحور)':   close,
                            'المستوى':              lv['price'],
                            'قوي ⭐':               '⭐' if lv['star'] else '',
                            'الدخول':               entry,
                            'TP':                   round(tp_px, 2),
                            'SL':                   round(sl_px, 2),
                            'TP (نقطة)':            tp_pts,
                            'SL (نقطة)':            sl_pts,
                            'Lot':                  lot,
                            'النتيجة':              '✅ WIN' if outcome == 'WIN' else '❌ LOSS',
                            'ربح ($)':              p_usd,
                            'رصيد ($)':             round(res['total_prof'], 2),
                        })
                        break  # صفقة واحدة لكل شمعة لمس

            # سجل الدورة
            dam_cycle = _utc_to_dam(t_start)
            lv_labels = ', '.join(f'{l["price"]:.2f}{"⭐" if l["star"] else ""}({("R" if l["dir"]=="up" else "S")})' for l in active_lv[:4])
            if cycle_trades > 0:
                reason = f'✅ {cycle_trades} صفقة'
            elif not any(len([c for c in cm if t_start <= c['time'] < t_end]) > 0 for cm in monitor_tfs_data.values()):
                reason = '⚠️ لا توجد بيانات للفريم في هذه الفترة'
            elif cycle_min_dist is not None:
                dist_pts = round(cycle_min_dist / pv)
                reason = f'🔴 لم يصل السعر — أقرب مستوى كان {dist_pts} نقطة'
            else:
                reason = '🔴 لا توجد شموع مراقبة'
            cycle_logs.append({
                '_cycle_idx':                  idx,
                'وقت بدء الدورة (DAM)':       dam_cycle.strftime('%Y-%m-%d %H:%M'),
                'وقت انتهاء الدورة (DAM)':    _utc_to_dam(t_end).strftime('%Y-%m-%d %H:%M'),
                'إغلاق H1':                   close,
                'أبرز المستويات':             lv_labels,
                'عدد المستويات النشطة':       len(active_lv),
                'الصفقات':                    reason,
            })

            await prog.tick(idx + 1, res['win'], res['loss'], res['be'], res['total_prof'])

        # ── Build Telegram summary ──
        total_trades = res['win'] + res['loss']
        wr = round(res['win'] / max(1, total_trades) * 100, 1) if total_trades else 0
        dd_pct = round(res['max_dd'] / res['peak_equity'] * 100, 1) if res['peak_equity'] else 0
        icon = 'PROFIT ▲' if res['total_prof'] >= 0 else 'LOSS ▼'
        tg_lines = [
            f'<b>باكتيست جان اكتمل ✅</b>', f'<b>{desc}</b>',
            f'{_utc_to_dam(start_dt).strftime("%Y-%m-%d")} → {_utc_to_dam(end_dt).strftime("%Y-%m-%d")}\n',
            f'Net: <b>{icon} ${round(res["total_prof"],2)}</b>',
            f'Win:  +${round(res["total_win_usd"],2)} ({res["win"]})',
            f'Loss: -${abs(round(res["total_loss_usd"],2))} ({res["loss"]})',
            f'WR: {wr}% ({total_trades} صفقة)',
            f'Max DD: ${round(res["max_dd"],2)} ({dd_pct}%)',
            f'دورات H1: {len(cycle_logs)}  |  TP/SL: {"ATR" if tpsl_mode=="atr" else "نقاط ثابتة"} | Lot: {lot}  |  cs={cs}',
            '\nإرسال ملف Excel...',
        ]
        await prog.done('\n'.join(tg_lines))

        # ── Excel export ──
        # لوحة ألوان للدورات — 12 لون فاتح متناوب
        _CYCLE_COLORS = [
            'FFF9C4','FFE0B2','F8BBD9','DCEDC8','B3E5FC','E1BEE7',
            'FFE082','C8E6C9','FFCCBC','B2EBF2','F5F5F5','D7CCC8',
        ]

        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            from openpyxl.styles import PatternFill, Font, Alignment

            # ورقة 1: الصفقات — مرتبة حسب TF ثم الوقت
            if res['trade_logs']:
                tf_order = ['1m','2m','3m','4m','5m','6m','10m','12m','15m','20m','30m','60m','120m']
                df_t = pd.DataFrame(res['trade_logs'])
                df_t['_tf_rank'] = df_t['TF'].apply(lambda x: tf_order.index(x) if x in tf_order else 99)
                df_t = df_t.sort_values(['_tf_rank','وقت الصفقة (DAM)']).drop(columns=['_tf_rank'])
                df_t.to_excel(writer, sheet_name='الصفقات', index=False)
                ws_t = writer.sheets['الصفقات']
                # رأس
                hf = PatternFill('solid', fgColor='1A237E'); hft = Font(bold=True, color='FFFFFF')
                for cell in ws_t[1]: cell.fill = hf; cell.font = hft; cell.alignment = Alignment(horizontal='center')
                # لون الصف حسب cycle_idx المخزن في عمود مخفي
                cycle_col_idx = None
                for ci, cell in enumerate(ws_t[1], 1):
                    if cell.value == '_cycle_idx': cycle_col_idx = ci; break
                outcome_col = None
                for ci, cell in enumerate(ws_t[1], 1):
                    if cell.value == 'النتيجة': outcome_col = ci; break
                for row in ws_t.iter_rows(min_row=2):
                    cidx = 0
                    if cycle_col_idx:
                        try: cidx = int(row[cycle_col_idx-1].value or 0)
                        except: cidx = 0
                    color = _CYCLE_COLORS[cidx % len(_CYCLE_COLORS)]
                    base_fill = PatternFill('solid', fgColor=color)
                    win_fill  = PatternFill('solid', fgColor='A5D6A7')
                    loss_fill = PatternFill('solid', fgColor='EF9A9A')
                    for cell in row:
                        if outcome_col and cell.column == outcome_col:
                            v = str(cell.value or '')
                            cell.fill = win_fill if 'WIN' in v else (loss_fill if 'LOSS' in v else base_fill)
                        else:
                            cell.fill = base_fill
                for col in ws_t.columns:
                    mx = max((len(str(c.value)) if c.value else 0) for c in col)
                    ws_t.column_dimensions[col[0].column_letter].width = min(mx + 4, 35)
            else:
                pd.DataFrame([{'ملاحظة': 'لا توجد صفقات'}]).to_excel(writer, sheet_name='الصفقات', index=False)

            # ورقة 2: سجل الدورات
            if cycle_logs:
                df_c = pd.DataFrame(cycle_logs)
                df_c.to_excel(writer, sheet_name='دورات H1', index=False)
                ws_c = writer.sheets['دورات H1']
                hf2 = PatternFill('solid', fgColor='1A237E'); hft2 = Font(bold=True, color='FFFFFF')
                for cell in ws_c[1]: cell.fill = hf2; cell.font = hft2; cell.alignment = Alignment(horizontal='center')
                trade_col = None
                cidx_col  = None
                for ci, cell in enumerate(ws_c[1], 1):
                    if cell.value == 'الصفقات':   trade_col = ci
                    if cell.value == '_cycle_idx': cidx_col  = ci
                for row in ws_c.iter_rows(min_row=2):
                    cidx = 0
                    if cidx_col:
                        try: cidx = int(row[cidx_col-1].value or 0)
                        except: cidx = 0
                    color  = _CYCLE_COLORS[cidx % len(_CYCLE_COLORS)]
                    base_f = PatternFill('solid', fgColor=color)
                    if trade_col:
                        v = str(row[trade_col-1].value or '')
                        if '✅' in v:   fill = PatternFill('solid', fgColor='A5D6A7')
                        elif '⚠️' in v: fill = PatternFill('solid', fgColor='FFF176')
                        else:           fill = PatternFill('solid', fgColor='EF9A9A')
                    else: fill = base_f
                    for cell in row:
                        cell.fill = base_f if (trade_col and cell.column != trade_col) else fill
                for col in ws_c.columns:
                    mx = max((len(str(c.value)) if c.value else 0) for c in col)
                    ws_c.column_dimensions[col[0].column_letter].width = min(mx + 4, 40)

        caption = (f"GannBT {days}d | {_utc_to_dam(start_dt).strftime('%Y-%m-%d')}→{_utc_to_dam(end_dt).strftime('%Y-%m-%d')}"
                   f" | Net: ${round(res['total_prof'],2)} | WR: {wr}% ({total_trades}T / {len(cycle_logs)}H1)")
        await send_tg_document(fname, caption)
        try: os.remove(fname)
        except Exception: pass

    except Exception as e:
        await prog.done(f'❌ خطأ في باكتيست جان: {e}')
        c_log(f'Gann backtest error: {e}')
    finally:
        bot_state['is_backtesting'] = False; _bt_progress = None

# ─────────────────────────────────────────────────────────────
# POSITION MONITOR
# ─────────────────────────────────────────────────────────────
async def position_monitor() -> None:
    while True:
        try:
            if bot_state['live_connected'] and bot_state['connection_obj']:
                pv = bot_state['pip_value']
                positions = await bot_state['connection_obj'].get_positions()
                
                current_ids = []
                for p in positions:
                    if p['symbol'] != bot_state['symbol']: continue
                    pid = p['id']; current_ids.append(pid)
                    bot_state['tracked_positions'][pid] = float(p.get('unrealizedProfit', 0)) + float(p.get('swap', 0))

                closed_ids = [pid for pid in list(bot_state['tracked_positions'].keys()) if pid not in current_ids]
                for pid in closed_ids:
                    last_profit = bot_state['tracked_positions'][pid]
                    if last_profit < 0:
                        bot_state['consecutive_losses'] += 1
                        c_log(f'Trade #{pid} closed LOSS. Streak: {bot_state["consecutive_losses"]}')
                    elif last_profit > 0:
                        bot_state['consecutive_losses'] = 0; c_log(f'Trade #{pid} closed WIN. Streak reset.')

                    # ── Gann engine: win ⇒ end cycle now / loss ⇒ keep watching same frozen ladder ──
                    if str(pid) in {str(k) for k in bot_state.get('gann_open_trades', {})}:
                        tf_of_trade = bot_state['gann_open_trades'].pop(str(pid), '?')
                        if last_profit > 0:
                            bot_state['gann_cycle_active']   = False
                            bot_state['gann_cycle_end_flag'] = 'win'
                            await send_tg_msg(f"🏆 <b>صفقة جان رابحة [{tf_of_trade}] (${last_profit:.2f}) — إنهاء الدورة</b>\nبانتظار إغلاق شمعة H1 التالية لسلّم جديد.")
                        else:
                            await send_tg_msg(f"📉 <b>صفقة جان خاسرة [{tf_of_trade}] (${last_profit:.2f})</b> — متابعة مراقبة بقية المستويات.")

                    del bot_state['tracked_positions'][pid]

                for p in positions:
                    if p['symbol'] != bot_state['symbol']: continue
                    op  = float(p['openPrice']); tp  = p.get('takeProfit')
                    sl  = p.get('stopLoss'); cp  = float(p['currentPrice'])
                    if tp is None: continue
                    is_buy = float(tp) > op

                    if bot_state['use_be'] and sl is not None and float(sl) != op:
                        be_tgt = op + (1 if is_buy else -1) * bot_state['be_points'] * pv
                        if (is_buy and cp >= be_tgt) or (not is_buy and cp <= be_tgt):
                            try: await bot_state['connection_obj'].modify_position(p['id'], stop_loss=round(op, 2)); await send_tg_msg(f'🔒 BE ({bot_state["be_points"]}p) activated — #{p["id"]}')
                            except Exception: pass

                    if bot_state['use_trailing'] and sl is not None:
                        trail_pts = bot_state['trail_points'] * pv; trail_off = bot_state['trail_offset'] * pv
                        if is_buy:
                            ideal_sl = cp - trail_off
                            if cp >= op + trail_pts and ideal_sl > float(sl):
                                try: await bot_state['connection_obj'].modify_position(p['id'], stop_loss=round(ideal_sl, 2))
                                except Exception: pass
                        else:
                            ideal_sl = cp + trail_off
                            if cp <= op - trail_pts and ideal_sl < float(sl):
                                try: await bot_state['connection_obj'].modify_position(p['id'], stop_loss=round(ideal_sl, 2))
                                except Exception: pass
        except Exception as e: c_log(f'Position monitor error: {e}')
        await asyncio.sleep(1)

# ─────────────────────────────────────────────────────────────
# COMMAND PARSERS
# ─────────────────────────────────────────────────────────────
# TELEGRAM UPDATE HANDLER
# ─────────────────────────────────────────────────────────────
async def process_tg_update(update: dict) -> None:
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip(); bot_state['chat_id'] = update['message']['chat']['id']

        if not msg.startswith('/') and msg in bot_state.get('menu_button_map', {}):
            cb = bot_state['menu_button_map'][msg]
            if cb != 'noop': await _handle_callback(cb, bot_state['chat_id'], None)
            return

        if msg == '/start':
            cyc = f'نشطة — إغلاق: {bot_state["gann_close_used"]:.2f}' if bot_state['gann_cycle_active'] and bot_state['gann_close_used'] else 'لا توجد دورة نشطة بعد'
            await send_tg_msg(
                '<b>Gold Scalper Bot v5.4 — Gann Levels Engine</b>\n\n'
                '📐 استراتيجية: دعوم ومقاومات جان (H1)\n'
                f'• الدورة الحالية: {cyc}\n\n'
                '<b>الأوامر:</b>\n'
                '/status  /dd  /ping\n'
                '/set 5m tp 200     — TP لفريم محدد\n'
                '/set 15m sl 120    — SL لفريم محدد\n'
                '/set be 25         — نقاط Break-Even\n'
                '/target 100        — هدف ربح يومي $\n'
                '/loss_limit 100    — حد خسارة يومي $\n'
                '/backtest 2026-06-24              — يوم واحد\n'
                '/backtest 2026-06-24 2026-06-26   — فترة\n'
                '/restart_sessions  — إصلاح تيليجرام\n\n'
                '👇 استخدم القائمة بالأسفل للتنقل...'
            )
            await send_tg_msg('Main Menu:', get_main_keyboard())

        elif msg.lower().startswith('/target'):
            parts = msg.strip().split()
            if len(parts) == 2:
                try:
                    val = float(parts[1])
                    if val > 0:
                        bot_state['daily_target_usd'] = val; status_txt = '✅ ENABLED' if bot_state['daily_target_enabled'] else '⬜ disabled (enable from Risk menu)'
                        await send_tg_msg(f'🎯 Daily profit target set to <b>${val:.2f}</b>\nStatus: {status_txt}')
                    else: await send_tg_msg('❌ Target must be greater than 0.')
                except ValueError: await send_tg_msg('Usage: /target VALUE\nExample: /target 100')
            else:
                cur = bot_state['daily_target_usd']; en = '✅ ON' if bot_state['daily_target_enabled'] else '⬜ OFF'
                await send_tg_msg(f'<b>Daily Profit Target</b>\nCurrent: ${cur:.2f}  ({en})\n\nUsage: /target VALUE\nExample: /target 100')

        elif msg.lower().startswith('/loss_limit'):
            parts = msg.strip().split()
            if len(parts) == 2:
                try:
                    val = float(parts[1])
                    if val > 0:
                        bot_state['daily_loss_usd'] = val; status_txt = '✅ ENABLED' if bot_state['daily_loss_enabled'] else '⬜ disabled (enable from Risk menu)'
                        await send_tg_msg(f'🛑 Daily loss limit set to <b>${val:.2f}</b>\nStatus: {status_txt}')
                    else: await send_tg_msg('❌ Loss limit must be greater than 0.')
                except ValueError: await send_tg_msg('Usage: /loss_limit VALUE\nExample: /loss_limit 100')
            else:
                cur = bot_state['daily_loss_usd']; en = '✅ ON' if bot_state['daily_loss_enabled'] else '⬜ OFF'
                await send_tg_msg(f'<b>Daily Loss Limit</b>\nCurrent: ${cur:.2f}  ({en})\n\nUsage: /loss_limit VALUE\nExample: /loss_limit 100')

        elif msg.lower().startswith('/set'):
            # /set 1m sl 100 | /set 1m tp 150 | /set be 25
            parts = msg.strip().split()
            try:
                if len(parts) == 3 and parts[1].lower() == 'be':
                    v = int(parts[2]); bot_state['be_points'] = max(5, v)
                    await send_tg_msg(f'✅ Break-Even ضُبط على {bot_state["be_points"]} نقطة')
                elif len(parts) == 4:
                    tf_arg = parts[1].lower(); key = parts[2].lower(); val = int(parts[3])
                    if key == 'tp':
                        bot_state['gann_tp_per_tf'][tf_arg] = max(0, val)
                        eff = val if val > 0 else bot_state['gann_tp_points']
                        await send_tg_msg(f'✅ [{tf_arg}] TP = {val}p  (فعلي: {eff}p)')
                    elif key == 'sl':
                        bot_state['gann_sl_per_tf'][tf_arg] = max(0, val)
                        eff = val if val > 0 else bot_state['gann_sl_points']
                        await send_tg_msg(f'✅ [{tf_arg}] SL = {val}p  (فعلي: {eff}p)')
                    else: await send_tg_msg('❌ استخدم: /set TF tp VALUE أو /set TF sl VALUE أو /set be VALUE')
                else: await send_tg_msg('📝 أمثلة:\n/set 5m tp 200\n/set 15m sl 120\n/set be 25')
            except (ValueError, KeyError) as e:
                await send_tg_msg(f'❌ {e}\nأمثلة:\n/set 5m tp 200\n/set 15m sl 120\n/set be 25')

        elif msg.lower().startswith('/backtest'):
            parts = msg.strip().split()
            try:
                if len(parts) < 2: raise ValueError('أدخل تاريخاً')
                start_dam = datetime.strptime(parts[1], '%Y-%m-%d')
                start_dt  = _dam_to_utc(start_dam)
                if len(parts) >= 3:
                    end_dam = datetime.strptime(parts[2], '%Y-%m-%d')
                    end_dt  = _dam_to_utc(end_dam) + timedelta(days=1)
                else:
                    end_dt = start_dt + timedelta(days=1)
                days = max(1, int((end_dt - start_dt).total_seconds() / 86400))
                if bot_state['is_backtesting']:
                    await send_tg_msg('⚠️ باكتيست يعمل بالفعل. أرسل /cancel_bt لإيقافه.')
                else:
                    asyncio.create_task(run_gann_backtest(days, start_dt=start_dt, end_dt=end_dt))
                    await send_tg_msg(f'⏳ <b>باكتيست يعمل</b>\nمن: {_utc_to_dam(start_dt).strftime("%Y-%m-%d")}\nإلى: {_utc_to_dam(end_dt).strftime("%Y-%m-%d")}\n({days} يوم)')
            except ValueError as e:
                await send_tg_msg(f'❌ {e}\nالصيغة:\n/backtest 2026-06-24\n/backtest 2026-06-24 2026-06-26')


            global _bt_progress
            if _bt_progress and bot_state['is_backtesting']: await _bt_progress.cancel(); await send_tg_msg('Cancel signal sent.')
            else: await send_tg_msg('No backtest running.')

        elif msg == '/dd':
            sod = bot_state['sod_balance']; date = bot_state['sod_date'] or '-'; trig = 'YES 🔴' if bot_state['dd_triggered'] else 'NO 🟢'
            if sod:
                lim = sod * (1 - DD_LIMIT_PCT)
                await send_tg_msg(f'<b>Daily DD</b>\nDate:{date}\nSOD:${sod:.2f}  Limit:${sod*DD_LIMIT_PCT:.2f}\nStop Equity:${lim:.2f}\nTriggered:{trig}')
            else: await send_tg_msg('DD: No SOD yet. Connect server first.')

        elif msg == '/restart_sessions':
            global _http, _poll_task
            if _poll_task and not _poll_task.done(): _poll_task.cancel()
            if _http and not _http.closed: await _http.close()
            _http = None; get_http()
            await send_tg_msg('✅ Sessions reset.\nPolling restarts in ~2s automatically.')

        elif msg == '/ping':
            uptime = str(datetime.now(timezone.utc) - _start_time).split('.')[0]
            await send_tg_msg(f'🏓 <b>Pong!</b>\nUptime: {uptime}\nBot: {bot_state["status"]}\nServer: {"🟢" if bot_state["live_connected"] else "🔴"}')

        elif msg == '/status':
            cyc   = f'نشطة — إغلاق: {bot_state["gann_close_used"]:.2f}' if bot_state['gann_cycle_active'] and bot_state['gann_close_used'] else 'لا توجد دورة نشطة'
            open_n = len(bot_state.get('gann_open_trades', {}))
            en_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on]
            await send_tg_msg(
                f'<b>Bot Status v5.3 — Gann Engine</b>\n'
                f'Status:   {bot_state["status"]}\n'
                f'Server:   {"🟢" if bot_state["live_connected"] else "🔴"}\n'
                f'الدورة:   {cyc}\n'
                f'صفقات مفتوحة: {open_n}\n'
                f'فريمات نشطة: {", ".join(en_tfs) or "لا يوجد"}\n'
                f'وضع الدخول: {bot_state["gann_entry_mode"]}\n'
                f'فلتر: {"⭐ قوية" if bot_state["gann_zone_filter"]=="star" else "كل المستويات"}\n'
                f'Lot:      {bot_state["lot_size"]}\n'
                f'DD:       {"TRIGGERED 🔴" if bot_state["dd_triggered"] else "OK 🟢"}\n'
                f'Target:   {"✅ ON ($" + str(bot_state["daily_target_usd"]) + ")" if bot_state["daily_target_enabled"] else "⬜ OFF"}{" — REACHED 🎯" if bot_state["profit_target_triggered"] else ""}\n'
                f'LossLim:  {"✅ ON ($" + str(bot_state["daily_loss_usd"]) + ")" if bot_state["daily_loss_enabled"] else "⬜ OFF"}{" — HIT 🛑" if bot_state["loss_limit_triggered"] else ""}'
            )
        return

    if 'callback_query' not in update: return
    q = update['callback_query']; d = q['data']; chat_id = q['message']['chat']['id']; msg_id = q['message']['message_id']
    bot_state['chat_id'] = chat_id; c_log(f'CB: {d}')
    try: await _handle_callback(d, chat_id, msg_id)
    except Exception as e: c_log(f'CB error [{d}]: {e}')
    finally: await answer_callback(q['id'])

# ─────────────────────────────────────────────────────────────
# CALLBACK HANDLER
# ─────────────────────────────────────────────────────────────
async def _handle_callback(d: str, chat_id: int, msg_id: int) -> None:
    global _bt_progress
    if d == 'noop': pass
    elif d == 'menu_main': await _show(chat_id, msg_id, 'Main Menu:', get_main_keyboard())
    elif d == 'menu_risk': await _show(chat_id, msg_id, 'Risk Settings:', get_risk_keyboard())

    # ── Gann menu ──
    elif d == 'menu_gann':
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'menu_gann_bt':
        await _show(chat_id, msg_id, '📊 باكتيست جان — اختر الفترة:', get_gann_bt_keyboard())
    elif d == 'gann_show_levels':
        if not bot_state['gann_levels'] or not bot_state['gann_close_used']:
            # لا يوجد سلّم نشط بعد — اجلب آخر شمعة H1 وولّد فوراً
            await send_tg_msg('⏳ لا يوجد سلّم نشط، جاري جلب آخر شمعة H1...')
            last_h1 = await _gann_fetch_last_closed_h1()
            if last_h1:
                h1_close = float(last_h1['close'])
                bot_state['gann_levels']          = gann_calc_levels(h1_close)
                bot_state['gann_close_used']       = h1_close
                bot_state['gann_last_h1_time']     = last_h1['time']
                bot_state['gann_cycle_started_at'] = datetime.now(timezone.utc)
                bot_state['gann_cycle_active']     = True
                bot_state['gann_cycle_end_flag']   = None
                bot_state['gann_level_status']     = {}
                bot_state['gann_open_trades']       = {}
            else:
                await send_tg_msg('❌ تعذّر جلب بيانات OANDA. تأكد من اتصال الخادم ثم حاول مجدداً.')
                await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard()); return
        await send_tg_msg(_gann_fmt_levels_msg(bot_state['gann_close_used']))
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_toggle_filter':
        bot_state['gann_zone_filter'] = 'all' if bot_state['gann_zone_filter'] == 'star' else 'star'
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_toggle_entry':
        bot_state['gann_entry_mode'] = 'breakout_retest' if bot_state['gann_entry_mode'] == 'touch' else 'touch'
        bot_state['gann_level_status'] = {}   # إعادة ضبط حالات الكسر عند تغيير الوضع
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_tpsl_tf':
        await _show(chat_id, msg_id, '⚙️ TP/SL مخصص لكل فريم — اختر الفريم:', get_gann_tpsl_tf_keyboard())
    elif d.startswith('gann_tptf_sel_'):
        sel_tf = d[len('gann_tptf_sel_'):]
        await _show(chat_id, msg_id, f'⚙️ TP/SL [{sel_tf}]:', get_gann_tpsl_tf_keyboard(sel_tf))
    elif d.startswith('gann_tptf_itp_'):
        tf = d[len('gann_tptf_itp_'):]
        bot_state['gann_tp_per_tf'][tf] = bot_state['gann_tp_per_tf'].get(tf, 0) + 10
        await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_dtp_'):
        tf = d[len('gann_tptf_dtp_'):]
        bot_state['gann_tp_per_tf'][tf] = max(0, bot_state['gann_tp_per_tf'].get(tf, 0) - 10)
        await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_isl_'):
        tf = d[len('gann_tptf_isl_'):]
        bot_state['gann_sl_per_tf'][tf] = bot_state['gann_sl_per_tf'].get(tf, 0) + 10
        await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_dsl_'):
        tf = d[len('gann_tptf_dsl_'):]
        bot_state['gann_sl_per_tf'][tf] = max(0, bot_state['gann_sl_per_tf'].get(tf, 0) - 10)
        await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_rst_'):
        tf = d[len('gann_tptf_rst_'):]
        bot_state['gann_tp_per_tf'][tf] = 0; bot_state['gann_sl_per_tf'][tf] = 0
        await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}] — تمت إعادة الضبط للقيمة العامة:', get_gann_tpsl_tf_keyboard(tf))


    elif d.startswith('gann_tf_'):
        tfk = d[len('gann_tf_'):]
        if tfk in bot_state['gann_monitor_tfs']:
            bot_state['gann_monitor_tfs'][tfk] = not bot_state['gann_monitor_tfs'][tfk]
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_toggle_tpsl':
        bot_state['gann_tpsl_mode'] = 'atr' if bot_state['gann_tpsl_mode'] == 'fixed' else 'fixed'
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_hours': bot_state['gann_cycle_hours'] = max(1, bot_state['gann_cycle_hours'] - 1);  await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_hours': bot_state['gann_cycle_hours'] = min(24, bot_state['gann_cycle_hours'] + 1); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_margin': bot_state['gann_touch_margin_pts'] = max(1, bot_state['gann_touch_margin_pts'] - 1);  await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_margin': bot_state['gann_touch_margin_pts'] = min(50, bot_state['gann_touch_margin_pts'] + 1); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_tp10': bot_state['gann_tp_points'] = max(10, bot_state['gann_tp_points'] - 10);  await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_tp10': bot_state['gann_tp_points'] = min(1000, bot_state['gann_tp_points'] + 10); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_sl10': bot_state['gann_sl_points'] = max(10, bot_state['gann_sl_points'] - 10);  await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_sl10': bot_state['gann_sl_points'] = min(1000, bot_state['gann_sl_points'] + 10); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_atrp':  bot_state['gann_atr_period'] = max(5,   bot_state['gann_atr_period'] - 1);   await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_atrp':  bot_state['gann_atr_period'] = min(50,  bot_state['gann_atr_period'] + 1);   await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_atrsl': bot_state['gann_atr_sl_mult'] = max(0.5, round(bot_state['gann_atr_sl_mult'] - 0.5, 1)); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_atrsl': bot_state['gann_atr_sl_mult'] = min(5.0, round(bot_state['gann_atr_sl_mult'] + 0.5, 1)); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_dec_atrtp': bot_state['gann_atr_tp_mult'] = max(0.5, round(bot_state['gann_atr_tp_mult'] - 0.5, 1)); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d == 'gann_inc_atrtp': bot_state['gann_atr_tp_mult'] = min(8.0, round(bot_state['gann_atr_tp_mult'] + 0.5, 1)); await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())
    elif d.startswith('gbt_'):
        days = int(d.split('_')[1])
        if not bot_state['is_backtesting']:
            asyncio.create_task(run_gann_backtest(days))
        await _show(chat_id, msg_id, f'⏳ باكتيست جان ({days} يوم) يعمل...', get_gann_bt_keyboard())
    elif d == 'hide_keyboard':
        bot_state['menu_button_map'] = {}
        await _show(chat_id, msg_id, 'تم إخفاء لوحة الأزرار 👁‍🗨\nلإظهار القائمة مجدداً، أرسل /start', {'remove_keyboard': True})

    elif d == 'toggle_live_conn':
        if not bot_state['live_connected']:
            await _show(chat_id, msg_id, '⏳ Connecting...', get_main_keyboard())
            try:
                api = MetaApi(METAAPI_TOKEN); bot_state['account_obj'] = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                bot_state['connection_obj'] = bot_state['account_obj'].get_rpc_connection()
                await bot_state['connection_obj'].connect(); await bot_state['connection_obj'].wait_synchronized()
                bot_state['live_connected'] = True
                await _capture_sod_balance()
                await _show(chat_id, msg_id, '✅ Connected!', get_main_keyboard())
            except Exception as e: await _show(chat_id, msg_id, f'❌ Failed:\n{e}', get_main_keyboard())
        else:
            bot_state['live_connected'] = False; bot_state['connection_obj'] = None; bot_state['account_obj'] = None
            await _show(chat_id, msg_id, '🔴 Disconnected.', get_main_keyboard())

    elif d == 'toggle_status':
        bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'
        if bot_state['status'] == 'RUNNING':
            bot_state['dd_triggered'] = False; bot_state['profit_target_triggered'] = False; bot_state['loss_limit_triggered'] = False
            await send_tg_msg('▶️ <b>Resumed</b> — bot restarted.')
        await _show(chat_id, msg_id, 'Main Menu:', get_main_keyboard())

    elif d == 'toggle_be': bot_state['use_be'] = not bot_state['use_be']; await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'dec_be': bot_state['be_points'] = max(5, bot_state['be_points'] - 10); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'inc_be': bot_state['be_points'] = min(500, bot_state['be_points'] + 10); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'toggle_trailing': bot_state['use_trailing'] = not bot_state['use_trailing']; await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'dec_trl': bot_state['trail_points'] = max(10, bot_state['trail_points'] - 10); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'inc_trl': bot_state['trail_points'] = min(1000, bot_state['trail_points'] + 10); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'dec_tro': bot_state['trail_offset'] = max(10, bot_state['trail_offset'] - 10); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'inc_tro': bot_state['trail_offset'] = min(1000, bot_state['trail_offset'] + 10); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'toggle_spread': bot_state['use_max_spread'] = not bot_state['use_max_spread']; await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())

    # ── Trend filter ──
    elif d == 'menu_trend_filter': await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_off':  bot_state['gann_trend_filter'] = 'off';     await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_ema200': bot_state['gann_trend_filter'] = 'ema200'; await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_dual': bot_state['gann_trend_filter'] = 'ema_dual'; await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_cycle_tf':
        _tfs = ['1m','5m','15m','30m','60m','4h','1d']
        cur = bot_state['gann_trend_tf']
        bot_state['gann_trend_tf'] = _tfs[(_tfs.index(cur)+1) % len(_tfs)] if cur in _tfs else '60m'
        await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_dec_s':  bot_state['gann_ema_single'] = max(10, bot_state['gann_ema_single']-10); await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_inc_s':  bot_state['gann_ema_single'] = min(500,bot_state['gann_ema_single']+10); await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_dec_f':  bot_state['gann_ema_fast']   = max(5,  bot_state['gann_ema_fast']-5);   await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_inc_f':  bot_state['gann_ema_fast']   = min(200,bot_state['gann_ema_fast']+5);   await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_dec_sl': bot_state['gann_ema_slow']   = max(20, bot_state['gann_ema_slow']-5);   await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())
    elif d == 'trend_inc_sl': bot_state['gann_ema_slow']   = min(500,bot_state['gann_ema_slow']+5);   await _show(chat_id, msg_id, '🔍 فلتر الاتجاه:', get_trend_filter_keyboard())

    # ── Anti-spam ──
    elif d == 'gann_toggle_spam':
        bot_state['gann_anti_spam'] = not bot_state['gann_anti_spam']
        await _show(chat_id, msg_id, '📐 محرك جان — الإعدادات:', get_gann_keyboard())


    elif d == 'toggle_daily_target':
        bot_state['daily_target_enabled'] = not bot_state['daily_target_enabled']
        if bot_state['daily_target_enabled']: bot_state['profit_target_triggered'] = False
        await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'toggle_daily_loss':
        bot_state['daily_loss_enabled'] = not bot_state['daily_loss_enabled']
        if bot_state['daily_loss_enabled']: bot_state['loss_limit_triggered'] = False
        await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'inc_lot': bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'dec_lot': bot_state['lot_size'] = max(0.01, round(bot_state['lot_size'] - 0.01, 2)); await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())

    elif d == 'cancel_bt':
        if _bt_progress and bot_state['is_backtesting']: await _bt_progress.cancel(); await _show(chat_id, msg_id, 'Stopping...', get_main_keyboard())
        else: await _show(chat_id, msg_id, 'No BT running.', get_main_keyboard())

    elif d == 'bt_show_progress':
        if _bt_progress: await send_tg_msg(f'BT phase: {_bt_progress.phase}')
        else: await send_tg_msg('No BT running.')

    elif d == 'report':
        cyc = f'نشطة — إغلاق H1: {bot_state["gann_close_used"]:.2f}' if bot_state['gann_cycle_active'] and bot_state['gann_close_used'] else '⚫ لا توجد دورة نشطة'
        open_n = len(bot_state.get('gann_open_trades', {}))
        en_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on]
        elapsed = ''
        if bot_state['gann_cycle_started_at']:
            mins = int((datetime.now(timezone.utc) - bot_state['gann_cycle_started_at']).total_seconds() / 60)
            elapsed = f' | منذ {mins}د'
        lines = [
            '<b>📐 تقرير الدورة الحالية</b>',
            f'الدورة: {cyc}{elapsed}',
            f'صفقات مفتوحة: {open_n}',
            f'فريمات نشطة: {", ".join(en_tfs) or "لا يوجد"}',
            f'وضع الدخول: {"لمس" if bot_state["gann_entry_mode"]=="touch" else "كسر+ريتيست"}',
            f'فلتر: {"⭐ قوية فقط" if bot_state["gann_zone_filter"]=="star" else "كل المستويات"}',
            f'TP/SL: {"نقاط ثابتة" if bot_state["gann_tpsl_mode"]=="fixed" else "ATR"} | Lot: {bot_state["lot_size"]}',
        ]
        if bot_state['gann_levels'] and bot_state['gann_close_used']:
            active_lv = gann_active_levels()
            used_keys = {k for k, v in bot_state['gann_level_status'].items() if v == 'used'}
            lines.append('')
            for lv in active_lv[:8]:
                mark = ' ✓' if lv['key'] in used_keys else ''
                icon = '🔴' if lv['dir'] == 'up' else '🟢'
                lines.append(f"{icon} {lv['price']:.2f}{'⭐' if lv['star'] else ''}{mark}")
        await _show(chat_id, msg_id, '\n'.join(lines), get_main_keyboard())

    elif d == 'account':
        if not (bot_state['live_connected'] and bot_state['connection_obj']): await _show(chat_id, msg_id, '🔴 Not connected.', get_main_keyboard())
        else:
            try:
                info = await bot_state['connection_obj'].get_account_information(); pos = await bot_state['connection_obj'].get_positions()
                sod = bot_state['sod_balance']; eq = float(info.get('equity', 0))
                text = (f'<b>Account</b>\nBalance:  ${info.get("balance","?")}\nEquity:   ${eq}\nMargin:   ${info.get("freeMargin","?")}\nTrades:   {len(pos)}\nSOD:      ${sod:.2f}\nDD Used:  ${sod-eq:.2f} ({round((sod-eq)/sod*100,2)})%') if sod else f'<b>Account</b>\nBalance:{info.get("balance","?")}  Equity:{eq}  Trades:{len(pos)}'
                await _show(chat_id, msg_id, text, get_main_keyboard())
            except Exception as e: await _show(chat_id, msg_id, f'Error: {e}', get_main_keyboard())

    elif d == 'dd_status':
        sod = bot_state['sod_balance']; date = bot_state['sod_date'] or '-'; trig = '🔴 TRIGGERED' if bot_state['dd_triggered'] else '🟢 OK'
        if sod:
            lim  = sod * (1 - DD_LIMIT_PCT)
            await _show(chat_id, msg_id, f'<b>Daily DD (3%)</b>\nDate:{date}\nSOD:${sod:.2f}  Limit:${sod*DD_LIMIT_PCT:.2f}\nStop:${lim:.2f}  Status:{trig}\n\nBlocked (Damascus UTC+3):\n13:xx | 18:xx | 21:xx | 22:xx', get_main_keyboard())
        else: await _show(chat_id, msg_id, 'DD: No SOD yet.', get_main_keyboard())

    elif d == 'close_all':
        if not (bot_state['live_connected'] and bot_state['connection_obj']): await _show(chat_id, msg_id, '🔴 Not connected.', get_main_keyboard())
        else:
            try:
                positions = await bot_state['connection_obj'].get_positions()
                if not positions: await _show(chat_id, msg_id, 'No open trades.', get_main_keyboard())
                else:
                    for p in positions: await bot_state['connection_obj'].close_position(p['id'])
                    await edit_tg_msg(chat_id, msg_id, f'✅ Closed {len(positions)} trade(s).', get_main_keyboard())
            except Exception as e: await _show(chat_id, msg_id, f'Error: {e}', get_main_keyboard())

    else: c_log(f'CB unhandled: {d!r}')

# ─────────────────────────────────────────────────────────────
# TELEGRAM POLLING  +  WATCHDOG
# ─────────────────────────────────────────────────────────────
_poll_task: asyncio.Task | None = None

async def telegram_polling_loop() -> None:
    c_log('Telegram polling started.'); url = f'https://api.telegram.org/bot{TG_TOKEN}/getUpdates'
    backoff = 1
    while True:
        # نفتح session جديدة في كل دورة — يمنع تراكم الاتصالات المعلّقة
        connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300, force_close=True)
        # total=None: لا نضع حداً كلياً لأن long-poll يستغرق 20 ثانية عمداً
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=28)
        sess = aiohttp.ClientSession(connector=connector, timeout=timeout)
        try:
            while True:
                try:
                    async with sess.get(url, params={'offset': bot_state['last_update_id'] + 1, 'timeout': 20}) as resp:
                        if resp.status == 200:
                            backoff = 1
                            bot_state['last_poll_ok'] = datetime.now(timezone.utc).timestamp()
                            data = await resp.json()
                            for upd in data.get('result', []):
                                bot_state['last_update_id'] = upd['update_id']
                                asyncio.create_task(_safe_process(upd))
                        elif resp.status == 429:
                            retry = int(resp.headers.get('Retry-After', 5))
                            c_log(f'Polling 429 — waiting {retry}s'); await asyncio.sleep(retry)
                        else:
                            c_log(f'Polling HTTP {resp.status}'); await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)
                except asyncio.CancelledError: raise
                except (aiohttp.ServerTimeoutError, asyncio.TimeoutError, TimeoutError):
                    # timeout على long-poll طبيعي تماماً — لا نزيد backoff ولا نسجّل خطأ
                    await asyncio.sleep(0.5); continue
                except aiohttp.ClientConnectorError as e:
                    c_log(f'Polling connect error: {e} — retry in {backoff}s')
                    await asyncio.sleep(backoff); backoff = min(backoff * 2, 30); break  # أعد بناء session
                except Exception as e:
                    c_log(f'Polling error: {e} — retry in {backoff}s')
                    await asyncio.sleep(backoff); backoff = min(backoff * 2, 30); break
        except asyncio.CancelledError: await sess.close(); raise
        finally:
            await sess.close()
        await asyncio.sleep(1)  # انتظار قصير قبل إنشاء session جديدة

async def _safe_process(upd: dict) -> None:
    try: await process_tg_update(upd)
    except Exception as e: c_log(f'Update processing error (upd_id={upd.get("update_id")}): {e}')

async def telegram_watchdog() -> None:
    global _poll_task
    await asyncio.sleep(30)
    while True:
        await asyncio.sleep(20)
        try:
            last = bot_state.get('last_poll_ok', 0.0); age = datetime.now(timezone.utc).timestamp() - last
            if age > 60 and _poll_task is not None and not _poll_task.done(): c_log(f'Watchdog: polling silent {age:.0f}s — cancelling task for restart.'); _poll_task.cancel()
            elif age > 60: c_log(f'Watchdog: polling silent {age:.0f}s — task already dead, supervised will restart.')
        except Exception as e: c_log(f'Watchdog error: {e}')

# ─────────────────────────────────────────────────────────────
# TASK SUPERVISOR
# ─────────────────────────────────────────────────────────────
async def supervised(coro_fn, *args, label: str = '') -> None:
    global _poll_task
    while True:
        try:
            task = asyncio.current_task()
            if label == 'tg_polling': _poll_task = task
            await coro_fn(*args)
        except asyncio.CancelledError: c_log(f'Task "{label}" cancelled — restarting.'); await asyncio.sleep(2)   
        except Exception as e: c_log(f'Task "{label or coro_fn.__name__}" crashed: {e} — restart in 5s'); await asyncio.sleep(5)

async def api_market_report(request: web.Request) -> web.Response:
    cyc  = f'active close={bot_state["gann_close_used"]:.2f}' if bot_state['gann_cycle_active'] and bot_state['gann_close_used'] else 'idle'
    lv   = [{'price': l['price'], 'dir': l['dir'], 'star': l['star']} for l in bot_state['gann_levels'] if l['dir'] != 'ref']
    return web.json_response({'status': 'success', 'cycle': cyc, 'levels': lv,
                              'open_trades': len(bot_state.get('gann_open_trades', {}))})

# ─────────────────────────────────────────────────────────────
# WEB SERVER
# ─────────────────────────────────────────────────────────────
_start_time = datetime.now(timezone.utc)

async def handle_ping(request: web.Request) -> web.Response:
    uptime = str(datetime.now(timezone.utc) - _start_time).split('.')[0]
    cyc    = f'active close={bot_state["gann_close_used"]:.2f}' if bot_state['gann_cycle_active'] and bot_state['gann_close_used'] else 'idle'
    last_p = datetime.now(timezone.utc).timestamp() - bot_state.get('last_poll_ok', 0)
    return web.Response(
        text=(f'Gold Scalper Bot v5.3 — Gann Levels Engine\n'
              f'Uptime: {uptime}\nServer: {"connected" if bot_state["live_connected"] else "disconnected"}\nBot: {bot_state["status"]}\n'
              f'Gann Cycle: {cyc}\nOpen Trades: {len(bot_state.get("gann_open_trades", {}))}\n'
              f'BT: {"RUNNING" if bot_state["is_backtesting"] else "idle"}\n'
              f'SOD: {"$"+str(round(bot_state["sod_balance"],2)) if bot_state["sod_balance"] else "N/A"}'
              f'\nDD: {"TRIGGERED" if bot_state["dd_triggered"] else "OK"}\nTG poll: {last_p:.0f}s ago'),
        content_type='text/plain',
    )

async def api_update_config(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        for k, v in data.items():
            if k in bot_state:
                bot_state[k] = v
        return web.json_response({'status': 'success', 'bot_state': _get_safe_state()})
    except Exception as e:
        return web.json_response({'status': 'error', 'message': str(e)}, status=400)

def _get_safe_state() -> dict:
    return {
        'status':               bot_state['status'],
        'live_connected':       bot_state['live_connected'],
        'sod_balance':          bot_state.get('sod_balance', 0.0) or 0.0,
        'dd_triggered':         bot_state['dd_triggered'],
        'lot_size':             bot_state['lot_size'],
        'gann_cycle_active':    bot_state['gann_cycle_active'],
        'gann_close_used':      bot_state['gann_close_used'],
        'gann_zone_filter':     bot_state['gann_zone_filter'],
        'gann_entry_mode':      bot_state['gann_entry_mode'],
        'gann_cycle_hours':     bot_state['gann_cycle_hours'],
        'daily_loss_enabled':   bot_state['daily_loss_enabled'],
        'daily_loss_usd':       bot_state['daily_loss_usd'],
        'daily_target_enabled': bot_state['daily_target_enabled'],
        'daily_target_usd':     bot_state['daily_target_usd'],
        'is_backtesting':       bot_state['is_backtesting'],
        'use_be':               bot_state.get('use_be', False),
        'use_trailing':         bot_state.get('use_trailing', False),
        'use_max_spread':       bot_state.get('use_max_spread', True),
        'max_spread_pips':      bot_state.get('max_spread_pips', 3.0),
    }

async def api_status(request: web.Request) -> web.Response:
    return web.json_response(_get_safe_state())

async def api_engine_toggle(request: web.Request) -> web.Response:
    bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'
    return web.json_response({'status': 'success'})

async def api_engine_live_conn(request: web.Request) -> web.Response:
    bot_state['live_connected'] = not bot_state['live_connected']
    return web.json_response({'status': 'success'})

async def api_positions_close_all(request: web.Request) -> web.Response:
    asyncio.create_task(_close_all_positions())
    return web.json_response({'status': 'success'})

async def api_backtest_start(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        days = int(data.get('days', 7))
        if not bot_state['is_backtesting']:
            asyncio.create_task(run_gann_backtest(days))
            return web.json_response({'status': 'started', 'days': days})
        return web.json_response({'status': 'error', 'message': 'Backtest already running'}, status=400)
    except Exception as e:
        return web.json_response({'status': 'error', 'message': str(e)}, status=400)

async def api_backtest_status(request: web.Request) -> web.Response:
    global _bt_progress
    if not bot_state['is_backtesting'] or not _bt_progress:
        return web.json_response({
            'status': 'idle',
            'result': bot_state.get('last_backtest_result')
        })
    return web.json_response({
        'status': 'running',
        'phase': _bt_progress.phase,
        'progress': _bt_progress.overall_progress
    })

async def api_backtest_download(request: web.Request) -> web.Response:
    import os
    if os.path.exists('/tmp/latest_backtest.xlsx'):
        return web.FileResponse('/tmp/latest_backtest.xlsx', headers={
            'Content-Disposition': 'attachment; filename="latest_backtest.xlsx"'
        })
    return web.Response(text="No file generated yet", status=404)

_ws_clients = set()
async def ws_stream(request: web.Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    _ws_clients.add(ws)
    try:
        async for msg in ws: pass
    finally:
        _ws_clients.remove(ws)
    return ws

async def ws_pulse_loop():
    while True:
        await asyncio.sleep(2)
        if not _ws_clients: continue
        pulse = {tf: "Waiting..." for tf in bot_state["gann_monitor_tfs"]}
        pnl = {}
        for ws in list(_ws_clients):
            try:
                await ws.send_json({"type": "market_pulse", "data": pulse})
                await ws.send_json({"type": "positions_pnl", "data": pnl})
            except Exception: pass

# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
async def main() -> None:
    get_http()
    app = web.Application()
    app.router.add_get('/', handle_ping)
    app.router.add_get('/api/status', api_status)
    app.router.add_put('/api/config', api_update_config)
    app.router.add_post('/api/engine/toggle', api_engine_toggle)
    app.router.add_post('/api/engine/live_conn', api_engine_live_conn)
    app.router.add_post('/api/positions/close_all', api_positions_close_all)
    app.router.add_post('/api/backtest/start', api_backtest_start)
    app.router.add_get('/api/backtest/status', api_backtest_status)
    app.router.add_get('/api/backtest/download', api_backtest_download)
    app.router.add_get('/api/report', api_market_report)
    app.router.add_get('/ws/stream', ws_stream)
    
    runner = web.AppRunner(app); await runner.setup()
    port = int(os.environ.get('PORT', 10000)); await web.TCPSite(runner, '0.0.0.0', port).start(); c_log(f'Web server on port {port}')

    bot_state['last_poll_ok'] = datetime.now(timezone.utc).timestamp()

    tasks = [
        asyncio.create_task(supervised(telegram_polling_loop, label='tg_polling')),
        asyncio.create_task(supervised(telegram_watchdog,     label='tg_watchdog')),
        asyncio.create_task(supervised(position_monitor,      label='pos_monitor')),
        asyncio.create_task(supervised(daily_drawdown_monitor, label='dd_monitor')),
        asyncio.create_task(supervised(daily_profit_target_monitor, label='profit_target_monitor')),
        asyncio.create_task(supervised(ws_pulse_loop,          label='ws_pulse_loop')),
        asyncio.create_task(supervised(gann_cycle_manager,    label='gann_cycle')),
        asyncio.create_task(supervised(gann_monitor_scanner,  label='gann_monitor')),
    ]

    c_log('Gold Scalper Bot v5.3 + Gann Engine started.')
    try: await asyncio.gather(*tasks)
    finally:
        if _http and not _http.closed: await _http.close()
        c_log('Bot shut down.')

if __name__ == '__main__':
    asyncio.run(main())
