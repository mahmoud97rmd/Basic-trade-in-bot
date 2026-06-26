"""
Gold Scalper Bot — v8.2 (Macro VWAP + Micro EMA Independent)
Strategy : Gann Levels + Pure Touch + (H1 VWAP OR Multi-TF EMA)
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

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
METAAPI_TOKEN = os.environ.get('METAAPI_TOKEN', 'YOUR_METAAPI_TOKEN')
ACCOUNT_ID    = os.environ.get('ACCOUNT_ID',    'YOUR_ACCOUNT_ID')
TG_TOKEN      = os.environ.get('TG_TOKEN',      '8647261254:AAEQnSYsmEFJ1ig8vhe_ciRrskuh6il1_PU')

OANDA_ACCOUNT  = os.environ.get('OANDA_ACCOUNT', '101-004-28533521-003')
OANDA_TOKEN    = os.environ.get('OANDA_TOKEN',   '0e282d5a3e65ad6fdd809e2c195bb1cd-9e2158e12fa13840e030ee3081b36fab')
OANDA_SYMBOL   = 'XAU_USD'
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

    'lot_size':         0.05,
    'pip_value':        0.1,     
    'contract_size':    100,     
    
    'menu_button_map': {},
    'last_poll_ok':     0.0,

    # ── Gann Levels Engine ──
    'gann_levels':            [],      
    'gann_level_status':      {},      
    'gann_close_used':        None,    
    'gann_last_h1_time':      None, 
    'gann_cycle_active':      False,   
    'gann_cycle_started_at':  None,
    'gann_cycle_hours':       1,        
    'gann_open_trades':       {},      
    'gann_zone_filter':       'star',  
    'gann_entry_mode':        'touch_trend', 
    
    # ── Trend Filters (Macro VWAP / Micro EMA) ──
    'trend_filter_type':      'vwap',     
    'trend_vwap_period':      100,                     # قيمة شاملة موحدة على H1
    'trend_vwap_per_tf':      {tf: 24 for tf in _TFS}, # متوفرة كقيمة لو احتجتها
    'trend_ema_per_tf':       {tf: 60 for tf in _TFS}, # قيمة مستقلة لكل فريم
    
    'gann_monitor_tfs':       {tf: False for tf in _TFS},
    'gann_touch_margin_pts':  5,       
    'gann_tpsl_mode':         'fixed', 
    
    'gann_tp_points':         140,
    'gann_sl_points':         110,
    'gann_tp_per_tf': {
        '1m': 0, '2m': 0, '3m': 0, '4m': 0, '5m': 0, '6m': 0,
        '10m': 0, '15m': 0, '20m': 0, '30m': 0, '1h': 0, '2h': 0
    },
    'gann_sl_per_tf': {
        '1m': 0, '2m': 0, '3m': 0, '4m': 0, '5m': 0, '6m': 0,
        '10m': 0, '15m': 0, '20m': 0, '30m': 0, '1h': 0, '2h': 0
    },
    'gann_atr_period':        14,
    'gann_atr_sl_mult':       1.5,
    'gann_atr_tp_mult':       2,
}

bot_state['gann_monitor_tfs']['1m'] = True
bot_state['gann_monitor_tfs']['2m'] = True
bot_state['gann_monitor_tfs']['3m'] = True
bot_state['gann_monitor_tfs']['5m'] = True
bot_state['gann_monitor_tfs']['10m'] = True
bot_state['gann_monitor_tfs']['15m'] = True
bot_state['gann_monitor_tfs']['20m'] = True
bot_state['gann_monitor_tfs']['30m'] = True
bot_state['gann_monitor_tfs']['1h'] = True
bot_state['gann_monitor_tfs']['4m'] = True
bot_state['gann_monitor_tfs']['6m'] = True
bot_state['gann_monitor_tfs']['2h'] = True

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
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True), timeout=aiohttp.ClientTimeout(total=12, connect=5)) as sess:
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

async def fetch_candles(granularity_str: str, count: int = 5000, end_time: datetime = None) -> list:
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
# GANN LEVELS ENGINE & ATR
# ─────────────────────────────────────────────────────────────
GANN_ACOEF  = [0.0208, 0.0417, 0.0625, 0.0833, 0.125, 0.25, 0.333, 0.5, 1, 2, 4]
GANN_AIMP   = [False,  False,  False,  True,   False, False, False, True, True, False, False]
GANN_TFC_H1 = 0.02

def gann_calc_levels(close: float) -> list[dict]:
    levels = []
    for i, coef in enumerate(GANN_ACOEF):
        offset = close * coef * GANN_TFC_H1
        up = round(close + offset, 2); dn = round(close - offset, 2); star = GANN_AIMP[i]
        levels.append({'key': f'up_{i}', 'price': up, 'dir': 'up', 'star': star})
        if dn > 0: levels.append({'key': f'dn_{i}', 'price': dn, 'dir': 'dn', 'star': star})
    levels.append({'key': 'ref', 'price': round(close, 2), 'dir': 'ref', 'star': False})
    levels.sort(key=lambda x: x['price'], reverse=True)
    return levels

def gann_active_levels() -> list[dict]:
    lv = [l for l in bot_state['gann_levels'] if l['dir'] != 'ref']
    if bot_state['gann_zone_filter'] == 'star': return [l for l in lv if l['star']]
    return lv

def _gann_tf_tp(tf: str) -> int:
    v = bot_state['gann_tp_per_tf'].get(tf, 0)
    return v if v > 0 else bot_state['gann_tp_points']

def _gann_tf_sl(tf: str) -> int:
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
    pv = bot_state['pip_value']
    if bot_state['gann_tpsl_mode'] == 'atr':
        atr = _gann_atr(candles, bot_state['gann_atr_period'])
        if not atr: atr = _gann_tf_sl(tf) * pv
        sl_dist = atr * bot_state['gann_atr_sl_mult']
        tp_dist = atr * bot_state['gann_atr_tp_mult']
    else:
        sl_dist = _gann_tf_sl(tf) * pv
        tp_dist = _gann_tf_tp(tf) * pv
    if is_buy: return round(entry + tp_dist, 2), round(entry - sl_dist, 2)
    return round(entry - tp_dist, 2), round(entry + sl_dist, 2)

async def _gann_fetch_last_closed_h1() -> dict | None:
    candles = await fetch_candles('1h', count=2)
    if not candles: return None
    candles = sorted(candles, key=lambda c: c['time'])
    return candles[-1]

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
    flt_trend = bot_state['trend_filter_type'].upper()
    mode = f'لمس مباشر + فلتر ({flt_trend})' if bot_state['gann_entry_mode'] == 'touch_trend' else 'لمس أعمى (بدون فلتر)'
    return (f"📐 <b>سلّم جان — دورة جديدة</b>\n"
            f"إغلاق H1: <b>{close:.2f}</b>\n"
            f"مدة المراقبة: {bot_state['gann_cycle_hours']}س  |  فلتر: {filt}\nالدخول: {mode}\n\n"
            + '\n'.join(lines))

async def _gann_open_trade(is_buy: bool, level: dict, candles: list, reason: str, tf: str) -> None:
    try:
        price = float(candles[-1]['close'])
        tp, sl = _gann_calc_tpsl(price, is_buy, candles, tf=tf)
        lot = bot_state['lot_size']; side = 'BUY' if is_buy else 'SELL'
        tp_pts = _gann_tf_tp(tf); sl_pts = _gann_tf_sl(tf)
        
        tpsl_lbl = (f"ATR({bot_state['gann_atr_period']})×{bot_state['gann_atr_sl_mult']}/{bot_state['gann_atr_tp_mult']}"
                    if bot_state['gann_tpsl_mode'] == 'atr' else f"SL:{sl_pts}p TP:{tp_pts}p")
        
        trade_id = f"sim_{int(datetime.now().timestamp())}_{tf}"
        bot_state['gann_open_trades'][trade_id]          = tf
        bot_state['gann_level_status'][level['key']]     = 'used'
        
        await send_tg_msg(
            f"<b>✅ {'BUY 📈' if is_buy else 'SELL 📉'} [جان {tf}]</b>  {reason}\n"
            f"المستوى: {level['price']:.2f}  |  الدخول: {price:.2f}\n"
            f"TP: {tp}  SL: {sl}  |  {tpsl_lbl}  |  Lot: {lot}\n"
            f"إغلاق H1: {bot_state['gann_close_used']:.2f}"
        )
    except Exception as e:
        bot_state['gann_level_status'][level['key']] = 'used'
        await send_tg_msg(f"<b>❌ فشل تنفيذ الصفقة [جان {tf}]</b>\nالمستوى: {level['price']:.2f}\n{e}")

# ─────────────────────────────────────────────────────────────
# BACKTEST PROGRESS TRACKER
# ─────────────────────────────────────────────────────────────
class BtProgress:
    BAR_LEN = 14; HEARTBEAT = 15
    def __init__(self, label: str, active_tfs: list):
        self.label = label; self.active_tfs = active_tfs; self.cancelled = False; self.phase = 'Initialising...'
        self.tf_done = 0; self.tf_total = len(active_tfs); self.current_tf = ''
        self.bars_done = 0; self.bars_total = 0; self.win = 0; self.loss = 0; self.profit = 0.0
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
        lines += ['', f'W:{self.win}  L:{self.loss}', f'{icon} {pnl}  WR:{wr}', '', f'Elapsed: {self._elapsed()}']
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
    async def tick(self, bar_n: int, win: int, loss: int, profit: float) -> None: self.bars_done = bar_n; self.win = win; self.loss = loss; self.profit = profit; await self._edit()
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
        [{'text': '📐 محرك جان (الاستراتيجية)', 'callback_data': 'menu_gann'}],
        [{'text': '📊 بدء الباكتيست', 'callback_data': 'menu_gann_bt'}],
    ]}

def get_gann_keyboard() -> dict:
    zf   = bot_state['gann_zone_filter']
    em   = bot_state['gann_entry_mode']
    mg   = bot_state['gann_touch_margin_pts']
    tpsm = bot_state['gann_tpsl_mode']
    hrs  = bot_state['gann_cycle_hours']
    cyc  = '🟢 نشطة' if bot_state['gann_cycle_active'] else '⚫ غير نشطة'
    open_n = len(bot_state.get('gann_open_trades', {}))
    
    flt_type = bot_state['trend_filter_type']
    
    zf_lbl  = '⭐ القوية فقط' if zf == 'star' else '📋 كل المستويات'
    em_lbl  = f'⚡ لمس + فلتر ({flt_type.upper()})' if em == 'touch_trend' else '⚡ لمس أعمى (بدون فلتر)'
    tps_lbl = f'🎯 TP/SL: {"نقاط ثابتة" if tpsm == "fixed" else "حسب ATR"}'
    filt_btn_lbl = "📉 الفلتر المعتمد: (EMA)" if flt_type == 'ema' else "🌊 الفلتر المعتمد: (VWAP الشامل)"

    tp = bot_state['gann_tp_points']; sl = bot_state['gann_sl_points']
    atp = bot_state['gann_atr_tp_mult']; asp = bot_state['gann_atr_sl_mult']
    ap  = bot_state['gann_atr_period']
    
    rows = [
        [{'text': f'📐 محرك جان  — دورة: {cyc}  |  صفقات: {open_n}', 'callback_data': 'noop'}],
        [{'text': '🔄 عرض الدعوم والمقاومات الحالية', 'callback_data': 'gann_show_levels'}],
        [{'text': '── الاستراتيجية والفلتر ──', 'callback_data': 'noop'}],
        [{'text': f'الاستراتيجية: {em_lbl}', 'callback_data': 'gann_toggle_entry'}],
        [{'text': f'الفلتر: {zf_lbl}', 'callback_data': 'gann_toggle_filter'}],
        [{'text': filt_btn_lbl, 'callback_data': 'gann_toggle_filter_type'}],
    ]
    
    if flt_type == 'vwap':
        vwap_val = bot_state['trend_vwap_period']
        rows.append([{'text': 'VWAP −10', 'callback_data': 'gann_dec_vwap'}, 
                     {'text': f'قيمة H1 VWAP: {vwap_val}', 'callback_data': 'noop'}, 
                     {'text': 'VWAP +10', 'callback_data': 'gann_inc_vwap'}])
    else:
        rows.append([{'text': '⚙️ تخصيص قيمة EMA لكل فريم', 'callback_data': 'gann_ema_tf'}])
        
    rows += [
        [{'text': '📝 مساعدة: تغيير القيم الخاصة بالأوامر', 'callback_data': 'gann_filter_help'}],
        [{'text': '── فريمات التنفيذ ──', 'callback_data': 'noop'}],
    ]
    
    tf_items = list(bot_state['gann_monitor_tfs'].items())
    for i in range(0, len(tf_items), 4):
        rows.append([{'text': ('✅' if on else '⬜') + f' {tfk}', 'callback_data': f'gann_tf_{tfk}'} for tfk, on in tf_items[i:i+4]])
        
    rows += [
        [{'text': '── إعدادات عامة ──', 'callback_data': 'noop'}],
        [{'text': '−ساعة', 'callback_data': 'gann_dec_hours'}, {'text': f'مدة تجميد السلّم: {hrs} ساعة', 'callback_data': 'noop'}, {'text': '+ساعة', 'callback_data': 'gann_inc_hours'}],
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

def get_ema_tf_keyboard(sel_tf: str = '') -> dict:
    rows = [[{'text': '⚙️ قيمة EMA لكل فريم', 'callback_data': 'noop'}],
            [{'text': 'أو أرسل: /set 1m ema 50', 'callback_data': 'noop'}]]
    tfs_list = list(bot_state['gann_monitor_tfs'].keys())
    tf_row = []
    for tfk in tfs_list:
        icon = '👉' if tfk == sel_tf else ''
        tf_row.append({'text': f'{icon}{tfk}', 'callback_data': f'gann_ematf_sel_{tfk}'})
        if len(tf_row) == 4: rows.append(tf_row); tf_row = []
    if tf_row: rows.append(tf_row)
    if sel_tf:
        ema_v = bot_state['trend_ema_per_tf'].get(sel_tf, 20)
        rows += [
            [{'text': f'── [{sel_tf}] ──', 'callback_data': 'noop'}],
            [{'text': 'EMA −10', 'callback_data': f'gann_ematf_d_{sel_tf}'}, 
             {'text': f'EMA: {ema_v}', 'callback_data': 'noop'}, 
             {'text': 'EMA +10', 'callback_data': f'gann_ematf_i_{sel_tf}'}],
        ]
    rows.append([{'text': '← رجوع', 'callback_data': 'menu_gann'}])
    return {'inline_keyboard': rows}

def get_gann_tpsl_tf_keyboard(sel_tf: str = '') -> dict:
    rows = [[{'text': '⚙️ TP/SL مخصص لكل فريم', 'callback_data': 'noop'}],
            [{'text': '(0 = يرجع للقيمة العامة)', 'callback_data': 'noop'}]]
    tfs_list = list(bot_state['gann_monitor_tfs'].keys())
    tf_row = []
    for tfk in tfs_list:
        icon = '👉' if tfk == sel_tf else ''
        tf_row.append({'text': f'{icon}{tfk}', 'callback_data': f'gann_tptf_sel_{tfk}'})
        if len(tf_row) == 4: rows.append(tf_row); tf_row = []
    if tf_row: rows.append(tf_row)
    if sel_tf:
        tp_v = bot_state['gann_tp_per_tf'].get(sel_tf, 0); sl_v = bot_state['gann_sl_per_tf'].get(sel_tf, 0)
        eff_tp = tp_v if tp_v > 0 else bot_state['gann_tp_points']
        eff_sl = sl_v if sl_v > 0 else bot_state['gann_sl_points']
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
# LIVE SCANNER (VWAP Macro / EMA Micro)
# ─────────────────────────────────────────────────────────────
async def gann_monitor_scanner() -> None:
    c_log('Gann live scanner started.')
    while True:
        try:
            if not (bot_state['status'] == 'RUNNING' and bot_state['gann_cycle_active'] and bot_state['gann_levels']):
                await asyncio.sleep(10); continue

            flt_type = bot_state['trend_filter_type']
            macro_trend_up = None

            # إذا كان الفلتر VWAP، نحسبه مرة واحدة على H1
            if bot_state['gann_entry_mode'] == 'touch_trend' and flt_type == 'vwap':
                period = bot_state['trend_vwap_period']
                h1_candles = await fetch_candles('1h', count=max(period+10, 120))
                if h1_candles:
                    df_h1 = pd.DataFrame(h1_candles)
                    df_h1['Typical_Price'] = (df_h1['high'] + df_h1['low'] + df_h1['close']) / 3
                    df_h1['VWAP'] = (df_h1['Typical_Price'] * df_h1['volume']).rolling(window=period).sum() / df_h1['volume'].rolling(window=period).sum()
                    
                    current_vwap = df_h1.iloc[-1]['VWAP']
                    current_h1_close = float(h1_candles[-1]['close'])
                    if pd.isna(current_vwap): current_vwap = current_h1_close
                    macro_trend_up = (current_h1_close > current_vwap)

            enabled_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on]
            levels      = gann_active_levels()
            margin      = bot_state['gann_touch_margin_pts'] * bot_state['pip_value']

            for tf in enabled_tfs:
                if tf in bot_state['gann_open_trades'].values(): continue 

                need = max(bot_state['gann_atr_period'], bot_state['trend_ema_per_tf'][tf]) + 50
                candles = await fetch_candles(tf, count=need)
                if not candles or len(candles) < 3: continue
                close_px = float(candles[-1]['close'])
                live_px  = close_px 

                trend_up = True
                if bot_state['gann_entry_mode'] == 'touch_trend':
                    if flt_type == 'vwap':
                        trend_up = macro_trend_up if macro_trend_up is not None else True
                    else: # EMA
                        ema_p = bot_state['trend_ema_per_tf'][tf]
                        df_tf = pd.DataFrame(candles)
                        df_tf['EMA'] = df_tf['close'].ewm(span=ema_p, adjust=False).mean()
                        current_ema = df_tf.iloc[-1]['EMA']
                        if pd.isna(current_ema): current_ema = close_px
                        trend_up = (close_px > current_ema)

                for lv in levels:
                    k = lv['key']; dir = lv['dir']
                    status = bot_state['gann_level_status'].get(k)
                    if status == 'used': continue

                    mode = 'touch'
                    is_buy = (dir == 'dn')
                    
                    if bot_state['gann_entry_mode'] == 'touch_trend':
                        if is_buy and not trend_up: continue
                        if not is_buy and trend_up: continue

                    if abs(live_px - lv['price']) <= margin:
                        flt_label = f"VWAP(H1)={bot_state['trend_vwap_period']}" if flt_type == 'vwap' else f"EMA={bot_state['trend_ema_per_tf'][tf]}"
                        reason = f"لمس دعم 🟢 (مع {flt_label})" if is_buy else f"لمس مقاومة 🔴 (مع {flt_label})"
                        await _gann_open_trade(is_buy, lv, candles, reason=reason, tf=tf)
                        break
                        
        except Exception as e: c_log(f'Gann monitor scanner error: {e}')
        await asyncio.sleep(15)

# ─────────────────────────────────────────────────────────────
# PRO BACKTEST ENGINE 
# ─────────────────────────────────────────────────────────────
async def run_gann_backtest(start_dt: datetime, end_dt: datetime) -> None:
    global _bt_progress
    bot_state['is_backtesting'] = True
    
    fname = f"GannBT_{datetime.now(timezone.utc).strftime('%H%M%S')}.xlsx"
    enabled_tfs = [tf for tf, on in bot_state['gann_monitor_tfs'].items() if on] or ['5m']
    
    flt_type = bot_state['trend_filter_type']
    if bot_state['gann_entry_mode'] == 'touch_trend':
        desc_mode = f"Touch(VWAP{bot_state['trend_vwap_period']}_Macro)" if flt_type == 'vwap' else "Touch(EMA_Micro)"
    else:
        desc_mode = "Pure Touch"
        
    desc_star = "⭐" if bot_state['gann_zone_filter'] == 'star' else "الكل"
    desc_tfs = "+".join(enabled_tfs)
    
    prog = BtProgress(label=f"جان H1→[{desc_tfs}] | {desc_mode} | {desc_star}", active_tfs=['H1']); _bt_progress = prog
    await prog.start(bot_state['chat_id'])

    res = {'win': 0, 'loss': 0, 'total_prof': 0.0, 'total_win_usd': 0.0, 'total_loss_usd': 0.0, 'peak_equity': 0.0, 'max_dd': 0.0, 'trade_logs': []}
    pv  = bot_state['pip_value']; lot = bot_state['lot_size']; margin = bot_state['gann_touch_margin_pts'] * pv
    cs  = bot_state['contract_size']; cycle_h = bot_state['gann_cycle_hours']; tpsl_mode = bot_state['gann_tpsl_mode']

    try:
        await prog.set_phase('جلب بيانات H1...')
        # احتساب عدد الساعات المطلوبة بين التاريخين زائد فترة المؤشر ليكون دقيقاً
        total_hours = int((end_dt - start_dt).total_seconds() / 3600) + max(bot_state['trend_vwap_period'], 100) + 10
        candles_h1 = await fetch_candles('1h', count=total_hours, end_time=end_dt)
        if not candles_h1: await prog.done('❌ لا توجد بيانات H1 ضمن هذا النطاق.'); return
        
        df_h1 = pd.DataFrame(candles_h1)
        if flt_type == 'vwap':
            period = bot_state['trend_vwap_period']
            df_h1['Typical_Price'] = (df_h1['high'] + df_h1['low'] + df_h1['close']) / 3
            df_h1['VWAP'] = (df_h1['Typical_Price'] * df_h1['volume']).rolling(window=period).sum() / df_h1['volume'].rolling(window=period).sum()
        df_h1.set_index('time', inplace=True)

        await prog.set_phase('جلب شموع الفريمات الصغيرة...')
        monitor_tfs_data = {}
        tf_indicators = {}
        
        days_diff = (end_dt - start_dt).days or 1
        for btf in enabled_tfs:
            bmin = int(''.join(filter(str.isdigit, btf)))
            if 'h' in btf: bmin *= 60
            need_m = days_diff * 24 * (60 // max(bmin, 1)) + 300
            mc = await fetch_candles(btf, count=need_m, end_time=end_dt)
            if mc: 
                monitor_tfs_data[btf] = sorted(mc, key=lambda c: c['time'])
                if flt_type == 'ema':
                    df_m = pd.DataFrame(monitor_tfs_data[btf])
                    p_ema = bot_state['trend_ema_per_tf'][btf]
                    df_m['EMA'] = df_m['close'].ewm(span=p_ema, adjust=False).mean()
                    df_m.set_index('time', inplace=True)
                    tf_indicators[btf] = df_m

        start_ts = start_dt.timestamp(); end_ts = end_dt.timestamp()
        valid_h1 = [c for c in candles_h1 if start_ts <= (c['time'].timestamp() + 3600) <= end_ts]
        await prog.set_tf('H1 Cycles', len(valid_h1))
        
        cycle_logs = []

        for idx, h1 in enumerate(valid_h1):
            if prog.cancelled: break
            await asyncio.sleep(0)

            t_start = h1['time'] + timedelta(hours=1)
            t_end   = t_start + timedelta(hours=cycle_h)
            close   = float(h1['close'])
            
            macro_trend_up = None
            if flt_type == 'vwap':
                ind_val = df_h1.loc[h1['time']]['VWAP']
                if pd.isna(ind_val): ind_val = close
                macro_trend_up = (close > ind_val)

            levels = gann_calc_levels(close)
            active_lv = [l for l in levels if l['dir'] != 'ref' and (bot_state['gann_zone_filter'] != 'star' or l['star'])]
            
            cycle_trades = 0; level_used = set()

            for btf, candles_m in monitor_tfs_data.items():
                m_window = [c for c in candles_m if t_start <= c['time'] < t_end]
                m_before = [c for c in candles_m if c['time'] < t_start]
                atr_val  = _gann_atr(m_before, bot_state['gann_atr_period']) if tpsl_mode == 'atr' else None

                for bar in m_window:
                    bar_close = float(bar['close']); bar_time = bar['time']
                    remaining_bars = [b for b in candles_m if b['time'] > bar_time]

                    trend_up = True
                    if bot_state['gann_entry_mode'] == 'touch_trend':
                        if flt_type == 'vwap':
                            trend_up = macro_trend_up if macro_trend_up is not None else True
                        else: # EMA
                            ind_val = tf_indicators[btf].loc[bar_time]['EMA']
                            if pd.isna(ind_val): ind_val = bar_close
                            trend_up = (bar_close > ind_val)

                    for lv in active_lv:
                        k = lv['key']; dir = lv['dir']; combo_key = f'{k}_{btf}'
                        if combo_key in level_used: continue

                        mode = 'touch'
                        is_buy = (dir == 'dn')
                        
                        if bot_state['gann_entry_mode'] == 'touch_trend':
                            if is_buy and not trend_up: continue
                            if not is_buy and trend_up: continue

                        if abs(bar_close - lv['price']) > margin: continue

                        entry = lv['price']
                        tf_tp = _gann_tf_tp(btf); tf_sl = _gann_tf_sl(btf)
                        if tpsl_mode == 'atr' and atr_val:
                            sl_d = atr_val * bot_state['gann_atr_sl_mult']
                            tp_d = atr_val * bot_state['gann_atr_tp_mult']
                        else:
                            sl_d = tf_sl * pv; tp_d = tf_tp * pv
                            
                        tp_px = entry + tp_d if is_buy else entry - tp_d
                        sl_px = entry - sl_d if is_buy else entry + sl_d

                        outcome = 'OPEN'; p_usd = 0.0
                        for fb in remaining_bars:
                            fh = float(fb['high']); fl = float(fb['low'])
                            if is_buy:
                                if fh >= tp_px: outcome = 'WIN'; p_usd = round(tp_d * lot * cs, 2); break
                                if fl <= sl_px: outcome = 'LOSS'; p_usd = -round(sl_d * lot * cs, 2); break
                            else:
                                if fl <= tp_px: outcome = 'WIN'; p_usd = round(tp_d * lot * cs, 2); break
                                if fh >= sl_px: outcome = 'LOSS'; p_usd = -round(sl_d * lot * cs, 2); break

                        if outcome == 'OPEN': continue

                        level_used.add(combo_key); cycle_trades += 1
                        if outcome == 'WIN': 
                            res['win'] += 1; res['total_win_usd'] += p_usd
                        else: 
                            res['loss'] += 1; res['total_loss_usd'] += abs(p_usd)
                            
                        res['total_prof'] += p_usd
                        res['peak_equity'] = max(res['peak_equity'], res['total_prof'])
                        res['max_dd'] = max(res['max_dd'], res['peak_equity'] - res['total_prof'])

                        res['trade_logs'].append({
                            'cycle_ts': t_start.timestamp(),
                            'دورة H1 (DAM)': _utc_to_dam(t_start).strftime('%Y-%m-%d %H:00'),
                            'إغلاق H1': close,
                            'وقت الصفقة (DAM)': _utc_to_dam(bar_time).strftime('%Y-%m-%d %H:%M'),
                            'TF': btf,
                            'اتجاه': 'BUY 📈' if is_buy else 'SELL 📉',
                            'المستوى (الدخول)': entry,
                            'الهدف (TP)': round(tp_px, 2),
                            'الوقف (SL)': round(sl_px, 2),
                            'النتيجة': outcome,
                            'ربح ($)': p_usd,
                            'رصيد تراكمي ($)': round(res['total_prof'], 2),
                        })
                        break 
            
            cycle_logs.append({
                'الدورة (DAM)': _utc_to_dam(t_start).strftime('%Y-%m-%d %H:00'),
                'إغلاق H1': close,
                'عدد الصفقات': cycle_trades,
                'ملاحظة': f'تم تنفيذ {cycle_trades} صفقة' if cycle_trades > 0 else 'لم يلمس السعر أي مستوى'
            })
            await prog.tick(idx + 1, res['win'], res['loss'], res['total_prof'])

        await prog.set_phase('إنشاء ملف Excel المنسق...')
        wb = openpyxl.Workbook()
        ws_trades = wb.active; ws_trades.title = 'الصفقات'; ws_trades.sheet_view.rightToLeft = True

        fill_win = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
        fill_loss = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
        fill_header = PatternFill(start_color='D3D3D3', end_color='D3D3D3', fill_type='solid')
        font_header = Font(bold=True, size=12); font_cycle = Font(bold=True, size=14)
        align_center = Alignment(horizontal='center', vertical='center')
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

        headers = ['وقت الصفقة (DAM)', 'TF', 'اتجاه', 'المستوى (الدخول)', 'الهدف (TP)', 'الوقف (SL)', 'النتيجة', 'ربح ($)', 'رصيد تراكمي ($)']
        ws_trades.append(headers)
        for col in range(1, len(headers) + 1):
            c = ws_trades.cell(row=1, column=col); c.font = font_header; c.alignment = align_center; c.fill = fill_header; c.border = thin_border

        if res['trade_logs']:
            df_trades = pd.DataFrame(res['trade_logs'])
            df_trades['TF_Sort'] = df_trades['TF'].apply(lambda x: int(''.join(filter(str.isdigit, x))) * (60 if 'h' in x else 1))
            df_trades = df_trades.sort_values(by=['cycle_ts', 'TF_Sort'])
            
            current_cycle = None
            for _, row in df_trades.iterrows():
                if row['دورة H1 (DAM)'] != current_cycle:
                    current_cycle = row['دورة H1 (DAM)']
                    cycle_text = f"دورة H1: {current_cycle}  |  إغلاق H1: {row['إغلاق H1']}"
                    ws_trades.append([cycle_text] + [''] * (len(headers) - 1))
                    mr = ws_trades.max_row
                    ws_trades.merge_cells(start_row=mr, start_column=1, end_row=mr, end_column=len(headers))
                    c = ws_trades.cell(row=mr, column=1); c.font = font_cycle; c.alignment = align_center; c.fill = PatternFill(start_color='E2E3E5', fill_type='solid')
                    for col in range(1, len(headers) + 1): ws_trades.cell(row=mr, column=col).border = thin_border

                trade_row = [row['وقت الصفقة (DAM)'], row['TF'], row['اتجاه'], row['المستوى (الدخول)'], row['الهدف (TP)'], row['الوقف (SL)'], row['النتيجة'], row['ربح ($)'], row['رصيد تراكمي ($)']]
                ws_trades.append(trade_row)
                cr = ws_trades.max_row
                f_color = fill_win if row['النتيجة'] == 'WIN' else fill_loss
                for col in range(1, len(headers) + 1):
                    c = ws_trades.cell(row=cr, column=col); c.alignment = Alignment(horizontal='center'); c.fill = f_color

        for col_cells in ws_trades.columns: ws_trades.column_dimensions[col_cells[0].column_letter].width = 18

        ws_cycles = wb.create_sheet('دورات H1'); ws_cycles.sheet_view.rightToLeft = True
        df_cycles = pd.DataFrame(cycle_logs)
        from openpyxl.utils.dataframe import dataframe_to_rows
        for r_idx, row in enumerate(dataframe_to_rows(df_cycles, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                c = ws_cycles.cell(row=r_idx, column=c_idx, value=value)
                if r_idx == 1: c.font = font_header; c.fill = fill_header
                c.alignment = align_center
        for col_cells in ws_cycles.columns: ws_cycles.column_dimensions[col_cells[0].column_letter].width = 22

        wb.save(fname)

        total = res['win'] + res['loss']
        wr = round(res['win'] / max(1, total) * 100, 1) if total else 0
        dd_pct = round(res['max_dd'] / max(1, res['peak_equity']) * 100, 1) if res['peak_equity'] else 0
        tpsl_lbl = "حسب ATR" if tpsl_mode == "atr" else "نقاط ثابتة"
        net_icon = "PROFIT ▲" if res["total_prof"] >= 0 else "LOSS ▼"
        
        tg_lines = [
            f'<b>باكتيست جان اكتمل ✅</b>',
            f'جان H1→[{desc_tfs}] | {desc_mode} | {desc_star}',
            f'{_utc_to_dam(start_dt).strftime("%Y-%m-%d")} → {_utc_to_dam(end_dt).strftime("%Y-%m-%d")}',
            '',
            f'Net: {net_icon} ${round(res["total_prof"], 1)}',
            f'Win:  +${round(res["total_win_usd"], 1)} ({res["win"]})',
            f'Loss: -${abs(round(res["total_loss_usd"], 1))} ({res["loss"]})',
            f'WR: {wr}% ({total} صفقة)',
            f'Max DD: ${round(res["max_dd"], 1)} ({dd_pct}%)',
            f'دورات H1: {len(valid_h1)}  |  TP/SL: {tpsl_lbl} | Lot: {lot}  |  cs={cs}',
            '',
            'إرسال ملف Excel...'
        ]
        await prog.done('\n'.join(tg_lines))
        await send_tg_document(fname, "نتائج الباكتيست")
        try: os.remove(fname)
        except Exception: pass
        bot_state['is_backtesting'] = False

    except Exception as e:
        c_log(f'BT Error: {e}'); bot_state['is_backtesting'] = False

# ─────────────────────────────────────────────────────────────
# TELEGRAM HANDLERS (With Advanced /set & /backtest)
# ─────────────────────────────────────────────────────────────
async def _handle_callback(d: str, chat_id: int, msg_id: int) -> None:
    if d == 'menu_main': await _show(chat_id, msg_id, 'القائمة الرئيسية:', get_main_keyboard())
    elif d == 'menu_gann': await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_show_levels':
        if not bot_state['gann_levels'] or not bot_state['gann_close_used']:
            await send_tg_msg('⏳ لا يوجد سلّم نشط، جاري جلب آخر شمعة H1...')
            last_h1 = await _gann_fetch_last_closed_h1()
            if last_h1:
                h1_close = float(last_h1['close'])
                bot_state['gann_levels']          = gann_calc_levels(h1_close)
                bot_state['gann_close_used']       = h1_close
                bot_state['gann_last_h1_time']     = last_h1['time']
                bot_state['gann_cycle_started_at'] = datetime.now(timezone.utc)
                bot_state['gann_cycle_active']     = True
            else:
                await send_tg_msg('❌ تعذّر جلب البيانات.'); return
        await send_tg_msg(_gann_fmt_levels_msg(bot_state['gann_close_used']))
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_entry':
        bot_state['gann_entry_mode'] = 'pure_touch' if bot_state['gann_entry_mode'] == 'touch_trend' else 'touch_trend'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_filter':
        bot_state['gann_zone_filter'] = 'all' if bot_state['gann_zone_filter'] == 'star' else 'star'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_filter_type':
        bot_state['trend_filter_type'] = 'vwap' if bot_state['trend_filter_type'] == 'ema' else 'ema'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_vwap': 
        bot_state['trend_vwap_period'] = max(10, bot_state['trend_vwap_period'] - 10)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_vwap': 
        bot_state['trend_vwap_period'] = min(500, bot_state['trend_vwap_period'] + 10)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_ema_tf': await _show(chat_id, msg_id, '⚙️ قيمة EMA لكل فريم:', get_ema_tf_keyboard())
    elif d.startswith('gann_ematf_sel_'):
        sel_tf = d[len('gann_ematf_sel_'):]; await _show(chat_id, msg_id, f'⚙️ قيمة EMA [{sel_tf}]:', get_ema_tf_keyboard(sel_tf))
    elif d.startswith('gann_ematf_i_'):
        tf = d[len('gann_ematf_i_'):]; bot_state['trend_ema_per_tf'][tf] = bot_state['trend_ema_per_tf'].get(tf, 20) + 10; await _show(chat_id, msg_id, f'⚙️ قيمة EMA [{tf}]:', get_ema_tf_keyboard(tf))
    elif d.startswith('gann_ematf_d_'):
        tf = d[len('gann_ematf_d_'):]; bot_state['trend_ema_per_tf'][tf] = max(10, bot_state['trend_ema_per_tf'].get(tf, 20) - 10); await _show(chat_id, msg_id, f'⚙️ قيمة EMA [{tf}]:', get_ema_tf_keyboard(tf))
    elif d == 'gann_filter_help':
        help_txt = ("<b>⚙️ دليل تخصيص القيم لكل فريم:</b>\n\n"
                    "أرسل أمراً مباشراً في الدردشة لتغيير أي قيمة لأي فريم تريده، بالصيغة التالية:\n\n"
                    "<code>/set [الفريم] [المتغير] [القيمة]</code>\n\n"
                    "<b>أمثلة على الفلاتر:</b>\n"
                    "<code>/set 1m ema 50</code>\n\n"
                    "<b>أمثلة على الأهداف والوقف:</b>\n"
                    "<code>/set 5m tp 40</code>\n"
                    "<code>/set 15m sl 25</code>\n\n"
                    "سيتم حفظ القيمة وتطبيقها على الفريم المحدد فوراً.")
        await _show(chat_id, msg_id, help_txt, get_gann_keyboard())
    elif d == 'gann_dec_margin': 
        bot_state['gann_touch_margin_pts'] = max(1, bot_state['gann_touch_margin_pts'] - 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_margin': 
        bot_state['gann_touch_margin_pts'] = min(50, bot_state['gann_touch_margin_pts'] + 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_hours': 
        bot_state['gann_cycle_hours'] = max(1, bot_state['gann_cycle_hours'] - 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_hours': 
        bot_state['gann_cycle_hours'] = min(24, bot_state['gann_cycle_hours'] + 1)
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_toggle_tpsl':
        bot_state['gann_tpsl_mode'] = 'atr' if bot_state['gann_tpsl_mode'] == 'fixed' else 'fixed'
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_tp10': bot_state['gann_tp_points'] = max(10, bot_state['gann_tp_points'] - 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_tp10': bot_state['gann_tp_points'] = min(1000, bot_state['gann_tp_points'] + 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_sl10': bot_state['gann_sl_points'] = max(10, bot_state['gann_sl_points'] - 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_sl10': bot_state['gann_sl_points'] = min(1000, bot_state['gann_sl_points'] + 10); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_atrp':  bot_state['gann_atr_period'] = max(5,   bot_state['gann_atr_period'] - 1); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_atrp':  bot_state['gann_atr_period'] = min(50,  bot_state['gann_atr_period'] + 1); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_atrsl': bot_state['gann_atr_sl_mult'] = max(0.5, round(bot_state['gann_atr_sl_mult'] - 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_atrsl': bot_state['gann_atr_sl_mult'] = min(5.0, round(bot_state['gann_atr_sl_mult'] + 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_dec_atrtp': bot_state['gann_atr_tp_mult'] = max(0.5, round(bot_state['gann_atr_tp_mult'] - 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_inc_atrtp': bot_state['gann_atr_tp_mult'] = min(8.0, round(bot_state['gann_atr_tp_mult'] + 0.5, 1)); await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d.startswith('gann_tf_'):
        tfk = d[len('gann_tf_'):]
        if tfk in bot_state['gann_monitor_tfs']: bot_state['gann_monitor_tfs'][tfk] = not bot_state['gann_monitor_tfs'][tfk]
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    elif d == 'gann_tpsl_tf': await _show(chat_id, msg_id, '⚙️ TP/SL مخصص لكل فريم:', get_gann_tpsl_tf_keyboard())
    elif d.startswith('gann_tptf_sel_'):
        sel_tf = d[len('gann_tptf_sel_'):]; await _show(chat_id, msg_id, f'⚙️ TP/SL [{sel_tf}]:', get_gann_tpsl_tf_keyboard(sel_tf))
    elif d.startswith('gann_tptf_itp_'):
        tf = d[len('gann_tptf_itp_'):]; bot_state['gann_tp_per_tf'][tf] = bot_state['gann_tp_per_tf'].get(tf, 0) + 10; await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_dtp_'):
        tf = d[len('gann_tptf_dtp_'):]; bot_state['gann_tp_per_tf'][tf] = max(0, bot_state['gann_tp_per_tf'].get(tf, 0) - 10); await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_isl_'):
        tf = d[len('gann_tptf_isl_'):]; bot_state['gann_sl_per_tf'][tf] = bot_state['gann_sl_per_tf'].get(tf, 0) + 10; await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_dsl_'):
        tf = d[len('gann_tptf_dsl_'):]; bot_state['gann_sl_per_tf'][tf] = max(0, bot_state['gann_sl_per_tf'].get(tf, 0) - 10); await _show(chat_id, msg_id, f'⚙️ TP/SL [{tf}]:', get_gann_tpsl_tf_keyboard(tf))
    elif d.startswith('gann_tptf_rst_'):
        tf = d[len('gann_tptf_rst_'):]; bot_state['gann_tp_per_tf'][tf] = 0; bot_state['gann_sl_per_tf'][tf] = 0; await _show(chat_id, msg_id, f'⚙️ تمت إعادة الضبط:', get_gann_tpsl_tf_keyboard(tf))
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
        await _show(chat_id, msg_id, 'إعدادات جان:', get_gann_keyboard())
    else: c_log(f'Unhandled callback: {d}')

# ─────────────────────────────────────────────────────────────
# TELEGRAM POLLING & WATCHDOG
# ─────────────────────────────────────────────────────────────
async def process_tg_update(update: dict) -> None:
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip(); bot_state['chat_id'] = update['message']['chat']['id']
        
        parts = msg.lower().split()
        # ── معالجة أوامر التخصيص (/set) الشاملة ──
        if parts[0] == '/set' and len(parts) == 4:
            _, tf, param, val = parts
            if tf in _TFS and param in ['ema', 'vwap', 'tp', 'sl'] and val.isdigit():
                val = int(val)
                if param == 'ema': bot_state['trend_ema_per_tf'][tf] = val
                elif param == 'vwap': bot_state['trend_vwap_per_tf'][tf] = val # للتوافق
                elif param == 'tp': bot_state['gann_tp_per_tf'][tf] = val
                elif param == 'sl': bot_state['gann_sl_per_tf'][tf] = val
                
                await send_tg_msg(f"✅ <b>تم التحديث بنجاح!</b>\n📌 الفريم: {tf}\n⚙️ {param.upper()}: {val}")
                return
            await send_tg_msg("❌ <b>صيغة خاطئة أو فريم غير مدعوم!</b>\n<b>أمثلة صحيحة:</b>\n<code>/set 1m ema 50</code>\n<code>/set 5m tp 40</code>\n<code>/set 15m sl 25</code>")
            return

        # ── معالجة أوامر الباكتيست (/backtest) ──
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

        if msg == '/start': await send_tg_msg('<b>مرحباً بك في Gold Scalper Bot v8.2</b>', get_main_keyboard())
        return

    if 'callback_query' not in update: return
    q = update['callback_query']; d = q['data']; chat_id = q['message']['chat']['id']; msg_id = q['message']['message_id']
    bot_state['chat_id'] = chat_id
    try: await _handle_callback(d, chat_id, msg_id)
    except Exception as e: c_log(f'CB error [{d}]: {e}')
    finally: await answer_callback(q['id'])

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
    ]
    
    c_log('Gold Scalper Bot v8.2 started successfully.')
    try: await asyncio.gather(*tasks)
    finally:
        if _http and not _http.closed: await _http.close()

if __name__ == '__main__':
    asyncio.run(main())
