# Gold Scalper Bot — v5.4 (Gann Levels Engine - Advanced Filters & Analytics)
import asyncio
import aiohttp
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from aiohttp import web
from openpyxl.styles import PatternFill

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

# الفريمات الجديدة التي طلبتها
_TFS = ['1m', '2m', '3m', '4m', '5m', '6m', '10m', '12m', '15m', '20m', '30m', '1h']
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
    
    'timeframes':       _TFS,
    'active_tfs':       {tf: (True if tf in ['1m', '3m', '5m'] else False) for tf in _TFS},
    
    # ── Gann Strategy ──
    'gann_zone_filter':      'star',
    'gann_entry_mode':       'touch', 
    'gann_touch_margin_pts': 5.0,     
    'gann_anti_spam':        True,    # منع التكرار الذي طلبته
    
    # ── Protection & Filters ──
    'use_trend_filter':  False,       # حماية الاتجاه
    'trend_filter_type': 'EMA_200',   # أو 'EMA_CROSS'
    'use_be':            False,       # حماية الأرباح
    'be_pips':           20,          # عدد نقاط حماية الأرباح

    # Risk
    'lot_size':         0.05,
    'pip_value':        0.1,
    'spread_pips':      2.2,
    'use_max_spread':   True,
    'max_spread_pips':  3.0,
    'tp_pips': {tf: 180 for tf in _TFS},
    'sl_pips': {tf: 100 for tf in _TFS},

    'menu_button_map': {},
    'last_poll_ok': 0.0,
    'use_danger_filter': True,
}
bot_state['sl_pips']['1m'] = 50

# ─────────────────────────────────────────────────────────────
# TIME UTILS
# ─────────────────────────────────────────────────────────────
_BLOCKED_DAMASCUS_HOURS = {13, 18, 21, 22}

def is_blocked_time(dt_utc: datetime) -> bool:
    return (dt_utc.hour + 3) % 24 in _BLOCKED_DAMASCUS_HOURS

def _to_utc(x) -> pd.Timestamp:
    if isinstance(x, pd.Timestamp): return x.tz_convert('UTC') if x.tzinfo else x.tz_localize('UTC')
    if isinstance(x, datetime):
        if x.tzinfo: return pd.Timestamp(x.astimezone(timezone.utc)).tz_localize('UTC')
        return pd.Timestamp(x).tz_localize('UTC')
    if isinstance(x, (int, float)): return pd.Timestamp(int(x), unit='s').tz_localize('UTC')
    ts = pd.Timestamp(str(x))
    return ts.tz_convert('UTC') if ts.tzinfo else ts.tz_localize('UTC')

_to_ts = _to_utc
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

# ─────────────────────────────────────────────────────────────
# OANDA WEBSOCKET FETCHER
# ─────────────────────────────────────────────────────────────
_OANDA_GRAN = {
    '1m': 'M1',  '2m': 'M2',  '3m': 'M3',  '4m': 'M4', '5m': 'M5',  '6m': 'M6',  '10m': 'M10',
    '12m': 'M12','15m': 'M15','20m': 'M20','30m': 'M30', '1h': 'H1'
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

# ─────────────────────────────────────────────────────────────
# GANN MATH
# ─────────────────────────────────────────────────────────────
def calculate_gann_levels_from_close(close_price: float, is_buy: bool) -> list:
    P = close_price
    try:
        sqrt_P = np.sqrt(P)
        if is_buy: levels = [(sqrt_P + (i * 0.125)) ** 2 for i in range(1, 17)]
        else:      levels = [(sqrt_P - (i * 0.125)) ** 2 for i in range(1, 17)]
        return [round(lvl, 2) for lvl in levels]
    except Exception: return []

def determine_strong_gann_levels(levels: list, close_price: float, is_buy: bool) -> list:
    P = close_price; strong = []
    try:
        sqrt_P = np.sqrt(P)
        degrees = [45, 90, 180, 270, 360]
        for deg in degrees:
            i = deg / 45.0
            if is_buy: strong.append(round((sqrt_P + (i * 0.125)) ** 2, 2))
            else:      strong.append(round((sqrt_P - (i * 0.125)) ** 2, 2))
        return sorted(list(set(strong)))
    except Exception: return []

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
# KEYBOARDS
# ─────────────────────────────────────────────────────────────
def get_main_keyboard() -> dict:
    st = '▶ يعمل' if bot_state['status'] == 'RUNNING' else '⏸ متوقف'
    bt = '⏳ فحص...' if bot_state['is_backtesting'] else '📊 باكتيست'
    return {'inline_keyboard': [
        [{'text': f'البوت: {st}', 'callback_data': 'toggle_status'}],
        [{'text': '⚙️ الاستراتيجية (فلاتر و Anti-Spam)', 'callback_data': 'menu_strategy'}],
        [{'text': '💰 المخاطر (تعديل الأهداف و BE)', 'callback_data': 'menu_risk'}],
        [{'text': '⏱ الفريمات النشطة', 'callback_data': 'menu_tfs'}],
        [{'text': bt, 'callback_data': 'menu_backtest'}, {'text': '❌ إخفاء', 'callback_data': 'hide_keyboard'}],
    ]}

def get_strategy_keyboard() -> dict:
    spam_i = '✅' if bot_state['gann_anti_spam'] else '⬜'
    trnd_i = '✅' if bot_state['use_trend_filter'] else '⬜'
    t_type = '200 EMA' if bot_state['trend_filter_type'] == 'EMA_200' else '50/150 Cross'
    
    return {'inline_keyboard': [
        [{'text': '── 📐 مستويات جان ──', 'callback_data': 'noop'}],
        [{'text': f'Anti-Spam (منع التكرار): {spam_i}', 'callback_data': 'toggle_spam'}],
        [{'text': '🛡️ ── حماية الاتجاه ── 🛡️', 'callback_data': 'noop'}],
        [{'text': f'الفلتر مفعل: {trnd_i}', 'callback_data': 'toggle_trend'}],
        [{'text': f'نوع الفلتر: {t_type} ↻', 'callback_data': 'cycle_trend_type'}],
        [{'text': '← رجوع للقائمة', 'callback_data': 'menu_main'}],
    ]}

def get_risk_keyboard() -> dict:
    be_i  = '✅' if bot_state['use_be'] else '⬜'
    be_p  = bot_state['be_pips']
    return {'inline_keyboard': [
        [{'text': f'حماية الأرباح (Break-Even): {be_i}', 'callback_data': 'toggle_be'}],
        [{'text': f'نقاط الحماية: {be_p}p', 'callback_data': 'noop'}],
        [{'text': '/set be 30 (أرسلها كرسالة لتغيير النقاط)', 'callback_data': 'noop'}],
        [{'text': '📋 تعديل TP/SL للفريمات', 'callback_data': 'view_tpsl'}],
        [{'text': '← رجوع', 'callback_data': 'menu_main'}],
    ]}

def get_tf_keyboard() -> dict:
    rows = [[{'text': 'حدد الفريمات المطلوبة:', 'callback_data': 'noop'}]]
    row_tfs = []
    for i, tf in enumerate(bot_state['timeframes']):
        icon = '✅' if bot_state['active_tfs'][tf] else '⬜'
        row_tfs.append({'text': f'{icon} {tf}', 'callback_data': f'toggle_tf_{tf}'})
        if len(row_tfs) == 3 or i == len(bot_state['timeframes']) - 1:
            rows.append(row_tfs)
            row_tfs = []
    rows.append([{'text': '← رجوع', 'callback_data': 'menu_main'}])
    return {'inline_keyboard': rows}

def get_tpsl_overview_keyboard() -> dict:
    rows = [[{'text': 'السهولة: أرسل رسالة مثلاً: /set 1m tp 150', 'callback_data': 'noop'}]]
    for tf in bot_state['timeframes']:
        if bot_state['active_tfs'][tf]:
            tp = bot_state['tp_pips'][tf]; sl = bot_state['sl_pips'][tf]
            rows.append([{'text': f'{tf} | TP: {tp} | SL: {sl}', 'callback_data': f'noop'}])
    rows.append([{'text': '← رجوع', 'callback_data': 'menu_risk'}])
    return {'inline_keyboard': rows}

def get_backtest_keyboard() -> dict:
    if bot_state['is_backtesting']: return {'inline_keyboard': [[{'text': 'الباكتيست يعمل...', 'callback_data': 'bt_show_progress'}], [{'text': '⏹ إيقاف', 'callback_data': 'cancel_bt'}]]}
    return {'inline_keyboard': [
        [{'text': 'أوامر الباكتيست (أرسل كرسالة):', 'callback_data': 'noop'}],
        [{'text': '/backtest 2026-06-24 (ليوم واحد)', 'callback_data': 'noop'}],
        [{'text': '/backtest 2026-06-20 2026-06-24 (عدة أيام)', 'callback_data': 'noop'}],
        [{'text': '← رجوع', 'callback_data': 'menu_main'}],
    ]}

# ─────────────────────────────────────────────────────────────
# COLORED BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────
async def run_gann_backtest_dates(start_dt: datetime, end_dt: datetime) -> None:
    global _bt_progress
    bot_state['is_backtesting'] = True
    
    enabled_tfs = [tf for tf, on in bot_state['active_tfs'].items() if on] or ['1m']
    desc = f"جان H1 → [{'+'.join(enabled_tfs)}] | AntiSpam:{'On' if bot_state['gann_anti_spam'] else 'Off'}"
    prog = BtProgress(label=desc, active_tfs=['H1']); _bt_progress = prog
    await prog.start(bot_state['chat_id'])

    warmup_hours = 210 if bot_state['use_trend_filter'] else 5
    total_min = int((end_dt - start_dt).total_seconds() / 60) + (warmup_hours * 60)
    
    await prog.set_phase('جلب البيانات...')
    try:
        candles_1m = await fetch_oanda_candles('1m', count=min(total_min, 120000), end_time=end_dt)
    except Exception as e:
        await prog.done(f'❌ خطأ في جلب البيانات: {e}'); bot_state['is_backtesting'] = False; return
        
    if not candles_1m:
        await prog.done('❌ لم يتم العثور على بيانات'); bot_state['is_backtesting'] = False; return
    
    await prog.set_phase('حساب المؤشرات وفلاتر الاتجاه...')
    df_1m = pd.DataFrame(candles_1m).set_index('time')
    
    df_h1 = df_1m.resample('1h', closed='right', label='right').agg({'close': 'last'}).dropna()
    df_h1['EMA_200'] = df_h1['close'].ewm(span=200, adjust=False).mean()
    df_h1['EMA_50']  = df_h1['close'].ewm(span=50, adjust=False).mean()
    df_h1['EMA_150'] = df_h1['close'].ewm(span=150, adjust=False).mean()
    
    df_1m['EMA_200'] = df_h1['EMA_200'].reindex(df_1m.index, method='ffill')
    df_1m['EMA_50']  = df_h1['EMA_50'].reindex(df_1m.index, method='ffill')
    df_1m['EMA_150'] = df_h1['EMA_150'].reindex(df_1m.index, method='ffill')

    trade_logs = []
    cycles_log = []
    cycle_colors = ['#FFB3BA', '#FFDFBA', '#FFFFBA', '#BAFFC9', '#BAE1FF', '#E8BAFF', '#FFBAE1', '#E0E0E0', '#D5AAFF', '#B5EAD7']
    color_index = 0
    cycle_color_map = {}

    start_ts = _to_ts(start_dt)
    cycle_starts = df_h1[(df_h1.index >= start_ts) & (df_h1.index < _to_ts(end_dt))].index
    await prog.set_tf('H1 Cycles', len(cycle_starts))
    
    pv = bot_state['pip_value']; lot = bot_state['lot_size']; margin_pts = bot_state['gann_touch_margin_pts']
    res = {'win': 0, 'loss': 0, 'be': 0, 'total_prof': 0.0, 'peak_equity': 0.0, 'max_dd': 0.0}

    for cycle_n, cycle_start in enumerate(cycle_starts):
        if prog.cancelled: break
        await asyncio.sleep(0)
        
        cycle_color = cycle_colors[color_index % len(cycle_colors)]
        cycle_dam_str = _utc_to_dam(cycle_start).strftime('%Y-%m-%d %H:00')
        cycle_color_map[cycle_dam_str] = cycle_color
        color_index += 1
        
        trades_in_this_cycle = 0
        level_used_this_cycle: set[str] = set()
        
        trend_allows_buy = True; trend_allows_sell = True
        
        if bot_state['use_trend_filter']:
            try:
                cycle_row = df_1m.loc[df_1m.index <= cycle_start].iloc[-1]
                if bot_state['trend_filter_type'] == 'EMA_200':
                    if cycle_row['close'] > cycle_row['EMA_200']: trend_allows_sell = False
                    else: trend_allows_buy = False
                else: 
                    if cycle_row['EMA_50'] > cycle_row['EMA_150']: trend_allows_sell = False
                    else: trend_allows_buy = False
            except: pass

        cycle_end = cycle_start + timedelta(hours=1)
        h1_close = df_h1.loc[cycle_start, 'close']
        
        buy_strong  = determine_strong_gann_levels(calculate_gann_levels_from_close(h1_close, True), h1_close, True)
        sell_strong = determine_strong_gann_levels(calculate_gann_levels_from_close(h1_close, False), h1_close, False)

        df_cycle = df_1m[(df_1m.index > cycle_start) & (df_1m.index <= cycle_end)]
        if df_cycle.empty: continue

        for tf in enabled_tfs:
            tf_min = int(tf.replace('m', '')) if 'm' in tf else int(tf.replace('h', '')) * 60
            df_tf = df_cycle.resample(f'{tf_min}min', closed='right', label='right').agg({'open':'first','high':'max','low':'min','close':'last'}).dropna()
            
            for tf_idx in range(len(df_tf)):
                bar_t = df_tf.index[tf_idx]
                h = float(df_tf.iloc[tf_idx]['high']); l = float(df_tf.iloc[tf_idx]['low'])
                
                signals = []
                for lvl in buy_strong:
                    combo_key = f"{lvl}_{tf}"
                    if bot_state['gann_anti_spam'] and combo_key in level_used_this_cycle: continue
                    if l <= lvl + (margin_pts * pv) and h >= lvl: signals.append({'type': 'BUY', 'lvl': lvl, 'combo': combo_key})

                for lvl in sell_strong:
                    combo_key = f"{lvl}_{tf}"
                    if bot_state['gann_anti_spam'] and combo_key in level_used_this_cycle: continue
                    if h >= lvl - (margin_pts * pv) and l <= lvl: signals.append({'type': 'SELL', 'lvl': lvl, 'combo': combo_key})

                for sig in signals:
                    is_buy = (sig['type'] == 'BUY')
                    if is_buy and not trend_allows_buy: continue
                    if not is_buy and not trend_allows_sell: continue

                    if bot_state['gann_anti_spam']: level_used_this_cycle.add(sig['combo'])
                    
                    entry_px = sig['lvl']
                    tp_pts_user = bot_state['tp_pips'].get(tf, 180)
                    sl_pts_user = bot_state['sl_pips'].get(tf, 100)
                    tp_d = tp_pts_user * pv; sl_d = sl_pts_user * pv
                    
                    tp_px = entry_px + tp_d if is_buy else entry_px - tp_d
                    sl_px = entry_px - sl_d if is_buy else entry_px + sl_d
                    be_px_target = entry_px + (bot_state['be_pips'] * pv) if is_buy else entry_px - (bot_state['be_pips'] * pv)

                    sim_df = df_1m[df_1m.index >= bar_t]
                    outcome = 'OPEN'; p_usd = 0.0; be_activated = False
                    
                    for _, row in sim_df.iterrows():
                        fh = float(row['high']); fl = float(row['low'])
                        if bot_state['use_be'] and not be_activated:
                            if (is_buy and fh >= be_px_target) or (not is_buy and fl <= be_px_target):
                                be_activated = True; sl_px = entry_px 
                        
                        if is_buy:
                            if fl <= sl_px: outcome = 'BREAK-EVEN' if be_activated else 'LOSS'; p_usd = 0.0 if be_activated else -round(sl_d * lot * 100, 2); break
                            if fh >= tp_px: outcome = 'WIN';  p_usd = round(tp_d * lot * 100, 2); break
                        else:
                            if fh >= sl_px: outcome = 'BREAK-EVEN' if be_activated else 'LOSS'; p_usd = 0.0 if be_activated else -round(sl_d * lot * 100, 2); break
                            if fl <= tp_px: outcome = 'WIN';  p_usd = round(tp_d * lot * 100, 2); break

                    if outcome == 'OPEN': continue
                    
                    trades_in_this_cycle += 1
                    if outcome == 'WIN': res['win'] += 1
                    elif outcome == 'LOSS': res['loss'] += 1
                    elif outcome == 'BREAK-EVEN': res['be'] += 1
                    
                    res['total_prof'] += p_usd
                    res['peak_equity'] = max(res['peak_equity'], res['total_prof'])
                    res['max_dd'] = max(res['max_dd'], res['peak_equity'] - res['total_prof'])

                    trade_logs.append({
                        'دورة H1 (DAM)': cycle_dam_str,
                        'وقت الصفقة (DAM)': _utc_to_dam(bar_t).strftime('%Y-%m-%d %H:%M'),
                        'TF': tf,
                        'اتجاه': f"{sig['type']}",
                        'إغلاق H1': h1_close,
                        'المستوى': sig['lvl'],
                        'TP (نقطة)': tp_pts_user, 'SL (نقطة)': sl_pts_user,
                        'النتيجة': outcome,
                        'ربح ($)': p_usd
                    })
                    
        cycles_log.append({
            'الدورة (DAM)': cycle_dam_str,
            'عدد الصفقات': trades_in_this_cycle,
            'ملاحظة': 'لم يلمس السعر أي مستوى!' if trades_in_this_cycle == 0 else f'تم تنفيذ {trades_in_this_cycle} صفقة'
        })
        await prog.tick(cycle_n + 1, res['win'], res['loss'], res['be'], res['total_prof'])

    await prog.set_phase('إنشاء ملف Excel الملون...')
    fname = f"GannBT_{datetime.now(timezone.utc).strftime('%H%M%S')}.xlsx"
    
    df_trades = pd.DataFrame(trade_logs)
    if not df_trades.empty:
        df_trades['TF_Sort'] = df_trades['TF'].apply(lambda x: int(x.replace('m','')) if 'm' in x else int(x.replace('h',''))*60)
        df_trades = df_trades.sort_values(by=['TF_Sort', 'دورة H1 (DAM)']).drop(columns=['TF_Sort'])

    with pd.ExcelWriter(fname, engine='openpyxl') as writer:
        df_trades.to_excel(writer, sheet_name='الصفقات', index=False)
        pd.DataFrame(cycles_log).to_excel(writer, sheet_name='دورات H1', index=False)
        
        if not df_trades.empty:
            ws_trades = writer.sheets['الصفقات']
            cycle_col_idx = list(df_trades.columns).index('دورة H1 (DAM)') + 1
            for row in ws_trades.iter_rows(min_row=2):
                c_val = row[cycle_col_idx-1].value
                if c_val in cycle_color_map:
                    fill = PatternFill(start_color=cycle_color_map[c_val].replace('#',''), fill_type='solid')
                    for cell in row: cell.fill = fill
                    
        ws_cycles = writer.sheets['دورات H1']
        for row in ws_cycles.iter_rows(min_row=2):
            c_val = row[0].value
            if c_val in cycle_color_map:
                fill = PatternFill(start_color=cycle_color_map[c_val].replace('#',''), fill_type='solid')
                for cell in row: cell.fill = fill

    await prog.done(f'باكتيست اكتمل ✅\nالربح: ${res["total_prof"]}')
    await send_tg_document(fname, 'نتائج الباكتيست (مفرز وملون)')
    try: os.remove(fname)
    except: pass
    bot_state['is_backtesting'] = False

# ─────────────────────────────────────────────────────────────
# TELEGRAM COMMANDS & CALLBACKS
# ─────────────────────────────────────────────────────────────
async def process_tg_update(update: dict) -> None:
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip(); bot_state['chat_id'] = update['message']['chat']['id']

        if not msg.startswith('/') and msg in bot_state.get('menu_button_map', {}):
            cb = bot_state['menu_button_map'][msg]
            if cb != 'noop': await _handle_callback(cb, bot_state['chat_id'], None)
            return

        if msg == '/start':
            await send_tg_msg('<b>Gold Scalper Bot v5.4</b>', get_main_keyboard())

        # التحكم اليدوي السريع في الإعدادات عبر الرسائل (كما طلبت)
        elif msg.lower().startswith('/set'):
            parts = msg.strip().lower().split()
            # حماية الأرباح /set be 30
            if len(parts) == 3 and parts[1] == 'be':
                try: bot_state['be_pips'] = int(parts[2]); await send_tg_msg(f'✅ تم تعديل الـ BE إلى {parts[2]} نقطة.')
                except ValueError: pass
                return
            # أوامر الهدف والوقف /set 1m sl 100
            if len(parts) == 4:
                _, tf, key, val = parts
                if tf in _TFS and key in ('tp', 'sl'):
                    try: bot_state[f'{key}_pips'][tf] = int(val); await send_tg_msg(f'✅ تم تحديث [{tf}]: {key.upper()} = {val}')
                    except ValueError: pass
                return

        # أمر الباكتيست حسب التاريخ
        elif msg.lower().startswith('/backtest'):
            parts = msg.strip().split()
            try:
                if len(parts) == 2:
                    # يوم واحد
                    start_dt = _dam_to_utc(f"{parts[1]} 00:00")
                    end_dt = start_dt + timedelta(days=1)
                elif len(parts) >= 3:
                    # عدة أيام
                    d1 = _dam_to_utc(f"{parts[1]} 00:00"); d2 = _dam_to_utc(f"{parts[2]} 00:00")
                    start_dt = min(d1, d2); end_dt = max(d1, d2) + timedelta(days=1)
                else: return
                
                if not bot_state['is_backtesting']: asyncio.create_task(run_gann_backtest_dates(start_dt, end_dt))
            except Exception: pass
            
        elif msg == '/restart_sessions':
            global _http, _poll_task
            if _poll_task and not _poll_task.done(): _poll_task.cancel()
            if _http and not _http.closed: await _http.close()
            _http = None; get_http()
            await send_tg_msg('✅ تم التجديد.')
        return

    if 'callback_query' not in update: return
    q = update['callback_query']; d = q['data']; chat_id = q['message']['chat']['id']; msg_id = q['message']['message_id']
    bot_state['chat_id'] = chat_id
    try: await _handle_callback(d, chat_id, msg_id)
    except Exception as e: c_log(f'CB error: {e}')
    finally: await answer_callback(q['id'])

async def _handle_callback(d: str, chat_id: int, msg_id: int) -> None:
    if d == 'noop': pass
    elif d == 'menu_main': await _show(chat_id, msg_id, 'Main Menu:', get_main_keyboard())
    elif d == 'menu_strategy': await _show(chat_id, msg_id, 'إعدادات الاستراتيجية:', get_strategy_keyboard())
    elif d == 'menu_risk': await _show(chat_id, msg_id, 'المخاطر:', get_risk_keyboard())
    elif d == 'menu_tfs': await _show(chat_id, msg_id, 'الفريمات النشطة:', get_tf_keyboard())
    elif d == 'menu_backtest': await _show(chat_id, msg_id, 'الباكتيست:', get_backtest_keyboard())
    elif d == 'hide_keyboard': bot_state['menu_button_map'] = {}; await _show(chat_id, msg_id, 'مخفية.', {'remove_keyboard': True})

    # أزرار الاستراتيجية (Anti-Spam وحماية الاتجاه)
    elif d == 'toggle_spam': bot_state['gann_anti_spam'] = not bot_state['gann_anti_spam']; await _show(chat_id, msg_id, 'Strategy:', get_strategy_keyboard())
    elif d == 'toggle_trend': bot_state['use_trend_filter'] = not bot_state['use_trend_filter']; await _show(chat_id, msg_id, 'Strategy:', get_strategy_keyboard())
    elif d == 'cycle_trend_type': bot_state['trend_filter_type'] = 'EMA_CROSS' if bot_state['trend_filter_type'] == 'EMA_200' else 'EMA_200'; await _show(chat_id, msg_id, 'Strategy:', get_strategy_keyboard())

    # أزرار المخاطر (الـ BE)
    elif d == 'toggle_be': bot_state['use_be'] = not bot_state['use_be']; await _show(chat_id, msg_id, 'Risk:', get_risk_keyboard())
    elif d == 'view_tpsl': await _show(chat_id, msg_id, 'TP/SL:', get_tpsl_overview_keyboard())

    # الفريمات
    elif d.startswith('toggle_tf_'):
        tf = d[len('toggle_tf_'):]
        if tf in bot_state['active_tfs']: bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
        await _show(chat_id, msg_id, 'الفريمات:', get_tf_keyboard())

    elif d == 'cancel_bt':
        global _bt_progress
        if _bt_progress: await _bt_progress.cancel(); await _show(chat_id, msg_id, 'جاري الإيقاف...', get_main_keyboard())

# ─────────────────────────────────────────────────────────────
# TELEGRAM POLLING & WATCHDOG
# ─────────────────────────────────────────────────────────────
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
                                bot_state['last_update_id'] = upd['update_id']; asyncio.create_task(process_tg_update(upd))
                        else: await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)
                except asyncio.CancelledError: raise
                except Exception: await asyncio.sleep(backoff); backoff = min(backoff * 2, 30); break
        except asyncio.CancelledError: await sess.close(); raise
        finally: await sess.close()
        await asyncio.sleep(1)

async def telegram_watchdog() -> None:
    global _poll_task
    await asyncio.sleep(30)
    while True:
        await asyncio.sleep(20)
        try:
            last = bot_state.get('last_poll_ok', 0.0); age = datetime.now(timezone.utc).timestamp() - last
            if age > 180 and _poll_task is not None and not _poll_task.done(): _poll_task.cancel()
        except Exception: pass

async def supervised(coro_fn, *args, label: str = '') -> None:
    global _poll_task
    while True:
        try:
            task = asyncio.current_task()
            if label == 'tg_polling': _poll_task = task
            await coro_fn(*args)
        except asyncio.CancelledError: await asyncio.sleep(2)   
        except Exception as e: c_log(f'Task {label} crashed: {e}'); await asyncio.sleep(5)

# ─────────────────────────────────────────────────────────────
# ENTRY POINT & DUMMY WEB SERVER (For Render)
# ─────────────────────────────────────────────────────────────
async def handle_ping(request):
    return web.Response(text="Bot is running smoothly!")

async def main() -> None:
    get_http()
    bot_state['last_poll_ok'] = datetime.now(timezone.utc).timestamp()
    
    # 🌐 تشغيل خادم ويب بسيط لإرضاء منصة Render
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    c_log(f'Web server started on port {port}')

    tasks = [
        asyncio.create_task(supervised(telegram_polling_loop, label='tg_polling')),
        asyncio.create_task(supervised(telegram_watchdog,     label='tg_watchdog')),
    ]
    c_log('Gold Scalper Bot v5.4 Advanced started.')
    try: await asyncio.gather(*tasks)
    finally:
        if _http and not _http.closed: await _http.close()

if __name__ == '__main__':
    asyncio.run(main())

