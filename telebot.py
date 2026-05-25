import asyncio
import aiohttp
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, timezone
from metaapi_cloud_sdk import MetaApi
from aiohttp import web

# --- METAAPI & OANDA CONFIGURATION ---
METAAPI_TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9..NRMo-BO9ezZBEb4XmCQzkMsRN1iAz1rVSk7XWFP-ZGS_AZEyxSfIjnJ5w-r4egazV7tnxNLjjMuAdUb25T3ur3XWKCL4Jo9LFPy9tZzhIMRtlhq8d6YAHK9uxJclqJv5BZQFDeMeiFtyalLNjaE100Lp2zEnGWwlloxF-dpCw5DXvVKeGfMyVx4L2kisshcysDo7OeMkDBU1UB7leHi2eviEl7XQCpmhxdzT4BwMkf8YERx2jouKVu8-koVy00aon0drktGBSlQDOFw2WV0hg-VUfeCBR_Hgw2czqKVJ_lj_ZN3EsjWirirpiuXWbtwdD-VPokjKtX1z3ugcSTS1nd2iFIzauUHdOfb7Jl0R6cm8FosVS-4Iu046DiMsrxiAJ4PBywOXQhsFzZiePqmil1w5HHCxrw_78HNR9XcjBETMpHx9W48llIeUOkBVbsKfBP5iYtGSjS52i0QgpvHkfKrtXfbkMT0_9yJFG2kfZJHwJ5BJzWT4aKXto3l6iGe45xe4ZJhYhZX_RkC6dxR2w84M-uY-wlqiv_sxjHNOguSyOx4lfaeoq5H-LuJiWpHAYxEJUQWoQAQ7PObZOXCDWLRc_vP2gcbv1qYxTjD54FHnqhyf-oTGzAkWG5CVQFKpp9jTHQ3pXEYTSgIUTfHDbtoesAY1HG3nHcHbwujnqo0'
ACCOUNT_ID = '7d54fa6f-eaf7-4637-92a1-e0356ee729f8'
TG_TOKEN = '8779425898:AAFDMBTe0eIUin25rz809CfuINU4pmmVs-M'
OANDA_ID = '101-004-28533521-003'
OANDA_API = 'c0f5b5df69c77e8bf35dcfd2fbde72da-a4c6cbadba7ae39d21143f65e2c2b8ba'
OANDA_URL = 'https://api-fxpractice.oanda.com/v3'

def c_log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# =============================================================
# GLOBAL STATE
# strategy:     'NEW' = K يتجاوز مستوى ثابت (10/15/20)
#               'OLD' = K يتقاطع مع D داخل المنطقة (ذروة)
# filter_mode:  'FULL'   = توازي ema15 > ema50 > ema150 الكامل
#               'SIMPLE' = توازي ema50 > ema150 فقط (تجاهل ema15)
#               'NO_MA'  = بلا موفينجات، ستوكاستيك فقط
# الثلاثة حصرية: تفعيل أحدها يلغي الآخرين تلقائياً
# =============================================================
bot_state = {
    'status': 'RUNNING',
    'strategy': 'NEW',
    'filter_mode': 'FULL',
    'symbol': 'XAUUSD@',
    'live_connected': False,
    'timeframes': ['1m', '2m', '3m', '5m', '15m'],
    'active_tfs': {'1m': False, '2m': False, '3m': False, '5m': True, '15m': False},
    'lot_size': 0.05, 'pip_value': 0.1, 'spread_pips': 2.2,
    'chat_id': None, 'last_update_id': 0,
    'tp_pips': {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80},
    'sl_pips': {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},
    # مستويات الستوكاستيك
    'stoch_k': 5, 'stoch_d': 5, 'stoch_smooth': 5,  # قيم الستوكاستيك
    'use_stoch_deep': True,   # 10/90
    'use_stoch_mid':  True,   # 15/85
    'use_stoch_shal': True,   # 20/80
    # فلاتر الوقت
    'use_time_filter':   False,
    'use_danger_filter': False,
    # إعدادات أخرى
    'use_f_cons': False, 'cons_count': 3,
    'use_be': False, 'use_atr': False, 'use_max_spread': True,
    'max_spread_pips': 3.0, 'atr_mult_tp': 1.5, 'atr_mult_sl': 3.0,
    'use_s5_ticks': False, 'tp_tolerance_pips': 5.0,
    'market_data': {tf: "⏸ بانتظار الاتصال (Offline)" for tf in ['1m', '2m', '3m', '5m', '15m']},
    'last_signal_time': {tf: None for tf in ['1m', '2m', '3m', '5m', '15m']},
    'connection_obj': None, 'account_obj': None, 'is_backtesting': False
}

# =============================================================
# CORE SIGNAL FUNCTIONS
# =============================================================

def compute_trend_ok(df, i, curr):
    """
    يحسب trend_buy_ok و trend_sell_ok بناءً على filter_mode
    FULL:   يشترط ema15 > ema50 > ema150 (توازي كامل)
    SIMPLE: يشترط ema50 > ema150 فقط (يتجاهل ema15)
    NO_MA:  لا يشترط أي توازي، ستوكاستيك فقط
    """
    mode = bot_state['filter_mode']
    if mode == 'NO_MA':
        return True, True

    # فحص ثبات الترند عبر عدة شموع
    cons = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema, s_ema = True, True
    for j in range(cons):
        c = df.loc[i - j]
        if not (c['ema50'] > c['ema150']): b_ema = False
        if not (c['ema150'] > c['ema50']): s_ema = False

    if mode == 'SIMPLE':
        # فقط ema50 مقابل ema150
        return b_ema, s_ema
    else:  # FULL
        # إضافة شرط التوازي الكامل مع ema15
        ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
        ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
        return (b_ema and ma_buy), (s_ema and ma_sell)


def compute_trend_ok_live(df, curr):
    """
    نسخة Live Scanner (تستخدم iloc بدلاً من loc)
    """
    mode = bot_state['filter_mode']
    if mode == 'NO_MA':
        return True, True

    cons = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
    b_ema, s_ema = True, True
    for j in range(cons):
        cc = df.iloc[(-2) - j]
        if not (cc['ema50'] > cc['ema150']): b_ema = False
        if not (cc['ema150'] > cc['ema50']): s_ema = False

    if mode == 'SIMPLE':
        return b_ema, s_ema
    else:  # FULL
        ma_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
        ma_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
        return (b_ema and ma_buy), (s_ema and ma_sell)


def get_stoch_signals(prev_k, prev_d, curr_k, curr_d):
    """
    يحسب إشارات الشراء والبيع بناءً على نوع الاستراتيجية:

    NEW: K يتجاوز مستوى ثابت من الأسفل (شراء) أو من الأعلى (بيع)
         buy:  prev_K كان داخل المنطقة, curr_K تجاوزها للأعلى
         sell: prev_K كان داخل المنطقة, curr_K تجاوزها للأسفل

    OLD: K يتقاطع مع D (يتجاوزه) داخل منطقة التشبع
         buy:  K يقطع D من الأسفل للأعلى وكلاهما في المنطقة <= 25
         sell: K يقطع D من الأعلى للأسفل وكلاهما في المنطقة >= 75

    يُعيد: (buy_sig, sell_sig, b_label, s_label)
    """
    strategy = bot_state['strategy']

    if strategy == 'NEW':
        # K يتجاوز المستويات الثابتة
        buy_deep  = (prev_k <= 10) and (curr_k > 10) and bot_state['use_stoch_deep']
        buy_mid   = (10 < prev_k <= 15) and (curr_k > 15) and bot_state['use_stoch_mid']
        buy_shal  = (15 < prev_k <= 20) and (curr_k > 20) and bot_state['use_stoch_shal']

        sell_deep = (prev_k >= 90) and (curr_k < 90) and bot_state['use_stoch_deep']
        sell_mid  = (85 <= prev_k < 90) and (curr_k < 85) and bot_state['use_stoch_mid']
        sell_shal = (80 <= prev_k < 85) and (curr_k < 80) and bot_state['use_stoch_shal']

        buy_sig  = buy_deep or buy_mid or buy_shal
        sell_sig = sell_deep or sell_mid or sell_shal

        b_label = "DEEP(10)" if buy_deep else "MID(15)" if buy_mid else "SHAL(20)"
        s_label = "DEEP(90)" if sell_deep else "MID(85)" if sell_mid else "SHAL(80)"

    else:  # OLD: K/D crossover في منطقة التشبع
        k_cross_up   = (prev_k < prev_d) and (curr_k >= curr_d)
        k_cross_down = (prev_k > prev_d) and (curr_k <= curr_d)
        avg_k = (prev_k + curr_k) / 2.0

        buy_deep  = k_cross_up and (avg_k <= 10) and bot_state['use_stoch_deep']
        buy_mid   = k_cross_up and (10 < avg_k <= 15) and bot_state['use_stoch_mid']
        buy_shal  = k_cross_up and (15 < avg_k <= 20) and bot_state['use_stoch_shal']

        sell_deep = k_cross_down and (avg_k >= 90) and bot_state['use_stoch_deep']
        sell_mid  = k_cross_down and (85 <= avg_k < 90) and bot_state['use_stoch_mid']
        sell_shal = k_cross_down and (80 <= avg_k < 85) and bot_state['use_stoch_shal']

        buy_sig  = buy_deep or buy_mid or buy_shal
        sell_sig = sell_deep or sell_mid or sell_shal

        b_label = "DEEP(10)" if buy_deep else "MID(15)" if buy_mid else "SHAL(20)"
        s_label = "DEEP(90)" if sell_deep else "MID(85)" if sell_mid else "SHAL(80)"

    return buy_sig, sell_sig, b_label, s_label


def is_danger_time(dt_utc):
    """فحص فترة الخطر 16:00-18:30 دمشق (= 13:00-15:30 UTC)"""
    dh = (dt_utc.hour + 3) % 24
    dm = dt_utc.minute
    return (dh == 16) or (dh == 17) or (dh == 18 and dm <= 30)


# =============================================================
# OANDA & TELEGRAM HELPERS
# =============================================================

async def fetch_oanda_candles(instrument, granularity, count=5000, end_time=None):
    tf_map = {'s5': 'S5', '1m': 'M1', '2m': 'M2', '3m': 'M3',
              '5m': 'M5', '15m': 'M15', '1h': 'H1'}
    url = f"{OANDA_URL}/instruments/{instrument}/candles"
    headers = {"Authorization": f"Bearer {OANDA_API}"}
    params = {"granularity": tf_map.get(granularity, 'M5'), "count": count, "price": "M"}
    if end_time:
        params["to"] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = []
                    for c in data.get('candles', []):
                        if c['complete']:
                            candles.append({
                                'time':  pd.to_datetime(c['time']),
                                'open':  float(c['mid']['o']),
                                'high':  float(c['mid']['h']),
                                'low':   float(c['mid']['l']),
                                'close': float(c['mid']['c'])
                            })
                    return candles
        except Exception as e:
            c_log(f"❌ خطأ Oanda: {e}")
    return []


async def send_tg_msg(text, reply_markup=None):
    if not bot_state['chat_id']: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as s:
        try: await s.post(url, json=payload)
        except: pass


async def edit_tg_msg(chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/editMessageText"
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as s:
        try: await s.post(url, json=payload)
        except: pass


async def answer_callback(callback_query_id, text=None):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery"
    payload = {'callback_query_id': callback_query_id}
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
                data.add_field('chat_id', str(bot_state['chat_id']))
                data.add_field('document', f)
                data.add_field('caption', caption)
                await s.post(url, data=data)
        except Exception as e:
            c_log(f"❌ خطأ إرسال الملف: {e}")


def calculate_indicators(df):
    if df.empty: return df
    df['ema15']  = df['close'].ewm(span=15,  adjust=False).mean()
    df['ema50']  = df['close'].ewm(span=50,  adjust=False).mean()
    df['ema150'] = df['close'].ewm(span=150, adjust=False).mean()
    _k = bot_state.get('stoch_k', 5)
    _d = bot_state.get('stoch_d', 5)
    _s = bot_state.get('stoch_smooth', 5)
    low_min  = df['low'].rolling(window=_k).min()
    high_max = df['high'].rolling(window=_k).max()
    denom = (high_max - low_min).replace(0, 1e-10)
    df['k_raw'] = 100 * ((df['close'] - low_min) / denom)
    df['K'] = df['k_raw'].ewm(span=_s, adjust=False).mean()
    df['D'] = df['K'].ewm(span=_d, adjust=False).mean()
    df['tr0'] = abs(df['high'] - df['low'])
    df['tr1'] = abs(df['high'] - df['close'].shift())
    df['tr2'] = abs(df['low']  - df['close'].shift())
    df['tr']  = df[['tr0', 'tr1', 'tr2']].max(axis=1)
    df['atr'] = df['tr'].rolling(14).mean().bfill()
    return df


# =============================================================
# UI KEYBOARDS

def get_stoch_settings_keyboard():
    k = bot_state['stoch_k']
    d = bot_state['stoch_d']
    s = bot_state['stoch_smooth']
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
         {"text": "14,3,3", "callback_data": "preset_14_3_3"}],
        [{"text": "🔙 رجوع", "callback_data": "menu_filters"}]
    ]}


# =============================================================

def get_main_keyboard():
    live_icon   = "🟢 متصل" if bot_state['live_connected'] else "🔴 غير متصل"
    status_icon = "🟢 RUN"  if bot_state['status'] == 'RUNNING' else "🔴 PAUSE"
    strat_lbl   = "🔵 NEW (K→مستوى)" if bot_state['strategy'] == 'NEW' else "🟡 OLD (K✕D)"
    fm_lbl = {"FULL": "📐 FULL(15+50+150)", "SIMPLE": "📏 SIMPLE(50+150)", "NO_MA": "🚫 NO MA"}[bot_state['filter_mode']]
    return {"inline_keyboard": [
        [{"text": f"🔌 سيرفر التداول الحي: {live_icon}", "callback_data": "toggle_live_conn"}],
        [{"text": f"Status: {status_icon}", "callback_data": "toggle_status"},
         {"text": f"Strategy: {strat_lbl}", "callback_data": "toggle_strat"}],
        [{"text": f"الفلتر الحالي: {fm_lbl}", "callback_data": "menu_filters"}],
        [{"text": "🎛 فلاتر وإعدادات", "callback_data": "menu_filters"},
         {"text": "⏱ فريمات", "callback_data": "menu_tfs"}],
        [{"text": "📊 Live Report", "callback_data": "report"},
         {"text": "💳 Account", "callback_data": "account"}],
        [{"text": "🛠 إعدادات المخاطرة", "callback_data": "menu_settings"},
         {"text": "🔬 BACKTEST", "callback_data": "menu_backtest"}],
        [{"text": "🛑 إغلاق جميع الصفقات", "callback_data": "close_all"}]
    ]}


def get_filters_keyboard():
    fm = bot_state['filter_mode']
    full_icon   = "✅" if fm == 'FULL'   else "⬜"
    simple_icon = "✅" if fm == 'SIMPLE' else "⬜"
    noma_icon   = "✅" if fm == 'NO_MA'  else "⬜"
    dp_icon = "🟢" if bot_state['use_stoch_deep'] else "🔴"
    md_icon = "🟢" if bot_state['use_stoch_mid']  else "🔴"
    sh_icon = "🟢" if bot_state['use_stoch_shal'] else "🔴"
    t_icon  = "🟢" if bot_state['use_time_filter']   else "🔴"
    d_icon  = "🟢" if bot_state['use_danger_filter'] else "🔴"
    c_icon  = "🟢" if bot_state['use_f_cons'] else "🔴"
    return {"inline_keyboard": [
        [{"text": "━━ فلتر الترند (اختر واحداً فقط) ━━", "callback_data": "noop"}],
        [{"text": f"{full_icon} FULL: توازي ema15 + ema50 + ema150", "callback_data": "set_filter_full"}],
        [{"text": f"{simple_icon} SIMPLE: توازي ema50 + ema150 فقط", "callback_data": "set_filter_simple"}],
        [{"text": f"{noma_icon} NO MA: بلا موفينجات (ستوكاستيك فقط)", "callback_data": "set_filter_noma"}],
        [{"text": "━━ مستويات الستوكاستيك ━━", "callback_data": "noop"}],
        [{"text": f"⚙️ إعدادات Stoch({bot_state['stoch_k']},{bot_state['stoch_smooth']},{bot_state['stoch_d']})", "callback_data": "menu_stoch_settings"}],
        [{"text": f"DEEP 10/90: {dp_icon}", "callback_data": "toggle_stoch_deep"},
         {"text": f"MID  15/85: {md_icon}", "callback_data": "toggle_stoch_mid"},
         {"text": f"SHAL 20/80: {sh_icon}", "callback_data": "toggle_stoch_shal"}],
        [{"text": "━━ فلاتر الوقت ━━", "callback_data": "noop"}],
        [{"text": f"Time Filter 08-17 UTC: {t_icon}", "callback_data": "toggle_time"},
         {"text": f"حظر 16-18:30 دمشق: {d_icon}", "callback_data": "toggle_danger"}],
        [{"text": f"ثبات الترند ({bot_state['cons_count']} شموع): {c_icon}", "callback_data": "toggle_f_cons"}],
        [{"text": "🔙 القائمة الرئيسية", "callback_data": "menu_main"}]
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
        [{"text": f"أهداف ATR: {atr_i}", "callback_data": "toggle_atr"}],
        [{"text": f"حماية السبريد: {spr_i}", "callback_data": "toggle_spread"}],
        [{"text": f"LOT SIZE: {bot_state['lot_size']}", "callback_data": "noop"}],
        [{"text": "➕ Lot", "callback_data": "inc_lot"},
         {"text": "➖ Lot", "callback_data": "dec_lot"}],
        [{"text": "📖 عرض TP/SL", "callback_data": "view_tpsl"}],
        [{"text": "🔙 رجوع", "callback_data": "menu_main"}]
    ]}


# =============================================================
# BACKTEST ENGINE (Standard)
# =============================================================

async def run_oanda_backtest(start_dt):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة.")
        return
    bot_state['is_backtesting'] = True
    c_log("بدء الباك تيست...")
    fname = f"BT_{datetime.now().strftime('%H%M%S')}.xlsx"
    trade_logs, blocked_logs = [], []
    total_prof, peak_equity, max_dd = 0.0, 0.0, 0.0
    total_win, total_loss, win_count, loss_count, be_count = 0.0, 0.0, 0, 0, 0

    fm_desc = {"FULL": "FULL(15+50+150)", "SIMPLE": "SIMPLE(50+150)", "NO_MA": "NO MA"}[bot_state['filter_mode']]
    await send_tg_msg(
        f"⏳ <b>بدء الباك تيست</b>\n"
        f"من: {start_dt.strftime('%Y-%m-%d')}\n"
        f"الاستراتيجية: {bot_state['strategy']} | الفلتر: {fm_desc}"
    )

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 150: continue
            df = calculate_indicators(
                pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(3, bot_state['cons_count'])

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                curr, prev = df.loc[i], df.loc[i - 1]

                # فلتر الوقت
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17):
                    continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']):
                    continue

                # فلتر الترند
                trend_buy_ok, trend_sell_ok = compute_trend_ok(df, i, curr)

                # إشارات الستوكاستيك
                raw_buy, raw_sell, b_label, s_label = get_stoch_signals(
                    prev['K'], prev['D'], curr['K'], curr['D'])

                buy_sig  = raw_buy  and trend_buy_ok
                sell_sig = raw_sell and trend_sell_ok

                # تسجيل المرفوضات
                if not buy_sig and raw_buy:
                    blocked_logs.append({
                        'Timeframe': tf, 'Type': f'BUY BLOCKED ({b_label})',
                        'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'
                    })
                if not sell_sig and raw_sell:
                    blocked_logs.append({
                        'Timeframe': tf, 'Type': f'SELL BLOCKED ({s_label})',
                        'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'
                    })

                if not (buy_sig or sell_sig): continue
                if i + 1 >= len(df): continue

                next_c  = df.loc[i + 1]
                entry_p = next_c['open']
                entry_t = next_c['time']
                m = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])

                if bot_state['use_atr']:
                    tp_dist = curr['atr'] * bot_state['atr_mult_tp']
                    sl_dist = curr['atr'] * bot_state['atr_mult_sl']
                else:
                    tp_dist = bot_state['tp_pips'][tf] * bot_state['pip_value']
                    sl_dist = bot_state['sl_pips'][tf] * bot_state['pip_value']

                tp_p = round(act_ent + (m * tp_dist), 2)
                sl_p = round(act_ent - (m * sl_dist), 2)
                tol  = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp = (tp_p - tol) if buy_sig else (tp_p + tol)

                max_ext = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                val_c   = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                outcome, exit_t = "EXPIRED", max_ext
                be_act  = False
                be_tgt  = act_ent + (m * 20 * bot_state['pip_value'])

                for vc in [v for v in val_c if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['low'] <= sl_p:
                            outcome, exit_t = ("BREAK-EVEN" if be_act else "LOSS"), vc['time']; break
                        if vc['high'] >= eff_tp:
                            outcome, exit_t = "WIN", vc['time']; break
                    else:
                        if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['high'] >= sl_p:
                            outcome, exit_t = ("BREAK-EVEN" if be_act else "LOSS"), vc['time']; break
                        if vc['low'] <= eff_tp:
                            outcome, exit_t = "WIN", vc['time']; break

                if outcome == "BREAK-EVEN":
                    p_usd = 0.0; be_count += 1
                elif outcome in ["WIN", "LOSS"]:
                    p_usd = round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p)) * 100 * bot_state['lot_size'], 2
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
                    'Type':        f"BUY {b_label}" if buy_sig else f"SELL {s_label}",
                    'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price': round(act_ent, 2),
                    'TP': tp_p, 'SL': sl_p,
                    'Pips': round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p)) / bot_state['pip_value'], 1
                    ) if outcome in ["WIN", "LOSS"] else 0,
                    'Outcome':    outcome,
                    'Profit ($)': p_usd
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على أي صفقات."); return

        from openpyxl.styles import PatternFill, Font
        df_logs = pd.DataFrame(trade_logs)
        total_trades = win_count + loss_count
        win_rate = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct   = round(max_dd / peak_equity * 100, 1) if peak_equity else 0

        summary_data = {
            'البند': ['✅ الربح الكلي', '❌ الخسارة الكلية', '💰 المحصلة النهائية',
                      '🎯 نسبة الفوز', '📉 أقصى سحب (DD)', '🔄 بريك إيفن',
                      '📌 الاستراتيجية', '📌 الفلتر'],
            'القيمة': [
                f'{win_count} صفقة | +${round(total_win, 2)}',
                f'{loss_count} صفقة | -${abs(round(total_loss, 2))}',
                f'${round(total_prof, 2)}',
                f'{win_rate}% ({total_trades} صفقة)',
                f'${round(max_dd, 2)} ({dd_pct}%)',
                str(be_count),
                bot_state['strategy'],
                bot_state['filter_mode']
            ]
        }

        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            df_logs.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='الملخص', index=False)
            if blocked_logs:
                pd.DataFrame(blocked_logs).to_excel(writer, sheet_name='المرفوضة', index=False)

            ws = writer.sheets['الصفقات']
            gf = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
            rf = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            hf = PatternFill(start_color='2E4057', end_color='2E4057', fill_type='solid')
            for cell in ws[1]:
                cell.fill = hf; cell.font = Font(color='FFFFFF', bold=True)
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
            f"✅ +${round(total_win,2)} ({win_count})\n"
            f"❌ -${abs(round(total_loss,2))} ({loss_count})\n"
            f"💰 ${round(total_prof,2)} | 🎯 {win_rate}% | 📉 DD:{round(max_dd,2)}"
        )
        os.remove(fname)

    except Exception as e:
        c_log(f"❌ خطأ باك تيست: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally:
        bot_state['is_backtesting'] = False


# =============================================================
# ADVANCED BACKTEST (MT5 Style)
# =============================================================

async def run_advanced_backtest(days=7):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة.")
        return
    bot_state['is_backtesting'] = True
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    fm_desc = {"FULL": "FULL(15+50+150)", "SIMPLE": "SIMPLE(50+150)", "NO_MA": "NO MA"}[bot_state['filter_mode']]
    await send_tg_msg(
        f"⏳ <b>Advanced Backtest</b>\n"
        f"من: {start_dt.strftime('%Y-%m-%d')} ({days} أيام)\n"
        f"الاستراتيجية: {bot_state['strategy']} | الفلتر: {fm_desc}"
    )

    trade_logs, blocked_logs = [], []
    total_prof, peak_equity, max_dd = 0.0, 0.0, 0.0
    total_win, total_loss, win_count, loss_count, be_count = 0.0, 0.0, 0, 0, 0
    long_win, long_loss, short_win, short_loss = 0, 0, 0, 0
    all_profits = []
    consec_win, consec_loss = 0, 0
    max_consec_win, max_consec_loss = 0, 0
    max_consec_win_usd, max_consec_loss_usd = 0.0, 0.0
    cur_w_usd, cur_l_usd = 0.0, 0.0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 150: continue
            df = calculate_indicators(
                pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(3, bot_state['cons_count'])

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                curr, prev = df.loc[i], df.loc[i - 1]

                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter'] and is_danger_time(curr['time']): continue

                trend_buy_ok, trend_sell_ok = compute_trend_ok(df, i, curr)
                raw_buy, raw_sell, b_label, s_label = get_stoch_signals(
                    prev['K'], prev['D'], curr['K'], curr['D'])

                buy_sig  = raw_buy  and trend_buy_ok
                sell_sig = raw_sell and trend_sell_ok

                if not buy_sig and raw_buy:
                    blocked_logs.append({
                        'Timeframe': tf, 'Type': f'BUY BLOCKED ({b_label})',
                        'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'
                    })
                if not sell_sig and raw_sell:
                    blocked_logs.append({
                        'Timeframe': tf, 'Type': f'SELL BLOCKED ({s_label})',
                        'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Entry Price': curr['close'], 'Reason': f'REJECTED ({bot_state["filter_mode"]})'
                    })

                if not (buy_sig or sell_sig): continue
                if i + 1 >= len(df): continue

                next_c  = df.loc[i + 1]
                entry_p = next_c['open']
                entry_t = next_c['time']
                m = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])

                tp_dist = (curr['atr'] * bot_state['atr_mult_tp']) if bot_state['use_atr'] else (bot_state['tp_pips'][tf] * bot_state['pip_value'])
                sl_dist = (curr['atr'] * bot_state['atr_mult_sl']) if bot_state['use_atr'] else (bot_state['sl_pips'][tf] * bot_state['pip_value'])
                tp_p   = round(act_ent + (m * tp_dist), 2)
                sl_p   = round(act_ent - (m * sl_dist), 2)
                tol    = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp = (tp_p - tol) if buy_sig else (tp_p + tol)

                max_ext = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                val_c   = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                outcome, exit_t = "EXPIRED", max_ext
                be_act  = False
                be_tgt  = act_ent + (m * 20 * bot_state['pip_value'])

                for vc in [v for v in val_c if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_act and vc['high'] >= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['low'] <= sl_p:
                            outcome, exit_t = ("BREAK-EVEN" if be_act else "LOSS"), vc['time']; break
                        if vc['high'] >= eff_tp:
                            outcome, exit_t = "WIN", vc['time']; break
                    else:
                        if bot_state['use_be'] and not be_act and vc['low'] <= be_tgt:
                            sl_p = act_ent; be_act = True
                        if vc['high'] >= sl_p:
                            outcome, exit_t = ("BREAK-EVEN" if be_act else "LOSS"), vc['time']; break
                        if vc['low'] <= eff_tp:
                            outcome, exit_t = "WIN", vc['time']; break

                if outcome == "BREAK-EVEN":
                    p_usd = 0.0; be_count += 1
                elif outcome in ["WIN", "LOSS"]:
                    p_usd = round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p)) * 100 * bot_state['lot_size'], 2
                    ) * (1 if outcome == "WIN" else -1)
                else:
                    p_usd = 0.0

                if outcome == "WIN":
                    total_win += p_usd; win_count += 1
                    consec_win += 1; cur_w_usd += p_usd; consec_loss = 0; cur_l_usd = 0.0
                    if consec_win > max_consec_win: max_consec_win = consec_win; max_consec_win_usd = cur_w_usd
                    if buy_sig: long_win += 1
                    else: short_win += 1
                elif outcome == "LOSS":
                    total_loss += p_usd; loss_count += 1
                    consec_loss += 1; cur_l_usd += p_usd; consec_win = 0; cur_w_usd = 0.0
                    if consec_loss > max_consec_loss: max_consec_loss = consec_loss; max_consec_loss_usd = cur_l_usd
                    if buy_sig: long_loss += 1
                    else: short_loss += 1

                total_prof  += p_usd
                peak_equity  = max(peak_equity, total_prof)
                max_dd       = max(max_dd, peak_equity - total_prof)
                all_profits.append(p_usd)
                _dh = (curr['time'].hour + 3) % 24

                trade_logs.append({
                    'Timeframe':   tf,
                    'Type':        f"BUY {b_label}" if buy_sig else f"SELL {s_label}",
                    'Entry Time':  (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':   (exit_t  + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price': round(act_ent, 2), 'TP': tp_p, 'SL': sl_p,
                    'Pips': round(
                        abs(act_ent - (tp_p if outcome == "WIN" else sl_p)) / bot_state['pip_value'], 1
                    ) if outcome in ["WIN", "LOSS"] else 0,
                    'Outcome': outcome, 'Profit ($)': p_usd,
                    'Hour_Damascus': _dh, 'Weekday': curr['time'].strftime('%a')
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على صفقات."); return

        total_trades   = win_count + loss_count
        win_rate       = round(win_count / total_trades * 100, 1) if total_trades else 0
        dd_pct         = round(max_dd / peak_equity * 100, 1) if peak_equity else 0
        profit_factor  = round(total_win / abs(total_loss), 2) if total_loss else 999
        expected_payoff= round(total_prof / total_trades, 2) if total_trades else 0
        recovery_factor= round(total_prof / max_dd, 2) if max_dd else 999
        wins_only   = [p for p in all_profits if p > 0]
        losses_only = [p for p in all_profits if p < 0]
        avg_win      = round(sum(wins_only)   / len(wins_only),   2) if wins_only   else 0
        avg_loss     = round(sum(losses_only) / len(losses_only), 2) if losses_only else 0
        largest_win  = round(max(wins_only),   2) if wins_only   else 0
        largest_loss = round(min(losses_only), 2) if losses_only else 0

        df_t = pd.DataFrame(trade_logs)
        actv = df_t[df_t['Outcome'].isin(['WIN', 'LOSS'])]
        hour_counts = actv.groupby('Hour_Damascus').size()
        day_counts  = actv.groupby('Weekday').size()

        def bar_chart(dd, width=18):
            if not dd: return "(لا بيانات)"
            mx = max(dd.values())
            return "\n".join(
                f"  {str(k):>4} |{'█' * int(v/mx*width):<{width}}| {v}"
                for k, v in sorted(dd.items())
            )

        report = (
            f"📊 <b>Advanced Strategy Report — {days} يوم</b>\n"
            f"📌 Strategy: {bot_state['strategy']} | Filter: {bot_state['filter_mode']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>💰 الأرباح</b>\n"
            f"  صافي الربح:      ${round(total_prof,2)}\n"
            f"  إجمالي الربح:    +${round(total_win,2)}\n"
            f"  إجمالي الخسارة:  -${abs(round(total_loss,2))}\n"
            f"  Profit Factor:   {profit_factor}\n"
            f"  Expected Payoff: ${expected_payoff}\n"
            f"  Recovery Factor: {recovery_factor}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📉 Drawdown</b>\n"
            f"  أقصى DD: ${round(max_dd,2)} ({dd_pct}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📈 الصفقات</b>\n"
            f"  الإجمالي: {total_trades} | فوز: {win_count} ({win_rate}%) | خسارة: {loss_count}\n"
            f"  Long  W/L: {long_win}/{long_loss} | Short W/L: {short_win}/{short_loss}\n"
            f"  بريك إيفن: {be_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🔢 إحصاءات</b>\n"
            f"  أكبر ربح: +${largest_win} | أكبر خسارة: ${largest_loss}\n"
            f"  متوسط ربح: +${avg_win} | متوسط خسارة: ${avg_loss}\n"
            f"  أكبر سلسلة فوز:   {max_consec_win} (+${round(max_consec_win_usd,2)})\n"
            f"  أكبر سلسلة خسارة: {max_consec_loss} (-${abs(round(max_consec_loss_usd,2))})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🕐 بالساعة (دمشق):</b>\n<pre>{bar_chart(hour_counts.to_dict())}</pre>\n"
            f"<b>📅 بالأيام:</b>\n<pre>{bar_chart(day_counts.to_dict())}</pre>"
        )
        await send_tg_msg(report)

        from openpyxl.styles import PatternFill, Font
        xlsx_adv = f"ADV_{datetime.now().strftime('%H%M%S')}.xlsx"
        df_exec  = df_t.drop(columns=['Hour_Damascus', 'Weekday'], errors='ignore')
        stats = {
            'المقياس': ['صافي الربح','إجمالي الربح','إجمالي الخسارة','Profit Factor',
                        'Expected Payoff','Recovery Factor','أقصى DD','DD%',
                        'إجمالي الصفقات','فوز','خسارة','نسبة الفوز','بريك إيفن',
                        'Long W/L','Short W/L','أكبر ربح','أكبر خسارة',
                        'متوسط ربح','متوسط خسارة','أكبر سلسلة فوز','أكبر سلسلة خسارة',
                        'الاستراتيجية','الفلتر'],
            'القيمة': [f'${round(total_prof,2)}',f'+${round(total_win,2)}',
                       f'-${abs(round(total_loss,2))}',profit_factor,expected_payoff,
                       recovery_factor,f'${round(max_dd,2)}',f'{dd_pct}%',
                       total_trades,win_count,loss_count,f'{win_rate}%',be_count,
                       f'{long_win}/{long_loss}',f'{short_win}/{short_loss}',
                       f'+${largest_win}',f'${largest_loss}',f'+${avg_win}',f'${avg_loss}',
                       f'{max_consec_win}(+${round(max_consec_win_usd,2)})',
                       f'{max_consec_loss}(-${abs(round(max_consec_loss_usd,2))})',
                       bot_state['strategy'],bot_state['filter_mode']]
        }
        with pd.ExcelWriter(xlsx_adv, engine='openpyxl') as writer:
            df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
            pd.DataFrame(stats).to_excel(writer, sheet_name='الإحصاءات', index=False)
            if blocked_logs:
                pd.DataFrame(blocked_logs).to_excel(writer, sheet_name='المرفوضة', index=False)
            ws = writer.sheets['الصفقات']
            gf = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
            rf = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
            hf = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')
            for cell in ws[1]: cell.fill = hf; cell.font = Font(color='FFFFFF', bold=True)
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
# LIVE POSITION MONITOR (Break-Even)
# =============================================================

async def position_monitor():
    while True:
        try:
            if bot_state['live_connected'] and bot_state['use_be'] and bot_state['connection_obj']:
                positions = await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] != bot_state['symbol']: continue
                    op, tp, sl, cp = p['openPrice'], p.get('takeProfit'), p.get('stopLoss'), p['currentPrice']
                    if tp and sl != op and abs(cp - op) >= (20 * bot_state['pip_value']):
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
                    c = await bot_state['account_obj'].get_historical_candles(
                        bot_state['symbol'], tf, limit=200)
                except:
                    await asyncio.sleep(15); continue

                df   = calculate_indicators(pd.DataFrame(c))
                curr = df.iloc[-2]
                prev = df.iloc[-3]
                now_utc = datetime.now(timezone.utc)

                # فلاتر الوقت
                danger_now = bot_state['use_danger_filter'] and is_danger_time(now_utc)
                time_block = bot_state['use_time_filter'] and not (8 <= now_utc.hour <= 17)

                if time_block or danger_now:
                    bot_state['market_data'][tf] = f"⏸ خمول | {c[-1]['close']}"
                else:
                    bot_state['market_data'][tf] = f"{c[-1]['close']} | K:{curr['K']:.1f} D:{curr['D']:.1f}"

                    if bot_state['last_signal_time'][tf] != curr['time']:
                        trend_buy_ok, trend_sell_ok = compute_trend_ok_live(df, curr)
                        raw_buy, raw_sell, b_label, s_label = get_stoch_signals(
                            prev['K'], prev['D'], curr['K'], curr['D'])

                        buy_sig  = raw_buy  and trend_buy_ok
                        sell_sig = raw_sell and trend_sell_ok

                        if raw_buy  and not buy_sig:
                            c_log(f"🛑 [{tf}] BUY {b_label} مرفوض (فلتر: {bot_state['filter_mode']})")
                        if raw_sell and not sell_sig:
                            c_log(f"🛑 [{tf}] SELL {s_label} مرفوض (فلتر: {bot_state['filter_mode']})")

                        skip = False
                        if bot_state['use_max_spread']:
                            try:
                                tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                                if ((tick['ask'] - tick['bid']) / bot_state['pip_value']) > bot_state['max_spread_pips']:
                                    skip = True
                            except: pass

                        if not skip and (buy_sig or sell_sig):
                            bot_state['last_signal_time'][tf] = curr['time']
                            p = c[-1]['close']
                            m = 1 if buy_sig else -1
                            t_str   = "شراء 🟢 BUY" if buy_sig else "بيع 🔴 SELL"
                            lbl_str = b_label if buy_sig else s_label

                            tp_dist = (curr['atr'] * bot_state['atr_mult_tp']) if bot_state['use_atr'] else (bot_state['tp_pips'][tf] * bot_state['pip_value'])
                            sl_dist = (curr['atr'] * bot_state['atr_mult_sl']) if bot_state['use_atr'] else (bot_state['sl_pips'][tf] * bot_state['pip_value'])
                            tp = round(p + (m * tp_dist), 2)
                            sl = round(p - (m * sl_dist), 2)

                            c_log(f"🎯 [{tf}] {t_str} ({lbl_str}) جاري التنفيذ...")
                            try:
                                if buy_sig:
                                    await bot_state['connection_obj'].create_market_buy_order(
                                        bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                                else:
                                    await bot_state['connection_obj'].create_market_sell_order(
                                        bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                                await send_tg_msg(
                                    f"🚨 <b>تم فتح صفقة!</b>\n"
                                    f"النوع: {t_str} ({lbl_str})\n"
                                    f"الفريم: {tf} | الفلتر: {bot_state['filter_mode']}\n"
                                    f"السعر: {p} | TP: {tp} | SL: {sl}"
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
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']

        if msg == '/start':
            await send_tg_msg("🤖 <b>مرحباً بك في لوحة التحكم!</b>", get_main_keyboard())

        elif msg == '/debug':
            if not bot_state['live_connected']:
                await send_tg_msg("⚠️ البوت غير متصل باللايف.")
            else:
                try:
                    tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                    c    = await bot_state['account_obj'].get_historical_candles(
                        bot_state['symbol'], '5m', limit=200)
                    df   = calculate_indicators(pd.DataFrame(c))
                    curr = df.iloc[-2]
                    prev = df.iloc[-3]
                    t_buy, t_sell = compute_trend_ok_live(df, curr)
                    rb, rs, bl, sl_l = get_stoch_signals(prev['K'], prev['D'], curr['K'], curr['D'])
                    await send_tg_msg(
                        f"✅ <b>حالة النظام:</b>\n"
                        f"الزوج: {bot_state['symbol']} | السبريد: {round((tick['ask']-tick['bid'])/bot_state['pip_value'],1)}\n"
                        f"الاستراتيجية: {bot_state['strategy']} | الفلتر: {bot_state['filter_mode']}\n"
                        f"K:{curr['K']:.1f} D:{curr['D']:.1f}\n"
                        f"ema15:{curr['ema15']:.2f} | ema50:{curr['ema50']:.2f} | ema150:{curr['ema150']:.2f}\n"
                        f"Trend Buy: {'✅' if t_buy else '❌'} | Trend Sell: {'✅' if t_sell else '❌'}\n"
                        f"Raw BUY: {'✅ '+bl if rb else '❌'} | Raw SELL: {'✅ '+sl_l if rs else '❌'}"
                    )
                except Exception as e:
                    await send_tg_msg(f"❌ خطأ: {e}")

        elif msg.startswith('/set'):
            p = msg.split()
            if len(p) == 4:
                bot_state[p[2] + '_pips'][p[1]] = int(p[3])
                await send_tg_msg(f"✅ تم تحديث {p[2]} لفريم {p[1]} إلى {p[3]}")

        elif msg.startswith('/backtest'):
            try:
                date_str = msg.split()[1]
                st = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                asyncio.create_task(run_oanda_backtest(st))
                await send_tg_msg(f"✅ باك تيست من: {date_str}")
            except:
                await send_tg_msg("⚠️ استخدم: /backtest YYYY-MM-DD")

    elif 'callback_query' in update:
        q = update['callback_query']
        d, chat_id, msg_id = q['data'], q['message']['chat']['id'], q['message']['message_id']
        bot_state['chat_id'] = chat_id

        # --- اتصال ---
        if d == "toggle_live_conn":
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

        elif d == "menu_main":
            await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())

        elif d == "toggle_status":
            bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'
            await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())

        elif d == "toggle_strat":
            bot_state['strategy'] = 'OLD' if bot_state['strategy'] == 'NEW' else 'NEW'
            strat_desc = "K يتجاوز مستوى ثابت" if bot_state['strategy'] == 'NEW' else "K يتقاطع مع D في المنطقة"
            await edit_tg_msg(chat_id, msg_id,
                f"🏠 القائمة الرئيسية:\n📌 الاستراتيجية الآن: {bot_state['strategy']} ({strat_desc})",
                get_main_keyboard())

        # --- فلاتر (حصرية) ---
        elif d == "menu_filters":
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        elif d == "set_filter_full":
            bot_state['filter_mode'] = 'FULL'
            await edit_tg_msg(chat_id, msg_id,
                "✅ <b>FULL</b> مُفعّل: توازي ema15 + ema50 + ema150 مطلوب",
                get_filters_keyboard())

        elif d == "set_filter_simple":
            bot_state['filter_mode'] = 'SIMPLE'
            await edit_tg_msg(chat_id, msg_id,
                "✅ <b>SIMPLE</b> مُفعّل: توازي ema50 + ema150 فقط (ema15 مُتجاهَل)",
                get_filters_keyboard())

        elif d == "set_filter_noma":
            bot_state['filter_mode'] = 'NO_MA'
            await edit_tg_msg(chat_id, msg_id,
                "✅ <b>NO MA</b> مُفعّل: بلا موفينجات، ستوكاستيك فقط",
                get_filters_keyboard())

        elif d == "toggle_stoch_deep":
            bot_state['use_stoch_deep'] = not bot_state['use_stoch_deep']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        elif d == "toggle_stoch_mid":
            bot_state['use_stoch_mid'] = not bot_state['use_stoch_mid']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        elif d == "toggle_stoch_shal":
            bot_state['use_stoch_shal'] = not bot_state['use_stoch_shal']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        elif d == "toggle_time":
            bot_state['use_time_filter'] = not bot_state['use_time_filter']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        elif d == "toggle_danger":
            bot_state['use_danger_filter'] = not bot_state['use_danger_filter']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        elif d == "toggle_f_cons":
            bot_state['use_f_cons'] = not bot_state['use_f_cons']
            await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وإعدادات التداول:</b>", get_filters_keyboard())

        # --- فريمات ---
        elif d == "menu_tfs":
            await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())

        elif d.startswith("toggle_tf_"):
            tf = d.split("_")[2]
            bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())

        # --- إعدادات ---
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
            bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2)
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "dec_lot":
            bot_state['lot_size'] = max(0.01, round(bot_state['lot_size'] - 0.01, 2))
            await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())

        elif d == "view_tpsl":
            txt = "📖 <b>أهداف الفريمات:</b>\n" + "\n".join(
                [f"[{tf}] TP:{bot_state['tp_pips'][tf]} | SL:{bot_state['sl_pips'][tf]}"
                 for tf in bot_state['timeframes']])
            await edit_tg_msg(chat_id, msg_id, txt, get_settings_keyboard())

        # --- تقارير ---
        elif d == "report":
            txt = "📊 <b>حالة السوق الحية:</b>\n" + "\n".join(
                [f"[{tf}] {bot_state['market_data'][tf]}"
                 for tf in bot_state['timeframes'] if bot_state['active_tfs'][tf]])
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

        # --- باك تيست ---
        elif d == "menu_backtest":
            kb = {"inline_keyboard": [
                [{"text": "📊 1 يوم",  "callback_data": "bto_1"},
                 {"text": "📊 3 أيام", "callback_data": "bto_3"},
                 {"text": "📊 7 أيام", "callback_data": "bto_7"}],
                [{"text": "🔬 Advanced Report (MT5 Style) — 7 أيام", "callback_data": "bto_adv_7"}],
                [{"text": "🔬 Advanced Report — 14 يوم", "callback_data": "bto_adv_14"}],
                [{"text": "🔙 رجوع", "callback_data": "menu_main"}]
            ]}
            await edit_tg_msg(chat_id, msg_id, "اختر المدة أو أرسل /backtest YYYY-MM-DD:", kb)

        elif d.startswith("bto_adv_"):
            adv_days = int(d.split('_')[2])
            asyncio.create_task(run_advanced_backtest(days=adv_days))

        elif d.startswith("bto_"):
            days = int(d.split('_')[1])
            asyncio.create_task(
                run_oanda_backtest(datetime.now(timezone.utc) - timedelta(days=days)))

        # --- إغلاق الصفقات ---
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

        elif d == "menu_stoch_settings":
            await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "inc_stoch_k": bot_state['stoch_k'] = min(bot_state['stoch_k']+1, 50); await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "dec_stoch_k": bot_state['stoch_k'] = max(bot_state['stoch_k']-1, 1);  await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "inc_stoch_s": bot_state['stoch_smooth'] = min(bot_state['stoch_smooth']+1, 50); await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "dec_stoch_s": bot_state['stoch_smooth'] = max(bot_state['stoch_smooth']-1, 1);  await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "inc_stoch_d": bot_state['stoch_d'] = min(bot_state['stoch_d']+1, 50); await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "dec_stoch_d": bot_state['stoch_d'] = max(bot_state['stoch_d']-1, 1);  await edit_tg_msg(chat_id, msg_id, "⚙️ <b>إعدادات الستوكاستيك:</b>", get_stoch_settings_keyboard())
        elif d == "reset_stoch":  bot_state['stoch_k'] = 5; bot_state['stoch_d'] = 5; bot_state['stoch_smooth'] = 5; await edit_tg_msg(chat_id, msg_id, "✅ تم إعادة الضبط لـ 5,5,5", get_stoch_settings_keyboard())
        elif d == "preset_14_3_3": bot_state['stoch_k'] = 14; bot_state['stoch_d'] = 3; bot_state['stoch_smooth'] = 3; await edit_tg_msg(chat_id, msg_id, "✅ تم ضبط 14,3,3", get_stoch_settings_keyboard())
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
    return web.Response(text="Gold Scalper Bot is ALIVE!")


async def main():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    tasks = [asyncio.create_task(timeframe_scanner(tf)) for tf in bot_state['timeframes']]
    tasks.append(asyncio.create_task(telegram_polling_loop()))
    tasks.append(asyncio.create_task(position_monitor()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
