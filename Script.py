import asyncio
import aiohttp
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, timezone
from metaapi_cloud_sdk import MetaApi
from aiohttp import web

# --- METAAPI & OANDA CONFIGURATION ---
METAAPI_TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiJjM2M1MWFlYjY3N2MwNzlkMmUzOTA3YjAzYmYzNzc4YiIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVzdC1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcnBjLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJtZXRhc3RhdHMtYXBpIiwibWV0aG9kcyI6WyJtZXRhc3RhdHMtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6InJpc2stbWFuYWdlbWVudC1hcGkiLCJtZXRob2RzIjpbInJpc2stbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoiY29weWZhY3RvcnktYXBpIiwibWV0aG9kcyI6WyJjb3B5ZmFjdG9yeS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoibXQtbWFuYWdlci1hcGkiLCJtZXRob2RzIjpbIm10LW1hbmFnZXItYXBpOnJlc3Q6ZGVhbGluZzoqOioiLCJtdC1tYW5hZ2VyLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJiaWxsaW5nLWFwaSIsIm1ldGhvZHMiOlsiYmlsbGluZy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiYzNjNTFhZWI2NzdjMDc5ZDJlMzkwN2IwM2JmMzc3OGIiLCJpYXQiOjE3Nzg3NDY0MzgsImV4cCI6MTc4NjUyMjQzOH0.NRMo-BO9ezZBEb4XmCQzkMsRN1iAz1rVSk7XWFP-ZGS_AZEyxSfIjnJ5w-r4egazV7tnxNLjjMuAdUb25T3ur3XWKCL4Jo9LFPy9tZzhIMRtlhq8d6YAHK9uxJclqJv5BZQFDeMeiFtyalLNjaE100Lp2zEnGWwlloxF-dpCw5DXvVKeGfMyVx4L2kisshcysDo7OeMkDBU1UB7leHi2eviEl7XQCpmhxdzT4BwMkf8YERx2jouKVu8-koVy00aon0drktGBSlQDOFw2WV0hg-VUfeCBR_Hgw2czqKVJ_lj_ZN3EsjWirirpiuXWbtwdD-VPokjKtX1z3ugcSTS1nd2iFIzauUHdOfb7Jl0R6cm8FosVS-4Iu046DiMsrxiAJ4PBywOXQhsFzZiePqmil1w5HHCxrw_78HNR9XcjBETMpHx9W48llIeUOkBVbsKfBP5iYtGSjS52i0QgpvHkfKrtXfbkMT0_9yJFG2kfZJHwJ5BJzWT4aKXto3l6iGe45xe4ZJhYhZX_RkC6dxR2w84M-uY-wlqiv_sxjHNOguSyOx4lfaeoq5H-LuJiWpHAYxEJUQWoQAQ7PObZOXCDWLRc_vP2gcbv1qYxTjD54FHnqhyf-oTGzAkWG5CVQFKpp9jTHQ3pXEYTSgIUTfHDbtoesAY1HG3nHcHbwujnqo0'
ACCOUNT_ID = '7d54fa6f-eaf7-4637-92a1-e0356ee729f8'
TG_TOKEN = '8647261254:AAH7AyzhBYvc9QjGmzgFW7NBb0a_SOAYCjc'
OANDA_ID = '101-004-28533521-003'
OANDA_API = 'c0f5b5df69c77e8bf35dcfd2fbde72da-a4c6cbadba7ae39d21143f65e2c2b8ba'
OANDA_URL = 'https://api-fxpractice.oanda.com/v3'

def c_log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# --- GLOBAL STATE ---
bot_state = {
    'status': 'RUNNING', 'symbol': 'XAUUSD@', 'live_connected': False, 
    'timeframes': ['1m', '2m', '3m', '5m', '15m'], 
    'active_tfs': {'1m': False, '2m': False, '3m': False, '5m': True, '15m': False},
    'lot_size': 0.05, 'pip_value': 0.1, 'spread_pips': 2.2, 'chat_id': None, 'last_update_id': 0,
    'tp_pips': {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80}, 
    'sl_pips': {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},
    
    # القناصات الرباعية المستقلة
    'levels': {
        'b10': True, 'b15': True, 'b20': True, 'b25': True,
        's90': True, 's85': True, 's80': True, 's75': True
    },
    
    # الفلاتر الاستراتيجية
    'use_trend_filter': True,   # فلتر המوفينجات الأساسي
    'use_smart_filter': False,  # تجاوز המوفينجات للإشارات العميقة جداً
    
    'use_time_filter': False, 'use_f_cons': False, 'cons_count': 3, 
    'use_be': False, 'use_atr': False, 'use_max_spread': True,    
    'max_spread_pips': 3.0, 'atr_mult_tp': 1.5, 'atr_mult_sl': 3.0,
    'use_s5_ticks': False, 'tp_tolerance_pips': 5.0,  
    'market_data': {tf: "⏸ بانتظار الاتصال (Offline)" for tf in ['1m', '2m', '3m', '5m', '15m']},
    'last_signal_time': {tf: None for tf in ['1m', '2m', '3m', '5m', '15m']},
    'connection_obj': None, 'account_obj': None, 'is_backtesting': False
}

def get_htf(tf): 
    if tf in ['1m', '2m']: return '5m'
    if tf in ['3m', '5m']: return '15m'
    return '1h'

async def fetch_oanda_candles(instrument, granularity, count=5000, end_time=None):
    tf_map = {'s5': 'S5', '1m': 'M1', '2m': 'M2', '3m': 'M3', '5m': 'M5', '15m': 'M15', '1h': 'H1'}
    url = f"{OANDA_URL}/instruments/{instrument}/candles"
    headers = {"Authorization": f"Bearer {OANDA_API}"}
    params = {"granularity": tf_map.get(granularity, 'M5'), "count": count, "price": "M"}
    if end_time: params["to"] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = []
                    for c in data.get('candles', []):
                        if c['complete']:
                            candles.append({
                                'time': pd.to_datetime(c['time']),
                                'open': float(c['mid']['o']), 'high': float(c['mid']['h']),
                                'low': float(c['mid']['l']), 'close': float(c['mid']['c'])
                            })
                    return candles
        except Exception as e: c_log(f"❌ خطأ Oanda: {e}")
        return []

async def send_tg_msg(text, reply_markup=None):
    if not bot_state['chat_id']: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {'chat_id': bot_state['chat_id'], 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as session:
        try: await session.post(url, json=payload)
        except: pass

async def edit_tg_msg(chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/editMessageText"
    payload = {'chat_id': chat_id, 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
    if reply_markup: payload['reply_markup'] = reply_markup
    async with aiohttp.ClientSession() as session:
        try: await session.post(url, json=payload)
        except: pass

async def answer_callback(callback_query_id, text=None):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery"
    payload = {'callback_query_id': callback_query_id}
    if text: payload['text'] = text
    async with aiohttp.ClientSession() as session:
        try: await session.post(url, json=payload)
        except: pass

async def send_tg_document(file_path, caption):
    if not bot_state['chat_id']: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
    async with aiohttp.ClientSession() as session:
        try:
            with open(file_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('chat_id', str(bot_state['chat_id']))
                data.add_field('document', f)
                data.add_field('caption', caption)
                await session.post(url, data=data)
        except Exception as e: c_log(f"❌ خطأ إرسال الملف: {e}")

def calculate_indicators(df):
    if df.empty: return df
    df['ema15'] = df['close'].ewm(span=15, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema150'] = df['close'].ewm(span=150, adjust=False).mean()
    
    low_min = df['low'].rolling(window=5).min()
    high_max = df['high'].rolling(window=5).max()
    denom = (high_max - low_min).replace(0, 1e-10)
    df['k_raw'] = 100 * ((df['close'] - low_min) / denom)
    df['K'] = df['k_raw'].ewm(span=5, adjust=False).mean()
    df['D'] = df['K'].ewm(span=5, adjust=False).mean()
    
    df['tr0'] = abs(df['high'] - df['low'])
    df['tr1'] = abs(df['high'] - df['close'].shift())
    df['tr2'] = abs(df['low'] - df['close'].shift())
    df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
    df['atr'] = df['tr'].rolling(14).mean().bfill()
    return df

def check_stoch_signal(prev_k, curr_k, side='buy'):
    levels = {'buy': [10, 15, 20, 25], 'sell': [90, 85, 80, 75]}
    triggered = []
    for lvl in levels[side]:
        lvl_key = f"{'b' if side=='buy' else 's'}{lvl}"
        if bot_state['levels'][lvl_key]:
            if side == 'buy':
                if prev_k <= lvl and curr_k > lvl: triggered.append(lvl)
            else:
                if prev_k >= lvl and curr_k < lvl: triggered.append(lvl)
    return triggered

# --- UI KEYBOARDS ---
def get_main_keyboard():
    live_icon = "🟢 متصل" if bot_state['live_connected'] else "🔴 غير متصل"
    status_icon = "🟢 RUN" if bot_state['status'] == 'RUNNING' else "🔴 PAUSE"
    return {"inline_keyboard": [
        [{"text": f"🔌 سيرفر التداول الحي: {live_icon}", "callback_data": "toggle_live_conn"}],
        [{"text": f"Status: {status_icon}", "callback_data": "toggle_status"}],
        [{"text": "🎛 فلاتر الترند", "callback_data": "menu_filters"}, {"text": "🎯 قناصات الستوكاستيك", "callback_data": "menu_levels"}],
        [{"text": "⏱ فريمات", "callback_data": "menu_tfs"}, {"text": "📊 Live Report", "callback_data": "report"}],
        [{"text": "💳 Account", "callback_data": "account"}, {"text": "🛠 إعدادات المخاطرة", "callback_data": "menu_settings"}],
        [{"text": "🔬 BACKTEST", "callback_data": "menu_backtest"}, {"text": "🛑 إغلاق الكل", "callback_data": "close_all"}]
    ]}

def get_filters_keyboard():
    t_icon = "🟢" if bot_state['use_trend_filter'] else "🔴"
    s_icon = "🟢" if bot_state['use_smart_filter'] else "🔴"
    return {"inline_keyboard": [
        [{"text": f"1. فلتر الترند (50/150): {t_icon}", "callback_data": "toggle_trend"}],
        [{"text": f"2. الفلتر الذكي (تجاهل 10/90): {s_icon}", "callback_data": "toggle_smart"}],
        [{"text": f"3. ثبات الترند: {'🟢' if bot_state['use_f_cons'] else '🔴'}", "callback_data": "toggle_f_cons"}],
        [{"text": f"4. Time Filter (08-18): {'🟢' if bot_state['use_time_filter'] else '🔴'}", "callback_data": "toggle_time"}],
        [{"text": "🔙 القائمة الرئيسية", "callback_data": "menu_main"}]
    ]}

def get_levels_keyboard():
    kb = []
    # Buy levels
    r1 = [{"text": f"B-10: {'🟢' if bot_state['levels']['b10'] else '🔴'}", "callback_data": "toggle_b10"},
          {"text": f"B-15: {'🟢' if bot_state['levels']['b15'] else '🔴'}", "callback_data": "toggle_b15"}]
    r2 = [{"text": f"B-20: {'🟢' if bot_state['levels']['b20'] else '🔴'}", "callback_data": "toggle_b20"},
          {"text": f"B-25: {'🟢' if bot_state['levels']['b25'] else '🔴'}", "callback_data": "toggle_b25"}]
    # Sell levels
    r3 = [{"text": f"S-90: {'🟢' if bot_state['levels']['s90'] else '🔴'}", "callback_data": "toggle_s90"},
          {"text": f"S-85: {'🟢' if bot_state['levels']['s85'] else '🔴'}", "callback_data": "toggle_s85"}]
    r4 = [{"text": f"S-80: {'🟢' if bot_state['levels']['s80'] else '🔴'}", "callback_data": "toggle_s80"},
          {"text": f"S-75: {'🟢' if bot_state['levels']['s75'] else '🔴'}", "callback_data": "toggle_s75"}]
    
    kb.extend([r1, r2, r3, r4])
    kb.append([{"text": "🔙 القائمة الرئيسية", "callback_data": "menu_main"}])
    return {"inline_keyboard": kb}

def get_tf_keyboard():
    kb = []
    row = []
    for tf in bot_state['timeframes']:
        row.append({"text": f"{tf}: {'🟢' if bot_state['active_tfs'][tf] else '🔴'}", "callback_data": f"toggle_tf_{tf}"})
        if len(row) == 2: kb.append(row); row = []
    if row: kb.append(row)
    kb.append([{"text": "🔙 رجوع", "callback_data": "menu_main"}])
    return {"inline_keyboard": kb}

def get_settings_keyboard():
    be_i = "🟢" if bot_state['use_be'] else "🔴"
    atr_i = "🟢" if bot_state['use_atr'] else "🔴"
    spr_i = "🟢" if bot_state['use_max_spread'] else "🔴"
    return {"inline_keyboard": [
        [{"text": f"تأمين الدخول (BE 20p): {be_i}", "callback_data": "toggle_be"}],
        [{"text": f"أهداف ATR: {atr_i}", "callback_data": "toggle_atr"}],
        [{"text": f"حماية السبريد: {spr_i}", "callback_data": "toggle_spread"}],
        [{"text": f"LOT SIZE: {bot_state['lot_size']}", "callback_data": "noop"}],
        [{"text": "➕ Lot", "callback_data": "inc_lot"}, {"text": "➖ Lot", "callback_data": "dec_lot"}],
        [{"text": "📖 View TP/SL", "callback_data": "view_tpsl"}],
        [{"text": "🔙 رجوع", "callback_data": "menu_main"}]
    ]}

# --- 🚀 BACKTEST ENGINE 🚀 ---
async def run_oanda_backtest(start_dt):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة حالياً.")
        return
        
    bot_state['is_backtesting'] = True
    c_log("بدء عملية الباك تيست مع الشبكة الرباعية والمفتش...")
    csv_filename = f"BT_Inspector_{datetime.now().strftime('%H%M%S')}.csv"
    trade_logs = []
    total_prof, peak_equity, max_dd, be_count = 0.0, 0.0, 0.0, 0
    
    await send_tg_msg(f"⏳ <b>بدء الباك تيست</b>\nمن: {start_dt.strftime('%Y-%m-%d')}")
    
    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 150: continue
            df = calculate_indicators(pd.DataFrame(c_data).sort_values(by='time').reset_index(drop=True))
            
            safe_start_idx = max(3, bot_state['cons_count'])
            
            for i in df[df['time'] >= start_dt].index:
                if i < safe_start_idx: continue 
                
                curr, prev = df.loc[i], df.loc[i-1]
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                
                # EMA Trend Check
                b_ema, s_ema = True, True
                cons = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
                for j in range(cons):
                    c = df.loc[i-j]
                    if not (c['ema50'] > c['ema150']): b_ema = False
                    if not (c['ema150'] > c['ema50']): s_ema = False

                trend_buy_ok = b_ema if bot_state['use_trend_filter'] else True
                trend_sell_ok = s_ema if bot_state['use_trend_filter'] else True
                
                # Check Stoch Signals (Quad-Net)
                buy_triggers = check_stoch_signal(prev['K'], curr['K'], 'buy')
                sell_triggers = check_stoch_signal(prev['K'], curr['K'], 'sell')
                
                entered_this_candle = False
                
                # Process Buy
                if buy_triggers:
                    for lvl in buy_triggers:
                        is_deep = (lvl == 10)
                        can_enter = (not bot_state['use_trend_filter']) or (bot_state['use_smart_filter'] and is_deep) or b_ema
                        
                        if can_enter and not entered_this_candle:
                            entered_this_candle = True
                            act_ent = curr['close'] + (bot_state['spread_pips'] * bot_state['pip_value'])
                            tp_p = round(act_ent + (bot_state['tp_pips'][tf] * bot_state['pip_value']), 2)
                            sl_p = round(act_ent - (bot_state['sl_pips'][tf] * bot_state['pip_value']), 2)
                            eff_tp_p = tp_p - (bot_state['tp_tolerance_pips'] * bot_state['pip_value'])
                            
                            max_ext = min(curr['time'] + timedelta(hours=72), datetime.now(timezone.utc))
                            v_cands = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                            outcome, ext_t = "EXPIRED", max_ext
                            
                            for c in [v for v in v_cands if curr['time'] <= v['time'] <= max_ext]:
                                if c['low'] <= sl_p: outcome, ext_t = "LOSS", c['time']; break
                                if c['high'] >= eff_tp_p: outcome, ext_t = "WIN", c['time']; break
                            
                            p_usd = round(abs(act_ent - (tp_p if outcome=="WIN" else sl_p)) * 100 * bot_state['lot_size'], 2) * (1 if outcome=="WIN" else -1) if outcome in ["WIN", "LOSS"] else 0.0
                            total_prof += p_usd
                            peak_equity = max(peak_equity, total_prof)
                            max_dd = max(max_dd, peak_equity - total_prof)
                            
                            trade_logs.append({'Timeframe': tf, 'Type': f'BUY (Lvl {lvl})', 'Entry Time': curr['time'].strftime('%Y-%m-%d %H:%M'), 'Exit Time': ext_t.strftime('%Y-%m-%d %H:%M'), 'Entry Price': round(act_ent, 2), 'TP': tp_p, 'SL': sl_p, 'Outcome': outcome, 'Profit ($)': p_usd})
                        elif not can_enter:
                            trade_logs.append({'Timeframe': tf, 'Type': f'BUY BLOCKED (Lvl {lvl})', 'Entry Time': curr['time'].strftime('%Y-%m-%d %H:%M'), 'Exit Time': '---', 'Entry Price': curr['close'], 'TP': 0, 'SL': 0, 'Outcome': 'REJECTED (Trend)', 'Profit ($)': 0.0})
                
                # Process Sell
                if sell_triggers and not entered_this_candle:
                    for lvl in sell_triggers:
                        is_deep = (lvl == 90)
                        can_enter = (not bot_state['use_trend_filter']) or (bot_state['use_smart_filter'] and is_deep) or s_ema
                        
                        if can_enter and not entered_this_candle:
                            entered_this_candle = True
                            act_ent = curr['close'] - (bot_state['spread_pips'] * bot_state['pip_value'])
                            tp_p = round(act_ent - (bot_state['tp_pips'][tf] * bot_state['pip_value']), 2)
                            sl_p = round(act_ent + (bot_state['sl_pips'][tf] * bot_state['pip_value']), 2)
                            eff_tp_p = tp_p + (bot_state['tp_tolerance_pips'] * bot_state['pip_value'])
                            
                            max_ext = min(curr['time'] + timedelta(hours=72), datetime.now(timezone.utc))
                            v_cands = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_ext)
                            outcome, ext_t = "EXPIRED", max_ext
                            
                            for c in [v for v in v_cands if curr['time'] <= v['time'] <= max_ext]:
                                if c['high'] >= sl_p: outcome, ext_t = "LOSS", c['time']; break
                                if c['low'] <= eff_tp_p: outcome, ext_t = "WIN", c['time']; break
                            
                            p_usd = round(abs(act_ent - (tp_p if outcome=="WIN" else sl_p)) * 100 * bot_state['lot_size'], 2) * (1 if outcome=="WIN" else -1) if outcome in ["WIN", "LOSS"] else 0.0
                            total_prof += p_usd
                            peak_equity = max(peak_equity, total_prof)
                            max_dd = max(max_dd, peak_equity - total_prof)
                            
                            trade_logs.append({'Timeframe': tf, 'Type': f'SELL (Lvl {lvl})', 'Entry Time': curr['time'].strftime('%Y-%m-%d %H:%M'), 'Exit Time': ext_t.strftime('%Y-%m-%d %H:%M'), 'Entry Price': round(act_ent, 2), 'TP': tp_p, 'SL': sl_p, 'Outcome': outcome, 'Profit ($)': p_usd})
                        elif not can_enter:
                            trade_logs.append({'Timeframe': tf, 'Type': f'SELL BLOCKED (Lvl {lvl})', 'Entry Time': curr['time'].strftime('%Y-%m-%d %H:%M'), 'Exit Time': '---', 'Entry Price': curr['close'], 'TP': 0, 'SL': 0, 'Outcome': 'REJECTED (Trend)', 'Profit ($)': 0.0})

                # Near Miss Logic
                if not buy_triggers and not sell_triggers:
                    if (25 < prev['K'] <= 28) and (curr['K'] > prev['K']):
                        trade_logs.append({'Timeframe': tf, 'Type': 'NEAR MISS (BUY)', 'Entry Time': curr['time'].strftime('%Y-%m-%d %H:%M'), 'Exit Time': '---', 'Entry Price': curr['close'], 'TP': 0, 'SL': 0, 'Outcome': f"Bounced at {prev['K']:.1f}", 'Profit ($)': 0.0})
                    elif (72 <= prev['K'] < 75) and (curr['K'] < prev['K']):
                        trade_logs.append({'Timeframe': tf, 'Type': 'NEAR MISS (SELL)', 'Entry Time': curr['time'].strftime('%Y-%m-%d %H:%M'), 'Exit Time': '---', 'Entry Price': curr['close'], 'TP': 0, 'SL': 0, 'Outcome': f"Bounced at {prev['K']:.1f}", 'Profit ($)': 0.0})

        if trade_logs:
            df_logs = pd.DataFrame(trade_logs)
            pd.concat([df_logs, pd.DataFrame([{'Timeframe': '--- SUMMARY ---', 'Type': '', 'Entry Time': '', 'Exit Time': '', 'Entry Price': 'NET PROFIT:', 'TP': '', 'SL': '', 'Outcome': f'${round(total_prof, 2)}', 'Profit ($)': ''}])]).to_csv(csv_filename, index=False)
            await send_tg_document(csv_filename, f"📊 اكتمل الباك تيست.\nالربح الصافي: ${round(total_prof, 2)}")
            os.remove(csv_filename)
        else: 
            await send_tg_msg("⚠️ لم يتم العثور على أي صفقات.")
    except Exception as e: 
        c_log(f"❌ خطأ باك تيست: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally: 
        bot_state['is_backtesting'] = False

# --- LIVE TRADING SCANNERS ---
async def position_monitor():
    while True:
        try:
            if bot_state['live_connected'] and bot_state['use_be'] and bot_state['connection_obj']:
                positions = await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] == bot_state['symbol']:
                        op, tp, sl, cp = p['openPrice'], p.get('takeProfit'), p.get('stopLoss'), p['currentPrice']
                        if tp and sl != op:
                            if abs(cp - op) >= (20 * bot_state['pip_value']):
                                is_buy = tp > op
                                if (is_buy and cp > op) or (not is_buy and cp < op):
                                    await bot_state['connection_obj'].modify_position(p['id'], stop_loss=op)
                                    await send_tg_msg(f"🛡️ <b>تأمين (BE)</b> لـ {p['id']}")
        except: pass
        await asyncio.sleep(5)

async def timeframe_scanner(tf):
    c_log(f"✅ ماسح [{tf}] يعمل.")
    while True:
        try:
            if bot_state['status'] == 'RUNNING' and bot_state['active_tfs'][tf]:
                if not bot_state['live_connected'] or not bot_state['account_obj']:
                    bot_state['market_data'][tf] = "⏸ بانتظار الاتصال"
                    await asyncio.sleep(5); continue
                    
                try: c = await bot_state['account_obj'].get_historical_candles(bot_state['symbol'], tf, limit=100)
                except: await asyncio.sleep(15); continue

                df = calculate_indicators(pd.DataFrame(c))
                curr, prev = df.iloc[-2], df.iloc[-3]
                
                if bot_state['use_time_filter'] and not (8 <= datetime.now(timezone.utc).hour <= 17):
                    bot_state['market_data'][tf] = f"⏸ خمول | {c[-1]['close']}"
                else:
                    bot_state['market_data'][tf] = f"{c[-1]['close']} | K:{curr['K']:.1f}"
                    
                    if bot_state['last_signal_time'][tf] != curr['time']:
                        b_ema, s_ema = True, True
                        cons = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
                        for j in range(cons):
                            cc = df.iloc[(-2)-j]
                            if not (cc['ema50'] > cc['ema150']): b_ema = False
                            if not (cc['ema150'] > cc['ema50']): s_ema = False

                        buy_triggers = check_stoch_signal(prev['K'], curr['K'], 'buy')
                        sell_triggers = check_stoch_signal(prev['K'], curr['K'], 'sell')

                        buy_sig, sell_sig = False, False
                        exec_lvl = None

                        if buy_triggers:
                            for lvl in buy_triggers:
                                is_deep = (lvl == 10)
                                if (not bot_state['use_trend_filter']) or (bot_state['use_smart_filter'] and is_deep) or b_ema:
                                    buy_sig, exec_lvl = True, lvl; break
                                else: c_log(f"🛑 [{tf}] شراء Lvl {lvl} مرفوض (Trend)")

                        if sell_triggers and not buy_sig:
                            for lvl in sell_triggers:
                                is_deep = (lvl == 90)
                                if (not bot_state['use_trend_filter']) or (bot_state['use_smart_filter'] and is_deep) or s_ema:
                                    sell_sig, exec_lvl = True, lvl; break
                                else: c_log(f"🛑 [{tf}] بيع Lvl {lvl} مرفوض (Trend)")

                        skip = False
                        if bot_state['use_max_spread']:
                            try:
                                tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                                if ((tick['ask'] - tick['bid']) / bot_state['pip_value']) > bot_state['max_spread_pips']: skip = True
                            except: pass

                        if not skip and (buy_sig or sell_sig):
                            bot_state['last_signal_time'][tf] = curr['time']
                            p = c[-1]['close']
                            m = 1 if buy_sig else -1
                            t_str = "شراء 🟢" if buy_sig else "بيع 🔴"
                            
                            tp = round(p + (m * bot_state['tp_pips'][tf] * bot_state['pip_value']), 2)
                            sl = round(p - (m * bot_state['sl_pips'][tf] * bot_state['pip_value']), 2)
                            
                            try:
                                if buy_sig: await bot_state['connection_obj'].create_market_buy_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                                else: await bot_state['connection_obj'].create_market_sell_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                                await send_tg_msg(f"🚨 <b>تم فتح صفقة!</b>\nالنوع: {t_str} (Lvl {exec_lvl})\nالفريم: {tf}\nالسعر: {p}\nTP: {tp}\nSL: {sl}")
                            except Exception as e: await send_tg_msg(f"❌ <b>فشل تنفيذ!</b>\n{e}")
            await asyncio.sleep(10)
        except: await asyncio.sleep(15)

# --- TELEGRAM HANDLER ---
async def process_tg_update(update):
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']
        
        if msg == '/start': await send_tg_msg("🤖 <b>مرحباً بك في لوحة التحكم!</b>", get_main_keyboard())
        elif msg.startswith('/set'):
            p = msg.split()
            if len(p) == 4:
                bot_state[p[2]+'_pips'][p[1]] = int(p[3])
                await send_tg_msg("✅ تم التحديث")
        elif msg.startswith('/backtest'):
            try: 
                date_str = msg.split()[1]
                st = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                asyncio.create_task(run_oanda_backtest(st))
            except: await send_tg_msg("⚠️ استخدم: /backtest YYYY-MM-DD")

    elif 'callback_query' in update:
        q = update['callback_query']
        d, chat_id, msg_id = q['data'], q['message']['chat']['id'], q['message']['message_id']
        bot_state['chat_id'] = chat_id
        
        if d == "toggle_live_conn":
            if not bot_state['live_connected']:
                await edit_tg_msg(chat_id, msg_id, "⏳ جاري الاتصال...", get_main_keyboard())
                try:
                    api = MetaApi(METAAPI_TOKEN)
                    bot_state['account_obj'] = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                    bot_state['connection_obj'] = bot_state['account_obj'].get_rpc_connection()
                    await bot_state['connection_obj'].connect()
                    await bot_state['connection_obj'].wait_synchronized()
                    bot_state['live_connected'] = True
                    await edit_tg_msg(chat_id, msg_id, "✅ تم الاتصال!", get_main_keyboard())
                except: await edit_tg_msg(chat_id, msg_id, "❌ فشل الاتصال", get_main_keyboard())
            else:
                bot_state['live_connected'] = False
                bot_state['connection_obj'] = bot_state['account_obj'] = None
                await edit_tg_msg(chat_id, msg_id, "🔌 تم فصل الاتصال.", get_main_keyboard())
                
        elif d == "menu_main": await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())
        elif d == "toggle_status": bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'; await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())
        
        # Filters Menu
        elif d == "menu_filters": await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر الترند:</b>", get_filters_keyboard())
        elif d == "toggle_trend": bot_state['use_trend_filter'] = not bot_state['use_trend_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر الترند:</b>", get_filters_keyboard())
        elif d == "toggle_smart": bot_state['use_smart_filter'] = not bot_state['use_smart_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر الترند:</b>", get_filters_keyboard())
        elif d == "toggle_f_cons": bot_state['use_f_cons'] = not bot_state['use_f_cons']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر الترند:</b>", get_filters_keyboard())
        elif d == "toggle_time": bot_state['use_time_filter'] = not bot_state['use_time_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر الترند:</b>", get_filters_keyboard())
        
        # Levels Menu (Quad-Net)
        elif d == "menu_levels": await edit_tg_msg(chat_id, msg_id, "🎯 <b>قناصات الستوكاستيك:</b>", get_levels_keyboard())
        elif d.startswith("toggle_b") or d.startswith("toggle_s"):
            lvl_key = d.split("_")[1]
            bot_state['levels'][lvl_key] = not bot_state['levels'][lvl_key]
            await edit_tg_msg(chat_id, msg_id, "🎯 <b>قناصات الستوكاستيك:</b>", get_levels_keyboard())

        elif d == "menu_tfs": await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())
        elif d.startswith("toggle_tf_"):
            tf = d.split("_")[2]; bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())
            
        elif d == "menu_settings": await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_be": bot_state['use_be'] = not bot_state['use_be']; await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات:", get_settings_keyboard())
        elif d == "toggle_atr": bot_state['use_atr'] = not bot_state['use_atr']; await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات:", get_settings_keyboard())
        elif d == "toggle_spread": bot_state['use_max_spread'] = not bot_state['use_max_spread']; await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات:", get_settings_keyboard())
        elif d == "inc_lot": bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2); await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات:", get_settings_keyboard())
        elif d == "dec_lot": bot_state['lot_size'] = max(0.01, round(bot_state['lot_size'] - 0.01, 2)); await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات:", get_settings_keyboard())
        
        elif d == "view_tpsl":
            txt = "📖 <b>الأهداف:</b>\n" + "\n".join([f"[{tf}] TP:{bot_state['tp_pips'][tf]} | SL:{bot_state['sl_pips'][tf]}" for tf in bot_state['timeframes']])
            await edit_tg_msg(chat_id, msg_id, txt, get_settings_keyboard())
            
        elif d == "report":
            txt = "📊 <b>السوق الحي:</b>\n" + "\n".join([f"[{tf}] {bot_state['market_data'][tf]}" for tf in bot_state['timeframes'] if bot_state['active_tfs'][tf]])
            await edit_tg_msg(chat_id, msg_id, txt, get_main_keyboard())
            
        elif d == "account":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                try:
                    acc = await bot_state['connection_obj'].get_account_information()
                    await edit_tg_msg(chat_id, msg_id, f"💳 <b>الحساب:</b>\nرصيد: {acc['balance']}", get_main_keyboard())
                except: pass
                
        elif d == "menu_backtest":
            kb = {"inline_keyboard": [[{"text": "📊 1 يوم", "callback_data": "bto_1"}, {"text": "📊 3 أيام", "callback_data": "bto_3"}], [{"text": "🔙 رجوع", "callback_data": "menu_main"}]]}
            await edit_tg_msg(chat_id, msg_id, "اختر المدة:", kb)
        elif d.startswith("bto_"):
            days = int(d.split('_')[1])
            asyncio.create_task(run_oanda_backtest(datetime.now(timezone.utc) - timedelta(days=days)))
            
        elif d == "close_all":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                async def close_positions():
                    try:
                        pos = await bot_state['connection_obj'].get_positions()
                        for p in pos: await bot_state['connection_obj'].close_position(p['id'])
                        await send_tg_msg("✅ تم إغلاق الصفقات.")
                    except Exception as e: await send_tg_msg(f"❌ خطأ: {e}")
                asyncio.create_task(close_positions())
        await answer_callback(q['id'])

async def telegram_polling_loop():
    c_log("✅ خدمة التلغرام جاهزة للاستماع.")
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(url, params={'offset': bot_state['last_update_id']+1, 'timeout': 10}) as r:
                    if r.status == 200:
                        for u in (await r.json()).get('result', []):
                            bot_state['last_update_id'] = u['update_id']
                            asyncio.create_task(process_tg_update(u))
            except: await asyncio.sleep(2)

# --- WEB SERVER ---
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
