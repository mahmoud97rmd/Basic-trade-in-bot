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

# --- GLOBAL STATE ---
bot_state = {
    'status': 'RUNNING', 'strategy': 'NEW', 'symbol': 'XAUUSD@',
    'live_connected': False, 
    'timeframes': ['1m', '2m', '3m', '5m', '15m'], 
    'active_tfs': {'1m': False, '2m': False, '3m': False, '5m': True, '15m': False},
    'lot_size': 0.05, 'pip_value': 0.1, 'spread_pips': 2.2, 'chat_id': None, 'last_update_id': 0,
    'tp_pips': {'1m': 25, '2m': 30, '3m': 40, '5m': 70, '15m': 80}, 
    'sl_pips': {'1m': 100, '2m': 100, '3m': 100, '5m': 100, '15m': 150},
    
    # الفلاتر الجديدة
    'use_trend_filter': True,  # فلتر الموفينجات الأساسي
    'use_smart_filter': False, # الفلتر الذكي لتجاهل الموفينجات في الإشارات العميقة
    
    'use_time_filter': False, 'use_danger_filter': False,
    'use_stoch_deep': True, 'use_stoch_mid': True, 'use_stoch_shal': True,
    'use_f_cons': False, 'cons_count': 3, 
    'use_f_gap': False, 'gap_pips': 5.0, 'use_f_mtf': False,
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

# --- UI MENUS ---
def get_main_keyboard():
    live_icon = "🟢 متصل" if bot_state['live_connected'] else "🔴 غير متصل"
    status_icon = "🟢 RUN" if bot_state['status'] == 'RUNNING' else "🔴 PAUSE"
    return {"inline_keyboard": [
        [{"text": f"🔌 سيرفر التداول الحي: {live_icon}", "callback_data": "toggle_live_conn"}],
        [{"text": f"Status: {status_icon}", "callback_data": "toggle_status"}, {"text": f"Strategy: {bot_state['strategy']}", "callback_data": "toggle_strat"}],
        [{"text": "🎛 فلاتر وشروط التداول", "callback_data": "menu_filters"}, {"text": "⏱ فريمات", "callback_data": "menu_tfs"}],
        [{"text": "📊 Live Report", "callback_data": "report"}, {"text": "💳 Account", "callback_data": "account"}],
        [{"text": "🛠 إعدادات المخاطرة", "callback_data": "menu_settings"}, {"text": "🔬 BACKTEST", "callback_data": "menu_backtest"}],
        [{"text": "🛑 إغلاق جميع الصفقات", "callback_data": "close_all"}]
    ]}

def get_filters_keyboard():
    t_icon  = "🟢" if bot_state['use_trend_filter']  else "🔴"
    s_icon  = "🟢" if bot_state['use_smart_filter']  else "🔴"
    d_icon  = "🟢" if bot_state['use_danger_filter'] else "🔴"
    dp_icon = "🟢" if bot_state['use_stoch_deep']    else "🔴"
    md_icon = "🟢" if bot_state['use_stoch_mid']     else "🔴"
    sh_icon = "🟢" if bot_state['use_stoch_shal']    else "🔴"
    return {"inline_keyboard": [
        [{"text": f"1. فلتر الترند الأساسي (50/150): {t_icon}", "callback_data": "toggle_trend"}],
        [{"text": f"2. الفلتر الذكي (تجاهل الترند 10/90): {s_icon}", "callback_data": "toggle_smart"}],
        [{"text": f"3. ثبات الترند ({bot_state['cons_count']} شموع): {'🟢' if bot_state['use_f_cons'] else '🔴'}", "callback_data": "toggle_f_cons"}],
        [{"text": f"4. Time Filter (08-17 UTC): {'🟢' if bot_state['use_time_filter'] else '🔴'}", "callback_data": "toggle_time"}],
        [{"text": f"5. 🚫 حظر 19:00-21:30 دمشق: {d_icon}", "callback_data": "toggle_danger"}],
        [{"text": "━━━ مستويات الستوكاستيك ━━━", "callback_data": "noop"}],
        [{"text": f"DEEP 10/90: {dp_icon}", "callback_data": "toggle_stoch_deep"},
         {"text": f"MID  15/85: {md_icon}", "callback_data": "toggle_stoch_mid"},
         {"text": f"SHAL 20/80: {sh_icon}", "callback_data": "toggle_stoch_shal"}],
        [{"text": "🔙 القائمة الرئيسية", "callback_data": "menu_main"}]
    ]}

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
        [{"text": "📖 عرض أهداف הפريمات", "callback_data": "view_tpsl"}],
        [{"text": "🔙 رجوع", "callback_data": "menu_main"}]
    ]}

# --- 🚀 ENGINE: SIGNAL & BACKTEST 🚀 ---

async def run_advanced_backtest(days=7):
    """تقرير متقدم على غرار MT5 Strategy Tester"""
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة.")
        return
    bot_state['is_backtesting'] = True
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    await send_tg_msg(f"⏳ <b>Advanced Backtest</b>\nمن: {start_dt.strftime('%Y-%m-%d')} ({days} أيام)\n⏳ قد يستغرق بضع دقائق...")

    trade_logs, blocked_logs = [], []
    total_prof, peak_equity, max_dd = 0.0, 0.0, 0.0
    total_win, total_loss, win_count, loss_count, be_count = 0.0, 0.0, 0, 0, 0
    long_win, long_loss, short_win, short_loss = 0, 0, 0, 0
    all_profits, consec_win, consec_loss = [], 0, 0
    max_consec_win, max_consec_loss = 0, 0
    max_consec_win_usd, max_consec_loss_usd = 0.0, 0.0
    cur_streak_usd = 0.0

    try:
        for tf in bot_state['timeframes']:
            if not bot_state['active_tfs'][tf]: continue
            c_data = await fetch_oanda_candles('XAU_USD', tf, 5000)
            if len(c_data) < 150: continue
            df = calculate_indicators(pd.DataFrame(c_data).sort_values('time').reset_index(drop=True))
            safe_start = max(3, bot_state['cons_count'])

            for i in df[df['time'] >= start_dt].index:
                if i < safe_start: continue
                curr, prev = df.loc[i], df.loc[i-1]
                if bot_state['use_time_filter'] and not (8 <= curr['time'].hour <= 17): continue
                if bot_state['use_danger_filter']:
                    _dh = (curr['time'].hour + 3) % 24
                    _dm = curr['time'].minute
                    if (_dh == 16) or (_dh == 17) or (_dh == 18 and _dm <= 30): continue

                b_ema, s_ema = True, True
                cons = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
                for j in range(cons):
                    cc = df.loc[i-j]
                    if not (cc['ema50'] > cc['ema150']): b_ema = False
                    if not (cc['ema150'] > cc['ema50']): s_ema = False
                ma_parallel_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
                ma_parallel_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
                trend_buy  = (b_ema and ma_parallel_buy)  if bot_state['use_trend_filter'] else True
                trend_sell = (s_ema and ma_parallel_sell) if bot_state['use_trend_filter'] else True

                buy_deep_stoch  = (prev['K'] <= 10) and (curr['K'] > 10)  and bot_state['use_stoch_deep']
                buy_mid_stoch   = (10 < prev['K'] <= 15) and (curr['K'] > 15) and bot_state['use_stoch_mid']
                buy_shal_stoch  = (15 < prev['K'] <= 20) and (curr['K'] > 20) and bot_state['use_stoch_shal']
                sell_deep_stoch = (prev['K'] >= 90) and (curr['K'] < 90)  and bot_state['use_stoch_deep']
                sell_mid_stoch  = (85 <= prev['K'] < 90) and (curr['K'] < 85) and bot_state['use_stoch_mid']
                sell_shal_stoch = (80 <= prev['K'] < 85) and (curr['K'] < 80) and bot_state['use_stoch_shal']

                stoch_buy_any  = buy_deep_stoch  or buy_mid_stoch  or buy_shal_stoch
                stoch_sell_any = sell_deep_stoch or sell_mid_stoch or sell_shal_stoch

                buy_deep_v  = buy_deep_stoch  and (True if bot_state['use_smart_filter'] else trend_buy)
                buy_mid_v   = buy_mid_stoch   and trend_buy
                buy_shal_v  = buy_shal_stoch  and trend_buy
                sell_deep_v = sell_deep_stoch and (True if bot_state['use_smart_filter'] else trend_sell)
                sell_mid_v  = sell_mid_stoch  and trend_sell
                sell_shal_v = sell_shal_stoch and trend_sell
                buy_sig  = buy_deep_v  or buy_mid_v  or buy_shal_v
                sell_sig = sell_deep_v or sell_mid_v or sell_shal_v

                if not (buy_sig or sell_sig):
                    if stoch_buy_any or stoch_sell_any:
                        blocked_logs.append({
                            'Timeframe': tf, 'Type': 'BUY (BLOCKED)' if stoch_buy_any else 'SELL (BLOCKED)',
                            'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                            'Entry Price': curr['close'], 'Reason': 'REJECTED (Trend blocked)'
                        })
                    continue

                if i + 1 >= len(df): continue
                next_c = df.loc[i + 1]
                entry_p = next_c['open']
                entry_t = next_c['time']
                m = 1 if buy_sig else -1
                act_ent = entry_p + (m * bot_state['spread_pips'] * bot_state['pip_value'])
                tp_dist = bot_state['tp_pips'][tf] * bot_state['pip_value']
                sl_dist = bot_state['sl_pips'][tf] * bot_state['pip_value']
                if bot_state['use_atr']:
                    tp_dist = curr['atr'] * bot_state['atr_mult_tp']
                    sl_dist = curr['atr'] * bot_state['atr_mult_sl']
                tp_p = round(act_ent + (m * tp_dist), 2)
                sl_p = round(act_ent - (m * sl_dist), 2)
                tol_val = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                eff_tp_p = (tp_p - tol_val) if buy_sig else (tp_p + tol_val)
                max_exit_time = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                outcome, exit_t = "EXPIRED", max_exit_time
                val_candles = await fetch_oanda_candles('XAU_USD', '1m', 4320, max_exit_time)
                be_activated = False
                be_target = act_ent + (m * 20 * bot_state['pip_value'])
                for vc in [v for v in val_candles if v['time'] >= entry_t]:
                    if buy_sig:
                        if bot_state['use_be'] and not be_activated and vc['high'] >= be_target:
                            sl_p = act_ent; be_activated = True
                        if vc['low'] <= sl_p: outcome, exit_t = ("BREAK-EVEN" if be_activated and sl_p == act_ent else "LOSS"), vc['time']; break
                        if vc['high'] >= eff_tp_p: outcome, exit_t = "WIN", vc['time']; break
                    else:
                        if bot_state['use_be'] and not be_activated and vc['low'] <= be_target:
                            sl_p = act_ent; be_activated = True
                        if vc['high'] >= sl_p: outcome, exit_t = ("BREAK-EVEN" if be_activated and sl_p == act_ent else "LOSS"), vc['time']; break
                        if vc['low'] <= eff_tp_p: outcome, exit_t = "WIN", vc['time']; break

                if outcome == "BREAK-EVEN": p_usd = 0.0; be_count += 1
                elif outcome in ["WIN","LOSS"]:
                    p_usd = round(abs(act_ent-(tp_p if outcome=="WIN" else sl_p))*100*bot_state['lot_size'],2)*(1 if outcome=="WIN" else -1)
                else: p_usd = 0.0

                # Streak tracking
                if outcome == "WIN":
                    total_win += p_usd; win_count += 1
                    consec_win += 1; cur_streak_usd += p_usd
                    if consec_win > max_consec_win: max_consec_win = consec_win; max_consec_win_usd = cur_streak_usd
                    consec_loss = 0; cur_streak_usd = 0.0 if outcome != "WIN" else cur_streak_usd
                    if buy_sig: long_win += 1
                    else: short_win += 1
                elif outcome == "LOSS":
                    total_loss += p_usd; loss_count += 1
                    consec_loss += 1; cur_streak_usd += p_usd
                    if consec_loss > max_consec_loss: max_consec_loss = consec_loss; max_consec_loss_usd = cur_streak_usd
                    consec_win = 0; cur_streak_usd = 0.0 if outcome != "LOSS" else cur_streak_usd
                    if buy_sig: long_loss += 1
                    else: short_loss += 1

                total_prof += p_usd
                peak_equity = max(peak_equity, total_prof)
                max_dd = max(max_dd, peak_equity - total_prof)
                all_profits.append(p_usd)

                b_type = "DEEP(10)" if buy_deep_v else "MID(15)" if buy_mid_v else "SHAL(20)"
                s_type = "DEEP(90)" if sell_deep_v else "MID(85)" if sell_mid_v else "SHAL(80)"
                _dh = (curr['time'].hour + 3) % 24
                trade_logs.append({
                    'Timeframe': tf,
                    'Type': f"BUY {b_type}" if buy_sig else f"SELL {s_type}",
                    'Entry Time': (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Exit Time':  (exit_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                    'Entry Price': round(act_ent, 2), 'TP': tp_p, 'SL': sl_p,
                    'Pips': round(abs(act_ent-(tp_p if outcome=="WIN" else sl_p))/bot_state['pip_value'],1) if outcome in ["WIN","LOSS"] else 0,
                    'Outcome': outcome, 'Profit ($)': p_usd,
                    'Hour_Damascus': _dh, 'Weekday': curr['time'].strftime('%a')
                })

        if not trade_logs:
            await send_tg_msg("⚠️ لم يتم العثور على صفقات."); return

        # ─── حساب الإحصائيات المتقدمة ───
        total_trades = win_count + loss_count
        win_rate = round(win_count/total_trades*100,1) if total_trades > 0 else 0
        dd_pct = round(max_dd/peak_equity*100,1) if peak_equity > 0 else 0
        profit_factor = round(total_win/abs(total_loss),2) if total_loss != 0 else 999
        expected_payoff = round(total_prof/total_trades,2) if total_trades > 0 else 0
        recovery_factor = round(total_prof/max_dd,2) if max_dd > 0 else 999
        wins_only = [p for p in all_profits if p > 0]
        losses_only = [p for p in all_profits if p < 0]
        avg_win  = round(sum(wins_only)/len(wins_only),2) if wins_only else 0
        avg_loss = round(sum(losses_only)/len(losses_only),2) if losses_only else 0
        largest_win  = round(max(wins_only),2) if wins_only else 0
        largest_loss = round(min(losses_only),2) if losses_only else 0

        # Entries by hour (Damascus)
        df_t = pd.DataFrame(trade_logs)
        hour_counts = df_t[df_t['Outcome'].isin(['WIN','LOSS'])].groupby('Hour_Damascus').size()
        hour_pnl    = df_t[df_t['Outcome'].isin(['WIN','LOSS'])].groupby('Hour_Damascus')['Profit ($)'].sum()
        day_counts  = df_t[df_t['Outcome'].isin(['WIN','LOSS'])].groupby('Weekday').size()

        def bar_chart(data_dict, width=20):
            if not data_dict: return "(لا يوجد بيانات)"
            mx = max(data_dict.values()) if data_dict.values() else 1
            lines = []
            for k, v in sorted(data_dict.items()):
                bar = "█" * int(v / mx * width)
                lines.append(f"  {str(k):>4} |{bar:<{width}}| {v}")
            return "\n".join(lines)

        hour_chart = bar_chart(hour_counts.to_dict())
        day_chart  = bar_chart(day_counts.to_dict())

        report = (
            f"📊 <b>Advanced Strategy Report — {days} يوم</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>💰 الأرباح</b>\n"
            f"  صافي الربح:      ${round(total_prof,2)}\n"
            f"  إجمالي الربح:    +${round(total_win,2)}\n"
            f"  إجمالي الخسارة:  -${abs(round(total_loss,2))}\n"
            f"  Profit Factor:   {profit_factor}\n"
            f"  Expected Payoff: ${expected_payoff}\n"
            f"  Recovery Factor: {recovery_factor}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📉 السحب (Drawdown)</b>\n"
            f"  أقصى DD:         ${round(max_dd,2)} ({dd_pct}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📈 الصفقات</b>\n"
            f"  الإجمالي:        {total_trades}\n"
            f"  فوز:             {win_count} ({win_rate}%)\n"
            f"  خسارة:           {loss_count} ({round(100-win_rate,1)}%)\n"
            f"  Long  Win/Loss:  {long_win}/{long_loss}\n"
            f"  Short Win/Loss:  {short_win}/{short_loss}\n"
            f"  بريك إيفن:       {be_count}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🔢 إحصاءات الصفقات</b>\n"
            f"  أكبر ربح:        +${largest_win}\n"
            f"  أكبر خسارة:      ${largest_loss}\n"
            f"  متوسط الربح:     +${avg_win}\n"
            f"  متوسط الخسارة:   ${avg_loss}\n"
            f"  أكبر سلسلة فوز:  {max_consec_win} صفقة (+${round(max_consec_win_usd,2)})\n"
            f"  أكبر سلسلة خسارة:{max_consec_loss} صفقة (-${abs(round(max_consec_loss_usd,2))})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🕐 توزيع بالساعة (دمشق):</b>\n<pre>{hour_chart}</pre>\n"
            f"<b>📅 توزيع بالأيام:</b>\n<pre>{day_chart}</pre>"
        )
        await send_tg_msg(report)

        # ─── Excel للتقرير المتقدم ───
        if trade_logs:
            from openpyxl.styles import PatternFill, Font
            xlsx_adv = f"ADV_Report_{datetime.now().strftime('%H%M%S')}.xlsx"
            df_exec = pd.DataFrame(trade_logs).drop(columns=['Hour_Damascus','Weekday'], errors='ignore')
            df_blocked = pd.DataFrame(blocked_logs) if blocked_logs else pd.DataFrame()
            stats_data = {
                'المقياس': ['صافي الربح','إجمالي الربح','إجمالي الخسارة','Profit Factor','Expected Payoff','Recovery Factor',
                             'أقصى Drawdown','DD%','إجمالي الصفقات','فوز','خسارة','نسبة الفوز','بريك إيفن',
                             'Long Win/Loss','Short Win/Loss','أكبر ربح','أكبر خسارة','متوسط ربح','متوسط خسارة',
                             'أكبر سلسلة فوز','أكبر سلسلة خسارة'],
                'القيمة': [f'${round(total_prof,2)}',f'+${round(total_win,2)}',f'-${abs(round(total_loss,2))}',
                            profit_factor, expected_payoff, recovery_factor,
                            f'${round(max_dd,2)}',f'{dd_pct}%', total_trades, win_count, loss_count,
                            f'{win_rate}%', be_count,
                            f'{long_win}/{long_loss}', f'{short_win}/{short_loss}',
                            f'+${largest_win}', f'${largest_loss}', f'+${avg_win}', f'${avg_loss}',
                            f'{max_consec_win} (+${round(max_consec_win_usd,2)})',
                            f'{max_consec_loss} (-${abs(round(max_consec_loss_usd,2))})']
            }
            df_stats = pd.DataFrame(stats_data)
            with pd.ExcelWriter(xlsx_adv, engine='openpyxl') as writer:
                df_exec.to_excel(writer, sheet_name='الصفقات', index=False)
                df_stats.to_excel(writer, sheet_name='الإحصاءات', index=False)
                if not df_blocked.empty:
                    df_blocked.to_excel(writer, sheet_name='المرفوضة', index=False)
                ws = writer.sheets['الصفقات']
                gf = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
                rf = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
                hf = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')
                for cell in ws[1]: cell.fill = hf; cell.font = Font(color='FFFFFF', bold=True)
                for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                    if len(row) >= 9:
                        val = str(row[8].value)
                        if val == 'WIN':
                            for cell in row: cell.fill = gf
                        elif val == 'LOSS':
                            for cell in row: cell.fill = rf
                for col in ws.columns:
                    ws.column_dimensions[col[0].column_letter].width = min(max((len(str(c.value or '')) for c in col), default=8)+3, 28)
            await send_tg_document(xlsx_adv, f"📊 Advanced Report — {days} يوم")
            os.remove(xlsx_adv)

    except Exception as e:
        c_log(f"❌ خطأ Advanced BT: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally:
        bot_state['is_backtesting'] = False

async def run_oanda_backtest(start_dt, mode='candle'):
    if bot_state['is_backtesting']:
        await send_tg_msg("⚠️ يوجد باك تيست قيد المعالجة حالياً.")
        return
        
    bot_state['is_backtesting'] = True
    c_log("بدء عملية الباك تيست مع המفتش الذكي...")
    csv_filename = f"BT_Oanda_Inspector_{datetime.now().strftime('%H%M%S')}.csv"
    trade_logs = []
    total_prof, peak_equity, max_dd = 0.0, 0.0, 0.0
    total_win, total_loss, win_count, loss_count = 0.0, 0.0, 0, 0
    be_count = 0
    
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
                if bot_state['use_danger_filter']:
                    _dh = (curr['time'].hour + 3) % 24
                    _dm = curr['time'].minute
                    if (_dh == 16) or (_dh == 17) or (_dh == 18 and _dm <= 30): continue
                
                # --- Trend Filter Logic (Only 50 and 150) ---
                b_ema, s_ema = True, True
                cons = bot_state['cons_count'] if bot_state['use_f_cons'] else 1
                for j in range(cons):
                    c = df.loc[i-j]
                    if not (c['ema50'] > c['ema150']): b_ema = False
                    if not (c['ema150'] > c['ema50']): s_ema = False

                # فلتر التوازي الكامل للموفينجات
                ma_parallel_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
                ma_parallel_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
                trend_buy  = (b_ema and ma_parallel_buy)  if bot_state['use_trend_filter'] else True
                trend_sell = (s_ema and ma_parallel_sell) if bot_state['use_trend_filter'] else True
                
                # --- Stochastic Triple-Net Logic ---
                buy_deep_stoch = (prev['K'] <= 10) and (curr['K'] > 10) and bot_state['use_stoch_deep']
                buy_mid_stoch  = (10 < prev['K'] <= 15) and (curr['K'] > 15) and bot_state['use_stoch_mid']
                buy_shal_stoch = (15 < prev['K'] <= 20) and (curr['K'] > 20) and bot_state['use_stoch_shal']

                sell_deep_stoch = (prev['K'] >= 90) and (curr['K'] < 90) and bot_state['use_stoch_deep']
                sell_mid_stoch  = (85 <= prev['K'] < 90) and (curr['K'] < 85) and bot_state['use_stoch_mid']
                sell_shal_stoch = (80 <= prev['K'] < 85) and (curr['K'] < 80) and bot_state['use_stoch_shal']
                
                stoch_buy_any = buy_deep_stoch or buy_mid_stoch or buy_shal_stoch
                stoch_sell_any = sell_deep_stoch or sell_mid_stoch or sell_shal_stoch

                # --- Smart Filter Application ---
                buy_deep_valid = buy_deep_stoch and (True if bot_state['use_smart_filter'] else trend_buy)
                buy_mid_valid  = buy_mid_stoch and trend_buy
                buy_shal_valid = buy_shal_stoch and trend_buy
                buy_sig = buy_deep_valid or buy_mid_valid or buy_shal_valid

                sell_deep_valid = sell_deep_stoch and (True if bot_state['use_smart_filter'] else trend_sell)
                sell_mid_valid  = sell_mid_stoch and trend_sell
                sell_shal_valid = sell_shal_stoch and trend_sell
                sell_sig = sell_deep_valid or sell_mid_valid or sell_shal_valid
                
                if buy_sig or sell_sig:
                    if i + 1 >= len(df):
                        continue
                    next_c = df.loc[i + 1]
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
                    
                    tol_val = bot_state['tp_tolerance_pips'] * bot_state['pip_value']
                    eff_tp_p = (tp_p - tol_val) if buy_sig else (tp_p + tol_val)
                    
                    max_exit_time = min(entry_t + timedelta(hours=72), datetime.now(timezone.utc))
                    outcome, exit_t = "EXPIRED", max_exit_time
                    
                    mode_str = 's5' if bot_state['use_s5_ticks'] else '1m'
                    val_candles = await fetch_oanda_candles('XAU_USD', mode_str, 4320, exit_t)
                    
                    be_activated = False
                    be_activation_dist = 20 * bot_state['pip_value']
                    be_target = act_ent + (m * be_activation_dist)
                    
                    for c in [v for v in val_candles if v['time'] >= entry_t]:
                        if buy_sig:
                            if bot_state['use_be'] and not be_activated and c['high'] >= be_target:
                                sl_p = act_ent
                                be_activated = True
                            if c['low'] <= sl_p: outcome, exit_t = ("BREAK-EVEN" if be_activated and sl_p == act_ent else "LOSS"), c['time']; break
                            if c['high'] >= eff_tp_p: outcome, exit_t = "WIN", c['time']; break
                        else:
                            if bot_state['use_be'] and not be_activated and c['low'] <= be_target:
                                sl_p = act_ent
                                be_activated = True
                            if c['high'] >= sl_p: outcome, exit_t = ("BREAK-EVEN" if be_activated and sl_p == act_ent else "LOSS"), c['time']; break
                            if c['low'] <= eff_tp_p: outcome, exit_t = "WIN", c['time']; break
                    
                    if outcome == "BREAK-EVEN": 
                        p_usd = 0.0
                        be_count += 1
                    elif outcome in ["WIN", "LOSS"]:
                        p_usd = round(abs(act_ent - (tp_p if outcome=="WIN" else sl_p)) * 100 * bot_state['lot_size'], 2) * (1 if outcome=="WIN" else -1)
                    if outcome == 'WIN': total_win += p_usd; win_count += 1
                    elif outcome == 'LOSS': total_loss += p_usd; loss_count += 1
                    else:
                        p_usd = 0.0
                    
                    total_prof += p_usd
                    peak_equity = max(peak_equity, total_prof)
                    max_dd = max(max_dd, peak_equity - total_prof)
                    
                    b_type = "DEEP(10)" if buy_deep_valid else "MID(15)" if buy_mid_valid else "SHAL(20)"
                    s_type = "DEEP(90)" if sell_deep_valid else "MID(85)" if sell_mid_valid else "SHAL(80)"
                    
                    trade_logs.append({
                        'Timeframe': tf, 'Type': f"BUY {b_type}" if buy_sig else f"SELL {s_type}",
                        'Entry Time': (entry_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Exit Time': (exit_t + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Entry Price': round(act_ent, 2),
                        'TP': tp_p, 'SL': sl_p,
                        'Pips': round(abs(act_ent - (tp_p if outcome=="WIN" else sl_p)) / bot_state['pip_value'], 1) if outcome in ["WIN", "LOSS"] else 0,
                        'Outcome': outcome, 'Profit ($)': p_usd
                    })
                
                # --- The Inspector (Logging Blocked & Near Misses) ---
                elif stoch_buy_any or stoch_sell_any:
                    reason = "REJECTED (Trend blocked)" if bot_state['use_trend_filter'] else "REJECTED (Unknown)"
                    trade_logs.append({
                        'Timeframe': tf, 'Type': 'BUY (BLOCKED)' if stoch_buy_any else 'SELL (BLOCKED)',
                        'Entry Time': (curr['time'] + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M'),
                        'Exit Time': '---', 'Entry Price': curr['close'],
                        'TP': 0, 'SL': 0, 'Pips': 0, 'Outcome': reason, 'Profit ($)': 0.0
                    })

        if trade_logs:
            from openpyxl.styles import PatternFill, Font
            df_logs = pd.DataFrame(trade_logs)
            total_trades = win_count + loss_count
            win_rate = round(win_count / total_trades * 100, 1) if total_trades > 0 else 0
            dd_pct = round(max_dd / peak_equity * 100, 1) if peak_equity > 0 else 0
            executed = df_logs[~df_logs['Outcome'].astype(str).str.contains('REJECTED')].copy()
            rejected = df_logs[df_logs['Outcome'].astype(str).str.contains('REJECTED')].copy()
            xlsx_filename = csv_filename.replace('.csv', '.xlsx')
            summary_data = {
                'البند': ['✅ الربح الكلي','❌ الخسارة الكلية','💰 المحصلة النهائية','🎯 نسبة الفوز','📉 أقصى سحب (DD)','🔄 بريك إيفن'],
                'القيمة': [
                    f'{win_count} صفقة | +${round(total_win,2)}',
                    f'{loss_count} صفقة | -${abs(round(total_loss,2))}',
                    f'${round(total_prof,2)}',
                    f'{win_rate}% ({total_trades} صفقة)',
                    f'${round(max_dd,2)} ({dd_pct}%)',
                    str(be_count)
                ]
            }
            df_summary = pd.DataFrame(summary_data)
            with pd.ExcelWriter(xlsx_filename, engine='openpyxl') as writer:
                executed.to_excel(writer, sheet_name='الصفقات', index=False)
                df_summary.to_excel(writer, sheet_name='الملخص', index=False)
                if not rejected.empty:
                    rejected.to_excel(writer, sheet_name='المرفوضة', index=False)
                ws = writer.sheets['الصفقات']
                gf = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
                rf = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
                hf = PatternFill(start_color='2E4057', end_color='2E4057', fill_type='solid')
                for cell in ws[1]:
                    cell.fill = hf; cell.font = Font(color='FFFFFF', bold=True)
                outcome_col = next((i+1 for i, cell in enumerate(ws[1]) if cell.value == 'Outcome'), 9)
                for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                    val = str(row[outcome_col-1].value) if len(row) >= outcome_col else ''
                    if val == 'WIN':
                        for cell in row: cell.fill = gf
                    elif val == 'LOSS':
                        for cell in row: cell.fill = rf
                for col in ws.columns:
                    ws.column_dimensions[col[0].column_letter].width = min(max((len(str(cell.value or '')) for cell in col), default=8)+3, 28)
            await send_tg_document(xlsx_filename, f"📊 <b>التقرير</b>\n✅ +${round(total_win,2)} ({win_count})\n❌ -${abs(round(total_loss,2))} ({loss_count})\n💰 ${round(total_prof,2)}\n🎯 {win_rate}%\n📉 DD: ${round(max_dd,2)}")
            os.remove(xlsx_filename)
        else: 
            await send_tg_msg("⚠️ لم يتم العثور على أي صفقات.")
    except Exception as e: 
        c_log(f"❌ خطأ باك تيست: {e}")
        await send_tg_msg(f"❌ خطأ: {e}")
    finally: 
        bot_state['is_backtesting'] = False

async def position_monitor():
    while True:
        try:
            if bot_state['live_connected'] and bot_state['use_be'] and bot_state['status'] == 'RUNNING' and bot_state['connection_obj']:
                positions = await bot_state['connection_obj'].get_positions()
                for p in positions:
                    if p['symbol'] == bot_state['symbol']:
                        open_price = p['openPrice']
                        tp = p.get('takeProfit')
                        sl = p.get('stopLoss')
                        current_price = p['currentPrice']
                        if tp and sl != open_price:
                            current_dist = abs(current_price - open_price)
                            if current_dist >= (20 * bot_state['pip_value']):
                                is_buy = tp > open_price
                                if (is_buy and current_price > open_price) or (not is_buy and current_price < open_price):
                                    await bot_state['connection_obj'].modify_position(p['id'], stop_loss=open_price)
                                    await send_tg_msg(f"🛡️ <b>تأمين دخول (Break-Even)</b>\nتم نقل الوقف لنقطة الدخول لحماية الصفقة: {p['id']}")
        except: pass
        await asyncio.sleep(5)

async def process_tg_update(update):
    if 'message' in update and 'text' in update['message']:
        msg = update['message']['text'].strip()
        bot_state['chat_id'] = update['message']['chat']['id']
        
        if msg == '/start': 
            await send_tg_msg("🤖 <b>مرحباً بك في لوحة التحكم السحابية المحدثة!</b>", get_main_keyboard())
        elif msg == '/debug':
            if not bot_state['live_connected']:
                await send_tg_msg("⚠️ البوت غير متصل باللايف.")
            else:
                try:
                    tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                    c = await bot_state['account_obj'].get_historical_candles(bot_state['symbol'], '5m', limit=10)
                    df = calculate_indicators(pd.DataFrame(c))
                    curr = df.iloc[-2]
                    
                    report = (
                        f"✅ **حالة النظام والبيانات الحية:**\n\n"
                        f"🔹 الزوج: {bot_state['symbol']} | السبريد: {round((tick['ask'] - tick['bid'])/bot_state['pip_value'], 1)}\n"
                        f"🔹 فلتر الترند الأساسي: {'🟢 مفعل' if bot_state['use_trend_filter'] else '🔴 معطل'}\n"
                        f"🔹 الترند الذكي (تجاهل 10/90): {'🟢 مفعل' if bot_state['use_smart_filter'] else '🔴 معطل'}\n\n"
                        f"🔹 قيمة הستوكاستيك المغلقة: {curr['K']:.1f}\n"
                        f"🔹 الترند الحالي (50 vs 150): {'صاعد 🟢' if curr['ema50'] > curr['ema150'] else 'هابط 🔴'}"
                    )
                    await send_tg_msg(report)
                except Exception as e: await send_tg_msg(f"❌ خطأ الفحص: {e}")

        elif msg.startswith('/set'):
            p = msg.split()
            if len(p) == 4:
                bot_state[p[2]+'_pips'][p[1]] = int(p[3])
                await send_tg_msg(f"✅ تم تحديث {p[2]} لفريم {p[1]} إلى {p[3]}")
        elif msg.startswith('/backtest'):
            try: 
                date_str = msg.split()[1]
                st = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                asyncio.create_task(run_oanda_backtest(st, mode='candle'))
                await send_tg_msg(f"✅ تم استلام أمر الباك تيست من تاريخ: {date_str}")
            except: await send_tg_msg("⚠️ صيغة الأمر خاطئة. استخدم: /backtest YYYY-MM-DD")

    elif 'callback_query' in update:
        q = update['callback_query']
        d = q['data']
        chat_id = q['message']['chat']['id']
        msg_id = q['message']['message_id']
        bot_state['chat_id'] = chat_id
        
        if d == "toggle_live_conn":
            if not bot_state['live_connected']:
                await edit_tg_msg(chat_id, msg_id, "⏳ جاري الاتصال بسيرفرات MetaApi...", get_main_keyboard())
                try:
                    api = MetaApi(METAAPI_TOKEN)
                    bot_state['account_obj'] = await api.metatrader_account_api.get_account(ACCOUNT_ID)
                    bot_state['connection_obj'] = bot_state['account_obj'].get_rpc_connection()
                    await bot_state['connection_obj'].connect()
                    await bot_state['connection_obj'].wait_synchronized()
                    bot_state['live_connected'] = True
                    await edit_tg_msg(chat_id, msg_id, "✅ تم الاتصال بنجاح!", get_main_keyboard())
                except Exception as e: await send_tg_msg(f"❌ فشل الاتصال: {e}")
            else:
                bot_state['live_connected'] = False
                bot_state['connection_obj'] = None
                bot_state['account_obj'] = None
                await edit_tg_msg(chat_id, msg_id, "🔌 تم فصل الاتصال.", get_main_keyboard())
                
        elif d == "menu_main": await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())
        elif d == "toggle_status": bot_state['status'] = 'PAUSED' if bot_state['status'] == 'RUNNING' else 'RUNNING'; await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())
        elif d == "toggle_strat": bot_state['strategy'] = 'NEW' if bot_state['strategy'] == 'OLD' else 'OLD'; await edit_tg_msg(chat_id, msg_id, "🏠 القائمة الرئيسية:", get_main_keyboard())
        
        # الأزرار الجديدة
        elif d == "menu_filters": await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_trend": bot_state['use_trend_filter'] = not bot_state['use_trend_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_smart": bot_state['use_smart_filter'] = not bot_state['use_smart_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_f_cons": bot_state['use_f_cons'] = not bot_state['use_f_cons']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_time": bot_state['use_time_filter'] = not bot_state['use_time_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_danger": bot_state['use_danger_filter'] = not bot_state['use_danger_filter']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_stoch_deep": bot_state['use_stoch_deep'] = not bot_state['use_stoch_deep']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_stoch_mid":  bot_state['use_stoch_mid']  = not bot_state['use_stoch_mid'];  await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        elif d == "toggle_stoch_shal": bot_state['use_stoch_shal'] = not bot_state['use_stoch_shal']; await edit_tg_msg(chat_id, msg_id, "🎛 <b>فلاتر وشروط التداول:</b>", get_filters_keyboard())
        
        elif d == "menu_tfs": await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())
        elif d.startswith("toggle_tf_"):
            tf = d.split("_")[2]; bot_state['active_tfs'][tf] = not bot_state['active_tfs'][tf]
            await edit_tg_msg(chat_id, msg_id, "⏱ إدارة الفريمات:", get_tf_keyboard())
            
        elif d == "menu_settings": await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_be": bot_state['use_be'] = not bot_state['use_be']; await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_atr": bot_state['use_atr'] = not bot_state['use_atr']; await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "toggle_spread": bot_state['use_max_spread'] = not bot_state['use_max_spread']; await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "inc_lot": bot_state['lot_size'] = round(bot_state['lot_size'] + 0.01, 2); await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        elif d == "dec_lot": bot_state['lot_size'] = max(0.01, round(bot_state['lot_size'] - 0.01, 2)); await edit_tg_msg(chat_id, msg_id, "🛠 إعدادات المخاطرة:", get_settings_keyboard())
        
        elif d == "view_tpsl":
            txt = "📖 <b>أهداف الفريمات:</b>\n" + "\n".join([f"[{tf}] TP:{bot_state['tp_pips'][tf]} | SL:{bot_state['sl_pips'][tf]}" for tf in bot_state['timeframes']])
            await edit_tg_msg(chat_id, msg_id, txt, get_settings_keyboard())
            
        elif d == "report":
            txt = "📊 <b>حالة السوق الحية:</b>\n" + "\n".join([f"[{tf}] {bot_state['market_data'][tf]}" for tf in bot_state['timeframes'] if bot_state['active_tfs'][tf]])
            await edit_tg_msg(chat_id, msg_id, txt, get_main_keyboard())
            
        elif d == "account":
            if bot_state['live_connected'] and bot_state['connection_obj']:
                try:
                    acc = await bot_state['connection_obj'].get_account_information()
                    txt = f"💳 <b>الحساب:</b>\nرصيد: {acc['balance']}\nإيكويتي: {acc['equity']}"
                    await edit_tg_msg(chat_id, msg_id, txt, get_main_keyboard())
                except: pass
            else: await send_tg_msg("يجب الاتصال بالسيرفر أولاً!")
                
        elif d == "menu_backtest":
            kb = {"inline_keyboard": [
                [{"text": "📊 1 يوم", "callback_data": "bto_1"}, {"text": "📊 3 أيام", "callback_data": "bto_3"}, {"text": "📊 7 أيام", "callback_data": "bto_7"}],
                [{"text": "🔬 Advanced Report (MT5 Style)", "callback_data": "bto_adv_7"}],
                [{"text": "🔙 رجوع", "callback_data": "menu_main"}]
            ]}
            await edit_tg_msg(chat_id, msg_id, "اختر المدة أو أرسل /backtest YYYY-MM-DD:", kb)
        elif d.startswith("bto_adv_"):
            adv_days = int(d.split('_')[2])
            asyncio.create_task(run_advanced_backtest(days=adv_days))
        elif d.startswith("bto_"):
            days = int(d.split('_')[1])
            asyncio.create_task(run_oanda_backtest(datetime.now(timezone.utc) - timedelta(days=days), mode='candle'))
            
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

async def timeframe_scanner(tf):
    c_log(f"✅ ماسح الشموع للفريم [{tf}] يعمل في الخلفية.")
    while True:
        try:
            if bot_state['status'] == 'RUNNING' and bot_state['active_tfs'][tf]:
                if not bot_state['live_connected'] or not bot_state['account_obj']:
                    bot_state['market_data'][tf] = "⏸ بانتظار الاتصال (Offline)"
                    await asyncio.sleep(5)
                    continue
                    
                try:
                    c = await bot_state['account_obj'].get_historical_candles(bot_state['symbol'], tf, limit=100)
                except: 
                    await asyncio.sleep(15); continue

                df = calculate_indicators(pd.DataFrame(c))
                curr, prev = df.iloc[-2], df.iloc[-3]
                h = datetime.now(timezone.utc).hour
                
                _danger_now = False
                if bot_state['use_danger_filter']:
                    _lh = (datetime.now(timezone.utc).hour + 3) % 24
                    _lm = datetime.now(timezone.utc).minute
                    _danger_now = (_lh == 16) or (_lh == 17) or (_lh == 18 and _lm <= 30)
                if (bot_state['use_time_filter'] and not (8 <= h <= 17)) or _danger_now:
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

                        ma_parallel_buy  = curr['ema15'] > curr['ema50'] > curr['ema150']
                        ma_parallel_sell = curr['ema15'] < curr['ema50'] < curr['ema150']
                        trend_buy  = (b_ema and ma_parallel_buy)  if bot_state['use_trend_filter'] else True
                        trend_sell = (s_ema and ma_parallel_sell) if bot_state['use_trend_filter'] else True

                        buy_deep_stoch = (prev['K'] <= 10) and (curr['K'] > 10) and bot_state['use_stoch_deep']
                        buy_mid_stoch  = (10 < prev['K'] <= 15) and (curr['K'] > 15) and bot_state['use_stoch_mid']
                        buy_shal_stoch = (15 < prev['K'] <= 20) and (curr['K'] > 20) and bot_state['use_stoch_shal']

                        sell_deep_stoch = (prev['K'] >= 90) and (curr['K'] < 90) and bot_state['use_stoch_deep']
                        sell_mid_stoch  = (85 <= prev['K'] < 90) and (curr['K'] < 85) and bot_state['use_stoch_mid']
                        sell_shal_stoch = (80 <= prev['K'] < 85) and (curr['K'] < 80) and bot_state['use_stoch_shal']
                        
                        stoch_buy_any = buy_deep_stoch or buy_mid_stoch or buy_shal_stoch
                        stoch_sell_any = sell_deep_stoch or sell_mid_stoch or sell_shal_stoch

                        buy_sig = (buy_deep_stoch and (True if bot_state['use_smart_filter'] else trend_buy)) or (buy_mid_stoch and trend_buy) or (buy_shal_stoch and trend_buy)
                        sell_sig = (sell_deep_stoch and (True if bot_state['use_smart_filter'] else trend_sell)) or (sell_mid_stoch and trend_sell) or (sell_shal_stoch and trend_sell)

                        if stoch_buy_any and not buy_sig: c_log(f"🛑 [{tf}] شراء مرفوض: الترند يعاكس")
                        if stoch_sell_any and not sell_sig: c_log(f"🛑 [{tf}] بيع مرفوض: الترند يعاكس")

                        skip_trade = False
                        if bot_state['use_max_spread']:
                            try:
                                tick = await bot_state['connection_obj'].get_tick(bot_state['symbol'])
                                if ((tick['ask'] - tick['bid']) / bot_state['pip_value']) > bot_state['max_spread_pips']: skip_trade = True
                            except: pass

                        if not skip_trade and (buy_sig or sell_sig):
                            bot_state['last_signal_time'][tf] = curr['time']
                            p = c[-1]['close']
                            m = 1 if buy_sig else -1
                            trade_type_str = "شراء 🟢 BUY" if buy_sig else "بيع 🔴 SELL"
                            c_log(f"🎯 [{tf}] إشارة {trade_type_str} حية! جاري التنفيذ...")
                            
                            tp_dist = bot_state['tp_pips'][tf] * bot_state['pip_value']
                            sl_dist = bot_state['sl_pips'][tf] * bot_state['pip_value']
                            if bot_state['use_atr']:
                                tp_dist = curr['atr'] * bot_state['atr_mult_tp']
                                sl_dist = curr['atr'] * bot_state['atr_mult_sl']
                                
                            tp = round(p + (m * tp_dist), 2)
                            sl = round(p - (m * sl_dist), 2)
                            
                            try:
                                if buy_sig: await bot_state['connection_obj'].create_market_buy_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                                else: await bot_state['connection_obj'].create_market_sell_order(bot_state['symbol'], bot_state['lot_size'], stop_loss=sl, take_profit=tp)
                                
                                await send_tg_msg(f"🚨 <b>تم فتح صفقة حية!</b>\nالنوع: {trade_type_str}\nالفريم: {tf}\nالسعر: {p}\nTP: {tp}\nSL: {sl}")
                            except Exception as e: await send_tg_msg(f"❌ <b>فشل تنفيذ الصفقة!</b>\n{e}")
            await asyncio.sleep(10)
        except: await asyncio.sleep(15)

async def handle_ping(request):
    return web.Response(text="Gold Scalper Bot is ALIVE and Trading 24/7!")

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
