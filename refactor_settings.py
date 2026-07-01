import re

with open('/root/tr/Script.py', 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Update bot_state initialization
settings_to_move = [
    "'lot_size':         0.03,",
    "'gann_cycle_hours':       1,",
    "'gann_zone_filter':       'star',  ",
    "'gann_entry_mode':        'touch_trend', ",
    "'trend_filter_type':      'vwap',     ",
    "'trend_vwap_period':      100,",
    "'trend_ema_period':       60,",
    "'trend_timeframe':        '1h',    ",
    "'break_even_enabled':     False,",
    "'gann_monitor_tfs':       {tf: False for tf in _TFS},",
    "'gann_touch_margin_pts':  5,       ",
    "'gann_tpsl_mode':         'fixed', ",
    "'gann_tp_points':         140,",
    "'gann_sl_points':         110,",
    "'gann_tp_per_tf': {\n        '1m': 0, '2m': 0, '3m': 0, '4m': 0, '5m': 0, '6m': 0,\n        '10m': 0, '15m': 0, '20m': 0, '30m': 0, '1h': 0, '2h': 0\n    },",
    "'gann_sl_per_tf': {\n        '1m': 0, '2m': 0, '3m': 0, '4m': 0, '5m': 0, '6m': 0,\n        '10m': 0, '15m': 0, '20m': 0, '30m': 0, '1h': 0, '2h': 0\n    },",
    "'gann_atr_period':        14,",
    "'gann_atr_sl_mult':       1.5,",
    "'gann_atr_tp_mult':       2,"
]

for s in settings_to_move:
    code = code.replace(s, "")

# Remove the old global gann_monitor_tfs initialization
code = re.sub(r"bot_state\['gann_monitor_tfs'\]\['\w+'\] = True\n", "", code)
# Actually, the original file has lines like:
# bot_state['gann_monitor_tfs']['1m'] = True
# I will replace them later. Let's just remove them cleanly.
for tf in ['1m', '2m', '3m', '5m', '10m', '15m', '20m', '30m', '1h', '4m', '6m', '2h']:
    code = code.replace(f"bot_state['gann_monitor_tfs']['{tf}'] = True\n", "")


new_sym_state = """'symbol_state': {s: {
        'gann_levels': [],
        'gann_level_status': {},
        'gann_close_used': None,
        'gann_last_h1_time': None,
        'gann_cycle_active': False,
        'gann_cycle_started_at': None,
        'gann_open_trades': {},
        'lot_size': 0.05,
        'gann_cycle_hours': 1,
        'gann_zone_filter': 'star',  
        'gann_entry_mode': 'touch_trend', 
        'trend_filter_type': 'vwap',     
        'trend_vwap_period': 100,
        'trend_ema_period': 60,
        'trend_timeframe': '1h',    
        'break_even_enabled': False,
        'gann_monitor_tfs': {tf: (tf in ['5m', '10m', '15m', '20m', '30m', '1h', '4m', '6m', '2h', '1m', '2m', '3m']) for tf in _TFS},
        'gann_touch_margin_pts': 5,       
        'gann_tpsl_mode': 'fixed', 
        'gann_tp_points': 140,
        'gann_sl_points': 110,
        'gann_tp_per_tf': {tf: 0 for tf in _TFS},
        'gann_sl_per_tf': {tf: 0 for tf in _TFS},
        'gann_atr_period': 14,
        'gann_atr_sl_mult': 1.5,
        'gann_atr_tp_mult': 2,
    } for s in AVAILABLE_SYMBOLS},"""

code = re.sub(r"'symbol_state': \{s: \{.*?\}.*?\} for s in AVAILABLE_SYMBOLS\},", new_sym_state, code, flags=re.DOTALL)

# 2. Replace bot_state['setting'] with sym_state['setting']
settings_keys = [
    "lot_size", "gann_cycle_hours", "gann_zone_filter", "gann_entry_mode", 
    "trend_filter_type", "trend_vwap_period", "trend_ema_period", "trend_timeframe", 
    "break_even_enabled", "gann_touch_margin_pts", "gann_tpsl_mode", "gann_tp_points", 
    "gann_sl_points", "gann_tp_per_tf", "gann_sl_per_tf", "gann_atr_period", 
    "gann_atr_sl_mult", "gann_atr_tp_mult", "gann_monitor_tfs"
]

pattern = r"bot_state\['(" + "|".join(settings_keys) + r")'\]"
code = re.sub(pattern, r"sym_state['\1']", code)

# 3. Inject sym_state definition into functions
# For get_gann_keyboard:
code = code.replace("def get_gann_keyboard() -> dict:", "def get_gann_keyboard() -> dict:\n    sym = bot_state['ui_selected_symbol']\n    sym_state = bot_state['symbol_state'][sym]")
# Wait, get_gann_keyboard already defines sym = bot_state['ui_selected_symbol'] later on. We'll just define sym_state early.
# Let's remove the old sym definition in get_gann_keyboard to avoid duplicates.
code = code.replace("    sym = bot_state['ui_selected_symbol']\n    cyc  = '🟢 نشطة' if bot_state['symbol_state'][sym]['gann_cycle_active'] else '⚫ غير نشطة'", 
                    "    cyc  = '🟢 نشطة' if sym_state['gann_cycle_active'] else '⚫ غير نشطة'")

# For run_gann_backtest:
# Inside `for symbol in active_symbols:`
code = code.replace("for symbol in active_symbols:\n", "for symbol in active_symbols:\n            sym_state = bot_state['symbol_state'][symbol]\n")

# For gann_monitor_scanner:
# Inside `for symbol, is_active in bot_state['active_symbols'].items():`
code = code.replace("        for symbol, is_active in bot_state['active_symbols'].items():\n            if not is_active: continue\n", 
                    "        for symbol, is_active in bot_state['active_symbols'].items():\n            if not is_active: continue\n            sym_state = bot_state['symbol_state'][symbol]\n")

# For _handle_callback:
# We can inject sym_state at the top of the function
code = code.replace("async def _handle_callback(d: str, chat_id: int, msg_id: int) -> None:\n", 
                    "async def _handle_callback(d: str, chat_id: int, msg_id: int) -> None:\n    sym = bot_state['ui_selected_symbol']\n    sym_state = bot_state['symbol_state'][sym]\n")

# For telegram_polling_loop (/set commands):
# It uses bot_state directly right now because sym_state isn't defined there.
code = code.replace("bot_state['trend_ema_period'] = val", "bot_state['symbol_state'][bot_state['ui_selected_symbol']]['trend_ema_period'] = val")
code = code.replace("bot_state['trend_vwap_period'] = val", "bot_state['symbol_state'][bot_state['ui_selected_symbol']]['trend_vwap_period'] = val")
code = code.replace("bot_state['gann_tp_per_tf'][tf] = val", "bot_state['symbol_state'][bot_state['ui_selected_symbol']]['gann_tp_per_tf'][tf] = val")
code = code.replace("bot_state['gann_sl_per_tf'][tf] = val", "bot_state['symbol_state'][bot_state['ui_selected_symbol']]['gann_sl_per_tf'][tf] = val")
# For the regex match that might have caught the above lines before:
# Actually, the regex `bot_state['trend_ema_period']` was already replaced by `sym_state['trend_ema_period']`. Let's fix that!
# The regex I ran earlier on `bot_state['...']` will replace it in telegram_polling_loop too!
# So in telegram_polling_loop it became `sym_state['trend_ema_period'] = val`. But sym_state is not defined there!
# I need to inject sym_state into telegram_polling_loop or fix the regex.
# Injecting sym_state into telegram_polling_loop under the `/set` handling:
code = code.replace("        if parts[0] == '/set':\n", 
                    "        if parts[0] == '/set':\n            sym_state = bot_state['symbol_state'][bot_state['ui_selected_symbol']]\n")


# 4. Presets feature
import json
presets_logic = """
    elif d == 'menu_presets':
        kbd = {'inline_keyboard': [
            [{'text': '💾 حفظ كـ Preset 1', 'callback_data': 'save_preset_1'}, {'text': '📂 تحميل Preset 1', 'callback_data': 'load_preset_1'}],
            [{'text': '💾 حفظ كـ Preset 2', 'callback_data': 'save_preset_2'}, {'text': '📂 تحميل Preset 2', 'callback_data': 'load_preset_2'}],
            [{'text': '💾 حفظ كـ Preset 3', 'callback_data': 'save_preset_3'}, {'text': '📂 تحميل Preset 3', 'callback_data': 'load_preset_3'}],
            [{'text': '🔙 رجوع', 'callback_data': 'menu_main'}]
        ]}
        await _show(chat_id, msg_id, '<b>إدارة الإعدادات (Presets):</b>\\nهنا يمكنك حفظ إعدادات جميع الأزواج واستعادتها لاحقاً.', kbd)
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
                            if k not in ['gann_levels', 'gann_level_status', 'gann_cycle_active', 'gann_open_trades', 'gann_last_h1_time', 'gann_cycle_started_at']:
                                bot_state['symbol_state'][s_name][k] = v
                await send_tg_msg(f"✅ تم تحميل الإعدادات من Preset {p_num} بنجاح!")
            else:
                await send_tg_msg("❌ لا يوجد إعدادات محفوظة في هذا الـ Preset.")
        except Exception as e:
            await send_tg_msg("❌ حدث خطأ أثناء التحميل.")
"""

code = code.replace("    elif d == 'menu_protection':", presets_logic + "\n    elif d == 'menu_protection':")

# Add Presets button to main menu
main_kbd_old = """        [{'text': '🛡️ إعدادات الحماية', 'callback_data': 'menu_protection'}],"""
main_kbd_new = """        [{'text': '🛡️ إعدادات الحماية', 'callback_data': 'menu_protection'}],
        [{'text': '💾 إدارة الإعدادات (Presets)', 'callback_data': 'menu_presets'}],"""
code = code.replace(main_kbd_old, main_kbd_new)

# Add import json at the top
code = "import json\n" + code

with open('/root/tr/refactored.py', 'w', encoding='utf-8') as f:
    f.write(code)

print("Refactored script generated to /root/tr/refactored.py")
