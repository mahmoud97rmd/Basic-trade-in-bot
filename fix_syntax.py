import os

# تحديد الملف (سواء كان باسم telebot.py أو gold_scalper_v4.py)
file_path = 'telebot.py'
if not os.path.exists(file_path):
    file_path = 'gold_scalper_v4.py'

if not os.path.exists(file_path):
    print("❌ لم يتم العثور على ملف الكود.")
    exit()

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# فحص وقائي لمنع مضاعفة المسافات إذا تم تشغيله مرتين
if "        elif d == 'menu_main':" in content:
    print("⚠️ الإزاحة صحيحة بالفعل! لا حاجة للتعديل.")
    exit()

lines = content.splitlines(keepends=True)
new_lines = []
shift_mode = False

for line in lines:
    # تفعيل الإزاحة عند أول elif معطوب
    if "elif d == 'menu_main':" in line:
        shift_mode = True
        
    # إيقاف الإزاحة عند الوصول لنهاية كتلة try 
    # (سطر except الذي يمتلك 4 مسافات فقط)
    if shift_mode and line.startswith('    except') and not line.startswith('        except'):
        shift_mode = False
        
    if shift_mode:
        if line.strip() == '':
            new_lines.append(line)
        else:
            new_lines.append('    ' + line) # إضافة 4 مسافات
    else:
        new_lines.append(line)

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"✅ تم إصلاح خطأ SyntaxError (الإزاحة) في ملف {file_path}")
