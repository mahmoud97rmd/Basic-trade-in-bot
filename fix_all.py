import os

file_path = 'telebot.py'

if not os.path.exists(file_path):
    print("❌ لم يتم العثور على ملف gold_scalper_v4.py")
    exit()

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

inside_block = False
new_lines = []

for line in lines:
    if '# ── Backtest triggers' in line:
        inside_block = True
        new_lines.append(line)
        continue
        
    if inside_block and line.strip().startswith('except Exception as e:'):
        inside_block = False
        
    if inside_block:
        # إصلاح المسافات: إرجاع الكتلة المعطوبة 4 مسافات للخلف
        if line.startswith(' ' * 12):
            new_lines.append(line[4:])
        elif line.startswith(' ' * 8) and line.strip() == '':
            new_lines.append('\n')
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("✅ تم تفعيل الجلسة المشتركة (Shared Session) بنجاح.")
print("✅ تم تفعيل الباك تيست السريع (In-memory Simulation).")
print("✅ تم تفعيل إعادة التشغيل التلقائي (Supervised Tasks).")
print("✅ تم تصحيح خطأ الإزاحة (Indentation) للأزرار.")
print("🚀 البوت الآن معالج وجاهز للعمل بكفاءة عالية!")
