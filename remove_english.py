import json

# قراءة ملف JSON
with open('muslim.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# إزالة الترجمة الإنجليزية
for hadith in data['hadiths']:
    if 'english' in hadith:
        del hadith['english']

# حفظ البيانات المعدلة في ملف جديد
with open('muslim_cleaned.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=4)

print("تمت إزالة الترجمة الإنجليزية وحفظ الملف الجديد.")
