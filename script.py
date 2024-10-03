import json

# تحميل البيانات من ملف JSON
with open('mainDataQuran.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# دالة لحذف الترجمة الإنجليزية وخصائص الصوت
def clean_data(sura):
    if "audio" in sura:
        del sura["audio"]  # حذف خاصية الصوت

    # حذف الترجمة الإنجليزية من النصوص
    for verse in sura.get("verses", []):
        if "text" in verse and "en" in verse["text"]:
            del verse["text"]["en"]
    
    # حذف الترجمة الإنجليزية من أسماء السور
    if "name" in sura and "en" in sura["name"]:
        del sura["name"]["en"]
    
    # حذف الترجمة الإنجليزية من أماكن الوحي
    if "revelation_place" in sura and "en" in sura["revelation_place"]:
        del sura["revelation_place"]["en"]
    
    return sura

# تنظيف كل السور
cleaned_data = [clean_data(sura) for sura in data]

# حفظ البيانات المعدلة في ملف جديد
with open('cleaned_file.json', 'w', encoding='utf-8') as file:
    json.dump(cleaned_data, file, ensure_ascii=False, indent=4)

print("تم تنظيف الملف وحذف الترجمة الإنجليزية وخاصية الصوت.")
