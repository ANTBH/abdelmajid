const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');

const token = 'YOUR_BOT_TOKEN'; // استبدل بالتوكن الخاص بك
const bot = new TelegramBot(token, { polling: true });

const ownerId = 6504095190;

let quranData = [];
let hadithData = [];

// تحميل البيانات من ملف mainDataQuran.json
fs.readFile('mainDataQuran.json', 'utf8', (err, data) => {
    if (err) {
        console.error(err);
        return;
    }
    quranData = JSON.parse(data);
    console.log("Quran data loaded successfully.");
});

// تحميل البيانات من ملف bukhari_cleaned.json
fs.readFile('bukhari_cleaned.json', 'utf8', (err, data) => {
    if (err) {
        console.error(err);
        return;
    }
    hadithData = JSON.parse(data);
    console.log("Hadith data loaded successfully. Total hadiths: ", hadithData.length);
});

// البحث عن آيات تتضمن عبارة أو كلمة معينة
function searchVerses(term) {
    const results = [];
    const regex = new RegExp(term, 'i'); // تعبير منتظم للبحث عن عبارة كاملة أو كلمة
    quranData.forEach(sura => {
        if (sura.verses) {
            sura.verses.forEach(verse => {
                if (regex.test(verse.text.ar)) { // البحث باستخدام التعبير المنتظم
                    results.push(`سورة ${sura.name.ar} (الآية ${verse.number}): ${verse.text.ar}`);
                }
            });
        }
    });
    return results;
}

// البحث عن أحاديث تتضمن عبارة أو كلمة معينة
function searchHadith(term) {
    const results = [];
    const regex = new RegExp(term, 'i'); // تعبير منتظم للبحث عن عبارة كاملة أو كلمة
    hadithData.forEach(hadith => {
        if (hadith.arabic && regex.test(hadith.arabic)) {
            results.push(`الحديث: ${hadith.arabic}`);
        }
    });
    return results;
}

// التعامل مع الرسائل الخاصة بالبحث في القرآن
bot.onText(/بحث (.+)/, (msg, match) => {
    const chatId = msg.chat.id;
    const searchTerm = match[1];
    const results = searchVerses(searchTerm);

    if (results.length > 0) {
        results.forEach(result => {
            bot.sendMessage(chatId, result);
        });
    } else {
        bot.sendMessage(chatId, "لم أجد نتائج.");
    }
});

// التعامل مع الرسائل الخاصة بالبحث في الأحاديث باستخدام كلمة "حديث"
bot.onText(/حديث (.+)/, (msg, match) => {
    const chatId = msg.chat.id;
    const searchTerm = match[1];
    const results = searchHadith(searchTerm);
    
    console.log(`Searching for: ${searchTerm}`); // إضافة رسالة تصحيح أخطاء

    if (results.length > 0) {
        results.forEach(result => {
            bot.sendMessage(chatId, result);
        });
    } else {
        bot.sendMessage(chatId, "لم أجد نتائج.");
    }
});

// إضافة رسالة عند بدء المحادثة
bot.onText(/\/start/, (msg) => {
    const chatId = msg.chat.id;

    if (msg.from.id === ownerId) {
        bot.sendMessage(chatId, "مرحبًا! هذا البوت خاص بمجموعة رضى الرحمن غايتي.");
    } else {
        bot.sendMessage(chatId, "مرحبًا! يمكنك استخدام هذا البوت للبحث في القرآن الكريم والأحاديث.");
    }
});

// منع استخدام البوت في المجموعات إذا لم يكن المالك
bot.on('message', (msg) => {
    if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
        if (msg.from.id !== ownerId) {
            bot.sendMessage(msg.chat.id, "لا يمكنك استخدام البوت في هذه المجموعة.");
            return;
        }
    }
});

// معالجة أخطاء polling
bot.on('polling_error', (error) => {
    console.error(`Polling error: ${error.code} - ${error.message}`);
});