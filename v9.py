import sqlite3
import json
import re
import asyncio
import logging
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler
)
from cachetools import TTLCache
import redis
from typing import Dict, List, Optional

# ---------------------- إعدادات النظام ----------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

DATABASE_NAME = 'v2.db'
JSON_DATA_SOURCE = 'input.json'
MAX_MESSAGE_LENGTH = 4096  # الحد الأقصى لطول رسالة التليجرام

CACHE_CONFIG = {
    'maxsize': 1000,
    'ttl': 300  # 5 دقائق
}

SEARCH_CONFIG = {
    'result_limit': 5,
    'max_display': 20,
    'min_query_length': 1,
    'rate_limit': 15  # عدد الطلبات المسموح بها لكل دقيقة
}

REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5
}

# ---------------------- فئات المساعدة ----------------------
class HadithDatabase:
    """فئة متخصصة في إدارة عمليات قاعدة البيانات والتخزين المؤقت"""
    
    def __init__(self):
        """تهيئة اتصالات قاعدة البيانات و Redis"""
        try:
            self.redis = redis.Redis(**REDIS_CONFIG)
            self.redis.ping()  # اختبار الاتصال
            
            self.conn = sqlite3.connect(
                DATABASE_NAME,
                check_same_thread=False,
                isolation_level=None,
                timeout=30
            )
            self.conn.row_factory = sqlite3.Row
            self._initialize_database()
            
        except Exception as e:
            logger.error(f"فشل في تهيئة قاعدة البيانات: {str(e)}")
            raise

    def _initialize_database(self):
        """تهيئة الجداول والفهارس"""
        with self.conn:
            # جدول الأحاديث الأساسي
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS hadiths (
                    id INTEGER PRIMARY KEY,
                    book TEXT NOT NULL,
                    text TEXT NOT NULL,
                    grading TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
            
            # جدول البحث الفوري
            self.conn.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts 
                USING fts5(text, content='hadiths')''')
            
            # جدول الإحصائيات
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    type TEXT PRIMARY KEY,
                    count INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
            
            # فهارس لتحسين الأداء
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_book ON hadiths(book)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_created ON hadiths(created_at)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_book_grading ON hadiths(book, grading)')
            
        self._load_initial_data()
    
    def _load_initial_data(self):
        """تحميل البيانات الأولية من ملف JSON"""
        try:
            with open(JSON_DATA_SOURCE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            current_data = self.conn.execute('SELECT COUNT(*) FROM hadiths').fetchone()[0]
            if current_data == 0:
                logger.info("جاري استيراد البيانات الأولية...")
                self._import_data(data)
                logger.info("تم استيراد البيانات بنجاح")
                
        except Exception as e:
            logger.error(f'خطأ في تحميل البيانات: {str(e)}')
            raise
    
    def _import_data(self, data: List[Dict]):
        """استيراد البيانات إلى قاعدة البيانات"""
        with self.conn:
            # تفريغ الجداول
            self.conn.execute('DELETE FROM hadiths')
            self.conn.execute('DELETE FROM hadiths_fts')
            self.conn.execute('DELETE FROM stats')
            
            batch = []
            for item in data:
                clean_text = self._sanitize_text(item.get('arabicText', ''))
                batch.append((
                    item.get('book', 'غير معروف'),
                    clean_text,
                    item.get('majlisiGrading', 'غير مصنف')
                ))
                
                if len(batch) >= 500:
                    self._insert_batch(batch)
                    batch = []
            
            if batch:
                self._insert_batch(batch)
            
            # تحديث فهرس البحث
            self.conn.execute('''
                INSERT INTO hadiths_fts (rowid, text)
                SELECT id, text FROM hadiths
            ''')
            self.conn.execute('INSERT INTO hadiths_fts(hadiths_fts) VALUES(\'rebuild\')')
            
    def _insert_batch(self, batch: List[tuple]):
        """إدخال دفعة من البيانات"""
        self.conn.executemany('''
            INSERT INTO hadiths (book, text, grading)
            VALUES (?, ?, ?)
        ''', batch)
    
    def _sanitize_text(self, text: str) -> str:
        """تنظيف النص من التشكيل والأخطاء"""
        text = re.sub(r'[\u064B-\u065F\u0610-\u061A]', '', text)  # إزالة التشكيل
        text = re.sub(r'\s+', ' ', text).strip()  # إزالة المسافات الزائدة
        return self.normalize_arabic(text)
    
    @staticmethod
    def normalize_arabic(text: str) -> str:
        """توحيد الأحرف العربية للبحث"""
        replacements = {'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ة': 'ه'}
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text
    
    def search_hadiths(self, query: str) -> List[Dict]:
        """
        بحث متقدم مع معالجة خاصة للواو والتطبيع العربي
        
        Args:
            query (str): نص البحث
            
        Returns:
            list: نتائج البحث كقائمة من dictionaries
        """
        normalized_query = self.normalize_arabic(query)
        cache_key = f'search:{normalized_query}'
        
        # التحقق من التخزين المؤقت أولاً
        if cached := self.redis.get(cache_key):
            return json.loads(cached)
            
        terms = []
        for term in normalized_query.split():
            term = term.strip()
            if not term:
                continue
                
            # معالجة الواو باحترافية
            if term.startswith('و'):
                variants = [term, term[1:]] if len(term) > 1 else [term]
            else:
                variants = [term, f'و{term}']
                
            terms.append(f'({" OR ".join(variants)})')
        
        if not terms:
            return []
        
        fts_query = ' AND '.join(terms)
        
        try:
            with self.conn:
                results = self.conn.execute('''
                    SELECT book, text, grading 
                    FROM hadiths
                    WHERE id IN (
                        SELECT rowid 
                        FROM hadiths_fts 
                        WHERE hadiths_fts MATCH ?
                        ORDER BY bm25(hadiths_fts)
                        LIMIT 1000
                    )
                ''', (fts_query,)).fetchall()
                
                # تحويل النتائج إلى قواميس وتخزينها في Redis
                results_dict = [dict(row) for row in results]
                self.redis.setex(cache_key, 300, json.dumps(results_dict))
                return results_dict
                
        except Exception as e:
            logger.error(f'خطأ في البحث: {str(e)}')
            return []
    
    def update_statistics(self, stat_type: str):
        """تحديث الإحصائيات باستخدام Redis"""
        try:
            # زيادة العداد في Redis
            self.redis.zincrby('stats', 1, stat_type)
            
            # مزامنة مع SQLite كل 10 طلبات
            if self.redis.incr(f'stats_sync:{stat_type}') % 10 == 1:
                self.redis.expire(f'stats_sync:{stat_type}', 3600)
                count = int(self.redis.zscore('stats', stat_type) or 0)
                
                # تخزين في SQLite
                with self.conn:
                    self.conn.execute('''
                        INSERT INTO stats (type, count) 
                        VALUES (?, ?)
                        ON CONFLICT(type) DO UPDATE SET 
                            count = excluded.count,
                            last_updated = CURRENT_TIMESTAMP
                    ''', (stat_type, count))
                    
        except Exception as e:
            logger.error(f'خطأ في تحديث الإحصائيات: {str(e)}')
    
    def get_statistics(self) -> Dict[str, int]:
        """استرجاع الإحصائيات من SQLite"""
        try:
            with self.conn:
                return {row['type']: row['count'] 
                        for row in self.conn.execute('SELECT type, count FROM stats')}
        except Exception as e:
            logger.error(f'خطأ في استرجاع الإحصائيات: {str(e)}')
            return {}

    def bulk_update(self, operations: List[Dict]):
        """تنفيذ عمليات متعددة على Redis دفعة واحدة"""
        try:
            pipe = self.redis.pipeline()
            for op in operations:
                if op['type'] == 'increment':
                    pipe.incr(op['key'])
                elif op['type'] == 'set':
                    pipe.set(op['key'], op['value'], ex=op.get('ttl'))
            pipe.execute()
        except Exception as e:
            logger.error(f'خطأ في bulk_update: {str(e)}')

# ---------------------- إدارة البوت ----------------------
db = HadithDatabase()

async def check_rate_limit(user_id: int) -> bool:
    """التحقق من معدل الطلبات للمستخدم"""
    key = f"ratelimit:{user_id}"
    current = db.redis.incr(key)
    if current == 1:
        db.redis.expire(key, 60)
    return current > SEARCH_CONFIG['rate_limit']

def split_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """تقسيم النص الطويل إلى أجزاء"""
    parts = []
    while len(text) > max_length:
        split_index = text.rfind(' ', 0, max_length)
        split_index = split_index if split_index != -1 else max_length
        parts.append(text[:split_index])
        text = text[split_index:].lstrip()
    parts.append(text)
    return parts

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /start"""
    user = update.effective_user
    keyboard = [[InlineKeyboardButton(
        "➕ أضفني إلى مجموعتك", 
        url=f"t.me/{context.bot.username}?startgroup=true"
    )]]
    
    welcome_message = f"""
    <b>مرحبا {user.first_name}!
    أنا بوت كاشف أحاديث الشيعة في قاعدة بياناتي اكثر من 26155 حديث 🔍</b>

    <i>مميزات البوت:</i>
    - كتاب الكافي للكليني مع التصحيح من مراة العقول للمجلسي
    - جميع الاحاديث الموجودة في عيون اخبار الرضا للصدوق
    - كتاب نهج البلاغة
    - كتاب الخصال للصدوق 
    - وسيتم اضافة باقي كتب الشيعة
    - كتاب الامالي للصدوق
    - كتاب الامالي للمفيد
    - كتاب التوحيد للصدوق
    - كتاب فضائل الشيعة للصدوق
    - كتاب كامل الزيارات لابن قولويه القمي
    - كتاب الضعفاء لابن الغضائري
    - كتاب الغيبة للنعماني
    - كتاب الغيبة للطوسي
    - كتاب المؤمن لحسين بن سعيد الكوفي الاهوازي
    - كتاب الزهد لحسين بن سعيد الكوفي الاهوازي
    - كتاب معاني الاخبار للصدوق
    - كتاب معجم الأحاديث المعتبرة لمحمد أصفر محسني
    - كتاب نهج البلاغة لعلي بن أبي طالب
    - كتاب رسالة الحقوق للإمام زين العابدين

    <b>طريقة الاستخدام:</b>
    <code>شيعة [جزء من النص]</code>

    <b>مثال:</b>
    <code>شيعة باهتوهم</code>

    <i>قناة البوت</i>
    @shia_b0t
    ادعو لوالدي بالرحمة بارك الله فيكم إن استفدتم من هذا العمل
    """
    
    try:
        await update.message.reply_html(
            welcome_message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )
        db.update_statistics('start')
    except Exception as e:
        logger.error(f"خطأ في start_command: {str(e)}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /help"""
    try:
        stats = db.get_statistics()
        response = f"""
        <b>📊 الإحصائيات:</b>
        • عمليات البحث: <code>{stats.get('search', 0)}</code>
        • المستخدمين: <code>{stats.get('start', 0)}</code>
        """
        await update.message.reply_html(response)
    except Exception as e:
        logger.error(f"خطأ في help_command: {str(e)}")
        await update.message.reply_text("❌ حدث خطأ في استرجاع الإحصائيات")

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة طلبات البحث"""
    try:
        user = update.effective_user
        if await check_rate_limit(user.id):
            await update.message.reply_text("⏳ تم تجاوز الحد المسموح من الطلبات! الرجاء الانتظار...")
            return
            
        if not update.message.text.startswith('شيعة'):
            return
            
        query = update.message.text[4:].strip()
        if not query or len(query) < SEARCH_CONFIG['min_query_length']:
            await update.message.reply_text("⚠️ الرجاء إدخال نص للبحث (3 أحرف على الأقل)")
            return
            
        db.update_statistics('search')
        
        results = db.search_hadiths(query)
        total = len(results)
        
        if not results:
            await update.message.reply_html("⚠️ لم يتم العثور على نتائج")
            return
            
        if total > SEARCH_CONFIG['max_display']:
            await update.message.reply_html(
                f"<b>⚠️ تم العثور على {total} نتيجة!</b>\n"
                "الرجاء تضييق نطاق البحث بإضافة كلمات أخرى."
            )
            return
            
        response = [f"<b>🔍 تم العثور على {total} نتيجة:</b>\n"]
        
        # تحليل السياق
        for idx, hadith in enumerate(results[:10], 1):
            text = hadith['text']
            match_index = text.find(query)
            
            if match_index != -1:
                start = max(0, match_index - 20)
                end = min(len(text), match_index + 50)
                snippet = text[start:end].replace(query, f"<b>{query}</b>")
                response.append(
                    f"{idx}. {snippet}\n"
                    f"📚 الكتاب: {hadith['book']}\n"
                    f"📌 صحة الحديث: {hadith['grading']}\n"
                )
        
        # إرسال النتائج الكاملة إذا كانت ضمن الحد
        if total <= SEARCH_CONFIG['result_limit']:
            for hadith in results:
                message = (
                    f"<b>📖 {hadith['book']}</b>\n\n"
                    f"{hadith['text']}\n\n"
                    f"<i>صحة الحديث: {hadith['grading']}</i>"
                )
                for part in split_text(message):
                    await update.message.reply_html(part)
                    await asyncio.sleep(0.1)
        else:
            response.append("\n<b>ℹ️ الرجاء إضافة كلمات أخرى من المتن للحصول على نتائج أدق.</b>")
            await update.message.reply_html('\n'.join(response))
            
    except Exception as e:
        logger.error(f"خطأ في handle_search: {str(e)}")
        await update.message.reply_text("❌ حدث خطأ أثناء معالجة طلبك")

async def real_time_analytics():
    """مهمة خلفية لمعالجة التحليلات في الوقت الحقيقي"""
    while True:
        try:
            messages = db.redis.xread({'analytics_stream': '$'}, block=0, count=10)
            for stream, message_list in messages:
                for message_id, message_data in message_list:
                    # معالجة البيانات التحليلية
                    logger.info(f"تحليل بيانات: {message_data}")
                    db.redis.xdel('analytics_stream', message_id)
                    
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"خطأ في real_time_analytics: {str(e)}")
            await asyncio.sleep(10)

# ---------------------- التشغيل الرئيسي ----------------------
def initialize_bot():
    """تهيئة وتشغيل البوت"""
    try:
        # الحصول على التوكن من متغيرات البيئة
        token = os.getenv('BOT_TOKEN', '7378891608:AAGEYCS7lCgukX8Uqg9vH1HLMWjiX-C4HXg')
        
        application = Application.builder().token(token).build()
        
        # إضافة معالجات الأوامر
        application.add_handler(CommandHandler('start', start_command))
        application.add_handler(CommandHandler('help', help_command))
        
        # إضافة معالجات الرسائل
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_search
        ))
        
        # بدء مهمة التحليلات في الخلفية
        application.job_queue.run_once(
            lambda _: asyncio.create_task(real_time_analytics()),
            when=5
        )
        
        logger.info("جاري تشغيل البوت...")
        application.run_polling(
            poll_interval=1.0,
            timeout=30,
            drop_pending_updates=True
        )
        
    except Exception as e:
        logger.critical(f"فشل في تشغيل البوت: {str(e)}")

if __name__ == '__main__':
    try:
        initialize_bot()
    except KeyboardInterrupt:
        logger.info("إيقاف البوت...")
    except Exception as e:
        logger.critical(f"خطأ غير متوقع: {str(e)}")