import sqlite3
import json
import re
import asyncio
import logging
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler
)
import redis
from typing import Dict, List, Optional, Tuple

# ---------------------- إعدادات النظام ----------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- تأكد من تعيين متغير البيئة BOT_TOKEN ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    logger.critical("لم يتم تعيين متغير البيئة BOT_TOKEN. لا يمكن تشغيل البوت.")
    # يمكنك إما الخروج من البرنامج هنا أو رفع خطأ
    # raise ValueError("لم يتم تعيين متغير البيئة BOT_TOKEN.")
    # في هذا المثال، سنسمح بالاستمرار ولكن البوت سيفشل عند التهيئة
    pass # أو exit(1)

DATABASE_NAME = 'v2.db'
JSON_DATA_SOURCE = 'input.json'
MAX_MESSAGE_LENGTH = 4096  # الحد الأقصى لطول رسالة التليجرام

SEARCH_CONFIG = {
    'result_limit': 10,   # زيادة الحد الأولي قليلاً للتعامل مع التكرارات المحتملة
    'max_display': 20,   # الحد الأقصى لعدد النتائج المعروضة لتجنب الإغراق
    'min_query_length': 3, # الحد الأدنى لطول نص البحث (3 أحرف مثلاً)
    'rate_limit': 15,  # عدد الطلبات المسموح بها لكل دقيقة للمستخدم الواحد
    'max_snippet_length': 200 # الحد الأقصى لطول المقتطف
}

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'), # اسمح بالتكوين عبر متغيرات البيئة
    'port': int(os.getenv('REDIS_PORT', 6379)), # اسمح بالتكوين عبر متغيرات البيئة
    'db': int(os.getenv('REDIS_DB', 0)),       # اسمح بالتكوين عبر متغيرات البيئة
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5
}

# نقطة تقسيم تقريبية للنصوص الطويلة (أقل قليلاً من الحد الأقصى للسماح بالترويسة/التذييل)
APPROX_SPLIT_POINT = MAX_MESSAGE_LENGTH - 200

# ---------------------- فئات المساعدة ----------------------
class HadithDatabase:
    """فئة متخصصة في إدارة عمليات قاعدة البيانات والتخزين المؤقت"""

    def __init__(self):
        """تهيئة اتصالات قاعدة البيانات و Redis"""
        self.redis = None # تهيئة كـ None أولاً
        try:
            self.redis = redis.Redis(**REDIS_CONFIG)
            self.redis.ping()  # اختبار الاتصال
            logger.info("تم الاتصال بـ Redis بنجاح.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"فشل الاتصال بـ Redis: {str(e)}. سيستمر البوت بدون تخزين مؤقت وميزات Redis الأخرى.")
            # لا نرفع خطأ هنا للسماح للبوت بالعمل (بشكل محدود) بدون Redis

        try:
            self.conn = sqlite3.connect(
                DATABASE_NAME,
                check_same_thread=False,
                isolation_level=None,  # وضع الـ Autocommit
                timeout=30
            )
            self.conn.row_factory = sqlite3.Row
            self._initialize_database()
            logger.info("تم تهيئة قاعدة البيانات بنجاح.")
        except sqlite3.Error as e:
            logger.error(f"فشل في تهيئة قاعدة البيانات: {str(e)}")
            raise # خطأ قاتل، لا يمكن المتابعة بدون قاعدة بيانات

    def _initialize_database(self):
        """تهيئة الجداول والفهارس"""
        try:
            with self.conn:
                # جدول الأحاديث الأساسي
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS hadiths (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, -- استخدام AUTOINCREMENT أفضل
                        book TEXT NOT NULL,
                        text TEXT NOT NULL,
                        grading TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')

                # جدول البحث الفوري باستخدام FTS5 مع تحسينات للغة العربية
                # ملاحظة: لا نضع id كـ PRIMARY KEY هنا، rowid هو المفتاح الأساسي الافتراضي لـ FTS
                self.conn.execute('''
                    CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts
                    USING fts5(
                        hadith_id UNINDEXED, -- لتجنب فهرسته بواسطة FTS ولكن للاحتفاظ به
                        text,
                        content='hadiths', -- اسم الجدول الأصلي
                        content_rowid='id', -- ربط بـ rowid في الجدول الأصلي (الآن هو id)
                        tokenize='unicode61 remove_diacritics 2', -- تحسين التوكن للغة العربية
                        prefix='1 2 3'  -- دعم البحث عن الكلمات التي تبدأ بحرف أو حرفين أو ثلاثة
                    )''')

                # مشغلات لمزامنة FTS تلقائيًا (مستحسن)
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_ai AFTER INSERT ON hadiths BEGIN
                        INSERT INTO hadiths_fts (rowid, hadith_id, text) VALUES (new.id, new.id, new.text);
                    END;
                ''')
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_ad AFTER DELETE ON hadiths BEGIN
                        DELETE FROM hadiths_fts WHERE rowid = old.id;
                    END;
                ''')
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_au AFTER UPDATE ON hadiths BEGIN
                        UPDATE hadiths_fts SET text = new.text WHERE rowid = old.id;
                    END;
                ''')


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
        except sqlite3.Error as e:
            logger.error(f"خطأ في تهيئة قاعدة البيانات أو الفهارس/المشغلات: {str(e)}")
            raise

    def _load_initial_data(self):
        """تحميل البيانات الأولية من ملف JSON إذا كانت قاعدة البيانات فارغة"""
        try:
            # تحقق مما إذا كان جدول hadiths فارغًا بالفعل
            cursor = self.conn.execute('SELECT COUNT(*) FROM hadiths')
            current_data_count = cursor.fetchone()[0]

            if current_data_count == 0:
                logger.info("قاعدة بيانات الأحاديث فارغة. جاري محاولة استيراد البيانات الأولية...")
                if not os.path.exists(JSON_DATA_SOURCE):
                     logger.error(f"ملف البيانات المصدر '{JSON_DATA_SOURCE}' غير موجود. لا يمكن استيراد البيانات الأولية.")
                     return # لا ترفع خطأ هنا، اسمح للبوت بالبدء بقاعدة بيانات فارغة

                with open(JSON_DATA_SOURCE, 'r', encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                        if isinstance(data, list):
                            logger.info(f"تم العثور على {len(data)} سجل في ملف JSON. جاري الاستيراد...")
                            self._import_data(data)
                            logger.info("تم استيراد البيانات الأولية بنجاح وإعادة بناء فهرس FTS.")
                        else:
                            logger.error("تنسيق ملف JSON غير صالح (يجب أن يكون قائمة من الكائنات).")
                    except json.JSONDecodeError as e:
                        logger.error(f'خطأ في تنسيق JSON في ملف {JSON_DATA_SOURCE}: {str(e)}')
                        raise # هذا خطأ جاد إذا كان الملف موجودًا ولكنه تالف
            else:
                logger.info(f"تم العثور على {current_data_count} حديث في قاعدة البيانات. تخطي استيراد البيانات الأولية.")

        except sqlite3.Error as e:
            logger.error(f'خطأ في قاعدة البيانات أثناء التحقق من البيانات الأولية: {str(e)}')
            raise
        except Exception as e:
            logger.error(f'خطأ غير متوقع أثناء تحميل البيانات الأولية: {str(e)}')
            raise

    def _import_data(self, data: List[Dict]):
        """استيراد البيانات إلى قاعدة البيانات وإعادة بناء فهرس FTS"""
        try:
            logger.info("بدء عملية الاستيراد... (قد يستغرق هذا بعض الوقت)")
            with self.conn: # استخدام transaction
                # تفريغ الجداول قبل الاستيراد (اختياري، قد ترغب في التحديث بدلاً من ذلك)
                logger.warning("سيتم حذف جميع البيانات الموجودة في جدولي hadiths و hadiths_fts قبل الاستيراد.")
                self.conn.execute('DELETE FROM hadiths')
                self.conn.execute('DELETE FROM hadiths_fts') # يجب حذفه أيضًا
                # إعادة تعيين عداد AUTOINCREMENT (اختياري)
                try:
                    self.conn.execute("DELETE FROM sqlite_sequence WHERE name='hadiths';")
                except sqlite3.Error:
                    pass # تجاهل الخطأ إذا لم يكن الجدول موجودًا بعد

                batch = []
                processed_count = 0
                total_count = len(data)
                for item in data:
                    clean_text = self._sanitize_text(item.get('arabicText', ''))
                    if not clean_text: # تخطي الإدخالات الفارغة
                        continue
                    batch.append((
                        item.get('book', 'غير معروف'),
                        clean_text,
                        item.get('majlisiGrading', 'غير مصنف')
                    ))

                    if len(batch) >= 500: # إدخال دفعات بحجم 500
                        self._insert_batch(batch)
                        processed_count += len(batch)
                        logger.info(f"تمت معالجة {processed_count}/{total_count} سجل...")
                        batch = []

                if batch: # إدخال الدفعة المتبقية
                    self._insert_batch(batch)
                    processed_count += len(batch)

                logger.info(f"اكتمل إدخال البيانات ({processed_count} سجل).")

                # لا حاجة لإعادة بناء فهرس FTS يدويًا بسبب المشغلات (Triggers)
                # إذا لم تستخدم المشغلات، ستحتاج إلى إلغاء تعليق هذا:
                # logger.info("جاري إعادة بناء فهرس البحث FTS...")
                # self.conn.execute("INSERT INTO hadiths_fts (rowid, hadith_id, text) SELECT id, id, text FROM hadiths;")
                # logger.info("اكتمل بناء فهرس FTS.")


        except sqlite3.Error as e:
            logger.error(f'خطأ في قاعدة البيانات أثناء استيراد البيانات: {str(e)}')
            raise
        except Exception as e:
            logger.error(f'خطأ غير متوقع أثناء الاستيراد: {str(e)}')
            raise

    def _insert_batch(self, batch: List[tuple]):
        """إدخال دفعة من البيانات"""
        try:
            # لا حاجة لـ `with self.conn` هنا لأنها مستدعاة داخل transaction في _import_data
            self.conn.executemany('''
                INSERT INTO hadiths (book, text, grading)
                VALUES (?, ?, ?)
            ''', batch)
        except sqlite3.Error as e:
            logger.error(f'خطأ في إدخال دفعة بيانات: {e}')
            # لا نرفع الخطأ هنا للسماح بمحاولة إكمال الاستيراد
            # ولكن يجب تسجيل الخطأ

    def _sanitize_text(self, text: str) -> str:
        """تنظيف النص من التشكيل والأخطاء وتطبيع الأحرف العربية"""
        if not isinstance(text, str):
            return ""
        # إزالة التشكيل وعلامات الترقيم الزائدة (يمكن تعديلها حسب الحاجة)
        text = re.sub(r'[\u064B-\u065F\u0610-\u061A\u06D6-\u06DC\u06DF-\u06E8\u06EA-\u06ED٠-٩]', '', text)
        # تطبيع الأحرف
        text = self.normalize_arabic(text)
        # إزالة المسافات الزائدة والأسطر الجديدة المتعددة
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def normalize_arabic(text: str) -> str:
        """توحيد الأحرف العربية للبحث (أإآ -> ا, ة -> ه, ي -> ى والعكس حسب الحاجة)"""
        if not isinstance(text, str):
             return ""
        replacements = {'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ة': 'ه', 'ى': 'ي'}
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    def search_hadiths(self, query: str) -> List[Dict]:
        """
        بحث متقدم باستخدام FTS5 مع معالجة خاصة للواو والتطبيع العربي.

        Args:
            query (str): نص البحث

        Returns:
            list: نتائج البحث كقائمة من dictionaries، مرتبة حسب الصلة.
        """
        if not query:
            return []

        normalized_query = self.normalize_arabic(query.strip())
        if not normalized_query:
            return []

        cache_key = f'search:{normalized_query}'

        # 1. التحقق من التخزين المؤقت أولاً
        if self.redis:
            try:
                cached_results = self.redis.get(cache_key)
                if cached_results:
                    logger.info(f"تم العثور على نتائج البحث لـ '{query}' في الذاكرة المؤقتة.")
                    return json.loads(cached_results)
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"خطأ في الاتصال بـ Redis عند محاولة القراءة من الذاكرة المؤقتة: {e}. سيتم البحث في قاعدة البيانات.")
            except json.JSONDecodeError as e:
                 logger.warning(f"خطأ في فك ترميز بيانات الذاكرة المؤقتة لـ '{query}': {e}. سيتم البحث في قاعدة البيانات.")


        # 2. بناء استعلام FTS
        terms = []
        # استخدام set لتجنب تكرار الكلمات المتشابهة بعد التطبيع
        processed_terms = set()
        for term in normalized_query.split():
            term = term.strip()
            if not term or term in processed_terms:
                continue
            processed_terms.add(term)

            # التعامل مع الواو: ابحث عن الكلمة مع وبدون الواو في البداية
            # نستخدم NEAR للبحث عن الكلمات القريبة من بعضها إذا كان الاستعلام متعدد الكلمات
            # ونستخدم '*' للبحث عن البادئات
            if term.startswith('و') and len(term) > 1:
                 # ابحث عن (الكلمة نفسها*) أو (الكلمة بدون الواو*)
                 variants = f'({re.escape(term)}* OR {re.escape(term[1:])}*)'
            else:
                 # ابحث عن (الكلمة نفسها*) أو (الكلمة مع الواو*)
                 variants = f'({re.escape(term)}* OR و{re.escape(term)}*)'

            terms.append(variants)

        if not terms:
            return []

        # ربط المصطلحات باستخدام AND (يجب أن تحتوي النتائج على جميع المصطلحات بشكل ما)
        # أو استخدام NEAR إذا أردت أن تكون الكلمات قريبة من بعضها
        fts_query = ' AND '.join(terms)
        logger.info(f"استعلام FTS المُنشأ: {fts_query}")

        # 3. تنفيذ البحث في قاعدة البيانات
        try:
            with self.conn:
                cursor = self.conn.execute(f'''
                    SELECT h.id, h.book, h.text, h.grading
                    FROM hadiths h
                    JOIN hadiths_fts fts ON h.id = fts.rowid
                    WHERE fts.hadiths_fts MATCH ?
                    ORDER BY bm25(fts.hadiths_fts) -- ترتيب حسب الصلة (BM25 هو الافتراضي والأكثر شيوعًا)
                    LIMIT {SEARCH_CONFIG['result_limit'] * 2} -- جلب المزيد قليلاً للتعامل مع التصفية المحتملة
                ''', (fts_query,))
                results = cursor.fetchall()

                # تحويل النتائج إلى قواميس
                results_dict = [dict(row) for row in results]

                # 4. تخزين النتائج في الذاكرة المؤقتة (إذا كان Redis متاحًا)
                if self.redis and results_dict:
                    try:
                        self.redis.setex(cache_key, 300, json.dumps(results_dict)) # تخزين لمدة 5 دقائق
                        logger.info(f"تم تخزين نتائج البحث لـ '{query}' في الذاكرة المؤقتة.")
                    except redis.exceptions.ConnectionError as e:
                        logger.warning(f"خطأ في الاتصال بـ Redis عند محاولة الكتابة إلى الذاكرة المؤقتة: {e}.")
                    except TypeError as e:
                        logger.error(f"خطأ في تحويل النتائج إلى JSON للتخزين المؤقت: {e}")


                return results_dict

        except sqlite3.Error as e:
            # قد يحدث خطأ إذا كان استعلام FTS غير صالح
            logger.error(f'خطأ في البحث باستخدام FTS عن "{query}" (الاستعلام: {fts_query}): {str(e)}')
            return [] # إرجاع قائمة فارغة في حالة الخطأ
        except Exception as e:
            logger.error(f'خطأ غير متوقع أثناء البحث عن "{query}": {str(e)}')
            return []


    # --- وظيفة تصحيح الإملاء (محدودة جدًا) ---
    # ملاحظة: هذه الوظيفة محدودة للغاية وتعتمد على قاموس ثابت صغير.
    # للحصول على نتائج أفضل، ستحتاج إلى قاموس أكبر بكثير أو مكتبة متخصصة.
    # قد يكون من الأفضل عدم استخدامها إذا لم يكن لديك قاموس جيد.
    def _correct_spelling(self, term: str, max_distance: int = 1) -> List[str]:
        """يقترح تصحيحات إملائية بسيطة بناءً على قاموس محدود."""
        # مثال بسيط جدًا - يجب توسيع هذا القاموس بشكل كبير
        dictionary = ["شيعة", "باهتوهم", "الكافي", "عيون", "اخبار", "الرضا", "نهج", "البلاغة", "الخصال",
                      "الامالي", "التوحيد", "فضائل", "كامل", "الزيارات", "الضعفاء", "الغيبة", "المؤمن",
                      "الزهد", "معاني", "الاخبار", "معجم", "الاحاديث", "المعتبرة", "رسالة", "الحقوق"]

        suggestions = []
        normalized_term = self.normalize_arabic(term) # تطبيع المصطلح قبل المقارنة
        for word in dictionary:
            normalized_word = self.normalize_arabic(word) # تطبيع كلمة القاموس
            distance = self._levenshtein_distance(normalized_term, normalized_word)
            if distance <= max_distance:
                suggestions.append(word) # أضف الكلمة الأصلية من القاموس
        return suggestions

    @staticmethod
    def _levenshtein_distance(s1: str, s2: str) -> int:
        """حساب مسافة ليفنشتاين بين سلسلتين نصيتين."""
        if len(s1) < len(s2):
            return HadithDatabase._levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        return previous_row[-1]

    def update_statistics(self, stat_type: str):
        """تحديث الإحصائيات باستخدام Redis (إن وجد) والمزامنة الدورية مع SQLite"""
        if not self.redis:
            # إذا لم يكن Redis متاحًا، قم بالتحديث مباشرة في SQLite (أقل كفاءة)
            try:
                with self.conn:
                    self.conn.execute('''
                        INSERT INTO stats (type, count) VALUES (?, 1)
                        ON CONFLICT(type) DO UPDATE SET
                            count = count + 1,
                            last_updated = CURRENT_TIMESTAMP
                    ''', (stat_type,))
            except sqlite3.Error as e:
                 logger.error(f'خطأ SQLite في تحديث الإحصائيات مباشرة لـ {stat_type}: {str(e)}')
            return

        # استخدام Redis للتحديث السريع
        try:
            # استخدام incr بسيط للعد
            current_count = self.redis.incr(f'stat:{stat_type}')

            # مزامنة مع SQLite كل 10 طلبات (أو أي رقم آخر)
            sync_key = f'stat_sync_trigger:{stat_type}'
            # استخدام incr للتحقق من الحاجة للمزامنة
            if self.redis.incr(sync_key) % 10 == 1:
                self.redis.expire(sync_key, 3600) # إعادة تعيين العداد كل ساعة

                # جلب القيمة الحالية من Redis للمزامنة
                # قد تكون القيمة تغيرت قليلاً منذ الزيادة الأولية، لذا نقرأها مرة أخرى
                sync_count = int(self.redis.get(f'stat:{stat_type}') or 0)

                logger.info(f"مزامنة إحصائية '{stat_type}' مع SQLite (القيمة: {sync_count}).")
                # تخزين في SQLite
                with self.conn:
                    self.conn.execute('''
                        INSERT INTO stats (type, count, last_updated)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(type) DO UPDATE SET
                            count = excluded.count,
                            last_updated = excluded.last_updated
                    ''', (stat_type, sync_count))

        except redis.exceptions.ConnectionError as e:
            logger.error(f'خطأ Redis في تحديث الإحصائيات لـ {stat_type}: {e}, تم تخطي التحديث.')
        except sqlite3.Error as e:
            logger.error(f'خطأ SQLite في مزامنة الإحصائيات لـ {stat_type}: {str(e)}')
        except Exception as e:
             logger.error(f'خطأ غير متوقع في تحديث الإحصائيات لـ {stat_type}: {str(e)}')


    def get_statistics(self) -> Dict[str, int]:
        """استرجاع الإحصائيات من SQLite (تعتبر المصدر الأكثر موثوقية بعد المزامنة)"""
        stats_data = {}
        # أولاً، حاول قراءة القيم الأحدث من Redis إن وجدت
        if self.redis:
            try:
                 # الحصول على جميع مفاتيح الإحصائيات من Redis
                 stat_keys = self.redis.keys('stat:*')
                 if stat_keys:
                     stat_values = self.redis.mget(stat_keys)
                     for key, value in zip(stat_keys, stat_values):
                         stat_type = key.decode('utf-8').split(':', 1)[1] # استخراج النوع
                         if value:
                              stats_data[stat_type] = int(value)
            except redis.exceptions.ConnectionError as e:
                 logger.warning(f"خطأ Redis عند جلب الإحصائيات الحالية: {e}. سيتم الاعتماد على SQLite فقط.")
                 stats_data = {} # مسح البيانات من Redis إذا فشل الاتصال

        # ثانيًا، اقرأ من SQLite وقم بتحديث/إضافة القيم إذا كانت أحدث أو غير موجودة
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT type, count FROM stats')
                sqlite_stats = {row['type']: row['count'] for row in cursor.fetchall()}

                # دمج النتائج: إعطاء الأولوية لـ Redis إذا كانت القيمة أكبر (أحدث غالبًا)
                for stat_type, count in sqlite_stats.items():
                    if stat_type not in stats_data or count > stats_data[stat_type]:
                        stats_data[stat_type] = count

            return stats_data

        except sqlite3.Error as e:
            logger.error(f'خطأ SQLite في استرجاع الإحصائيات: {str(e)}')
            # إرجاع ما تم جمعه من Redis (إن وجد) أو قاموس فارغ
            return stats_data if stats_data else {}


    # وظيفة bulk_update (لم يتم استخدامها في الكود الحالي ولكنها مفيدة)
    def bulk_update(self, operations: List[Dict]):
        """تنفيذ عمليات متعددة على Redis دفعة واحدة باستخدام pipeline"""
        if not self.redis:
            logger.warning("لا يمكن تنفيذ bulk_update لأن Redis غير متاح.")
            return
        try:
            pipe = self.redis.pipeline()
            for op in operations:
                op_type = op.get('type')
                key = op.get('key')
                if not op_type or not key:
                    logger.warning(f"تجاهل عملية bulk غير صالحة: {op}")
                    continue

                if op_type == 'increment':
                    pipe.incr(key, op.get('amount', 1))
                elif op_type == 'set':
                    value = op.get('value')
                    if value is not None:
                        pipe.set(key, value, ex=op.get('ttl'))
                # أضف أنواع عمليات أخرى حسب الحاجة (e.g., zincrby, sadd, etc.)
            pipe.execute()
        except redis.exceptions.ConnectionError as e:
            logger.error(f'خطأ Redis في bulk_update: {e}')
        except Exception as e:
            logger.error(f'خطأ غير متوقع في bulk_update: {str(e)}')


# ---------------------- إدارة البوت ----------------------
# إنشاء نسخة واحدة من قاعدة البيانات
try:
    db = HadithDatabase()
except Exception as e:
    logger.critical(f"فشل في تهيئة HadithDatabase بشكل كامل: {e}. قد لا يعمل البوت بشكل صحيح.")
    # يمكنك اختيار الخروج هنا إذا كانت قاعدة البيانات ضرورية تمامًا
    # exit(1)
    db = None # تعيينه إلى None للإشارة إلى فشل التهيئة

async def check_rate_limit(user_id: int) -> bool:
    """التحقق من معدل الطلبات للمستخدم باستخدام Redis (إن وجد)"""
    if not db or not db.redis:
        return False # لا يوجد حد للمعدل إذا لم يكن Redis متاحًا

    key = f"ratelimit:{user_id}"
    try:
        # استخدام INCR مع EXPIRE للحصول على نافذة منزلقة بسيطة
        current = db.redis.incr(key)
        if current == 1:
            db.redis.expire(key, 60) # تعيين انتهاء الصلاحية لأول مرة فقط (60 ثانية)

        is_limited = current > SEARCH_CONFIG['rate_limit']
        if is_limited:
             logger.warning(f"تم تجاوز حد المعدل للمستخدم {user_id} (الطلبات: {current})")
        return is_limited
    except redis.exceptions.ConnectionError as e:
        logger.error(f"خطأ Redis عند التحقق من حد المعدل للمستخدم {user_id}: {e}. السماح بالطلب.")
        return False # اسمح بالطلب إذا فشل Redis لتجنب حظر المستخدمين
    except Exception as e:
         logger.error(f"خطأ غير متوقع عند التحقق من حد المعدل للمستخدم {user_id}: {e}. السماح بالطلب.")
         return False


def split_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """تقسيم النص الطويل إلى أجزاء مع الحفاظ على الكلمات"""
    parts = []
    if not text or not isinstance(text, str):
         return parts

    while len(text) > max_length:
        # ابحث عن آخر مسافة قبل الحد الأقصى للطول
        split_index = text.rfind(' ', 0, max_length)
        # إذا لم تجد مسافة، قم بالقطع عند الحد الأقصى (قد يقطع الكلمة)
        if split_index == -1:
            split_index = max_length
        parts.append(text[:split_index])
        text = text[split_index:].lstrip() # إزالة المسافة البادئة للجزء التالي
    parts.append(text) # إضافة الجزء المتبقي (أو النص الأصلي إذا كان قصيرًا)
    return parts

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /start"""
    if not db: # تحقق مما إذا تم تهيئة قاعدة البيانات
        await update.message.reply_text("حدث خطأ في تهيئة البوت. يرجى المحاولة مرة أخرى لاحقًا أو الاتصال بالمسؤول.")
        return

    user = update.effective_user
    try:
        # الحصول على اسم البوت ديناميكيًا
        bot_username = context.bot.username
        keyboard = [[InlineKeyboardButton(
            "➕ أضفني إلى مجموعتك",
            url=f"https://t.me/{bot_username}?startgroup=true"
        )]]

        # تحديث قائمة الكتب لتكون أكثر دقة ومطابقة لما يتم استيراده فعليًا
        # يمكنك جعل هذه القائمة ديناميكية بقراءة أسماء الكتب المميزة من قاعدة البيانات
        welcome_message = f"""
        <b>مرحبا {user.first_name}!</b> 👋
        أنا بوت كاشف الأحاديث الشيعية. أبحث في قاعدة بياناتي التي تحتوي على آلاف الأحاديث من مصادر مختلفة.

        📚 <b>المصادر المضمنة حاليًا (قد تتغير):</b>
        <i>(قائمة المصادر من ملف JSON الأصلي أو ما تم استيراده)</i>
        - الكافي للكليني (مع تصحيح مرآة العقول للمجلسي إذا توفر)
        - عيون أخبار الرضا للصدوق
        - نهج البلاغة
        - الخصال للصدوق
        - الأمالي (للصدوق والمفيد)
        - التوحيد للصدوق
        - فضائل الشيعة للصدوق
        - كامل الزيارات لابن قولويه
        - الضعفاء لابن الغضائري
        - الغيبة (للنعماني والطوسي)
        - المؤمن والزهد لحسين بن سعيد الأهوازي
        - معاني الأخبار للصدوق
        - معجم الأحاديث المعتبرة لمحسن الأسدي
        - رسالة الحقوق للإمام زين العابدين
        <i>(سيتم إضافة المزيد من الكتب إن شاء الله)</i>

        <b>طريقة الاستخدام:</b>
        أرسل كلمة <code>شيعة</code> متبوعة بجزء من نص الحديث الذي تبحث عنه.
        <code>شيعة [جزء من النص]</code>

        <b>مثال:</b>
        <code>شيعة باهتوهم</code>

        <b>ملاحظات:</b>
        • الحد الأدنى لطول البحث هو {SEARCH_CONFIG['min_query_length']} أحرف.
        • يتم تجاهل التشكيل والهمزات (أ، إ، آ تعتبر كلها ا) والتاء المربوطة (ة تعتبر ه).
        • يتم البحث عن الكلمة مع وبدون واو التعريف (مثال: البحث عن "علم" يجد "العلم" و "وعلم").

        <i>قناة البوت: @shia_b0t</i>
        <i>ادعوا لوالدي بالرحمة إن استفدتم من هذا العمل.</i>
        """

        await update.message.reply_html(
            welcome_message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )
        # تحديث إحصائية البدء
        db.update_statistics('start')
    except Exception as e:
        logger.error(f"خطأ في start_command للمستخدم {user.id}: {str(e)}", exc_info=True)
        try:
            # محاولة إرسال رسالة خطأ بسيطة
            await update.message.reply_text("حدث خطأ ما. يرجى المحاولة مرة أخرى.")
        except Exception as inner_e:
             logger.error(f"فشل إرسال رسالة الخطأ في start_command للمستخدم {user.id}: {inner_e}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /help وإظهار الإحصائيات"""
    if not db:
        await update.message.reply_text("حدث خطأ في تهيئة البوت. لا يمكن عرض الإحصائيات.")
        return

    try:
        stats = db.get_statistics()
        total_hadiths_cursor = db.conn.execute("SELECT COUNT(*) FROM hadiths")
        total_hadiths = total_hadiths_cursor.fetchone()[0]

        response = f"""
        <b>ℹ️ مساعدة وإحصائيات</b>

        <b>طريقة الاستخدام:</b>
        أرسل <code>شيعة [جزء من نص الحديث]</code> للبحث.

        <b>📊 الإحصائيات التقريبية:</b>
        • إجمالي الأحاديث المفهرسة: <code>{total_hadiths:,}</code>
        • عمليات البحث المنفذة: <code>{stats.get('search', 0):,}</code>
        • مرات استخدام أمر /start: <code>{stats.get('start', 0):,}</code>
        <i>(الإحصائيات قد لا تكون محدثة لحظيًا)</i>

        لأي استفسار أو مشكلة، يمكنك التواصل مع المطور (إذا كان متاحًا).
        قناة البوت: @shia_b0t
        """
        await update.message.reply_html(response, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"خطأ في help_command: {str(e)}", exc_info=True)
        await update.message.reply_text("❌ حدث خطأ أثناء استرجاع الإحصائيات.")


def highlight_keywords(text: str, query: str) -> str:
    """تسليط الضوء على الكلمات المفتاحية (مع تطبيعها) في النص باستخدام HTML bold"""
    if not query or not text:
        return text

    highlighted_text = text
    normalized_query = db.normalize_arabic(query.strip()) # تأكد من تطبيع الاستعلام

    # استخدم set لتجنب تكرار التظليل لنفس الكلمة
    processed_terms = set()
    for term in normalized_query.split():
        term = term.strip()
        if not term or term in processed_terms:
            continue
        processed_terms.add(term)

        # إنشاء قائمة بالمتغيرات المحتملة (مع وبدون الواو)
        variants = [term]
        if term.startswith('و') and len(term) > 1:
            variants.append(term[1:])
        else:
            variants.append(f'و{term}')

        # تظليل كل متغير في النص (مع تجاهل حالة الأحرف والحروف المجاورة باستخدام حدود الكلمات \b)
        for variant in set(variants): # استخدم set لتجنب تكرار المتغيرات
            try:
                # استخدام حدود الكلمات (\b) لتجنب مطابقة جزء من كلمة أخرى
                # واستخدام re.escape لضمان التعامل مع أي رموز خاصة في المتغير
                # نستخدم الدالة lambda في re.sub للحفاظ على النص الأصلي داخل التظليل
                 highlighted_text = re.sub(
                     r'\b' + re.escape(variant) + r'\b',
                     lambda match: f'<b>{match.group(0)}</b>',
                     highlighted_text,
                     flags=re.IGNORECASE # على الرغم من أن العربية لا تعتمد على الحالة، إلا أنه لا يضر
                 )
            except re.error as e:
                 logger.warning(f"خطأ Regex أثناء محاولة تظليل '{variant}': {e}")

    return highlighted_text


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة طلبات البحث عن الأحاديث"""
    if not db:
        await update.message.reply_text("حدث خطأ في تهيئة البوت. لا يمكن البحث حاليًا.")
        return

    user = update.effective_user
    message_text = update.message.text

    # 1. التحقق المبدئي
    if not message_text or not message_text.lower().startswith('شيعة'):
        # يمكنك إضافة رسالة هنا إذا أردت إخبار المستخدم بالتنسيق الصحيح
        # logger.info(f"رسالة تم تجاهلها من {user.id}: لا تبدأ بـ 'شيعة'")
        return

    # 2. التحقق من حد المعدل
    if await check_rate_limit(user.id):
        try:
            await update.message.reply_text("⏳ لقد أرسلت طلبات كثيرة بسرعة. الرجاء الانتظار قليلاً ثم المحاولة مرة أخرى.")
        except Exception as e:
            logger.error(f"فشل في إرسال رسالة حد المعدل للمستخدم {user.id}: {e}")
        return

    # 3. استخلاص نص البحث والتحقق من طوله
    query = message_text[4:].strip() # إزالة "شيعة" والمسافات
    if not query or len(query) < SEARCH_CONFIG['min_query_length']:
        try:
            await update.message.reply_text(
                f"⚠️ الرجاء إدخال نص للبحث بعد كلمة 'شيعة'.\n"
                f"يجب أن يكون طول النص {SEARCH_CONFIG['min_query_length']} أحرف على الأقل."
            )
        except Exception as e:
            logger.error(f"فشل في إرسال رسالة طول البحث للمستخدم {user.id}: {e}")
        return

    logger.info(f"بدء البحث عن '{query}' للمستخدم {user.id}")
    # تحديث إحصائية البحث
    db.update_statistics('search')

    # 4. تنفيذ البحث
    try:
        results = db.search_hadiths(query)
        total_found = len(results)
        logger.info(f"تم العثور على {total_found} نتيجة أولية لـ '{query}' للمستخدم {user.id}")

        if not results:
            await update.message.reply_html("❌ لم يتم العثور على نتائج تطابق بحثك. حاول استخدام كلمات أخرى أو تحقق من الإملاء.")
            return

        # 5. معالجة وعرض النتائج
        response_parts = [] # قائمة لتجميع الأحاديث القصيرة
        sent_count = 0
        sent_hadith_ids = set() # تتبع الأحاديث المرسلة لتجنب التكرار

        # رسالة أولية بعدد النتائج
        initial_message = f"<b>🔍 تم العثور على {total_found} نتيجة تقريبية لـ \"{query}\":</b>\n"
        if total_found > SEARCH_CONFIG['max_display']:
            initial_message += f"<i>(سيتم عرض أول {SEARCH_CONFIG['max_display']} نتيجة فقط)</i>\n"
        await update.message.reply_html(initial_message)
        await asyncio.sleep(0.1) # فاصل بسيط

        for hadith in results:
            if sent_count >= SEARCH_CONFIG['max_display']:
                break # توقف عن المعالجة إذا وصلنا للحد الأقصى للعرض

            hadith_id = hadith['id']
            if hadith_id in sent_hadith_ids:
                continue # تخطي الحديث المكرر

            sent_hadith_ids.add(hadith_id)
            sent_count += 1

            text = hadith['text']
            book = hadith['book']
            grading = hadith['grading'] if hadith['grading'] else "غير مصنف"

            # إنشاء مقتطف وتسليط الضوء على الكلمات المفتاحية
            # حاول إيجاد أول كلمة مفتاحية في النص لعرض المقتطف حولها
            first_match_index = -1
            normalized_query_terms = db.normalize_arabic(query).split()
            normalized_text = db.normalize_arabic(text)
            for term in normalized_query_terms:
                 if not term: continue
                 try:
                      # ابحث عن الكلمة أو الكلمة مع الواو
                      term_pattern = r'\b(?:و?' + re.escape(term) + r')\b'
                      match = re.search(term_pattern, normalized_text, re.IGNORECASE)
                      if match:
                           first_match_index = match.start()
                           break
                 except re.error:
                      pass # تجاهل أخطاء regex هنا

            if first_match_index != -1:
                 start = max(0, first_match_index - SEARCH_CONFIG['max_snippet_length'] // 3)
                 end = min(len(text), first_match_index + (SEARCH_CONFIG['max_snippet_length'] * 2) // 3)
                 # ابحث عن مسافات لقص المقتطف بشكل أنظف
                 start = text.find(' ', start) + 1 if text.find(' ', start) != -1 else start
                 end = text.rfind(' ', start, end) if text.rfind(' ', start, end) != -1 else end
                 snippet = text[start:end]
                 if start > 0: snippet = "..." + snippet
                 if end < len(text): snippet = snippet + "..."
            else:
                 # إذا لم يتم العثور على تطابق (نادر الحدوث)، خذ بداية النص
                 snippet = text[:SEARCH_CONFIG['max_snippet_length']]
                 if len(text) > SEARCH_CONFIG['max_snippet_length']:
                      snippet += "..."

            # تسليط الضوء على الكلمات في المقتطف
            highlighted_snippet = highlight_keywords(snippet, query)

            # بناء جزء الرسالة لهذا الحديث
            hadith_entry = (
                f"<b>{sent_count}. {book}</b>\n"
                f"<i>{highlighted_snippet}</i>\n"
                f"<b>التصحيح:</b> {grading}\n"
                f"<a href=\"https://example.com/hadith/{hadith_id}\">رابط (مثال)</a>\n" # استبدل بالرابط الفعلي إن وجد
            )


            # التحقق مما إذا كان النص الكامل طويلاً
            if len(text) > APPROX_SPLIT_POINT:
                # إذا كان النص طويلاً، أرسل الإدخال الحالي فورًا مع زر "للمزيد"
                keyboard = [[InlineKeyboardButton(
                    "📜 عرض النص الكامل", callback_data=f"hadith_full:{hadith_id}"
                )]]
                try:
                    await update.message.reply_html(hadith_entry, reply_markup=InlineKeyboardMarkup(keyboard))
                    await asyncio.sleep(0.2) # فاصل بسيط بين الرسائل
                except Exception as e:
                     logger.error(f"فشل إرسال جزء حديث طويل {hadith_id}: {e}")
            else:
                # إذا كان النص قصيرًا، أضفه إلى القائمة لتجميعه
                response_parts.append(hadith_entry)

                # إرسال الدفعة إذا وصلت إلى حد معين لتجنب رسالة واحدة طويلة جدًا
                if len(response_parts) >= 5: # إرسال كل 5 نتائج قصيرة مثلاً
                     full_message = "\n".join(response_parts)
                     # تحقق من الطول قبل الإرسال
                     if len(full_message) > MAX_MESSAGE_LENGTH:
                          # هذا لا ينبغي أن يحدث كثيرًا إذا كانت النصوص قصيرة، لكنه احتياط
                          await update.message.reply_html("<i>... (نتائج إضافية) ...</i>") # أو قسمها
                          response_parts = [hadith_entry] # ابدأ دفعة جديدة
                     else:
                         try:
                              await update.message.reply_html(full_message)
                              response_parts = [] # أفرغ القائمة بعد الإرسال
                              await asyncio.sleep(0.2)
                         except Exception as e:
                             logger.error(f"فشل إرسال دفعة نتائج بحث: {e}")
                             # حاول إرسال الإدخال الحالي بمفرده
                             try:
                                  await update.message.reply_html(hadith_entry)
                                  response_parts = []
                                  await asyncio.sleep(0.2)
                             except Exception as inner_e:
                                  logger.error(f"فشل إرسال إدخال فردي بعد فشل الدفعة: {inner_e}")
                                  response_parts = [] # تجاهل هذه الدفعة لمنع تكرار الأخطاء


        # إرسال أي أجزاء متبقية في القائمة
        if response_parts:
            try:
                await update.message.reply_html("\n".join(response_parts))
            except Exception as e:
                 logger.error(f"فشل إرسال الدفعة الأخيرة من نتائج البحث: {e}")


    except Exception as e:
        logger.error(f"خطأ فادح في handle_search للمستخدم {user.id} عن '{query}': {str(e)}", exc_info=True)
        try:
            await update.message.reply_text("❌ حدث خطأ غير متوقع أثناء معالجة طلب البحث. يرجى المحاولة مرة أخرى لاحقًا.")
        except Exception as inner_e:
             logger.error(f"فشل إرسال رسالة الخطأ العامة في handle_search: {inner_e}")


async def hadith_full_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الضغط على زر 'عرض النص الكامل'"""
    if not db:
        await update.callback_query.answer("حدث خطأ في تهيئة البوت.", show_alert=True)
        return

    query: CallbackQuery = update.callback_query
    try:
        await query.answer("جاري جلب النص الكامل...") # رد فوري للمستخدم
        callback_data = query.data
        if not callback_data or not callback_data.startswith("hadith_full:"):
             logger.warning(f"بيانات رد اتصال غير صالحة: {callback_data}")
             await query.message.reply_text("حدث خطأ في بيانات الزر.")
             return

        hadith_id_str = callback_data.split(":", 1)[1]
        if not hadith_id_str.isdigit():
             logger.warning(f"معرف حديث غير صالح في رد الاتصال: {hadith_id_str}")
             await query.message.reply_text("معرف الحديث غير صالح.")
             return

        hadith_id = int(hadith_id_str)

        with db.conn:
            cursor = db.conn.execute("SELECT text, book, grading FROM hadiths WHERE id = ?", (hadith_id,))
            result = cursor.fetchone()

        if result:
            full_text = result['text']
            book = result['book']
            grading = result['grading'] if result['grading'] else "غير مصنف"

            # تقسيم النص الكامل إلى أجزاء إذا لزم الأمر
            text_parts = split_text(full_text, MAX_MESSAGE_LENGTH - 50) # اترك بعض المساحة للترويسة

            header = f"📜 <b>النص الكامل للحديث (رقم {hadith_id})</b>\n" \
                     f"📚 <b>الكتاب:</b> {book}\n" \
                     f"📌 <b>التصحيح:</b> {grading}\n" \
                     f"➖➖➖➖➖➖➖➖➖➖\n"

            first_part = True
            for part in text_parts:
                message_part = (header + part) if first_part else part
                try:
                    # استخدام edit_message_text للرسالة الأولى، و reply_html للبقية
                    if first_part:
                        await query.edit_message_text(message_part, parse_mode='HTML')
                        first_part = False
                    else:
                        await query.message.reply_html(message_part)
                    await asyncio.sleep(0.3) # فاصل بين الأجزاء
                except Exception as e:
                    logger.error(f"فشل في إرسال جزء من النص الكامل للحديث {hadith_id}: {e}")
                    # حاول إرسال رسالة خطأ للمستخدم
                    if first_part:
                         await query.message.reply_text("حدث خطأ أثناء إرسال النص الكامل.")
                    break # توقف عن إرسال الأجزاء إذا حدث خطأ

        else:
            logger.warning(f"لم يتم العثور على الحديث بالمعرف {hadith_id} لرد الاتصال.")
            await query.edit_message_text("لم يتم العثور على الحديث المطلوب.")

    except sqlite3.Error as e:
        logger.error(f"خطأ SQLite في hadith_full_callback للحديث {hadith_id_str}: {e}")
        try:
             # استخدم edit_message_text إذا أمكن لتعديل الرسالة الأصلية
             await query.edit_message_text("حدث خطأ في قاعدة البيانات أثناء جلب الحديث.")
        except Exception:
             # إذا فشل التعديل، أرسل ردًا جديدًا
             await query.message.reply_text("حدث خطأ في قاعدة البيانات أثناء جلب الحديث.")
    except Exception as e:
        logger.error(f"خطأ غير متوقع في hadith_full_callback للحديث {hadith_id_str}: {e}", exc_info=True)
        try:
            await query.edit_message_text("حدث خطأ غير متوقع أثناء عرض النص الكامل.")
        except Exception:
            await query.message.reply_text("حدث خطأ غير متوقع أثناء عرض النص الكامل.")


async def real_time_analytics():
    """
    مهمة خلفية لمعالجة التحليلات في الوقت الحقيقي باستخدام Redis Streams (إذا تم تمكينها).
    ملاحظة: هذه الوظيفة تتطلب أن يقوم جزء آخر من الكود بإضافة بيانات إلى 'analytics_stream'.
    """
    if not db or not db.redis:
        logger.info("Redis غير متاح، لن يتم تشغيل مهمة real_time_analytics.")
        return

    stream_key = 'analytics_stream'
    last_id = '$' # ابدأ بالقراءة من آخر رسالة

    logger.info(f"بدء مهمة الاستماع إلى Redis Stream: {stream_key}")
    while True:
        try:
            # استخدام block=0 يعني عدم الانتظار إذا لم تكن هناك رسائل جديدة
            # يمكنك استخدام block=ms (مثل 5000 لـ 5 ثوانٍ) للانتظار
            response = db.redis.xread({stream_key: last_id}, block=5000, count=100) # قراءة 100 رسالة كل 5 ثوانٍ

            if not response:
                continue # لا توجد رسائل جديدة

            for stream, messages in response:
                for message_id, message_data in messages:
                    last_id = message_id # تحديث آخر معرف تمت قراءته
                    try:
                        # --- هنا تقوم بمعالجة بيانات التحليلات ---
                        # مثال: تسجيل البيانات أو تحديث عدادات أخرى
                        logger.info(f"[Analytics] Received: ID={message_id}, Data={message_data}")
                        # -----------------------------------------

                        # حذف الرسالة من الـ Stream بعد معالجتها (اختياري)
                        # db.redis.xdel(stream_key, message_id)

                    except Exception as e:
                        logger.error(f"خطأ أثناء معالجة رسالة التحليلات {message_id}: {e}", exc_info=True)
                        # قرر ما إذا كنت ستستمر أم تتوقف عند الخطأ

                # قد ترغب في إضافة تأخير بسيط هنا إذا كان هناك تدفق كبير للرسائل
                # await asyncio.sleep(0.1)

        except redis.exceptions.ConnectionError as e:
            logger.error(f"خطأ اتصال Redis في real_time_analytics: {e}. إعادة المحاولة خلال 10 ثوانٍ...")
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"خطأ غير متوقع في real_time_analytics: {e}", exc_info=True)
            await asyncio.sleep(10) # انتظر قبل إعادة المحاولة

# ---------------------- التشغيل الرئيسي ----------------------
def main():
    """تهيئة وتشغيل البوت"""
    global db # اجعل db متاحًا هنا للتحقق منه

    if not BOT_TOKEN:
        logger.critical("لا يمكن بدء البوت: لم يتم توفير BOT_TOKEN.")
        return # خروج إذا لم يتم توفير التوكن

    if not db:
         logger.critical("لا يمكن بدء البوت: فشل تهيئة قاعدة البيانات.")
         return # خروج إذا فشلت تهيئة قاعدة البيانات


    try:
        # بناء التطبيق
        application = Application.builder().token(BOT_TOKEN).build()

        # إضافة معالجات الأوامر
        application.add_handler(CommandHandler('start', start_command))
        application.add_handler(CommandHandler('help', help_command))

        # إضافة معالج ردود الأزرار (CallbackQuery)
        application.add_handler(CallbackQueryHandler(hadith_full_callback, pattern=r"^hadith_full:"))

        # إضافة معالج الرسائل النصية (للبحث)
        # تأكد من أنه لا يلتقط الأوامر
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_search
        ))

        # بدء مهمة التحليلات في الخلفية (إذا كان Redis متاحًا)
        # يتم تشغيلها مرة واحدة عند بدء التشغيل، وهي ستبدأ حلقة المراقبة الخاصة بها
        # application.job_queue غير متاح مباشرة في Application builder،
        # تحتاج إلى الوصول إليه بعد إنشاء الكائن أو استخدام asyncio.create_task
        # asyncio.create_task(real_time_analytics()) # الطريقة المباشرة باستخدام asyncio

        logger.info("جاري تشغيل البوت...")
        # بدء البوت باستخدام Polling
        application.run_polling(
            allowed_updates=Update.ALL_TYPES, # اسمح بجميع أنواع التحديثات
            poll_interval=0.5, # تقليل الفاصل الزمني للاستجابة أسرع قليلاً
            timeout=30,
            drop_pending_updates=True # تجاهل التحديثات القديمة عند إعادة التشغيل
        )

    except Exception as e:
        logger.critical(f"فشل فادح في تشغيل البوت: {e}", exc_info=True)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("تم استلام إشارة إيقاف (Ctrl+C). جاري إيقاف البوت...")
    except Exception as e:
        # تسجيل أي خطأ غير متوقع يحدث خارج دالة main
        logger.critical(f"خطأ غير متوقع في المستوى الأعلى: {e}", exc_info=True)
    finally:
        # يمكنك إضافة أي عمليات تنظيف هنا إذا لزم الأمر
        logger.info("تم إيقاف البوت.")
