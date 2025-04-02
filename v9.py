# -*- coding: utf-8 -*-
import sqlite3
import json
import re
import asyncio
import logging
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, constants
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)
from telegram.error import TelegramError, BadRequest
import redis
from typing import Dict, List, Optional, Tuple, Any

# Import fuzzy matching library
from thefuzz import fuzz, process

# ---------------------- إعدادات النظام ----------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Configuration ---
DATABASE_NAME = os.getenv('DATABASE_NAME', 'v4.db') # Changed DB name for new structure/features
JSON_DATA_SOURCE = os.getenv('JSON_DATA_SOURCE', 'input.json')
BOT_TOKEN = os.getenv('BOT_TOKEN')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))

# --- Constants ---
MAX_MESSAGE_LENGTH = constants.MessageLimit.TEXT_LENGTH
SEARCH_CONFIG = {
    'max_display_warning': 20,
    'min_query_length': 3,
    'rate_limit_per_minute': 15,
    'max_snippet_length': 150,
    'fts_result_limit': 50,      # How many results to fetch initially from FTS
    'fuzzy_score_threshold': 75, # Minimum score for fuzzy match (partial_ratio)
    'fuzzy_max_results': 10,     # Max results to show from fuzzy search if FTS fails
    'max_search_history': 50     # Max search queries to store per user in Redis
}

REDIS_CONFIG = {
    'host': REDIS_HOST,
    'port': REDIS_PORT,
    'db': REDIS_DB,
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5
}

# ---------------------- فئات المساعدة ----------------------
class HadithDatabase:
    """فئة متخصصة في إدارة عمليات قاعدة البيانات والتخزين المؤقت"""

    def __init__(self):
        """تهيئة اتصالات قاعدة البيانات و Redis"""
        self.redis = None
        try:
            self.redis = redis.Redis(**REDIS_CONFIG)
            self.redis.ping()
            logger.info("Successfully connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"فشل الاتصال بـ Redis: {str(e)}. Bot will run without caching, rate limiting, and history.")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Redis connection: {e}")

        try:
            # Enable Write-Ahead Logging for better concurrency
            self.conn = sqlite3.connect(
                DATABASE_NAME,
                check_same_thread=False,
                isolation_level=None, # Autocommit
                timeout=30
            )
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.row_factory = sqlite3.Row
            self._initialize_database()
            logger.info(f"Successfully connected to SQLite database: {DATABASE_NAME}")
        except sqlite3.Error as e:
            logger.error(f"فشل في تهيئة قاعدة البيانات SQLite: {str(e)}")
            raise

    def _initialize_database(self):
        """تهيئة الجداول والفهارس إذا لم تكن موجودة"""
        try:
            with self.conn:
                # جدول الأحاديث الأساسي
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS hadiths (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        book TEXT NOT NULL,
                        text TEXT NOT NULL UNIQUE,
                        normalized_text TEXT, -- Store normalized text for potential fuzzy matching optimization
                        grading TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')

                # Add index on normalized_text if using it for fuzzy matching later
                self.conn.execute('CREATE INDEX IF NOT EXISTS idx_normalized_text ON hadiths(normalized_text)')

                # جدول البحث الفوري باستخدام FTS5
                self.conn.execute('''
                    CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts
                    USING fts5(
                        text,
                        content='hadiths',
                        content_rowid='id',
                        tokenize='unicode61 remove_diacritics 2',
                        prefix='1 2 3'
                    )''')

                # Triggers to keep FTS table synchronized
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_ai AFTER INSERT ON hadiths BEGIN
                        INSERT INTO hadiths_fts (rowid, text) VALUES (new.id, new.text);
                    END;
                ''')
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_ad AFTER DELETE ON hadiths BEGIN
                        DELETE FROM hadiths_fts WHERE rowid=old.id;
                    END;
                ''')
                # Update trigger needs to handle normalized_text as well if it changes
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_au AFTER UPDATE ON hadiths BEGIN
                        UPDATE hadiths_fts SET text = new.text WHERE rowid=old.id;
                        -- Also update normalized_text in the main table if text changes
                        UPDATE hadiths SET normalized_text = (SELECT _normalize_internal(new.text)) WHERE id=old.id;
                    END;
                ''')

                 # Register the normalization function within SQLite for the trigger
                self.conn.create_function("_normalize_internal", 1, self._sanitize_text)


                # جدول الإحصائيات
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS stats (
                        type TEXT PRIMARY KEY,
                        count INTEGER DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')

                # فهارس لتحسين الأداء
                self.conn.execute('CREATE INDEX IF NOT EXISTS idx_book ON hadiths(book)')
                self.conn.execute('CREATE INDEX IF NOT EXISTS idx_book_grading ON hadiths(book, grading)')

            # Check if initial data loading is needed
            cursor = self.conn.execute('SELECT COUNT(*) FROM hadiths')
            count = cursor.fetchone()[0]
            if count == 0:
                logger.info("Database is empty. Attempting to load initial data...")
                self._load_initial_data()
            else:
                 # Check if normalized_text column needs population
                 try:
                     self.conn.execute("SELECT normalized_text FROM hadiths LIMIT 1")
                 except sqlite3.OperationalError:
                     logger.info("Populating 'normalized_text' column for existing data...")
                     self._populate_normalized_text()

                 logger.info(f"Database already contains {count} hadiths.")

        except sqlite3.Error as e:
            logger.error(f"خطأ في تهيئة الجداول أو الفهارس: {str(e)}")
            raise

    def _populate_normalized_text(self):
        """Adds and populates the normalized_text column if it doesn't exist."""
        try:
            with self.conn:
                self.conn.execute("ALTER TABLE hadiths ADD COLUMN normalized_text TEXT")
                logger.info("Added 'normalized_text' column.")
        except sqlite3.OperationalError:
            logger.info("'normalized_text' column already exists.") # Column likely exists

        try:
            logger.info("Updating 'normalized_text' for all records. This might take a while...")
            cursor = self.conn.execute("SELECT id, text FROM hadiths WHERE normalized_text IS NULL")
            updates = []
            for row in cursor.fetchall():
                normalized = self._sanitize_text(row['text'])
                updates.append((normalized, row['id']))

            if updates:
                with self.conn: # Start transaction
                    self.conn.executemany("UPDATE hadiths SET normalized_text = ? WHERE id = ?", updates)
                logger.info(f"Finished populating 'normalized_text' for {len(updates)} records.")
            else:
                 logger.info("'normalized_text' column seems already populated.")

        except sqlite3.Error as e:
            logger.error(f"Error populating 'normalized_text' column: {e}")


    def _load_initial_data(self):
        """تحميل البيانات الأولية من ملف JSON إذا كانت قاعدة البيانات فارغة"""
        # (Same as before, but ensures normalized_text is populated during import)
        try:
            if not os.path.exists(JSON_DATA_SOURCE):
                 logger.error(f'لم يتم العثور على ملف البيانات المصدر: {JSON_DATA_SOURCE}')
                 return

            with open(JSON_DATA_SOURCE, 'r', encoding='utf-8') as f:
                data = json.load(f)

            logger.info(f"Starting initial data import from {JSON_DATA_SOURCE}...")
            self._import_data(data)
            logger.info("Initial data imported successfully.")

        except json.JSONDecodeError as e:
            logger.error(f'خطأ في تنسيق ملف JSON: {str(e)}')
        except sqlite3.Error as e:
            logger.error(f'خطأ في قاعدة البيانات أثناء تحميل البيانات الأولية: {str(e)}')
        except Exception as e:
            logger.error(f'خطأ غير متوقع أثناء تحميل البيانات الأولية: {str(e)}')

    def _import_data(self, data: List[Dict]):
        """استيراد البيانات إلى قاعدة البيانات مع تخطي النصوص الفارغة وحساب النص المُنظَّم"""
        imported_count = 0
        skipped_count = 0
        batch_size = 500
        batch = []

        try:
            with self.conn: # Use transaction
                for item in data:
                    raw_text = item.get('arabicText', '').strip()
                    if not raw_text:
                        skipped_count += 1
                        continue

                    clean_text = self._sanitize_text(raw_text) # This is the normalized text
                    book = item.get('book', 'غير معروف').strip()
                    grading = item.get('majlisiGrading', 'غير مصنف').strip()

                    batch.append((book, raw_text, clean_text, grading)) # Add normalized_text

                    if len(batch) >= batch_size:
                        self._insert_batch(batch)
                        imported_count += len(batch)
                        batch = []
                        logger.info(f"Imported {imported_count} records...")

                if batch:
                    self._insert_batch(batch)
                    imported_count += len(batch)

            logger.info(f"Data import complete. Imported: {imported_count}, Skipped (empty): {skipped_count}")

        except sqlite3.Error as e:
            logger.error(f'خطأ في استيراد البيانات: {str(e)}')
            raise
        except Exception as e:
            logger.error(f'خطأ غير متوقع أثناء الاستيراد: {str(e)}')
            raise

    def _insert_batch(self, batch: List[tuple]):
        """إدخال دفعة من البيانات (ضمن معاملة موجودة)"""
        try:
            # Insert raw text and normalized text
            self.conn.executemany('''
                INSERT OR IGNORE INTO hadiths (book, text, normalized_text, grading)
                VALUES (?, ?, ?, ?)
            ''', batch)
        except sqlite3.Error as e:
            logger.error(f'Error inserting batch: {e}')
            raise

    def _sanitize_text(self, text: str) -> str:
        """تنظيف النص من التشكيل وتوحيد الأحرف العربية (للفهرسة والمقارنة)"""
        if not isinstance(text, str): return "" # Handle potential non-string input
        # إزالة التشكيل (حركات الإعراب)
        text = re.sub(r'[\u064B-\u065F\u0610-\u061A]', '', text)
        # إزالة التطويل (ــ)
        text = text.replace('ـ', '')
        # توحيد الألف والهاء/التاء المربوطة
        text = self.normalize_arabic(text)
        # إزالة المسافات الزائدة
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def normalize_arabic(text: str) -> str:
        """توحيد أشكال بعض الأحرف العربية للبحث"""
        replacements = {
            'أ': 'ا', 'إ': 'ا', 'آ': 'ا', # توحيد الألف
            'ة': 'ه',                  # توحيد التاء المربوطة
            'ى': 'ي'                   # توحيد الألف المقصورة
        }
        # Ensure text is string before replacement
        if not isinstance(text, str): return ""
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    def search_hadiths_fts(self, query: str, result_limit: int) -> List[Dict]:
         """Perform search using FTS5 only."""
         sanitized_query = self._sanitize_text(query)
         if not sanitized_query: return []

         terms = []
         for term in sanitized_query.split():
             term = term.strip()
             if not term: continue
             if term.startswith('و') and len(term) > 1:
                 terms.append(f'({term} OR {term[1:]})')
             else:
                 terms.append(f'({term} OR "و{term}")')

         if not terms: return []

         fts_query = ' NEAR('.join(terms) + ')' * (len(terms) -1) if len(terms) > 1 else terms[0]
         logger.debug(f"Executing FTS query: {fts_query}")

         try:
             with self.conn:
                 cursor = self.conn.execute(f'''
                     SELECT h.id, h.book, h.text, h.grading
                     FROM hadiths h
                     JOIN hadiths_fts fts ON h.id = fts.rowid
                     WHERE fts.hadiths_fts MATCH ?
                     ORDER BY bm25(fts.hadiths_fts)
                     LIMIT ?
                 ''', (fts_query, result_limit))
                 return [dict(row) for row in cursor.fetchall()]
         except sqlite3.Error as e:
             logger.error(f'خطأ في البحث بقاعدة البيانات (FTS): {str(e)}')
             return []

    def search_hadiths_fuzzy(self, query: str, threshold: int, limit: int) -> List[Dict]:
        """Perform search using fuzzy matching as a fallback."""
        sanitized_query = self._sanitize_text(query)
        if not sanitized_query: return []

        logger.debug(f"Executing Fuzzy search for: {sanitized_query}")
        try:
            # Fetch texts for comparison. Fetching all can be slow!
            # Option 1: Fetch all (slow for large DB)
            # cursor = self.conn.execute("SELECT id, text, normalized_text FROM hadiths")
            # Option 2: Fetch a sample or based on some criteria (faster but might miss results)
            cursor = self.conn.execute("SELECT id, book, text, normalized_text, grading FROM hadiths LIMIT 5000") # Limit scope initially

            all_hadiths = cursor.fetchall()
            if not all_hadiths: return []

            # Prepare choices for fuzzywuzzy: Use normalized text for matching
            # Store mapping from normalized text back to original hadith data
            choices_map = {f"{h['id']}_{h['normalized_text']}": dict(h) for h in all_hadiths if h['normalized_text']}
            choices = list(choices_map.keys())

            if not choices: return []

            # Use process.extract to find best matches
            # partial_ratio is good for finding substrings within longer texts
            fuzzy_results = process.extract(sanitized_query, choices, scorer=fuzz.partial_ratio, limit=limit * 2) # Get more initially

            matched_hadiths = []
            seen_ids = set()
            for match_key, score in fuzzy_results:
                if score >= threshold:
                    hadith_data = choices_map.get(match_key)
                    if hadith_data and hadith_data['id'] not in seen_ids:
                         # Add score for potential ranking later if needed
                         hadith_data['score'] = score
                         matched_hadiths.append(hadith_data)
                         seen_ids.add(hadith_data['id'])
                         if len(matched_hadiths) >= limit:
                              break # Stop once we have enough good matches

            # Sort by score descending (optional, process.extract usually does a good job)
            # matched_hadiths.sort(key=lambda x: x.get('score', 0), reverse=True)

            logger.info(f"Fuzzy search found {len(matched_hadiths)} results with score >= {threshold}")
            return matched_hadiths

        except sqlite3.Error as e:
            logger.error(f'Database error during fuzzy search prep: {e}')
            return []
        except Exception as e:
            logger.error(f'Unexpected error during fuzzy search: {e}')
            return []


    def search_hadiths(self, query: str) -> List[Dict]:
        """
        بحث مركب: يبدأ بـ FTS السريع، وإذا لم يجد نتائج كافية، يستخدم Fuzzy Matching.
        """
        sanitized_query = self._sanitize_text(query)
        cache_key = f'search_v4:{sanitized_query}' # Use different cache key

        # 1. Check Cache
        if self.redis:
            try:
                cached_results = self.redis.get(cache_key)
                if cached_results:
                    logger.debug(f"Cache hit for combined search: {sanitized_query}")
                    return json.loads(cached_results)
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"Redis connection error during cache GET: {e}.")
            except json.JSONDecodeError as e:
                 logger.error(f"Error decoding cached JSON for query '{sanitized_query}': {e}")

        # 2. FTS Search
        results = self.search_hadiths_fts(query, SEARCH_CONFIG['fts_result_limit'])
        logger.info(f"FTS search for '{query}' found {len(results)} results.")

        # 3. Fuzzy Search (if FTS failed or returned few results)
        if not results: # Or maybe if len(results) < some_threshold
            logger.info(f"FTS found no results for '{query}', trying fuzzy search...")
            results = self.search_hadiths_fuzzy(
                query,
                SEARCH_CONFIG['fuzzy_score_threshold'],
                SEARCH_CONFIG['fuzzy_max_results']
            )

        # 4. Cache the final results
        if self.redis:
            try:
                self.redis.setex(cache_key, 300, json.dumps(results)) # Cache for 5 mins
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"Redis connection error during cache SET: {e}.")
            except TypeError as e:
                 logger.error(f"Error serializing results to JSON for caching: {e}")

        return results


    def get_hadith_by_id(self, hadith_id: int) -> Optional[Dict]:
         """استرداد حديث معين بواسطة معرفه (ID)"""
         # (Implementation remains largely the same as v3, maybe adjust cache key/TTL)
         cache_key = f'hadith_v4:{hadith_id}'
         if self.redis:
             try:
                 cached_hadith = self.redis.get(cache_key)
                 if cached_hadith:
                     return json.loads(cached_hadith)
             except redis.exceptions.ConnectionError as e:
                 logger.warning(f"Redis connection error during cache GET for hadith ID {hadith_id}: {e}")
             except json.JSONDecodeError as e:
                 logger.error(f"Error decoding cached JSON for hadith ID {hadith_id}: {e}")

         try:
             with self.conn:
                 cursor = self.conn.execute(
                     "SELECT id, book, text, grading FROM hadiths WHERE id = ?", (hadith_id,)
                 )
                 result = cursor.fetchone()
                 if result:
                     result_dict = dict(result)
                     if self.redis:
                         try:
                             self.redis.setex(cache_key, 3600, json.dumps(result_dict)) # Cache 1 hour
                         except redis.exceptions.ConnectionError as e:
                             logger.warning(f"Redis connection error during cache SET for hadith ID {hadith_id}: {e}")
                         except TypeError as e:
                             logger.error(f"Error serializing hadith {hadith_id} to JSON for caching: {e}")
                     return result_dict
                 else:
                     return None
         except sqlite3.Error as e:
             logger.error(f'خطأ في استرداد الحديث بالمعرف {hadith_id}: {str(e)}')
             return None

    def log_search_query(self, user_id: int, query: str):
        """Log user's search query into Redis."""
        if not self.redis:
            return # Cannot log without Redis

        key = f"user_search_history:{user_id}"
        try:
            # Add the query to the beginning of the list
            self.redis.lpush(key, query)
            # Trim the list to keep only the latest N queries
            self.redis.ltrim(key, 0, SEARCH_CONFIG['max_search_history'] - 1)
            logger.debug(f"Logged search query for user {user_id}: {query}")
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"Redis connection error logging search history for user {user_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error logging search history for user {user_id}: {e}")


    def update_statistics(self, stat_type: str):
        """تحديث الإحصائيات"""
        # (Same as v3)
        if self.redis:
            try:
                redis_key = f'stat:{stat_type}'
                self.redis.incr(redis_key)
            except redis.exceptions.ConnectionError as e:
                logger.warning(f'Redis connection error during stats update for {stat_type}: {e}. Updating SQLite directly.')
                self._sync_stat_to_db(stat_type)
            except Exception as e:
                 logger.error(f"Unexpected error updating Redis stat '{stat_type}': {e}")
                 self._sync_stat_to_db(stat_type)
        else:
            self._sync_stat_to_db(stat_type)

    def _sync_stat_to_db(self, stat_type: str, count: Optional[int] = None):
        """تحديث عداد الإحصاء في قاعدة بيانات SQLite"""
        # (Same as v3)
        try:
            with self.conn:
                if count is not None:
                     self.conn.execute('''
                        INSERT INTO stats (type, count, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(type) DO UPDATE SET count = excluded.count, last_updated = CURRENT_TIMESTAMP
                    ''', (stat_type, count))
                else:
                     self.conn.execute('''
                        INSERT INTO stats (type, count, last_updated) VALUES (?, 1, CURRENT_TIMESTAMP)
                        ON CONFLICT(type) DO UPDATE SET count = count + 1, last_updated = CURRENT_TIMESTAMP
                    ''', (stat_type,))
        except sqlite3.Error as e:
            logger.error(f'خطأ في تحديث إحصائيات SQLite لـ {stat_type}: {str(e)}')

    def get_statistics(self) -> Dict[str, int]:
        """استرجاع الإحصائيات من SQLite"""
        # (Same as v3)
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT type, count FROM stats')
                return {row['type']: row['count'] for row in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f'خطأ في استرجاع الإحصائيات من SQLite: {str(e)}')
            return {}

    def close(self):
        """إغلاق اتصالات قاعدة البيانات و Redis"""
        # (Same as v3)
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed.")
        if self.redis:
            try:
                self.redis.close()
                logger.info("Redis connection closed.")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")


# ---------------------- إدارة البوت ----------------------
try:
    db = HadithDatabase()
except Exception as e:
    logger.critical(f"Failed to initialize HadithDatabase: {e}. Bot cannot start.")
    exit(1)

async def check_rate_limit(user_id: int) -> bool:
    """التحقق من معدل الطلبات للمستخدم باستخدام Redis"""
    # (Same as v3)
    if not db.redis: return False
    key = f"ratelimit:{user_id}"
    try:
        current = db.redis.incr(key)
        if current == 1: db.redis.expire(key, 60)
        return current > SEARCH_CONFIG['rate_limit_per_minute']
    except redis.exceptions.ConnectionError as e:
        logger.warning(f"Redis connection error during rate limit check for user {user_id}: {e}. Allowing request.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during rate limit check for user {user_id}: {e}. Allowing request.")
        return False

def split_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """تقسيم النص الطويل إلى أجزاء"""
    # (Same as v3)
    if len(text) <= max_length: return [text]
    parts = []
    while len(text) > 0:
        if len(text) <= max_length:
            parts.append(text)
            break
        else:
            split_index = text.rfind(' ', 0, max_length)
            if split_index == -1: split_index = max_length
            parts.append(text[:split_index])
            text = text[split_index:].lstrip()
    return parts

def highlight_keywords(text: str, query: str) -> str:
    """تسليط الضوء على الكلمات المفتاحية (من الاستعلام الأصلي) في النص."""
    # (Improved version from v3, should work reasonably well)
    highlighted_text = text
    # Use the *original* query for highlighting, but normalize it for finding terms
    normalized_query_for_terms = db.normalize_arabic(query)
    query_terms = set(term for term in normalized_query_for_terms.split() if term)

    if not query_terms: return text

    variants_to_highlight = set()
    for term in query_terms:
         variants_to_highlight.add(term)
         # Add variants with/without 'و' based on the *normalized* term
         if term.startswith('و') and len(term) > 1: variants_to_highlight.add(term[1:])
         else: variants_to_highlight.add(f'و{term}')

    # Filter out empty strings that might result from splitting
    variants_to_highlight = {v for v in variants_to_highlight if v}
    if not variants_to_highlight: return text


    sorted_variants = sorted(list(variants_to_highlight), key=len, reverse=True)
    # Escape terms for regex and join with |
    # Add word boundaries (\b) to match whole words more accurately,
    # but handle Arabic characters correctly (may need adjustment)
    # Using simple non-boundary matching for wider compatibility first.
    pattern = '|'.join(re.escape(variant) for variant in sorted_variants)

    if not pattern: return text

    processed_indices = set()
    def replace_match(match):
        start, end = match.span()
        if any(i in processed_indices for i in range(start, end)):
            return match.group(0)
        else:
            for i in range(start, end): processed_indices.add(i)
            return f"<b>{match.group(0)}</b>"
    try:
        highlighted_text = re.sub(pattern, replace_match, text, flags=re.IGNORECASE)
    except re.error as e:
        logger.error(f"Regex error during highlighting for pattern '{pattern}': {e}")
        return text # Return original text on regex error
    except Exception as e:
        logger.error(f"Unexpected error during highlighting: {e}")
        return text

    return highlighted_text


def create_result_snippet(hadith: Dict, query: str, max_len: int) -> str:
    """إنشاء مقتطف للنتيجة مع تسليط الضوء على الكلمات المفتاحية."""
    # (Same as v3, uses the improved highlight_keywords)
    text = hadith['text']
    highlighted_text = highlight_keywords(text, query) # Use original query for highlighting

    if len(highlighted_text) <= max_len:
        return highlighted_text

    first_highlight_index = highlighted_text.find('<b>')
    start_index = 0
    if first_highlight_index > max_len * 0.6 : # Adjust centering logic if needed
         start_index = max(0, first_highlight_index - int(max_len / 3))

    # Ensure start index doesn't split inside a tag if possible (basic check)
    potential_snippet = highlighted_text[start_index:]
    first_space = potential_snippet.find(' ')
    if start_index > 0 and first_space != -1 and first_space < 10: # Avoid starting with partial word
        start_index += first_space + 1

    snippet = highlighted_text[start_index : start_index + max_len]

    prefix = "..." if start_index > 0 else ""
    suffix = "..." if start_index + max_len < len(highlighted_text) else ""

    # Basic tag balancing
    if snippet.count('<b>') > snippet.count('</b>'): snippet += '</b>'
    # This case is harder, might leave opening tag if prefix is added
    # elif snippet.count('</b>') > snippet.count('<b>') and prefix == "...": pass

    # Ensure snippet doesn't end mid-tag
    last_open = snippet.rfind('<')
    last_close = snippet.rfind('>')
    if last_open > last_close:
        snippet = snippet[:last_open]
        suffix = "..." # Ensure suffix is added if we truncated

    return f"{prefix}{snippet}{suffix}"


# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /start (بدون وسائط)"""
    # (Same as v3 - displays welcome message)
    user = update.effective_user
    bot_username = context.bot.username
    start_group_url = f"https://t.me/{bot_username}?startgroup=true"
    keyboard = [[InlineKeyboardButton("➕ أضفني إلى مجموعتك", url=start_group_url)]]
    welcome_message = f"""
    <b>مرحبا {user.first_name}! 👋</b>
    أنا بوت كاشف أحاديث الشيعة. قاعدة بياناتي تحتوي على آلاف الأحاديث من مصادر متعددة. 🔍

    📚 <b>المصادر الحالية تشمل (على سبيل المثال):</b>
    - الكافي للكليني (مع تصحيح مرآة العقول)
    - عيون أخبار الرضا للصدوق
    - نهج البلاغة
    - الخصال للصدوق
    - الأمالي (للصدوق والمفيد)
    - التوحيد للصدوق
    - فضائل الشيعة للصدوق
    - كامل الزيارات
    - الغيبة (للنعماني والطوسي)
    - والمزيد قيد الإضافة...

    💡 <b>طريقة الاستخدام:</b>
    أرسل كلمة <code>شيعة</code> متبوعة بجزء من نص الحديث الذي تبحث عنه.

    <b>مثال:</b>
    <code>شيعة باهتوهم</code>
    أو فقط أرسل الكلمات المفتاحية مباشرة:
    <code>باهتوهم</code>

    <i>قناة البوت (إذا وجدت):</i> @shia_b0t
    نسألكم الدعاء لوالدي بالرحمة والمغفرة إن استفدتم من هذا العمل.
    """
    try:
        await update.message.reply_html(
            welcome_message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )
        db.update_statistics('start_command')
    except TelegramError as e:
        logger.error(f"TelegramError in start_command for user {user.id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in start_command for user {user.id}: {str(e)}")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /help وعرض الإحصائيات"""
    # (Same as v3)
    try:
        stats = db.get_statistics()
        stats_lines = [f"• {stype.replace('_', ' ').title()}: <code>{count}</code>"
                       for stype, count in stats.items()]
        stats_text = "\n".join(stats_lines) if stats_lines else "لا توجد إحصائيات متاحة حالياً."
        help_message = f"""
        <b>مساعدة وإحصائيات 📊</b>

        <b>كيفية البحث:</b>
        أرسل <code>شيعة</code> ثم الكلمات التي تبحث عنها, أو أرسل الكلمات مباشرةً.
        البحث يدعم الآن إيجاد الكلمات حتى لو كانت مختلفة قليلاً (مثل 'انه' و 'لقد').
        مثال: <code>شيعة انما الاعمال بالنيات</code>
        مثال: <code>الاعمال بالنيات</code>

        <b>الإحصائيات الحالية:</b>
        {stats_text}
        """
        await update.message.reply_html(help_message)
        db.update_statistics('help_command')
    except TelegramError as e:
        logger.error(f"TelegramError in help_command: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in help_command: {str(e)}")
        try: await update.message.reply_text("❌ حدث خطأ أثناء استرجاع الإحصائيات.")
        except TelegramError: pass


async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة رسائل البحث"""
    # (Modified to use combined search and log queries)
    if not update.message or not update.message.text: return

    user = update.effective_user
    query_text = update.message.text.strip()

    if query_text.lower().startswith('شيعة'):
        query = query_text[5:].strip()
    else:
        query = query_text

    # 1. Validation and Rate Limit
    if not query or len(query) < SEARCH_CONFIG['min_query_length']:
        await update.message.reply_text(f"⚠️ الرجاء إدخال نص للبحث ({SEARCH_CONFIG['min_query_length']} أحرف على الأقل).")
        return
    if await check_rate_limit(user.id):
        await update.message.reply_text("⏳ لقد تجاوزت الحد المسموح به من الطلبات. الرجاء الانتظار قليلاً.")
        return

    # 2. Log Search Query & Update Stats
    db.log_search_query(user.id, query)
    db.update_statistics('search_query')

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=constants.ChatAction.TYPING)

        # 3. Perform Combined Search (FTS + Fuzzy Fallback)
        results = db.search_hadiths(query) # Uses the combined search method
        total_found = len(results)

        if not results:
            await update.message.reply_html(f"⚠️ لم يتم العثور على نتائج للبحث عن: \"<code>{query}</code>\"")
            return

        # 4. Build and Send Response
        response_parts = []
        response_header = f"<b>🔍 تم العثور على {total_found} نتيجة للبحث عن \"<code>{query}</code>\":</b>\n{'-'*20}\n"
        # response_parts.append(response_header) # Add header to the first message part later

        current_message = response_header # Start first message with header
        results_to_display = results[:SEARCH_CONFIG['max_display_warning']]

        for idx, hadith in enumerate(results_to_display, 1):
            # Use original query for snippet highlighting
            snippet = create_result_snippet(hadith, query, SEARCH_CONFIG['max_snippet_length'])
            # Use deep link format for "عرض كامل"
            deep_link_url = f"https://t.me/{context.bot.username}?start=hadith_{hadith['id']}"

            hadith_entry = (
                f"<b>{idx}.</b> {snippet}\n"
                f"📚 <b>الكتاب:</b> {hadith['book']}\n"
                f"📌 <b>الصحة:</b> {hadith['grading'] or 'غير مصنف'}\n"
                f"<a href=\"{deep_link_url}\">🔗 عرض كامل</a> | ID: {hadith['id']}\n"
                # Add fuzzy score if available (mostly for debugging/info)
                # f"Score: {hadith.get('score', 'N/A')}\n"
                f"{'-'*20}\n"
            )

            if len(current_message) + len(hadith_entry) > MAX_MESSAGE_LENGTH:
                response_parts.append(current_message)
                current_message = hadith_entry
            else:
                current_message += hadith_entry

        if current_message: response_parts.append(current_message)

        if total_found > len(results_to_display):
             warning = (f"\n⚠️ <b>ملاحظة:</b> تم عرض أول {len(results_to_display)} نتيجة فقط من أصل {total_found}. "
                        "حاول تضييق نطاق البحث بإضافة كلمات أخرى.")
             if len(response_parts[-1]) + len(warning) <= MAX_MESSAGE_LENGTH:
                 response_parts[-1] += warning
             else:
                 response_parts.append(warning)

        # Send response parts
        for part in response_parts:
             if part.strip():
                 try:
                     await update.message.reply_html(part, disable_web_page_preview=True)
                     await asyncio.sleep(0.1)
                 except TelegramError as e:
                     logger.error(f"TelegramError sending search result part: {e}")
                     if "message is too long" in str(e):
                         await update.message.reply_text("حدث خطأ: إحدى الرسائل كانت أطول من اللازم.")
                     # Consider adding more specific error handling if needed
                     break # Stop sending further parts on error

    except sqlite3.Error as e:
        logger.error(f"Database error during search for query '{query}': {e}")
        await update.message.reply_text("❌ حدث خطأ في قاعدة البيانات أثناء البحث.")
    except TelegramError as e:
        logger.error(f"TelegramError in handle_search for query '{query}': {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in handle_search for query '{query}': {str(e)}")
        try: await update.message.reply_text("❌ حدث خطأ غير متوقع أثناء معالجة طلبك.")
        except TelegramError: pass


async def handle_deep_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles deep linking (/start hadith_123) and message splitting with 'More' button."""
    if not context.args or not context.args[0].startswith("hadith_"):
        # Not a hadith deep link, treat as normal /start
        await start_command(update, context)
        return

    try:
        hadith_id_str = context.args[0].split("_")[1]
        hadith_id = int(hadith_id_str)
        logger.info(f"Handling deep link for hadith ID: {hadith_id}")

        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=constants.ChatAction.TYPING)
        hadith = db.get_hadith_by_id(hadith_id)

        if not hadith:
            await update.message.reply_text(f"⚠️ لم يتم العثور على الحديث بالمعرف: {hadith_id}")
            return

        # --- Message Splitting Logic ---
        full_text = hadith['text']
        header = (
            f"📜 <b>الحديث  (ID: {hadith['id']})</b>\n"
            f"📚 <b>الكتاب:</b> {hadith['book']}\n"
            f"📌 <b>صحة الحديث:</b> {hadith['grading'] or 'غير مصنف'}\n"
            f"{'-'*20}\n"
        )
        # Calculate max length for the first part considering header
        max_first_part_len = MAX_MESSAGE_LENGTH - len(header) - 50 # Reserve some buffer

        message_parts = split_text(full_text, max_first_part_len)

        # Send first part (with header)
        first_part_content = f"{header}{message_parts[0]}"
        reply_markup = None

        if len(message_parts) > 1:
            # More parts exist, add the "More" button
            callback_data = f"more:{hadith_id}:1" # Request index 1 (second part)
            keyboard = [[InlineKeyboardButton("للمزيد 🔽", callback_data=callback_data)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            logger.debug(f"Adding 'More' button for hadith {hadith_id}, requesting part 1")

        await update.message.reply_html(first_part_content, reply_markup=reply_markup)
        db.update_statistics('deep_link_view')

    except (IndexError, ValueError):
        logger.warning(f"Invalid deep link argument: {context.args}")
        await update.message.reply_text("⚠️ رابط غير صالح لعرض الحديث.")
    except TelegramError as e:
         logger.error(f"TelegramError handling deep link for hadith {hadith_id_str}: {e}")
         # Handle potential "message is too long" error even for the first part
         if "message is too long" in str(e):
              await update.message.reply_text("❌ نص الحديث طويل جدًا ولا يمكن عرضه بالكامل.")
    except Exception as e:
        logger.exception(f"Unexpected error handling deep link for hadith {hadith_id_str}: {e}")
        await update.message.reply_text("❌ حدث خطأ أثناء عرض الحديث الكامل.")


async def more_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the 'More' button click to show subsequent parts of a long hadith."""
    query = update.callback_query
    await query.answer() # Acknowledge the button press

    try:
        callback_data = query.data.split(":")
        if len(callback_data) != 3 or callback_data[0] != 'more':
            logger.warning(f"Invalid 'more' callback data: {query.data}")
            return

        hadith_id = int(callback_data[1])
        part_index_to_show = int(callback_data[2])

        logger.info(f"Handling 'more' callback for hadith {hadith_id}, part index {part_index_to_show}")

        hadith = db.get_hadith_by_id(hadith_id)
        if not hadith:
            await query.edit_message_text("⚠️ لم يتم العثور على نتيجة.", reply_markup=None)
            return

        # --- Split text again (consistent splitting needed) ---
        # Use a slightly larger max_length for subsequent parts as they don't need the header
        message_parts = split_text(hadith['text'], MAX_MESSAGE_LENGTH - 50) # Reserve buffer

        if part_index_to_show >= len(message_parts):
            logger.warning(f"Requested part index {part_index_to_show} out of bounds for hadith {hadith_id}")
            # Remove button from original message anyway
            await query.edit_message_reply_markup(reply_markup=None)
            return

        # --- Send the requested part ---
        part_content = message_parts[part_index_to_show]
        part_header = f"📜 <b>(الجزء {part_index_to_show + 1}/{len(message_parts)})</b> - تابع حديث ID: {hadith_id}\n{'-'*20}\n"
        full_part_message = f"{part_header}{part_content}"

        # Check if there are *more* parts after this one
        next_part_index = part_index_to_show + 1
        reply_markup = None
        if next_part_index < len(message_parts):
            # Add a "More" button for the *next* part
            next_callback_data = f"more:{hadith_id}:{next_part_index}"
            keyboard = [[InlineKeyboardButton(f"للمزيد ({next_part_index + 1}/{len(message_parts)}) 🔽", callback_data=next_callback_data)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            logger.debug(f"Adding 'More' button for hadith {hadith_id}, requesting part {next_part_index}")


        # Send the new part as a *new message* replying to the original user message
        # (Editing the original message might be confusing if it becomes very long)
        # Alternatively, edit the message *if* it's the last part being shown.
        await query.message.reply_html(full_part_message, reply_markup=reply_markup)


        # --- Disable the button on the *previous* message ---
        # Edit the message that contained the button *that was just clicked*
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            logger.debug(f"Removed 'More' button from previous message for hadith {hadith_id}, part {part_index_to_show -1}")
        except BadRequest as e:
             # Might fail if the message is too old or wasn't modified, ignore gracefully
             if "message is not modified" in str(e):
                 logger.debug("Button removal failed: Message not modified.")
             else:
                 logger.error(f"BadRequest error removing 'More' button: {e}")
        except TelegramError as e:
            logger.error(f"TelegramError removing 'More' button: {e}")


    except (IndexError, ValueError):
        logger.warning(f"Invalid 'more' callback data format: {query.data}")
        # Try to remove the button from the original message if possible
        try: await query.edit_message_reply_markup(reply_markup=None)
        except: pass
    except TelegramError as e:
        logger.error(f"TelegramError in more_callback_handler: {e}")
        # Don't try to send another message on Telegram error
    except Exception as e:
        logger.exception(f"Unexpected error in more_callback_handler: {e}")
        # Don't try to send another message


# --- Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log Errors caused by Updates."""
    # (Same as v3)
    logger.error(f"Update {update} caused error {context.error}", exc_info=context.error)


# ---------------------- التشغيل الرئيسي ----------------------
def main() -> None:
    """Start the bot."""
    # (Modified to add CallbackQueryHandler for 'more:')
    if not BOT_TOKEN:
        logger.critical("FATAL: BOT_TOKEN environment variable is not set.")
        exit(1)

    try:
        application = Application.builder().token(BOT_TOKEN).build()

        # --- Register Handlers ---
        application.add_handler(CommandHandler('start', handle_deep_link)) # Handles /start and deep links
        application.add_handler(CommandHandler('help', help_command))
        # Add handler for the 'more' button clicks
        application.add_handler(CallbackQueryHandler(more_callback_handler, pattern=r"^more:"))

        # Handle regular text messages for searching
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_search
        ))

        # --- Register Error Handler ---
        application.add_error_handler(error_handler)

        # --- Start the Bot ---
        logger.info("Starting bot polling...")
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            poll_interval=1.0,
            timeout=30,
            drop_pending_updates=True
        )

    except Exception as e:
        logger.critical(f"Failed to initialize or run the bot application: {e}", exc_info=True)
        exit(1)
    finally:
        logger.info("Shutting down bot and closing connections...")
        db.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"Unhandled exception in __main__: {e}", exc_info=True)

