# ============================================================
# MUROJAAT BOT  —  Production v3.0
# ============================================================
# Features:
#   • FSM: fullname → mahalla → phone → text/photo
#   • Local keyword classifier (instant, no API needed)
#   • OpenAI GPT classifier with few-shot prompt + graceful degradation
#   • Rate limit / antispam (cooldown + 5-per-5min window)
#   • Message deduplication (hash-based)
#   • SQLite with extensible schema (status, response tracking, attachments)
#   • Admin-only commands: /stat /top /urgent /mahalla /today
#   • Google Sheets sync with exponential retry (non-blocking)
#   • Startup self-check (OpenAI / SQLite / Sheets)
#   • Structured logging
# ============================================================

# ============================================================
# IMPORTS
# ============================================================

import os
import re
import json
import time
import hashlib
import asyncio
import aiohttp
import sqlite3
import logging

from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dotenv import load_dotenv
from aiohttp import web

from openai import AsyncOpenAI

from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InputMediaPhoto,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================
# ENV & VALIDATION
# ============================================================

load_dotenv()

TOKEN          = os.getenv("BOT_TOKEN")
GROUP_ID       = int(os.getenv("GROUP_ID", "0"))
SHEET_URL      = os.getenv("SHEET_URL", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Список Telegram ID администраторов (кроме GROUP_ID).
# Формат в .env:  ADMIN_IDS=123456789,987654321
_raw_admins    = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: set[int] = {int(x) for x in _raw_admins.split(",") if x.strip().isdigit()}

_missing = [k for k, v in {
    "BOT_TOKEN":  TOKEN,
    "GROUP_ID":   GROUP_ID or None,
    "SHEET_URL":  SHEET_URL or None,
}.items() if not v]

if _missing:
    raise RuntimeError(f"Отсутствуют обязательные переменные окружения: {', '.join(_missing)}")

# ============================================================
# OPENAI CLIENT
# ============================================================

ai_client: AsyncOpenAI | None = None
if OPENAI_API_KEY:
    ai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
else:
    logger.warning("OPENAI_API_KEY не задан — будет использован только local classifier")

# ============================================================
# DATABASE  (extensible schema)
# ============================================================

DB_PATH = "appeals.db"


def db_connect() -> sqlite3.Connection:
    """Каждый вызов — своё соединение (thread/asyncio safe)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # параллельные чтения без блокировок
    return conn


def db_init() -> None:
    with db_connect() as conn:
        conn.executescript("""
            -- Основная таблица обращений
            CREATE TABLE IF NOT EXISTS appeals (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Данные заявителя
                fullname         TEXT    NOT NULL,
                mahalla          TEXT    NOT NULL,
                phone            TEXT    NOT NULL,
                text             TEXT    NOT NULL,
                text_hash        TEXT,               -- для дедупликации

                -- AI классификация
                category         TEXT    DEFAULT 'Другое',
                urgency          TEXT    DEFAULT 'Средняя',
                sentiment        TEXT    DEFAULT 'neutral',
                resonance_risk   TEXT    DEFAULT 'low',
                summary          TEXT,
                classifier       TEXT    DEFAULT 'local',  -- 'local' | 'openai'

                -- Статус
                status           TEXT    DEFAULT 'new',    -- new | in_progress | resolved

                -- Telegram метаданные
                username         TEXT,
                telegram_id      TEXT,

                -- Хронология (для response time analytics)
                created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
                first_response_at DATETIME,
                closed_at        DATETIME,

                -- Расширения (future dashboard)
                assignee_id      INTEGER,            -- FK → users (future)
                organization_id  INTEGER,            -- FK → organizations (future)
                escalated        INTEGER DEFAULT 0,  -- bool: 1 = escalated
                has_attachments  INTEGER DEFAULT 0,  -- bool: 1 = photos present

                -- Attachments metadata (JSON array of file_ids)
                attachments_json TEXT
            );

            -- Organizations (foundation для situational center)
            CREATE TABLE IF NOT EXISTS organizations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL,
                category    TEXT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- Индексы для быстрых аналитик
            CREATE INDEX IF NOT EXISTS idx_appeals_mahalla   ON appeals(mahalla);
            CREATE INDEX IF NOT EXISTS idx_appeals_category  ON appeals(category);
            CREATE INDEX IF NOT EXISTS idx_appeals_urgency   ON appeals(urgency);
            CREATE INDEX IF NOT EXISTS idx_appeals_status    ON appeals(status);
            CREATE INDEX IF NOT EXISTS idx_appeals_created   ON appeals(created_at);
            CREATE INDEX IF NOT EXISTS idx_appeals_tg_id     ON appeals(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_appeals_hash      ON appeals(text_hash);
        """)
        conn.commit()
    logger.info("✅ SQLite инициализирован (%s)", DB_PATH)


def db_save_appeal(
    fullname: str, mahalla: str, phone: str, text: str, text_hash: str,
    category: str, urgency: str, sentiment: str, resonance_risk: str,
    summary: str, classifier: str,
    username: str, telegram_id: int,
    has_attachments: bool, attachments_json: str,
) -> int:
    with db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO appeals
            (fullname, mahalla, phone, text, text_hash,
             category, urgency, sentiment, resonance_risk, summary, classifier,
             username, telegram_id,
             has_attachments, attachments_json)
            VALUES (?,?,?,?,?, ?,?,?,?,?,?, ?,?, ?,?)
            """,
            (fullname, mahalla, phone, text, text_hash,
             category, urgency, sentiment, resonance_risk, summary, classifier,
             username, str(telegram_id),
             int(has_attachments), attachments_json),
        )
        conn.commit()
        return cur.lastrowid


def db_is_duplicate(telegram_id: int, text_hash: str) -> bool:
    """Возвращает True, если такой же текст уже был от этого пользователя за последние 10 мин."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    with db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM appeals WHERE telegram_id=? AND text_hash=? AND created_at>? LIMIT 1",
            (str(telegram_id), text_hash, cutoff),
        ).fetchone()
    return row is not None


# ---------- Admin queries ----------

def db_stat_by_category() -> tuple[int, list[tuple]]:
    with db_connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM appeals").fetchone()[0]
        rows  = conn.execute(
            "SELECT category, COUNT(*) cnt FROM appeals GROUP BY category ORDER BY cnt DESC"
        ).fetchall()
    return total, [(r["category"], r["cnt"]) for r in rows]


def db_stat_by_mahalla() -> list[tuple]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT mahalla, COUNT(*) cnt FROM appeals GROUP BY mahalla ORDER BY cnt DESC LIMIT 10"
        ).fetchall()
    return [(r["mahalla"], r["cnt"]) for r in rows]


def db_today_appeals() -> list[sqlite3.Row]:
    today = datetime.now().strftime("%Y-%m-%d")
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM appeals WHERE DATE(created_at)=? ORDER BY created_at DESC LIMIT 20",
            (today,),
        ).fetchall()


def db_urgent_appeals() -> list[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM appeals WHERE urgency='Высокая' AND status!='resolved' "
            "ORDER BY created_at DESC LIMIT 15"
        ).fetchall()


def db_top_categories(limit: int = 5) -> list[tuple]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT category, COUNT(*) cnt FROM appeals GROUP BY category ORDER BY cnt DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [(r["category"], r["cnt"]) for r in rows]


# ============================================================
# BOT & DISPATCHER
# ============================================================

bot = Bot(token=TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# ============================================================
# IN-MEMORY BUFFERS
# ============================================================

message_buffers: dict[int, list[str]]    = {}
photo_buffers:   dict[int, list[str]]    = {}
message_tasks:   dict[int, asyncio.Task] = {}
active_tasks:    set[asyncio.Task]       = set()

# ============================================================
# RATE LIMIT / ANTISPAM
# ============================================================

# {user_id: last_submission_timestamp}
_last_submission: dict[int, float] = {}

# {user_id: [timestamp, timestamp, ...]} — sliding window
_submission_window: dict[int, list[float]] = defaultdict(list)

COOLDOWN_SECONDS  = 15      # минимум между обращениями
WINDOW_SECONDS    = 300     # 5 минут
WINDOW_MAX_COUNT  = 5       # не более 5 обращений за окно


def antispam_check(user_id: int) -> tuple[bool, str]:
    """
    Возвращает (allowed: bool, reason: str).
    reason пустой если allowed=True.
    """
    now = time.time()

    # 1. Cooldown
    last = _last_submission.get(user_id, 0)
    if now - last < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - (now - last))
        return False, f"cooldown:{remaining}"

    # 2. Sliding window
    window = _submission_window[user_id]
    window = [t for t in window if now - t < WINDOW_SECONDS]   # очищаем старые
    _submission_window[user_id] = window

    if len(window) >= WINDOW_MAX_COUNT:
        return False, "window_exceeded"

    return True, ""


def antispam_record(user_id: int) -> None:
    now = time.time()
    _last_submission[user_id] = now
    _submission_window[user_id].append(now)


# ============================================================
# LOCAL KEYWORD CLASSIFIER  (instant, no API)
# ============================================================

# Список: (keywords, category, base_urgency)
# Порядок важен — первый совпавший побеждает
_KEYWORD_RULES: list[tuple[list[str], str, str]] = [
    (["электр", "elektr", "свет", "svet", "токи", "тока", "обесточ", "рубильник",
      "подстанц", "трансформ", "провод", "кабел"],            "Электричество", "Высокая"),
    (["газ", "gaz", "газопровод", "утечка", "запах газ"],      "Газ",           "Высокая"),
    (["вод", "suv", "водопровод", "напор", "нет воды",
      "канализ", "sewage"],                                    "Вода",          "Высокая"),
    (["дорог", "yo'l", "asphalt", "asfalt", "яма", "колдоб",
      "тротуар", "мост", "ko'prik"],                           "Дороги",        "Средняя"),
    (["освещ", "fonar", "фонар", "lamp", "лампа", "темно"],    "Освещение",     "Средняя"),
    (["мусор", "chiqindi", "свалка", "контейнер", "вывоз",
      "полигон", "axlat"],                                     "Мусор",         "Средняя"),
    (["кадастр", "kadastr", "yer", "земл", "участ", "hujjat",
      "документ", "право собств"],                             "Кадастр",       "Средняя"),
    (["субсид", "subsidiya", "пособ", "льгот", "nafaqa",
      "пенси"],                                                "Субсидии",      "Средняя"),
    (["махалл", "mahalla", "mfy", "комитет"],                  "Махалля",       "Низкая"),
    (["медиц", "врач", "больниц", "clinic", "poliklinika",
      "поликлиник", "скорая", "hospital", "doktor"],           "Медицина",      "Высокая"),
    (["школ", "maktab", "детсад", "bog'cha", "универ",
      "образован", "ta'lim"],                                  "Образование",   "Средняя"),
    (["транспорт", "avtobus", "автобус", "маршрут",
      "остановк", "bekat"],                                    "Транспорт",     "Средняя"),
    (["эколог", "загрязн", "выброс", "iflos", "atrofsiz",
      "дым", "tutun"],                                         "Экология",      "Средняя"),
    (["благоустр", "obodonlash", "парк", "сквер", "двор",
      "детская площадк", "озеленен"],                          "Благоустройство","Низкая"),
    (["бизнес", "tadbirkor", "предприним", "лицензи",
      "разрешен", "налог"],                                    "Предпринимательство","Средняя"),
    (["коррупц", "взятк", "порра", "bribe", "злоупотреб",
      "незаконн"],                                             "Коррупция",     "Высокая"),
    (["соцзащит", "ijtimoiy", "инвалид", "nogiro", "малоимущ",
      "бедн"],                                                 "Соцзащита",     "Средняя"),
]


def local_classify(text: str) -> dict:
    """
    Быстрая keyword-классификация без API.
    Возвращает dict совместимый с AI-ответом.
    """
    lower = text.lower()

    # Определяем базовый sentiment по ключевым словам
    angry_words  = ["злой", "ужасно", "кошмар", "безобразие", "бездельник",
                    "позор", "yomon", "dahshat", "жалоба", "шикоят"]
    positive_words = ["рахмат", "спасибо", "благодар", "tashakkur", "minnatdor"]

    sentiment = "neutral"
    if any(w in lower for w in angry_words):
        sentiment = "angry"
    elif any(w in lower for w in positive_words):
        sentiment = "positive"

    # Resonance risk по sentiment + urgency
    for keywords, category, urgency in _KEYWORD_RULES:
        if any(kw in lower for kw in keywords):
            resonance_risk = (
                "high"   if urgency == "Высокая" and sentiment == "angry" else
                "medium" if urgency == "Высокая" or sentiment == "angry"  else
                "low"
            )
            # Краткое резюме — первые 120 символов текста
            short = text.strip().replace("\n", " ")[:120]
            summary = (
                text.strip().replace("\n", " ")[:120]
                + ("..." if len(text) > 120 else "")
            )

            return {
                "category":       category,
                "urgency":        urgency,
                "sentiment":      sentiment,
                "resonance_risk": resonance_risk,
                "summary":        summary,
                "classifier":     "local",
            }

    # Ничего не совпало
    return {
        "category":       "Другое",
        "urgency":        "Средняя",
        "sentiment":      sentiment,
        "resonance_risk": "low",
        "summary": (
            text.strip().replace("\n", " ")[:120]
            + ("..." if len(text) > 120 else "")
        ),
        "classifier":     "local",
    }


# ============================================================
# OPENAI CLASSIFIER  (few-shot, strict JSON)
# ============================================================

_AI_SYSTEM = (
    "Sen tuman hokimligi fuqarolar murojaatlarini tahlil qiluvchi AI assistentsan. "
    "Faqat valid JSON qaytar. Hech qanday markdown yoki qo‘shimcha matn yozma."
)

_AI_PROMPT = """\
Проанализируй обращение гражданина. Верни ТОЛЬКО JSON, без markdown-блоков.

=== КАТЕГОРИИ (выбери ровно одну) ===
Электричество | Вода | Газ | Дороги | Освещение | Мусор | Кадастр | Субсидии |
Махалля | Медицина | Образование | Транспорт | Экология | Благоустройство |
Предпринимательство | Коррупция | Соцзащита | Другое

=== ПАРАМЕТРЫ ===
urgency        : Высокая | Средняя | Низкая
sentiment      : angry | neutral | positive
resonance_risk : high | medium | low   (вероятность, что обращение получит огласку/эскалацию)
summary        : 1–2 конкретных предложения. Указывай адрес/объект если есть в тексте.

=== FEW-SHOT ПРИМЕРЫ ===
Обращение: "Ул. Навоий 12-доме уже 3 кун свет йук. Болалар дарс кила олмаяпти."
Ответ: {"category":"Электричество","urgency":"Высокая","sentiment":"angry","resonance_risk":"high","summary":"Отсутствует электроснабжение по ул. Навоий д.12 уже 3 дня. Дети не могут учиться."}

Обращение: "Сувимиз тиндирилмаган келяпти, болалар касал болиб колди"
Ответ: {"category":"Вода","urgency":"Высокая","sentiment":"angry","resonance_risk":"high","summary":"Подача загрязнённой воды привела к заболеванию детей."}

Обращение: "Маҳалламизда чиқинди уюмлари йиғилиб қолган, ҳид чиқаяпти"
Ответ: {"category":"Мусор","urgency":"Средняя","sentiment":"neutral","resonance_risk":"medium","summary":"Накопление мусора в махалле, неприятный запах."}

Обращение: "Рахмат сизларга, йул тузатилди!"
Ответ: {"category":"Дороги","urgency":"Низкая","sentiment":"positive","resonance_risk":"low","summary":"Гражданин благодарит за ремонт дороги."}

=== ОБРАЩЕНИЕ ===
{text}

=== ОТВЕТ (только JSON) ==="""


_AI_DEGRADED = False   # глобальный флаг деградации OpenAI


async def openai_classify(text: str) -> dict | None:
    """
    Классифицирует через OpenAI.
    Возвращает dict или None при любой ошибке.
    Устанавливает _AI_DEGRADED при billing/quota проблемах.
    """
    global _AI_DEGRADED

    if not ai_client:
        return None

    if _AI_DEGRADED:
        logger.info("OpenAI в режиме деградации — пропускаем вызов")
        return None

    try:
        response = await asyncio.wait_for(
            ai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": _AI_SYSTEM},
                    {"role": "user", "content": _AI_PROMPT.format(text=text)},
                ],
                temperature=0,
                max_tokens=250,
            ),
            timeout=12,
        )

        raw = (response.choices[0].message.content or "").strip()
        logger.info("GPT RAW RESPONSE: %s", raw)

        # Удаляем markdown fences
        raw = raw.replace("```json", "").replace("```", "").strip()

        # ----------------------------------------------------
        # SAFE JSON PARSING
        # ----------------------------------------------------
        try:
            data = json.loads(raw)
            if isinstance(data, str):
                data = json.loads(data)
        except Exception:
            logger.warning("GPT вернул невалидный JSON: %s", raw)
            return None

        # ----------------------------------------------------
        # SAFE NORMALIZATION
        # ----------------------------------------------------
        data = {
            "category": data.get("category", "Другое"),
            "urgency": data.get("urgency", "Средняя"),
            "sentiment": data.get("sentiment", "neutral"),
            "resonance_risk": data.get("resonance_risk", "low"),
            "summary": data.get("summary", text[:120]),
            "classifier": "openai",
        }

        logger.info("GPT NORMALIZED: %s", data)
        return data

    except asyncio.TimeoutError:
        logger.warning("OpenAI timeout")
    except json.JSONDecodeError as exc:
        logger.warning("OpenAI невалидный JSON: %s", exc)
    except Exception as exc:
        msg = str(exc).lower()
        if any(kw in msg for kw in ("quota", "billing", "rate_limit", "insufficient_quota")):
            _AI_DEGRADED = True
            logger.error("OpenAI деградация (billing/quota): %s — переключаемся на local", exc)
        else:
            logger.warning("OpenAI ошибка: %s", exc)

    return None


async def analyze_appeal(text: str) -> dict:
    """
    Pipeline:
      1. local classifier (мгновенно, всегда)
      2. OpenAI classifier (если доступен) → перезаписывает результат
    При недоступности OpenAI возвращает local результат.
    """
    local_result = local_classify(text)

    ai_result = await openai_classify(text)
    if ai_result:
        return ai_result

    logger.info("Используем local classifier (OpenAI недоступен или деградирован)")
    return local_result


# ============================================================
# GOOGLE SHEETS  (non-blocking, exponential retry)
# ============================================================

async def _send_to_sheets(payload: dict, retries: int = 3) -> None:
    if not SHEET_URL:
        return
    for attempt in range(1, retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    SHEET_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status == 200:
                        return
                    logger.warning("Sheets: статус %s (попытка %d/%d)", resp.status, attempt, retries)
        except Exception as exc:
            logger.warning("Sheets ошибка (попытка %d/%d): %s", attempt, retries, exc)
        await asyncio.sleep(2 ** attempt)
    logger.error("Sheets: не записано после %d попыток", retries)


# ============================================================
# STARTUP SELF-CHECK
# ============================================================

async def startup_self_check() -> None:
    logger.info("━━━ STARTUP SELF-CHECK ━━━")

    # 1. SQLite
    try:
        with db_connect() as conn:
            conn.execute("SELECT 1")
        logger.info("  ✅ SQLite         — OK")
    except Exception as exc:
        logger.error("  ❌ SQLite         — FAIL: %s", exc)

    # 2. OpenAI
    if ai_client:
        try:
            await asyncio.wait_for(
                ai_client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": "ping"}],
                    max_tokens=5,
                ),
                timeout=8,
            )
            logger.info("  ✅ OpenAI         — OK (model: %s)", OPENAI_MODEL)
        except asyncio.TimeoutError:
            logger.warning("  ⚠️  OpenAI         — TIMEOUT (local fallback active)")
        except Exception as exc:
            logger.warning("  ⚠️  OpenAI         — FAIL: %s (local fallback active)", exc)
    else:
        logger.warning("  ⚠️  OpenAI         — SKIP (нет OPENAI_API_KEY)")

    # 3. Google Sheets
    if SHEET_URL:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    SHEET_URL,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    status = "OK" if resp.status < 500 else f"HTTP {resp.status}"
            logger.info("  ✅ Google Sheets  — %s", status)
        except Exception as exc:
            logger.warning("  ⚠️  Google Sheets  — FAIL: %s", exc)
    else:
        logger.warning("  ⚠️  Google Sheets  — SKIP (нет SHEET_URL)")

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━")


# ============================================================
# MAHALLA
# ============================================================

MAHALLALAR = [
    "Bekobod MFY", "Saidobod MFY", "Chimqo'rg'on MFY", "Murot Ali MFY",
    "Fayzobod MFY", "Do'ngqo'rg'on MFY", "Mo'minobod MFY", "Birlik MFY",
    "Yangiobod MFY", "Ko'lota MFY", "Guliston MFY", "Navoiy MFY",
    "Lolaariq MFY", "Ming tepa MFY", "G'ayrat MFY", "Do'stlik MFY",
    "Oqtepa MFY", "Mitan MFY", "Oybek MFY", "Kultepa MFY",
    "Mustaqillik MFY", "Taraqqiyot MFY", "Oqtom MFY",
]
MAHALLALAR_SET = set(MAHALLALAR)

# ============================================================
# FSM STATES
# ============================================================

class Form(StatesGroup):
    fullname = State()
    mahalla  = State()
    phone    = State()
    text     = State()

# ============================================================
# HELPERS
# ============================================================

def _clean_buffers(user_id: int) -> None:
    message_buffers.pop(user_id, None)
    photo_buffers.pop(user_id, None)
    message_tasks.pop(user_id, None)


def _cancel_pending(user_id: int) -> None:
    task = message_tasks.get(user_id)
    if task and not task.done():
        task.cancel()


def _is_admin(chat_id: int) -> bool:
    return chat_id == GROUP_ID or chat_id in ADMIN_IDS


def _text_hash(text: str, user_id: int) -> str:
    raw = f"{user_id}:{text.strip().lower()}"
    return hashlib.md5(raw.encode()).hexdigest()


URGENCY_EMOJI   = {"Высокая": "🔴", "Средняя": "🟡", "Низкая": "🟢"}
SENTIMENT_EMOJI = {"angry": "😠", "neutral": "😐", "positive": "😊"}
RESONANCE_EMOJI = {"high": "🔥", "medium": "⚡", "low": "💧"}
# ============================================================
# LOCALIZATION
# ============================================================

CATEGORY_UZ = {
    "Electricity": "Elektr",
    "Water": "Suv",
    "Gas": "Gaz",
    "Roads": "Yo‘llar",
    "Lighting": "Yoritish",
    "Garbage": "Chiqindi",
    "Cadastre": "Kadastr",
    "Subsidies": "Subsidiya",
    "Mahalla": "Mahalla",
    "Medicine": "Tibbiyot",
    "Education": "Ta'lim",
    "Transport": "Transport",
    "Ecology": "Ekologiya",
    "Beautification": "Obodonlashtirish",
    "Entrepreneurship": "Tadbirkorlik",
    "Corruption": "Korrupsiya",
    "Social Protection": "Ijtimoiy himoya",

    "Электричество": "Elektr",
    "Вода": "Suv",
    "Газ": "Gaz",
    "Дороги": "Yo‘llar",
    "Освещение": "Yoritish",
    "Мусор": "Chiqindi",
    "Кадастр": "Kadastr",
    "Субсидии": "Subsidiya",
    "Махалля": "Mahalla",
    "Медицина": "Tibbiyot",
    "Образование": "Ta'lim",
    "Транспорт": "Transport",
    "Экология": "Ekologiya",
    "Благоустройство": "Obodonlashtirish",
    "Предпринимательство": "Tadbirkorlik",
    "Коррупция": "Korrupsiya",
    "Соцзащита": "Ijtimoiy himoya",

    "Другое": "Boshqa"
}

URGENCY_UZ = {
    "Высокая": "Yuqori",
    "Средняя": "O‘rta",
    "Низкая": "Past",

    "High": "Yuqori",
    "Medium": "O‘rta",
    "Low": "Past"
}

SENTIMENT_UZ = {
    "angry": "Salbiy",
    "neutral": "Neytral",
    "positive": "Ijobiy"
}

RESONANCE_UZ = {
    "high": "Yuqori",
    "medium": "O‘rta",
    "low": "Past"
}
# ============================================================
# /start
# ============================================================

@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer(
            "❌ Bot faqat shaxsiy chatda ishlaydi.\n"
            "❌ Бот работает только в личных сообщениях."
        )
        return

    _cancel_pending(message.from_user.id)
    _clean_buffers(message.from_user.id)
    await state.clear()

    await message.answer(
        "Assalomu alaykum!\n\n"
        "Siz tuman hokimligining murojaatlar botiga murojaat qildingiz.\n\n"
        "Ushbu bot orqali:\n"
        "• muammo\n• taklif\n• shikoyat\n• va boshqa murojaatlarni yuborishingiz mumkin.\n\n"
        "————————————\n\n"
        "Здравствуйте!\n\n"
        "Вы обратились в бот обращений районного хокимията.\n\n"
        "Через данного бота вы можете отправить:\n"
        "• проблему\n• предложение\n• жалобу\n• и другие обращения.",
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer("👤 F.I.O kiriting:\n👤 Введите Ф.И.О.")
    await state.set_state(Form.fullname)

# ============================================================
# FULLNAME
# ============================================================

@dp.message(Form.fullname)
async def get_name(message: Message, state: FSMContext):
    name = message.text.strip() if message.text else ""

    if not re.fullmatch(r"[A-Za-zА-Яа-яЁёЎўҚқҒғҲҳİıŞşÇçÖöÜü'''\- ]+", name):
        await message.answer(
            "❌ F.I.O noto'g'ri formatda. Faqat harflar.\n"
            "❌ Неверный формат Ф.И.О. Только буквы."
        )
        return

    await state.update_data(fullname=name)

    builder = ReplyKeyboardBuilder()
    for m in MAHALLALAR:
        builder.add(KeyboardButton(text=m))
    builder.adjust(2)

    await message.answer(
        "🏠 Mahallani tanlang:\n🏠 Выберите махаллю:",
        reply_markup=builder.as_markup(resize_keyboard=True, one_time_keyboard=True),
    )
    await state.set_state(Form.mahalla)

# ============================================================
# MAHALLA
# ============================================================

@dp.message(Form.mahalla)
async def get_mahalla(message: Message, state: FSMContext):
    mahalla = message.text.strip() if message.text else ""

    if mahalla not in MAHALLALAR_SET:
        await message.answer(
            "❌ Mahalla topilmadi. Tugmadan tanlang.\n"
            "❌ Махалля не найдена. Выберите из кнопок."
        )
        return

    await state.update_data(mahalla=mahalla)

    await message.answer(
        "✅ Mahalla tanlandi!\n✅ Махалля выбрана!",
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer(
        "📞 Telefon raqam yuboring yoki yozing:\n"
        "📞 Отправьте или напишите номер телефона:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(
                text="📞 Raqam yuborish / Отправить номер",
                request_contact=True,
            )]],
            resize_keyboard=True,
            one_time_keyboard=True,
        ),
    )
    await state.set_state(Form.phone)

# ============================================================
# PHONE
# ============================================================

@dp.message(Form.phone)
async def get_phone(message: Message, state: FSMContext):
    if message.contact:
        phone = message.contact.phone_number
    else:
        phone = message.text.strip() if message.text else ""
        if len("".join(filter(str.isdigit, phone))) < 7:
            await message.answer(
                "❌ Telefon raqam noto'g'ri.\n"
                "❌ Неверный номер телефона."
            )
            return

    await state.update_data(
        phone=phone,
        tg_username=f"@{message.from_user.username}" if message.from_user.username else "Нет username",
        tg_fullname=message.from_user.full_name,
    )

    await message.answer(
        "📝 Murojaatingizni yozing.\n"
        "📷 Rasm yuborishingiz ham mumkin.\n\n"
        "📝 Напишите обращение.\n"
        "📷 Можно также отправить фото.",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.text)

# ============================================================
# SEND APPEAL  (core pipeline)
# ============================================================

async def send_appeal(user_id: int, state: FSMContext) -> None:
    await asyncio.sleep(5)   # накапливаем буфер

    try:
        data   = await state.get_data()
        texts  = message_buffers.get(user_id, [])
        photos = photo_buffers.get(user_id, [])

        if not texts and not photos:
            logger.warning("Пустой буфер user_id=%s — пропуск", user_id)
            return

        full_text = (
            "\n".join(texts) if texts
            else "📷 Murojaat faqat rasmdan iborat / Обращение только из фото"
        )

        # --- Antispam (финальная проверка перед отправкой) ---
        allowed, reason = antispam_check(user_id)
        if not allowed:
            if reason.startswith("cooldown"):
                sec = reason.split(":")[1]
                await bot.send_message(
                    user_id,
                    f"⏳ Iltimos, {sec} soniya kuting.\n"
                    f"⏳ Пожалуйста, подождите {sec} секунд перед следующим обращением.",
                )
            else:
                await bot.send_message(
                    user_id,
                    "🚫 Siz juda ko'p murojaat yubordingiz (5 ta / 5 daqiqa).\n"
                    "🚫 Вы превысили лимит обращений (5 за 5 минут). Попробуйте позже.",
                )
            return

        # --- Дедупликация ---
        t_hash = _text_hash(full_text, user_id)
        if db_is_duplicate(user_id, t_hash):
            await bot.send_message(
                user_id,
                "ℹ️ Bu murojaat allaqachon yuborilgan.\n"
                "ℹ️ Такое обращение уже было отправлено недавно.",
            )
            return

        # --- AI анализ (local → openai) ---
        analysis       = await analyze_appeal(full_text)
        category       = analysis.get("category",       "Другое")
        urgency        = analysis.get("urgency",        "Средняя")
        sentiment      = analysis.get("sentiment",      "neutral")
        resonance_risk = analysis.get("resonance_risk", "low")
        summary        = analysis.get("summary",        "—")
        classifier     = analysis.get("classifier",     "local")

        username = data.get("tg_username", "—")
        tg_name  = data.get("tg_fullname", "—")

        has_attachments  = bool(photos)
        attachments_json = json.dumps(photos) if photos else "[]"

        # --- Сохранение в SQLite ---
        row_id    = db_save_appeal(
            fullname=data["fullname"], mahalla=data["mahalla"],
            phone=data["phone"],      text=full_text,
            text_hash=t_hash,
            category=category,        urgency=urgency,
            sentiment=sentiment,      resonance_risk=resonance_risk,
            summary=summary,          classifier=classifier,
            username=username,        telegram_id=user_id,
            has_attachments=has_attachments,
            attachments_json=attachments_json,
        )
        appeal_id = str(row_id).zfill(5)

        # --- Запись успешной отправки для antispam ---
        antispam_record(user_id)

        # --- Сообщение в группу ---
        ue = URGENCY_EMOJI.get(urgency, "⚪")
        se = SENTIMENT_EMOJI.get(sentiment, "❓")
        re_ = RESONANCE_EMOJI.get(resonance_risk, "❓")
        cl_badge = "🤖 GPT" if classifier == "openai" else "⚡ Local"

        category_uz = CATEGORY_UZ.get(category, category)
        urgency_uz = URGENCY_UZ.get(urgency, urgency)
        sentiment_uz = SENTIMENT_UZ.get(sentiment, sentiment)
        resonance_uz = RESONANCE_UZ.get(resonance_risk, resonance_risk)

        group_msg = (
            f"📨 Yangi murojaat\n\n"
            f"🆔 ID: #{appeal_id}\n\n"
            f"👤 F.I.O: {data['fullname']}\n"
            f"🏠 Mahalla: {data['mahalla']}\n"
            f"📞 Telefon: {data['phone']}\n\n"
            f"📂 Kategoriya: {category_uz}\n"
            f"{ue} Muhimlik: {urgency_uz}\n"
            f"{se} Kayfiyat: {sentiment_uz}\n"
            f"{re_} Rezonans xavfi: {resonance_uz}\n"
            f"{cl_badge}\n\n"
            f"🧠 AI Xulosa:\n{summary}\n\n"
            f"👤 Telegram: {tg_name}\n"
            f"🔗 Username: {username}\n"
            f"🆔 Telegram ID: {user_id}\n\n"
            f"📝 Murojaat:\n{full_text}"
        )
        await bot.send_message(GROUP_ID, group_msg)

        # --- Фото ---
        if photos:
            try:
                await bot.send_media_group(
                    GROUP_ID,
                    media=[InputMediaPhoto(media=fid) for fid in photos],
                )
            except Exception as exc:
                logger.warning("send_media_group failed (%s), fallback", exc)
                for fid in photos:
                    await bot.send_photo(GROUP_ID, fid)

         # --- Подтверждение пользователю ---
        try:
            await bot.send_message(
                user_id,
                f"✅ Murojaatingiz qabul qilindi.\n\n"
                f"🆔 ID: #{appeal_id}\n\n"
                f"📨 Murojaat mas'ul xodimlarga yuborildi.\n\n"
                f"➕ Yangi murojaat yuborish uchun tugmani bosing.",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="➕ Yangi murojaat")]
                    ],
                    resize_keyboard=True,
                ),
            )
        except Exception as confirm_exc:
            logger.error(
                "Foydalanuvchiga tasdiq yuborilmadi user_id=%s: %s",
                user_id,
                confirm_exc,
            )

        # --- Google Sheets (фоновая задача) ---
        asyncio.create_task(_send_to_sheets({
            "id": appeal_id, "fullname": data["fullname"],
            "mahalla": data["mahalla"], "phone": data["phone"],
            "text": full_text, "category": category,
            "urgency": urgency, "sentiment": sentiment,
            "resonance_risk": resonance_risk, "summary": summary,
            "classifier": classifier, "username": username,
            "telegram_id": user_id,
        }))

    except asyncio.CancelledError:
        logger.info("Задача отменена user_id=%s", user_id)
        raise
    except Exception as exc:
        logger.exception("Ошибка send_appeal user_id=%s: %s", user_id, exc)
        try:
            await bot.send_message(
                user_id,
                "⚠️ Xatolik yuz berdi. /start ni bosing.\n"
                "⚠️ Произошла ошибка. Нажмите /start",
            )
        except Exception:
            pass
    finally:
        await state.clear()
        _clean_buffers(user_id)

# ============================================================
# TEXT / PHOTO  (state handler)
# ============================================================

@dp.message(Form.text)
async def get_text(message: Message, state: FSMContext):
    user_id = message.from_user.id

    message_buffers.setdefault(user_id, [])
    photo_buffers.setdefault(user_id, [])

    valid = False

    if message.text:
        message_buffers[user_id].append(message.text)
        valid = True
    if message.caption:
        message_buffers[user_id].append(message.caption)
        valid = True
    if message.photo:
        photo_buffers[user_id].append(message.photo[-1].file_id)
        valid = True

    if not valid:
        await message.answer(
            "❌ Faqat matn yoki rasm yuboring.\n"
            "❌ Отправьте только текст или фото."
        )
        return

    _cancel_pending(user_id)

    task = asyncio.create_task(send_appeal(user_id, state))
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)
    message_tasks[user_id] = task

# ============================================================
# ADMIN COMMANDS  (только GROUP_ID или ADMIN_IDS)
# ============================================================

def _admin_only(handler):
    """Декоратор: пропускает только администраторов."""
    async def wrapper(message: Message, **kwargs):
        if not _is_admin(message.chat.id):
            return
        await handler(message, **kwargs)
    return wrapper


@dp.message(lambda m: m.text == "/stat")
@_admin_only
async def cmd_stat(message: Message):
    google_script_url = os.getenv("GOOGLE_SCRIPT_URL")
    
    if not google_script_url:
        await message.answer("❌ Система настроена неверно: отсутствует ссылка на таблицу.")
        return

    await message.answer("🔄 Statistika yuklanmoqda... / Статистика загружается...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(google_script_url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if "error" in data:
                        await message.answer(f"❌ Ошибка таблицы: {data['error']}")
                        return
                    
                    total = data.get("total", 0)
                    month = data.get("month", 0)
                    today = data.get("today", 0)
                    
                    text = (
                        "📊 **Murojaatlar Statistikasi / Статистика обращений**\n\n"
                        f"📝 **Jami / Всего:** {total}\n"
                        f"📅 **Shu oyda / В этом месяце:** {month}\n"
                        f"📌 **Bugun / Сегодня:** {today}"
                    )
                    
                    await message.answer(text, parse_mode="Markdown")
                else:
                    await message.answer("❌ Ma'lumotlarni olib bo'lmadi. / Не удалось получить данные.")
                    
    except Exception as e:
        print(f"Ошибка статистики: {e}")
        await message.answer("❌ Tizimда xatolik yuz berdi. / Произошла ошибка в системе.")


@dp.message(lambda m: m.text == "/top")
@_admin_only
async def cmd_top(message: Message):
    rows = db_top_categories(5)
    lines = ["🏆 TOP-5 kategoriyalar:\n"]
    for i, (cat, cnt) in enumerate(rows, 1):
        lines.append(f"  {i}. {cat} — {cnt}")
    await message.answer("\n".join(lines))


@dp.message(lambda m: m.text == "/mahalla")
@_admin_only
async def cmd_mahalla(message: Message):
    rows = db_stat_by_mahalla()
    lines = ["🏘 Mahallalar bo'yicha TOP-10:\n"]
    for mah, cnt in rows:
        lines.append(f"  {mah} — {cnt}")
    await message.answer("\n".join(lines))


@dp.message(lambda m: m.text == "/today")
@_admin_only
async def cmd_today(message: Message):
    rows = db_today_appeals()
    if not rows:
        await message.answer("📭 Bugun murojaat yo'q.")
        return
    lines = [f"📅 Bugungi murojaatlar: {len(rows)} ta\n"]
    for r in rows[:10]:
        ue = URGENCY_EMOJI.get(r["urgency"], "⚪")
        lines.append(
            f"{ue} #{str(r['id']).zfill(5)} | {r['mahalla']} | "
            f"{r['category']} | {r['fullname']}"
        )
    if len(rows) > 10:
        lines.append(f"  ... va yana {len(rows)-10} ta")
    await message.answer("\n".join(lines))


@dp.message(lambda m: m.text == "/urgent")
@_admin_only
async def cmd_urgent(message: Message):
    rows = db_urgent_appeals()
    if not rows:
        await message.answer("✅ Shoshilinch murojaatlar yo'q.")
        return
    lines = [f"🔴 Shoshilinch murojaatlar ({len(rows)} ta):\n"]
    for r in rows:
        re_ = RESONANCE_EMOJI.get(r["resonance_risk"], "❓")
        lines.append(
            f"🔴 #{str(r['id']).zfill(5)} {re_} | {r['mahalla']}\n"
            f"   {r['category']} | {r['fullname']} | {r['phone']}\n"
            f"   {r['summary'] or '—'}\n"
        )
    await message.answer("\n".join(lines))


# ============================================================
# RESTART
# ============================================================

@dp.message(lambda m: m.text == "➕ Yangi murojaat")
async def restart_form(message: Message, state: FSMContext):
    user_id = message.from_user.id
    _cancel_pending(user_id)
    _clean_buffers(user_id)
    await state.clear()

    await message.answer(
        "👤 F.I.O kiriting:\n👤 Введите Ф.И.О.",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.fullname)

# ============================================================
# UNKNOWN MESSAGE
# ============================================================

@dp.message()
async def unknown_message(message: Message):
    if message.chat.type != "private":
        return
    await message.answer(
        "ℹ️ Sizning murojaatingiz allaqachon yuborilgan.\n\n"
        "➕ Yangi murojaat uchun:\n"
        "— «➕ Yangi murojaat» tugmasini bosing\n"
        "yoki /start yuboring.\n\n"
        "————————————\n\n"
        "ℹ️ Ваше обращение уже отправлено.\n\n"
        "➕ Для нового обращения:\n"
        "— нажмите «➕ Yangi murojaat»\n"
        "или отправьте /start"
    )

# ============================================================
# HEALTH CHECK
# ============================================================

async def health_check(request):
    return web.Response(text="OK")

# ============================================================
# WEB SERVER
# ============================================================

async def start_web_server() -> None:
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info("✅ Веб-сервер запущен на порту %s", port)

# ============================================================
# MAIN
# ============================================================

async def main() -> None:
    db_init()
    await startup_self_check()
    await start_web_server()
    logger.info("✅ Бот запущен. Polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())