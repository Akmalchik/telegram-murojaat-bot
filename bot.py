# ============================================================
# MUROJAAT BOT  —  Production v3.0 (Clean Version)
# ============================================================
# Features:
#   • FSM: fullname → mahalla → phone → text/photo
#   • Rate limit / antispam (cooldown + 5-per-5min window)
#   • Message deduplication (hash-based)
#   • SQLite with extensible schema
#   • Admin-only command: /stat (connected to Google Sheets)
#   • Google Sheets sync with exponential retry (non-blocking)
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

# Список Telegram ID администраторов (кроме GROUP_ID).
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
# DATABASE
# ============================================================

DB_PATH = "appeals.db"


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def db_init() -> None:
    with db_connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS appeals (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                fullname         TEXT    NOT NULL,
                mahalla          TEXT    NOT NULL,
                phone            TEXT    NOT NULL,
                text             TEXT    NOT NULL,
                text_hash        TEXT,
                status           TEXT    DEFAULT 'new',
                username         TEXT,
                telegram_id      TEXT,
                created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
                has_attachments  INTEGER DEFAULT 0,
                attachments_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_appeals_mahalla   ON appeals(mahalla);
            CREATE INDEX IF NOT EXISTS idx_appeals_created   ON appeals(created_at);
            CREATE INDEX IF NOT EXISTS idx_appeals_tg_id     ON appeals(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_appeals_hash      ON appeals(text_hash);
        """)
        conn.commit()
    logger.info("✅ SQLite initialized (%s)", DB_PATH)


def db_save_appeal(
    fullname: str, mahalla: str, phone: str, text: str, text_hash: str,
    username: str, telegram_id: int, has_attachments: bool, attachments_json: str,
) -> int:
    with db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO appeals
            (fullname, mahalla, phone, text, text_hash, username, telegram_id, has_attachments, attachments_json)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (fullname, mahalla, phone, text, text_hash, username, str(telegram_id), int(has_attachments), attachments_json),
        )
        conn.commit()
        return cur.lastrowid


def db_is_duplicate(telegram_id: int, text_hash: str) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    with db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM appeals WHERE telegram_id=? AND text_hash=? AND created_at>? LIMIT 1",
            (str(telegram_id), text_hash, cutoff),
        ).fetchone()
    return row is not None


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

_last_submission: dict[int, float] = {}
_submission_window: dict[int, list[float]] = defaultdict(list)

COOLDOWN_SECONDS  = 15      
WINDOW_SECONDS    = 300     
WINDOW_MAX_COUNT  = 5       


def antispam_check(user_id: int) -> tuple[bool, str]:
    now = time.time()
    last = _last_submission.get(user_id, 0)
    if now - last < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - (now - last))
        return False, f"cooldown:{remaining}"

    window = _submission_window[user_id]
    window = [t for t in window if now - t < WINDOW_SECONDS]
    _submission_window[user_id] = window

    if len(window) >= WINDOW_MAX_COUNT:
        return False, "window_exceeded"

    return True, ""


def antispam_record(user_id: int) -> None:
    now = time.time()
    _last_submission[user_id] = now
    _submission_window[user_id].append(now)


# ============================================================
# GOOGLE SHEETS INTERACTION
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


# ============================================================
# MAHALLA LIST
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
# SEND APPEAL  (Чистая отправка в группу хокимията)
# ============================================================

async def send_appeal(user_id: int, state: FSMContext) -> None:
    await asyncio.sleep(5)   # Сборщик буфера сообщений

    try:
        data   = await state.get_data()
        texts  = message_buffers.get(user_id, [])
        photos = photo_buffers.get(user_id, [])

        if not texts and not photos:
            return

        full_text = (
            "\n".join(texts) if texts
            else "📷 Murojaat faqat rasmdan iborat / Обращение только из фото"
        )

        # --- Antispam ---
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

        username = data.get("tg_username", "—")
        tg_name  = data.get("tg_fullname", "—")

        has_attachments  = bool(photos)
        attachments_json = json.dumps(photos) if photos else "[]"

        # --- Сохранение в локальный SQLite ---
        row_id = db_save_appeal(
            fullname=data["fullname"], mahalla=data["mahalla"],
            phone=data["phone"],      text=full_text, text_hash=t_hash,
            username=username,        telegram_id=user_id,
            has_attachments=has_attachments, attachments_json=attachments_json
        )
        appeal_id = str(row_id).zfill(5)

        antispam_record(user_id)

        # --- Идеальный Двуязычный Шаблон для Группы Хокимията ---
        group_msg = (
            f"📨 **Yangi murojaat / Новое обращение**\n\n"
            f"🆔 **ID:** #{appeal_id}\n\n"
            f"👤 **F.I.O:** {data['fullname']}\n"
            f"🏠 **Mahalla:** {data['mahalla']}\n"
            f"📞 **Telefon:** {data['phone']}\n\n"
            f"👤 **Telegram:** {tg_name}\n"
            f"🔗 **Username:** {username}\n"
            f"🆔 **Telegram ID:** {user_id}\n\n"
            f"📝 **Murojaat / Обращение:**\n{full_text}"
        )
        await bot.send_message(GROUP_ID, group_msg, parse_mode="Markdown")

        # --- Отправка Медиафайлов ---
        if photos:
            try:
                await bot.send_media_group(GROUP_ID, media=[InputMediaPhoto(media=fid) for fid in photos])
            except Exception:
                for fid in photos:
                    await bot.send_photo(GROUP_ID, fid)

         # --- Подтверждение заявителю ---
        try:
            await bot.send_message(
                user_id,
                f"✅ Murojaatingiz qabul qilindi.\n\n"
                f"🆔 ID: #{appeal_id}\n\n"
                f"📨 Murojaat mas'ul xodimlarga yuborildi.\n\n"
                f"➕ Yangi murojaat yuborish uchun tugmani bosing.",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="➕ Yangi murojaat")]],
                    resize_keyboard=True,
                ),
            )
        except Exception as confirm_exc:
            logger.error("Ошибка отправки подтверждения: %s", confirm_exc)

        # --- Синхронизация с Google Sheets (Фоновый режим) ---
        asyncio.create_task(_send_to_sheets({
            "id": appeal_id, "fullname": data["fullname"],
            "mahalla": data["mahalla"], "phone": data["phone"],
            "text": full_text, "username": username, "telegram_id": user_id,
        }))

    except Exception as exc:
        logger.exception("Ошибка send_appeal: %s", exc)
        try:
            await bot.send_message(user_id, "⚠️ Xatolik yuz berdi. /start ni bosing.\n⚠️ Произошла ошибка. Нажмите /start")
        except Exception:
            pass
    finally:
        await state.clear()
        _clean_buffers(user_id)

# ============================================================
# TEXT / PHOTO HANDLER
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
        await message.answer("❌ Faqat matn yoki rasm yuboring.\n❌ Отправьте только текст или фото.")
        return

    _cancel_pending(user_id)
    task = asyncio.create_task(send_appeal(user_id, state))
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)
    message_tasks[user_id] = task

# ============================================================
# ADMIN COMMANDS
# ============================================================

def _admin_only(handler):
    async def wrapper(message: Message, **kwargs):
        if not _is_admin(message.chat.id):
            return
        await handler(message, **kwargs)
    return wrapper


@dp.message(lambda m: m.text == "/stat")
@_admin_only
async def cmd_stat(message: Message):
    google_script_url = os.getenv("SHEET_URL")
    
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
        await message.answer("❌ Tizimda xatolik yuz berdi. / Произошла ошибка в системе.")

# ============================================================
# RESTART FORM
# ============================================================

@dp.message(lambda m: m.text == "➕ Yangi murojaat")
async def restart_form(message: Message, state: FSMContext):
    user_id = message.from_user.id
    _cancel_pending(user_id)
    _clean_buffers(user_id)
    await state.clear()
    await message.answer("👤 F.I.O kiriting:\n👤 Введите Ф.И.О.", reply_markup=ReplyKeyboardRemove())
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
# WEB SERVER & MAIN
# ============================================================

async def health_check(request):
    return web.Response(text="OK")


async def start_web_server() -> None:
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info("✅ Веб-сервер запущен на порту %s", port)


async def main() -> None:
    db_init()
    await start_web_server()
    logger.info("✅ Бот запущен. Polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())