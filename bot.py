# =========================
# IMPORTS
# =========================

import os
import re
import asyncio
import aiohttp

from dotenv import load_dotenv
from collections import Counter
from aiohttp import web

from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InputMediaPhoto
)
from aiogram.filters import CommandStart
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder

# =========================
# ENV
# =========================

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("SHEET_URL")

# =========================
# BOT
# =========================

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# =========================
# GLOBAL DATA
# =========================

appeals_data = []
active_tasks = set()

message_buffers = {}
photo_buffers = {}
message_tasks = {}

# =========================
# MAHALLA
# =========================

MAHALLALAR = [
    "Bekobod MFY", "Saidobod MFY", "Chimqo‘rg‘on MFY", "Murot Ali MFY",
    "Fayzobod MFY", "Do‘ngqo‘rg‘on MFY", "Mo‘minobod MFY", "Birlik MFY",
    "Yangiobod MFY", "Ko‘lota MFY", "Guliston MFY", "Navoiy MFY",
    "Lolaariq MFY", "Ming tepa MFY", "G‘ayrat MFY", "Do‘stlik MFY",
    "Oqtepa MFY", "Mitan MFY", "Oybek MFY", "Kultepa MFY",
    "Mustaqillik MFY", "Taraqqiyot MFY", "Oqtom MFY"
]

# =========================
# STATES
# =========================

class Form(StatesGroup):
    fullname = State()
    mahalla = State()
    phone = State()
    text = State()
    sending = State()  # Новый стейт для блокировки спама во время отправки

# =========================
# START
# =========================

@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer(
            "❌ Bot faqat shaxsiy chatda ishlaydi.\n"
            "❌ Бот работает только в личных сообщениях."
        )
        return

    welcome_text = """
Siz tuman hokimligining murojaatlar botiga murojaat qilmoqchimisiz?.

Ushbu bot orqali:

Ushbu bot orqali:
• muammo
• taklif
• shikoyat
• va boshqa murojaatlarni yuborishingiz mumkin.

————————————

Здравствуйте!

Вы хотите обратиться в бот обращений районного хокимията?

Через данного бота вы можете отправить:
• проблему
• предложение
• жалобу
• и другие обращения.
"""
    await message.answer(welcome_text)
    await message.answer(
        "👤 F.I.O kiriting:\n"
        "👤 Введите Ф.И.О."
    )
    await state.set_state(Form.fullname)

# =========================
# FULLNAME
# =========================

@dp.message(Form.fullname)
async def get_name(message: Message, state: FSMContext):
    name = message.text.strip()

    # Смягчили регулярное выражение, добавив поддержку любых букв, точек и пробелов
    if not re.fullmatch(r"[A-Za-zА-Яа-яЁёЎўҚқҒғҲҳЦцЧчШшЩщЪъЬьЭэЮюЯяİıŞşÇçÖöÜü'‘’` \-\.]+", name):
        await message.answer(
            "❌ F.I.O noto‘g‘ri formatda.\n"
            "❌ Неверный формат Ф.И.О."
        )
        return

    await state.update_data(fullname=name)

    builder = ReplyKeyboardBuilder()
    for mahalla in MAHALLALAR:
        builder.add(KeyboardButton(text=mahalla))
    builder.adjust(2)

    mahalla_kb = builder.as_markup(resize_keyboard=True)

    await message.answer(
        "🏠 Mahallani tanlang yoki yozing:\n"
        "🏠 Выберите или напишите махаллю:",
        reply_markup=mahalla_kb
    )
    await state.set_state(Form.mahalla)

# =========================
# MAHALLA
# =========================

@dp.message(Form.mahalla)
async def get_mahalla(message: Message, state: FSMContext):
    mahalla = message.text.strip()

    if mahalla not in MAHALLALAR:
        await message.answer(
            "❌ Mahalla ro‘yxatda topilmadi.\n"
            "❌ Махалля не найдена в списке."
        )
        return

    await state.update_data(mahalla=mahalla)

    phone_kb = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text="📞 Raqam yuborish / Отправить номер",
                    request_contact=True
                )
            ]
        ],
        resize_keyboard=True
    )

    await message.answer(
        "📞 Telefon raqam yuboring yoki yozing:\n"
        "📞 Отправьте или напишите номер:",
        reply_markup=phone_kb
    )
    await state.set_state(Form.phone)

# =========================
# PHONE
# =========================

@dp.message(Form.phone)
async def get_phone(message: Message, state: FSMContext):
    if message.contact:
        phone = message.contact.phone_number
    else:
        phone = message.text.strip()
        cleaned = ''.join(filter(str.isdigit, phone))
        if len(cleaned) < 7:
            await message.answer(
                "❌ Telefon raqam noto‘g‘ri.\n"
                "❌ Неверный номер телефона."
            )
            return

    username = f"@{message.from_user.username}" if message.from_user.username else "Username yo‘q / Нет username"
    full_name = message.from_user.full_name

    await state.update_data(
        phone=phone,
        tg_username=username,
        tg_fullname=full_name
    )

    await message.answer(
        "📝 Murojaatingizni yozing.\n"
        "📷 Rasm yuborishingiz ham mumkin.\n\n"
        "📝 Напишите обращение.\n"
        "📷 Можно также отправить фото.",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Form.text)

# =========================
# SEND APPEAL (BACKGROUND TASK)
# =========================

async def send_appeal(user_id, state: FSMContext):
    # Ждем 5 секунд накопления буфера медиагруппы
    await asyncio.sleep(5)

    try:
        data = await state.get_data()

        texts = message_buffers.get(user_id, [])
        photos = photo_buffers.get(user_id, [])

        if not texts and not photos:
            return

        # Переводим пользователя в стейт отправки, чтобы он не слал новые сообщения в процессе
        await state.set_state(Form.sending)

        if texts:
            full_text = "\n".join(texts)
        else:
            full_text = "📷 Murojaat faqat rasmdan iborat\n📷 Обращение состоит только из фото"

        appeals_data.append({
            "mahalla": data.get('mahalla', 'Noma\'lum')
        })

        appeal_id = str(len(appeals_data)).zfill(5)

        username = data.get('tg_username')
        full_name = data.get('tg_fullname')

        result = f"""
📨 Yangi murojaat / Новое обращение

🆔 ID: #{appeal_id}

👤 F.I.O: {data.get('fullname')}
🏠 Mahalla: {data.get('mahalla')}
📞 Telefon: {data.get('phone')}

👤 Telegram: {full_name}
🔗 Username: {username}
🆔 Telegram ID: {user_id}

📝 Murojaat / Обращение:
{full_text}
"""

        # 1. Сначала отправляем текст в группу хокимията
        await bot.send_message(GROUP_ID, result)

        # 2. Если есть фотки, упаковываем в альбом
        if photos:
            media_group = [InputMediaPhoto(media=photo_id) for photo_id in photos]
            try:
                await bot.send_media_group(GROUP_ID, media=media_group)
            except Exception as e:
                print("Ошибка отправки медиагруппы:", e)
                for photo_id in photos:
                    try:
                        await bot.send_photo(GROUP_ID, photo_id)
                    except Exception:
                        pass

        # 3. Кнопка перезапуска для жителя
        restart_kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="➕ Yangi murojaat")]],
            resize_keyboard=True
        )

        await bot.send_message(
            user_id,
            f"✅ Murojaatingiz qabul qilindi.\n"
            f"✅ Ваше обращение принято.\n\n"
            f"🆔 ID: #{appeal_id}\n\n"
            f"📨 Murojaatingiz mas'ul xodimlarga yuborildi.\n"
            f"📨 Ваше обращение направлено ответственным сотрудникам.",
            reply_markup=restart_kb
        )

        # 4. Сбрасываем стейт СРАЗУ ПОСЛЕ успешной отправки в Telegram, не дожидаясь Sheets API
        await state.clear()

        # 5. Логирование в Google Таблицы (в бэкграунде)
        payload = {
            "id": appeal_id,
            "fullname": data.get('fullname'),
            "mahalla": data.get('mahalla'),
            "phone": data.get('phone'),
            "text": full_text,
            "username": username,
            "telegram_id": user_id
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(SHEET_URL, json=payload, timeout=8) as response:
                    if response.status != 200:
                        print(f"Sheets API Error: {response.status}")
            except Exception as e:
                print("Google Sheets Error:", e)

    except asyncio.CancelledError:
        print(f"Task for user {user_id} was cancelled.")
    finally:
        # Блок finally выполнится ВСЕГДА, что защищает от зависания буферов
        message_buffers.pop(user_id, None)
        photo_buffers.pop(user_id, None)
        message_tasks.pop(user_id, None)

# =========================
# TEXT / PHOTO
# =========================

@dp.message(Form.text)
async def get_text(message: Message, state: FSMContext):
    user_id = message.from_user.id

    if user_id not in message_buffers:
        message_buffers[user_id] = []
    if user_id not in photo_buffers:
        photo_buffers[user_id] = []

    valid_content = False

    if message.text:
        message_buffers[user_id].append(message.text)
        valid_content = True

    if message.caption:
        message_buffers[user_id].append(message.caption)
        valid_content = True

    if message.photo:
        largest_photo = message.photo[-1].file_id
        photo_buffers[user_id].append(largest_photo)
        valid_content = True

    if not valid_content:
        await message.answer(
            "❌ Faqat matn yoki rasm yuboring.\n"
            "❌ Отправьте только текст или фото."
        )
        return

    # Если пользователь досылает медиа/текст в течение 5 секунд, продлеваем таймер
    old_task = message_tasks.get(user_id)
    if old_task and not old_task.done():
        old_task.cancel()

    task = asyncio.create_task(send_appeal(user_id, state))
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)
    message_tasks[user_id] = task

# =========================
# ANTI-SPAM ON SENDING
# =========================

@dp.message(Form.sending)
async def processing_appeal(message: Message):
    # Если данные уже отправляются, просим подождать пару секунд
    if message.chat.type == "private":
        await message.answer(
            "⏳ Murojaatingiz yuborilmoqda, iltimos kuting...\n"
            "⏳ Ваше обращение отправляется, пожалуйста, подождите..."
        )

# =========================
# STATISTICS
# =========================

@dp.message(lambda message: message.text == "/stat")
async def statistics(message: Message):
    if message.chat.id != GROUP_ID:
        return

    total = len(appeals_data)
    mahalla_counter = Counter(item['mahalla'] for item in appeals_data)

    stat_text = (
        f"📊 Statistika / Статистика\n\n"
        f"📝 Jami murojaatlar: {total}\n\n"
    )

    for mahalla, count in mahalla_counter.items():
        stat_text += f"{mahalla} — {count}\n"

    await message.answer(stat_text)

# =========================
# RESTART
# =========================

@dp.message(lambda message: message.text == "➕ Yangi murojaat")
async def restart_form(message: Message, state: FSMContext):
    await message.answer(
        "👤 F.I.O kiriting:\n"
        "👤 Введите Ф.И.О.",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Form.fullname)

# =========================
# UNKNOWN
# =========================

@dp.message()
async def unknown_message(message: Message):
    if message.chat.type != "private":
        return

    await message.answer(
        "ℹ️ Yangi murojaat yuborish uchun “➕ Yangi murojaat” tugmasini bosing yoki /start buyrug‘ini yuboring.\n\n"
        "————————————\n\n"
        "ℹ️ Чтобы создать новое обращение, нажмите кнопку “➕ Yangi murojaat” или отправьте команду /start"
    )

# =========================
# HEALTH CHECK
# =========================

async def health_check(request):
    return web.Response(text="Bot is running")

# =========================
# WEB SERVER
# =========================

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

# =========================
# MAIN
# =========================
@dp.message()
async def get_group_id_debug(message: Message):
    # Этот хэндлер сработает на любое сообщение в группе
    print("\n====================================")
    print(f"ID ЭТОГО ЧАТА: {message.chat.id}")
    print(f"ТИП ЧАТА: {message.chat.type}")
    print(f"ТЕКСТ: {message.text}")
    print("====================================\n")

async def main():
    print("✅ Bot успешно запущен...")
    await start_web_server()
    await dp.start_polling(bot)

# =========================
# RUN
# =========================

if __name__ == "__main__":
    asyncio.run(main())