from aiogram import Bot, Dispatcher
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.filters import CommandStart
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from collections import Counter
from aiohttp import web

import asyncio
import os
import requests

# ENV yuklash
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))

# BOT
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Murojaat ID
appeal_counter = 1

# Statistika
appeals_data = []

# Mahallalar
MAHALLALAR = [
    "Bekobod MFY",
    "Saidobod MFY",
    "Chimqo‘rg‘on MFY",
    "Murot Ali MFY",
    "Fayzobod MFY",
    "Do‘ngqo‘rg‘on MFY",
    "Mo‘minobod MFY",
    "Birlik MFY",
    "Yangiobod MFY",
    "Ko‘lota MFY",
    "Guliston MFY",
    "Navoiy MFY",
    "Lolaariq MFY",
    "Ming tepa MFY",
    "G‘ayrat MFY",
    "Do‘stlik MFY",
    "Oqtepa MFY",
    "Mitan MFY",
    "Oybek MFY",
    "Kultepa MFY",
    "Mustaqillik MFY",
    "Taraqqiyot MFY",
    "Oqtom MFY"
]

# STATES
class Form(StatesGroup):
    fullname = State()
    mahalla = State()
    phone = State()
    text = State()

# START
@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):

    # Faqat private chat
    if message.chat.type != "private":

        await message.answer(
            "❌ Bot faqat shaxsiy chatda ishlaydi.\n"
            "❌ Бот работает только в личных сообщениях."
        )

        return

    welcome_text = """
Assalomu alaykum!

Siz tuman hokimligining murojaatlar botiga murojaat qildingiz.

Ushbu bot orqali:
- muammo,
- taklif,
- shikoyat,
- va boshqa murojaatlarni yuborishingiz mumkin.

Ma'lumotlaringiz mas'ul xodimlarga yuboriladi.

————————————

Здравствуйте!

Вы обратились в бот обращений районного хокимията.

Через данного бота вы можете отправить:
- проблему,
- предложение,
- жалобу,
- и другие обращения.

Ваше обращение будет направлено ответственным сотрудникам.
"""

    await message.answer(welcome_text)

    await message.answer(
        "👤 Davom etish uchun F.I.O kiriting.\n"
        "👤 Для продолжения введите Ф.И.О."
    )

    await state.set_state(Form.fullname)

# FIO
@dp.message(Form.fullname)
async def get_name(message: Message, state: FSMContext):

    name = message.text.strip()

    # Faqat harf
    if not all(
        char.isalpha() or char.isspace()
        for char in name
    ):

        await message.answer(
            "❌ F.I.O faqat harflardan iborat bo‘lishi kerak.\n"
            "❌ Ф.И.О должно содержать только буквы."
        )

        return

    await state.update_data(fullname=name)

    mahalla_keyboard = []

    for mahalla in MAHALLALAR:
        mahalla_keyboard.append(
            [KeyboardButton(text=mahalla)]
        )

    mahalla_kb = ReplyKeyboardMarkup(
        keyboard=mahalla_keyboard,
        resize_keyboard=True
    )

    await message.answer(
        "🏠 Mahallani tanlang yoki yozing:\n"
        "🏠 Выберите или напишите махаллю:",
        reply_markup=mahalla_kb
    )

    await state.set_state(Form.mahalla)

# MAHALLA
@dp.message(Form.mahalla)
async def get_mahalla(message: Message, state: FSMContext):

    mahalla = message.text.strip()

    # Agar listda bo‘lmasa
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
        "📞 Отправьте или напишите номер телефона:",
        reply_markup=phone_kb
    )

    await state.set_state(Form.phone)

# PHONE
@dp.message(Form.phone)
async def get_phone(message: Message, state: FSMContext):

    if message.contact:
        phone = message.contact.phone_number
    else:
        phone = message.text.strip()

    await state.update_data(phone=phone)

    await message.answer(
        "📝 Murojaatingizni yozing:\n"
        "📝 Напишите обращение:",
        reply_markup=ReplyKeyboardRemove()
    )

    await state.set_state(Form.text)

# TEXT
@dp.message(Form.text)
async def get_text(message: Message, state: FSMContext):

    global appeal_counter

    await state.update_data(text=message.text)

    data = await state.get_data()

    # Statistika
    appeals_data.append({
        "mahalla": data['mahalla']
    })

    appeal_id = str(appeal_counter).zfill(5)
    appeal_counter += 1

    result = f"""
📨 Yangi murojaat / Новое обращение

🆔 ID: #{appeal_id}

👤 F.I.O: {data['fullname']}
🏠 Mahalla: {data['mahalla']}
📞 Telefon: {data['phone']}

📝 Murojaat / Обращение:
{data['text']}
"""

    # Telegram group
    await bot.send_message(GROUP_ID, result)

    # State clear
    await state.clear()

    # Restart keyboard
    restart_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Yangi murojaat")]
        ],
        resize_keyboard=True
    )

    # USER RESPONSE
    await message.answer(
        f"✅ Murojaatingiz qabul qilindi.\n"
        f"✅ Ваше обращение принято.\n\n"

        f"🆔 ID: #{appeal_id}\n\n"

        f"📨 Murojaatingiz mas'ul xodimlarga yuborildi.\n"
        f"📨 Ваше обращение направлено ответственным сотрудникам.\n\n"

        f"ℹ️ Zarurat bo‘lsa siz bilan bog‘laniladi.\n"
        f"ℹ️ При необходимости с вами свяжутся.",

        reply_markup=restart_kb
    )

    # GOOGLE SHEETS
    sheet_url = os.getenv("SHEET_URL")

    payload = {
        "id": appeal_id,
        "fullname": data['fullname'],
        "mahalla": data['mahalla'],
        "phone": data['phone'],
        "text": data['text']
    }

    try:
        response = requests.post(
            sheet_url,
            json=payload,
            timeout=5
        )

        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text)

    except Exception as e:
        print("ERROR:", e)

# STATISTIKA
@dp.message(lambda message: message.text == "/stat")
async def statistics(message: Message):

    if message.chat.id != GROUP_ID:
        return

    total = len(appeals_data)

    mahalla_counter = Counter(
        item['mahalla']
        for item in appeals_data
    )

    stat_text = "📊 Statistika\n\n"
    stat_text += f"📝 Jami murojaatlar: {total}\n\n"
    stat_text += "🏠 Mahallalar:\n\n"

    for mahalla, count in mahalla_counter.items():
        stat_text += f"{mahalla} — {count}\n"

    await message.answer(stat_text)

# RESTART
@dp.message(lambda message: message.text == "➕ Yangi murojaat")
async def restart_form(message: Message, state: FSMContext):

    await message.answer(
        "👤 F.I.O kiriting.\n"
        "👤 Введите Ф.И.О.",
        reply_markup=ReplyKeyboardRemove()
    )

    await state.set_state(Form.fullname)

# HEALTH CHECK
async def health_check(request):
    return web.Response(text="Bot is running")

# WEB SERVER
async def start_web_server():

    app = web.Application()
    app.router.add_get("/", health_check)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 10000))

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port
    )

    await site.start()

# MAIN
async def main():

    print("✅ Bot ishga tushdi...")

    await start_web_server()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())