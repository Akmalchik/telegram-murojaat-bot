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
    ReplyKeyboardRemove
)
from aiogram.filters import CommandStart
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder

# Загрузка переменных окружения из файла .env
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("SHEET_URL")

# Инициализация бота и диспетчера с хранилищем в оперативной памяти
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Глобальный список для хранения махаллей отправленных обращений (для статистики)
appeals_data = []

# Множество для защиты активных asyncio-тасков от удаления сборщиком мусора
active_tasks = set()

# Глобальные буферы для накопления медиа и текста при прерывистом вводе
message_buffers = {}  # Хранит списки текстовых строк для каждого пользователя
photo_buffers = {}    # Хранит списки file_id фотографий для каждого пользователя
message_tasks = {}    # Хранит активные таски таймера отправки (asyncio.Task)

# Список махаллей Пскентского района для клавиатуры
MAHALLALAR = [
    "Bekobod MFY", "Saidobod MFY", "Chimqo‘rg‘on MFY", "Murot Ali MFY",
    "Fayzobod MFY", "Do‘ngqo‘rg‘on MFY", "Mo‘minobod MFY", "Birlik MFY",
    "Yangiobod MFY", "Ko‘lota MFY", "Guliston MFY", "Navoiy MFY",
    "Lolaariq MFY", "Ming tepa MFY", "G‘ayrat MFY", "Do‘stlik MFY",
    "Oqtepa MFY", "Mitan MFY", "Oybek MFY", "Kultepa MFY",
    "Mustaqillik MFY", "Taraqqiyot MFY", "Oqtom MFY"
]

# Машина состояний (FSM) для контекстного опроса жителя
class Form(StatesGroup):
    fullname = State()  # Ожидание ввода Ф.И.О.
    mahalla = State()   # Ожидание выбора махалли
    phone = State()     # Ожидание номера телефона
    text = State()      # Ожидание текста обращения и фото

# Хэндлер команды /start
@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):

    # Бот должен обрабатывать обращения только в личных сообщениях
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
• muammo
• taklif
• shikoyat
• va boshqa murojaatlarni yuborishingiz mumkin.

————————————

Здравствуйте!

Вы обратились в бот обращений районного хокимията.

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
    # Переводим пользователя в состояние ожидания имени
    await state.set_state(Form.fullname)

# Хэндлер ввода Ф.И.О.
@dp.message(Form.fullname)
async def get_name(message: Message, state: FSMContext):
    name = message.text.strip()

    # Регулярка валидации: поддерживает кириллицу, латиницу, узбекские апострофы и дефисы
    if not re.fullmatch(r"[A-Za-zА-Яа-яЁёЎўҚқҒғҲҳİıŞşÇçÖöÜü'‘’\- ]+", name):
        await message.answer(
            "❌ F.I.O noto‘g‘ri formatda.\n"
            "❌ Неверный формат Ф.И.О."
        )
        return

    # Сохраняем имя в контекст FSM
    await state.update_data(fullname=name)

    # Динамическое построение красивой клавиатуры махаллей в 2 колонки
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
    # Переводим в состояние ожидания махалли
    await state.set_state(Form.mahalla)

# Хэндлер выбора махалли
@dp.message(Form.mahalla)
async def get_mahalla(message: Message, state: FSMContext):
    mahalla = message.text.strip()

    # Строгая проверка на присутствие махалли в утвержденном списке
    if mahalla not in MAHALLALAR:
        await message.answer(
            "❌ Mahalla ro‘yxatda topilmadi.\n"
            "❌ Махалля не найдена в списке."
        )
        return

    await state.update_data(mahalla=mahalla)

    # Клавиатура для быстрой отправки контакта в один клик
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
    # Переводим в состояние ожидания телефона
    await state.set_state(Form.phone)

# Хэндлер получения номера телефона
@dp.message(Form.phone)
async def get_phone(message: Message, state: FSMContext):
    # Проверяем, как отправлен номер: кнопкой-контактом или обычным текстом
    if message.contact:
        phone = message.contact.phone_number
    else:
        phone = message.text.strip()

    await state.update_data(phone=phone)

    # Удаляем старую клавиатуру, подготавливая чат к свободному вводу текста/медиа
    await message.answer(
        "📝 Murojaatingizni yozing.\n"
        "📷 Rasm yuborishingiz ham mumkin.\n\n"
        "📝 Напишите обращение.\n"
        "📷 Можно также отправить фото.",
        reply_markup=ReplyKeyboardRemove()
    )
    # Переводим в финальное состояние сбора обращения
    await state.set_state(Form.text)

# Асинхронная функция финализации и отправки обращения (вызывается по таймеру)
async def send_appeal(user_id, state):
    # Анти-спам задержка: ждем 5 секунд. Если придут новые сообщения, этот таск отменится
    await asyncio.sleep(5)

    try:
        # Вытаскиваем накопленные данные из FSM context
        data = await state.get_data()
        
        # Получаем данные из буферов (если пусто — возвращаем пустой список)
        texts = message_buffers.get(user_id, [])
        photos = photo_buffers.get(user_id, [])

        # Если пользователь умудрился сбросить буфер до завершения таска, выходим
        if not texts and not photos:
            return

        # Склеиваем весь присланный текст через перенос строки
        full_text = "\n".join(texts) if texts else "📷 Murojaat faqat rasmdan iborat / Обращение состоит только из фото"

        # Фиксируем махаллю в глобальной статистике оперативной памяти
        appeals_data.append({
            "mahalla": data['mahalla']
        })

        # Генерируем уникальный ID на основе общей длины накопленных данных
        appeal_id = str(len(appeals_data)).zfill(5)

        # Формируем красивый шаблон сообщения для группы хокимията
        result = f"""
📨 Yangi murojaat / Новое обращение

🆔 ID: #{appeal_id}

👤 F.I.O: {data['fullname']}
🏠 Mahalla: {data['mahalla']}
📞 Telefon: {data['phone']}

📝 Murojaat / Обращение:
{full_text}
"""

        # 1. Отправляем текстовые данные в рабочую группу хокимията
        await bot.send_message(GROUP_ID, result)

        # 2. Если в буфере есть фотографии, пересылаем их в группу по очереди
        for photo_id in photos:
            await bot.send_photo(GROUP_ID, photo_id)

        # 3. Уведомляем жителя об успешной регистрации обращения
        restart_kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="➕ Yangi murojaat")]
            ],
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

        # 4. Асинхронно логируем данные в Google Sheets без блокировки основного потока
        payload = {
            "id": appeal_id,
            "fullname": data['fullname'],
            "mahalla": data['mahalla'],
            "phone": data['phone'],
            "text": full_text
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(SHEET_URL, json=payload, timeout=5) as response:
                    if response.status != 200:
                        print(f"Ошибка Google Sheets API: {response.status}")
            except Exception as e:
                print("Критическая ошибка при отправке в Google Sheets:", e)

        # 5. Очищаем состояние FSM и буферы СТРОГО в самом конце успешной отправки!
        await state.clear()
        message_buffers.pop(user_id, None)
        photo_buffers.pop(user_id, None)
        message_tasks.pop(user_id, None)

    except asyncio.CancelledError:
        # Перехватываем отмену таска. Происходит, когда пользователь дописал сообщение в течение 5 секунд
        pass

# Хэндлер накопления контента обращения (принимает текст, фото и подписи к фото)
@dp.message(Form.text)
async def get_text(message: Message, state: FSMContext):
    user_id = message.from_user.id

    # Инициализируем пустые списки в буфере, если пользователь пишет впервые
    if user_id not in message_buffers:
        message_buffers[user_id] = []
    if user_id not in photo_buffers:
        photo_buffers[user_id] = []

    # Флаг для проверки, валидный ли тип сообщения прислал пользователь
    valid_content = False

    # Если пришел чистый текст
    if message.text:
        message_buffers[user_id].append(message.text)
        valid_content = True

    # Если пришло фото с подписью (caption)
    if message.caption:
        message_buffers[user_id].append(message.caption)
        valid_content = True

    # Если пришло фото (захватываем самый максимальный размер из массива)
    if message.photo:
        largest_photo = message.photo[-1].file_id
        photo_buffers[user_id].append(largest_photo)
        valid_content = True

    # Если юзер отправил неподдерживаемый контент (стикер, локацию, войс, документ)
    if not valid_content:
        await message.answer(
            "❌ Faqat matn yoki rasm yuboring.\n"
            "❌ Отправьте только текст или фото."
        )
        return

    # Сброс (отмена) предыдущего таймера отправки, если пользователь продолжает писать/скидывать медиа
    old_task = message_tasks.get(user_id)
    if old_task:
        old_task.cancel()

    # Создаем новую задачу планирования отправки обращения через 5 секунд
    task = asyncio.create_task(send_appeal(user_id, state))
    
    # Регистрируем таск в глобальном set, чтобы защитить его от Garbage Collector
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)

    # Запоминаем текущий таск пользователя для возможности его отмены на следующем шаге
    message_tasks[user_id] = task

# Административный хэндлер вызова внутренней статистики (доступен только внутри группы хокимията)
@dp.message(lambda message: message.text == "/stat")
async def statistics(message: Message):
    if message.chat.id != GROUP_ID:
        return

    total = len(appeals_data)
    
    # Считаем количество упоминаний каждой махалли
    mahalla_counter = Counter(item['mahalla'] for item in appeals_data)

    stat_text = (
        f"📊 Statistika / Статистика\n\n"
        f"📝 Jami murojaatlar (Всего обращений): {total}\n\n"
    )

    for mahalla, count in mahalla_counter.items():
        stat_text += f"{mahalla} — {count}\n"

    await message.answer(stat_text)

# Хэндлер кнопки перезапуска формы для отправки повторного обращения
@dp.message(lambda message: message.text == "➕ Yangi murojaat")
async def restart_form(message: Message, state: FSMContext):
    await message.answer(
        "👤 F.I.O kiriting:\n"
        "👤 Введите Ф.И.О.",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Form.fullname)
# Fallback handler после завершённого обращения
@dp.message()
async def unknown_message(message: Message):

    # Игнорируем группы
    if message.chat.type != "private":
        return

    await message.answer(
        "ℹ️ Sizning murojaatingiz allaqachon yuborilgan.\n\n"

        "➕ Yangi murojaat yuborish uchun:\n"
        "- “➕ Yangi murojaat” tugmasini bosing\n"
        "yoki\n"
        "- /start buyrug‘ini yuboring.\n\n"

        "————————————\n\n"

        "ℹ️ Ваше обращение уже отправлено.\n\n"

        "➕ Чтобы создать новое обращение:\n"
        "- нажмите кнопку “➕ Yangi murojaat”\n"
        "или\n"
        "- отправьте команду /start"
    )

# Эндпоинт для прохождения проверки работоспособности (Health Check) на хостингах
async def health_check(request):
    return web.Response(text="Bot is running")

# Инициализация и запуск фонового веб-сервера aiohttp
async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()

    # Извлечение порта из переменных среды хостинга (по умолчанию 10000 для Render)
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

# Главная точка входа в приложение (Main)
async def main():
    print("✅ Бот успешно запущен и готов к работе...")
    
    # Параллельно поднимаем веб-сервер для удержания деплоя в онлайне
    await start_web_server()
    
    # Запускаем лонг-поллинг (прослушивание серверов Telegram)
    await dp.start_polling(bot)

if __name__ == "__main__":
    # Запуск асинхронного цикла событий
    asyncio.run(main())