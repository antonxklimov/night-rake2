import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print("Bot function started!")
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Update
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from datetime import datetime, timedelta
from google_sheets import get_sheet, row_to_user, upload_photo_to_drive, COLUMNS, delete_user_by_telegram_id
import gspread
# --- Для вебхуков ---
import logging
from aiohttp import web

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API_TOKEN = "7427155199:AAEqoEJw71PwOdnGFCQVLNV8ueskJ3gglBo"
API_TOKEN = os.environ.get("TELEGRAM_API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_API_TOKEN is not set in environment variables!")

# --- Настройки вебхука ---
WEBHOOK_PATH = "/webhook"
WEBHOOK_BASE_URL = os.environ.get("WEBHOOK_BASE_URL", "https://685a75f4003afd31f1be.fra.appwrite.run")
WEBHOOK_URL = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
USE_WEBHOOK = os.environ.get("USE_WEBHOOK", "0") == "1"

# --- Для деплоя через Appwrite: создаём credentials.json из секрета ---
CREDENTIALS_PATH = "/tmp/credentials.json"
if os.environ.get("GOOGLE_CREDENTIALS"):
    with open(CREDENTIALS_PATH, "w") as f:
        f.write(os.environ["GOOGLE_CREDENTIALS"])
    print(f"[INFO] credentials.json written to {CREDENTIALS_PATH}")
else:
    print("[WARN] GOOGLE_CREDENTIALS env var not set!")

# --- КЭШ пользователей ---
users_cache = {}

# --- Инициализация кэша при старте ---
def load_users_cache():
    ws = get_sheet()
    records = ws.get_all_values()
    from google_sheets import get_header_mapping
    header_mapping = get_header_mapping(ws)
    for row in records[1:]:
        user = row_to_user(row, header_mapping)
        users_cache[user['Telegram ID']] = user

# --- Универсальный поиск пользователя по никнейму: сначала кэш, потом Google Sheets ---
def get_user_by_username_anywhere(username):
    username = username.lstrip('@')
    # 1. Поиск в кэше
    for user in users_cache.values():
        if user.get('Никнейм', '').lower() == username.lower():
            return user, 'cache'
    # 2. Поиск в Google Sheets
    ws = get_sheet()
    records = ws.get_all_values()
    from google_sheets import get_header_mapping
    header_mapping = get_header_mapping(ws)
    norm_nickname = None
    for code_key in COLUMNS:
        if 'никнейм' in code_key.lower():
            norm_nickname = get_header_mapping(ws).get(normalize_header(code_key), (None, 1))[1]
            break
    for row in records[1:]:
        if len(row) > norm_nickname and row[norm_nickname].lower() == username.lower():
            return row_to_user(row, header_mapping), 'sheet'
    return None, None

# --- sync_users_cache: уменьшить интервал до 15 секунд ---
async def sync_users_cache():
    while True:
        ws = get_sheet()
        from google_sheets import get_header_mapping, find_user_row, update_user, add_user
        header_mapping = get_header_mapping(ws)
        records = ws.get_all_values()
        # Сопоставление Telegram ID -> (row_idx, row_dict)
        sheet_users = {}
        for idx, row in enumerate(records[1:], start=2):  # первая строка — заголовки
            user = row_to_user(row, header_mapping)
            sheet_users[user['Telegram ID']] = (idx, user)
        # Обновить существующих и добавить новых
        for telegram_id, cache_user in users_cache.items():
            sheet_entry = sheet_users.get(telegram_id)
            if sheet_entry:
                idx, sheet_user = sheet_entry
                # Если данные отличаются — обновить
                if any(str(cache_user.get(col, '')) != str(sheet_user.get(col, '')) for col in COLUMNS):
                    row = [cache_user.get(col, '') for col in COLUMNS]
                    start_col = gspread.utils.rowcol_to_a1(1, 1)[0]
                    end_col = gspread.utils.rowcol_to_a1(1, len(header_mapping))[0]
                    ws.update(f'{start_col}{idx}:{end_col}{idx}', [row])
            else:
                # Добавить нового пользователя
                add_user(cache_user['Telegram ID'], cache_user.get('Имя', ''), cache_user.get('Никнейм', ''))
                # После добавления можно обновить остальные поля, если нужно
        print(f"[SYNC] Users cache synced at {datetime.now()}")
        await asyncio.sleep(15)  # 15 секунд

# Условия для статуса "Резидент"
CONDITIONS = [
    "Пришёл хотя бы раз",
    "Привёл друга",
    "История из зала",
    "Подготовленное выступление",
    "Фото с табличкой",
    "3 посещения подряд"
]

# Награды
REWARDS = [
    (1, "Можно поесть с кейтеринга"),
    (5, "Бронь места в первом ряду (5 мест)"),
    (10, "Пицца с собой"),
    (15, "Футболка или худи")
]

# FSM для чек-ина с фото
class CheckinPhoto(StatesGroup):
    waiting_for_photo = State()

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Главная клавиатура
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Зачекиниться")],
        [KeyboardButton(text="Прогресс")],
        [KeyboardButton(text="Баланс")]
    ],
    resize_keyboard=True
)

# Хелпер: рассчитать сколько до следующей награды
def next_reward(balance):
    for points, reward in REWARDS:
        if balance < points:
            return points - balance, reward
    return None, None

def parse_visits(visits_str):
    if not visits_str:
        return []
    return [datetime.strptime(d, "%Y-%m-%d").date() for d in visits_str.split(",") if d]

def visits_to_str(visits):
    return ",".join([str(d) for d in visits])

def get_conditions(user):
    conds = [False]*6
    if user['Баллы'] > 0:
        conds[0] = True
    if user['Привел друга'] == 'yes':
        conds[1] = True
    if user['История'] == 'yes':
        conds[2] = True
    if user['Выступление'] == 'yes':
        conds[3] = True
    if user.get('Фото с табличкой', '') == 'yes':
        conds[4] = True
    if user['3 визита подряд'] == 'yes':
        conds[5] = True
    # Автоматически выставляем статус резидента
    if all(conds):
        if user.get('Резидент') != 'yes':
            user['Резидент'] = 'yes'
            update_user(user['Telegram ID'], user)
    else:
        if user.get('Резидент') != 'no':
            user['Резидент'] = 'no'
            update_user(user['Telegram ID'], user)
    return conds

def get_main_kb(user):
    visits = parse_visits(user['Даты посещений'])
    today = datetime.now().date()
    if visits and visits[-1] == today:
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Прогресс")],
                [KeyboardButton(text="Баланс")]
            ],
            resize_keyboard=True
        )
    else:
        return main_kb

# --- Вспомогательные функции для работы с кэшем ---
def get_user(user_id):
    return users_cache.get(str(user_id))

def add_user(user_id, name, username=""):
    users_cache[str(user_id)] = {
        'Telegram ID': str(user_id),
        'Имя': name,
        'Никнейм': username,
        'Баллы': 0,
        'Даты посещений': '',
        'Фото': 'no',
        'Ссылка на фото': '',
        'Фото с табличкой': 'no',
        'История': 'no',
        'Выступление': 'no',
        'Привел друга': 'no',
        '3 визита подряд': 'no',
        'Резидент': 'no',
    }

def update_user(user_id, data):
    users_cache[str(user_id)] = data

# --- Список админов ---
ADMINS = {216453}

# --- Вспомогательная функция: поиск пользователя по username (никнейму) ---
def get_user_by_username(username):
    username = username.lstrip('@')
    for user in users_cache.values():
        if user.get('Никнейм', '').lower() == username.lower():
            return user
    return None

def delete_user_by_username(username):
    for user_id, user in list(users_cache.items()):
        if user.get('Никнейм', '').lower() == username.lower():
            del users_cache[user_id]
            return True
    return False

# --- Обновление никнейма пользователя при любом действии ---
async def update_nickname_on_action(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if user:
        username = message.from_user.username or ""
        if user.get('Никнейм', '') != username:
            user['Никнейм'] = username
            update_user(user_id, user)

# --- Декоратор для обновления никнейма ---
def nickname_updater(handler):
    async def wrapper(message: Message, *args, **kwargs):
        await update_nickname_on_action(message)
        return await handler(message, *args, **kwargs)
    return wrapper

# /start — сразу регистрация и показ двух кнопок
@dp.message(Command("start"))
@nickname_updater
async def cmd_start(message: Message, **kwargs):
    user_id = message.from_user.id
    name = message.from_user.full_name
    username = message.from_user.username or ""
    user = get_user(user_id)
    if not user:
        add_user(user_id, name, username)
        user = get_user(user_id)
    else:
        # Обновляем имя и никнейм, если пользователь уже есть
        user['Имя'] = name
        user['Никнейм'] = username
        update_user(user_id, user)
    text = (
        "Привет, друг! 💔\n"
        "Здесь за каждый факап — баллы.\n"
        "За баллы — футболка, пицца и место в первом ряду.\n"
        "Всё просто."
    )
    await message.answer(text, reply_markup=get_main_kb(user))

# /чек-ин
@dp.message(Command("чек-ин"))
async def cmd_checkin(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала напиши /start!", reply_markup=main_kb)
        return
    visits = parse_visits(user['Даты посещений'])
    today = datetime.now().date()
    if visits and visits[-1] == today:
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Прогресс")]],
            resize_keyboard=True
        )
        await message.answer("Ты уже отмечался сегодня! 🦶", reply_markup=kb)
        return
    await message.answer("Пришли селфи для чек-ина! 📸", reply_markup=ReplyKeyboardRemove())
    await state.set_state(CheckinPhoto.waiting_for_photo)

@dp.message(CheckinPhoto.waiting_for_photo)
async def process_checkin_photo(message: Message, state: FSMContext):
    # Если пришла команда — сбрасываем состояние и передаём обработку дальше
    if message.text and message.text.startswith("/"):
        await state.clear()
        await dp.feed_update(message)
        return
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала напиши /start!", reply_markup=main_kb)
        await state.clear()
        return
    if not message.photo:
        await message.answer("Пожалуйста, пришли именно фото!")
        return
    # Сохраняем фото локально
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    file_path = file.file_path
    local_path = f"checkin_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    await bot.download_file(file_path, local_path)
    # Обновляем пользователя в кэше (без ссылки на фото)
    visits = parse_visits(user['Даты посещений'])
    today = datetime.now().date()
    visits.append(today)
    balance = int(user['Баллы']) + 1
    conds = get_conditions(user)
    conds[0] = True
    user['Фото'] = 'yes'
    # Проверка 3 посещения подряд (≤10 дней между каждым)
    if len(visits) >= 3:
        last3 = visits[-3:]
        if (last3[2] - last3[0]).days <= 20:
            if not conds[5]:
                balance += 1
                conds[5] = True
                user['3 визита подряд'] = 'yes'
    user['Баллы'] = balance
    user['Даты посещений'] = visits_to_str(visits)
    update_user(user_id, user)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Прогресс")]],
        resize_keyboard=True
    )
    await message.answer(f"Чек-ин с селфи засчитан! +1 грабля. Фото обрабатывается, всё ок 👍\nВсего грабель: {balance}", reply_markup=kb)
    await state.clear()
    # Загрузка фото в Google Drive и обновление ссылки — в фоне
    asyncio.create_task(_upload_photo_and_update_user(user_id, local_path))

async def _upload_photo_and_update_user(user_id, local_path):
    from google_sheets import upload_photo_to_drive
    drive_link = upload_photo_to_drive(local_path, os.path.basename(local_path))
    os.remove(local_path)
    user = get_user(user_id)
    if user:
        user['Ссылка на фото'] = drive_link
        update_user(user_id, user)

# /баланс
@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала напиши /start!", reply_markup=main_kb)
        return
    conds = get_conditions(user)
    to_next, reward = next_reward(int(user['Баллы']))
    status = "Резидент Граблей! 🎖" if all(conds) else "Почётный гость"
    text = (
        f"<b>Баланс</b>: {user['Баллы']}\n"
        f"{f'<b>До следующей награды</b>: {to_next} — {reward}' if to_next else 'Ты собрал все награды!'}\n"
        f"<b>Статус</b>: {status}"
    )
    await message.answer(text, reply_markup=get_main_kb(user), parse_mode="HTML")

# /прогресс
@dp.message(Command("прогресс"))
async def cmd_progress(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала напиши /start!", reply_markup=main_kb)
        return
    conds = get_conditions(user)
    text = "\n".join([
        f"{'✅' if done else '❌'} {cond}" for done, cond in zip(conds, CONDITIONS)
    ])
    visits = ", ".join([str(d) for d in parse_visits(user['Даты посещений'])])
    await message.answer(
        f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
        reply_markup=get_main_kb(user),
        parse_mode="HTML"
    )

# /checkin (дублирует /чек-ин)
@dp.message(Command("checkin"))
async def cmd_checkin_alias(message: Message):
    await cmd_checkin(message)

# /balance (дублирует /баланс)
@dp.message(Command("balance"))
async def cmd_balance_alias(message: Message):
    await cmd_balance(message)

# /progress (дублирует /прогресс, но с кнопками)
@dp.message(Command("progress"))
async def cmd_progress_buttons(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала напиши /start!", reply_markup=main_kb)
        return
    conds = get_conditions(user)
    text = "\n".join([
        f"{'✅' if done else '❌'} {cond}" for done, cond in zip(conds, CONDITIONS)
    ])
    visits = ", ".join([str(d) for d in parse_visits(user['Даты посещений'])])
    # Если не было чек-ина — не показываем остальные кнопки
    if not conds[0]:
        await message.answer(
            f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}\n\nЕсли хочешь обновить прогресс — сначала зачекинься!",
            reply_markup=get_main_kb(user),
            parse_mode="HTML"
        )
        return
    # Кнопки для условий 2-5 + Назад к меню
    buttons = []
    if not conds[1]:
        buttons.append([KeyboardButton(text="Привёл друга")])
    if not conds[2]:
        buttons.append([KeyboardButton(text="История из зала")])
    if not conds[3]:
        buttons.append([KeyboardButton(text="Подготовленное выступление")])
    if not conds[4]:
        buttons.append([KeyboardButton(text="Фото с табличкой")])
    if buttons:
        buttons.append([KeyboardButton(text="Назад к меню")])
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True) if buttons else get_main_kb(user)
    await message.answer(
        f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(lambda m: m.text == "Зачекиниться")
async def handle_checkin_button(message: Message, state: FSMContext):
    await cmd_checkin(message, state)

@dp.message(lambda m: m.text == "Прогресс")
async def handle_progress_button(message: Message):
    await cmd_progress_buttons(message)

@dp.message(lambda m: m.text == "Баланс")
async def handle_balance_button(message: Message):
    await cmd_balance(message)

@dp.message(lambda m: m.text == "Назад к меню")
async def handle_back_to_menu(message: Message):
    user = get_user(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=get_main_kb(user))

@dp.message(lambda m: m.text == "Привёл друга")
async def handle_friend_brought(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if not conds[1]:
        conds[1] = True
        user['Баллы'] = int(user['Баллы']) + 1
        user['Привел друга'] = 'yes'
        update_user(user_id, user)
        await message.answer("Привёл друга засчитано! +1 грабля 🏅", reply_markup=get_main_kb(user))
    else:
        await message.answer("Это уже засчитано!", reply_markup=get_main_kb(user))
    await cmd_progress_buttons(message)

@dp.message(lambda m: m.text == "История из зала")
async def handle_story(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if not conds[2]:
        conds[2] = True
        user['Баллы'] = int(user['Баллы']) + 1
        user['История'] = 'yes'
        update_user(user_id, user)
        await message.answer("История из зала засчитано! +1 грабля 🏅", reply_markup=get_main_kb(user))
    else:
        await message.answer("Это уже засчитано!", reply_markup=get_main_kb(user))
    await cmd_progress_buttons(message)

@dp.message(lambda m: m.text == "Подготовленное выступление")
async def handle_performance(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if not conds[3]:
        conds[3] = True
        user['Баллы'] = int(user['Баллы']) + 2
        user['Выступление'] = 'yes'
        update_user(user_id, user)
        await message.answer("Выступление засчитано! +2 грабли 🎤", reply_markup=get_main_kb(user))
    else:
        await message.answer("Это уже засчитано!", reply_markup=get_main_kb(user))
    await cmd_progress_buttons(message)

@dp.message(lambda m: m.text == "Фото с табличкой / с другом" or m.text == "Фото с табличкой")
async def handle_photo_with_sign(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if not conds[4]:
        conds[4] = True
        user['Баллы'] = int(user['Баллы']) + 1
        user['Фото с табличкой'] = 'yes'
        update_user(user_id, user)
        await message.answer("Фото с табличкой засчитано! +1 грабля 🏅", reply_markup=get_main_kb(user))
    else:
        await message.answer("Это уже засчитано!", reply_markup=get_main_kb(user))
    await cmd_progress_buttons(message)

@dp.message(Command("delete"))
@nickname_updater
async def cmd_delete(message: Message, **kwargs):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].lstrip("@").isalnum():
        await message.answer("Используй: /delete @username или /delete username")
        return
    username = args[1].lstrip("@")
    user, source = get_user_by_username_anywhere(username)
    if not user:
        await message.answer(f"Пользователь {args[1]} не найден или у него не установлен username.")
        return
    # Удаляем из кэша и из таблицы
    deleted_cache = delete_user_by_username(username)
    deleted_sheet = False
    if user:
        deleted_sheet = delete_user_by_telegram_id(int(user['Telegram ID']))
    load_users_cache()
    await message.answer(f"Пользователь @{username} удалён из базы (кэш: {deleted_cache}, таблица: {deleted_sheet}). Прогресс сброшен. (Источник: {source})")

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMINS:
        return
    text = (
        "<b>Админ-команды:</b>\n"
        "/add @username N — добавить N баллов участнику\n"
        "/check @username — посмотреть участника\n"
        "/broadcast текст — рассылка всем участникам\n"
        "/residentify @username — присвоить статус резидента\n"
        "/delete @username — удалить пользователя и сбросить прогресс"
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: {message.from_user.id}")

# /add @username N — добавить N баллов участнику
@dp.message(Command("add"))
@nickname_updater
async def cmd_add(message: Message, **kwargs):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split()
    if len(args) != 3 or not args[1].lstrip("@").isalnum() or not args[2].isdigit():
        await message.answer("Используй: /add @username N или /add username N")
        return
    username = args[1].lstrip("@")
    n = int(args[2])
    user, source = get_user_by_username_anywhere(username)
    if not user:
        await message.answer(f"Пользователь {args[1]} не найден или у него не установлен username.")
        return
    if not user.get('Никнейм'):
        await message.answer(f"У пользователя нет username. Операция невозможна.")
        return
    user['Баллы'] = int(user['Баллы']) + n
    update_user(user['Telegram ID'], user)
    await message.answer(f"@{username}: +{n} баллов. Теперь {user['Баллы']} баллов. (Источник: {source})")

# /check @username — посмотреть участника
@dp.message(Command("check"))
@nickname_updater
async def cmd_check(message: Message, **kwargs):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].lstrip("@").isalnum():
        await message.answer("Используй: /check @username или /check username")
        return
    username = args[1].lstrip("@")
    user, source = get_user_by_username_anywhere(username)
    if not user:
        await message.answer(f"Пользователь {args[1]} не найден или у него не установлен username.")
        return
    text = (
        f"<b>@{username}</b>\n"
        f"Имя: {user['Имя']}\n"
        f"Баллы: {user['Баллы']}\n"
        f"Даты посещений: {user['Даты посещений']}\n"
        f"Резидент: {'yes' if user.get('Резидент') == 'yes' else 'no'}\n"
        f"Фото: {user.get('Ссылка на фото', '—') or '—'}\n"
        f"<i>Источник: {source}</i>"
    )
    await message.answer(text, parse_mode="HTML")

# /broadcast текст — рассылка всем участникам
@dp.message(Command("broadcast"))
@nickname_updater
async def cmd_broadcast(message: Message, **kwargs):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.answer("Используй: /broadcast текст")
        return
    text = args[1]
    count = 0
    for user in users_cache.values():
        try:
            await bot.send_message(user['Telegram ID'], text)
            count += 1
        except Exception:
            pass
    await message.answer(f"Рассылка завершена. Отправлено {count} пользователям.")

# /residentify @username — присвоить статус резидента
@dp.message(Command("residentify"))
@nickname_updater
async def cmd_residentify(message: Message, **kwargs):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].lstrip("@").isalnum():
        await message.answer("Используй: /residentify @username или /residentify username")
        return
    username = args[1].lstrip("@")
    user, source = get_user_by_username_anywhere(username)
    if not user:
        await message.answer(f"Пользователь {args[1]} не найден или у него не установлен username.")
        return
    user['Резидент'] = 'yes'
    update_user(user['Telegram ID'], user)
    await message.answer(f"@{username} теперь резидент! (Источник: {source})")

# Appwrite Function entry point
async def main(context):
    try:
        context.log(f"Received request: {context.req.method} {context.req.path}")
        context.log(f"Context dir: {dir(context)}")
        if hasattr(context, 'req'):
            context.log(f"Context.req dir: {dir(context.req)}")
        # Универсально получить тело запроса
        request_body = None
        if hasattr(context, 'req_body'):
            request_body = context.req_body
        elif hasattr(context, 'req') and hasattr(context.req, 'body'):
            request_body = context.req.body
        elif hasattr(context, 'data'):
            request_body = context.data
        elif hasattr(context, 'req') and hasattr(context.req, 'json'):
            # Возможно, это async-метод
            try:
                request_body = await context.req.json()
            except Exception as e:
                context.error(f"context.req.json() error: {e}")
        if request_body is None:
            context.error("Не удалось найти тело запроса!")
            return context.res.json({"error": "No request body found"}, 400)
        context.log(f"Request body: {request_body}")
        update = Update.model_validate(request_body)
        await dp.feed_update(bot, update)
        return context.res.json({"status": "ok"})
    except Exception as e:
        context.error(f"Error: {e}")
        return context.res.json({"error": str(e)}) 