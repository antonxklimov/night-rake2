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
from google_sheets import get_sheet, row_to_user, upload_photo_to_drive, COLUMNS, delete_user_by_telegram_id, normalize_header
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

# --- Получить пользователя: сначала кэш, потом Google Sheets ---
def get_user(user_id):
    user = users_cache.get(str(user_id))
    if user is None:
        from google_sheets import get_user as gs_get_user
        user = gs_get_user(user_id)
        if user:
            users_cache[str(user_id)] = user
    return user

# --- Добавить пользователя ---
def add_user(user_id, name, username=""):
    from google_sheets import add_user as gs_add_user, get_user as gs_get_user
    gs_add_user(user_id, name, username)
    user = gs_get_user(user_id)
    if user:
        users_cache[str(user_id)] = user

# --- Обновить пользователя ---
def update_user(user_id, data):
    from google_sheets import update_user as gs_update_user, get_user as gs_get_user, COLUMNS
    # Гарантируем, что все ключи есть
    for col in COLUMNS:
        if col not in data:
            data[col] = ''
    gs_update_user(user_id, data)
    user = gs_get_user(user_id)
    if user:
        users_cache[str(user_id)] = user

# --- Удалить пользователя из кэша ---
def delete_user_by_username(username):
    for user_id, user in list(users_cache.items()):
        if user.get('Никнейм', '').lower() == username.lower():
            del users_cache[user_id]
            return True
    return False

# --- Загрузка фото и обновление пользователя (синхронно) ---
async def _upload_photo_and_update_user(user_id, local_path):
    from google_sheets import upload_photo_to_drive
    drive_link = upload_photo_to_drive(local_path, os.path.basename(local_path))
    os.remove(local_path)
    user = get_user(user_id)
    if user:
        user['Ссылка на фото'] = drive_link
        update_user(user_id, user)

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
    "Пришёл минимум один раз",
    "Привёл друга",
    "История из зала",
    "Подготовленное выступление",
    "Фото с табличкой",
    "3 посещения подряд"
]

# Награды
REWARDS = [
    (1, "Добро пожаловать к кейтерингу! 😋"),
    (5, "Бронь места в первом ряду (5 мест) 💺"),
    (10, "Пицца с собой 🍕"),
    (15, "Футболка или худи 👕")
]

# FSM для чек-ина с фото
class CheckinPhoto(StatesGroup):
    waiting_for_photo = State()

# FSM для фото с другом
class FriendPhoto(StatesGroup):
    waiting_for_photo = State()

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Главная клавиатура
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Зачекиниться ✔️")],
        [KeyboardButton(text="Прогресс ✏️")],
        [KeyboardButton(text="Баланс 🏦")]
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
                [KeyboardButton(text="Прогресс ✏️")],
                [KeyboardButton(text="Баланс 🏦")]
            ],
            resize_keyboard=True
        )
    else:
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Зачекиниться ✔️")],
                [KeyboardButton(text="Прогресс ✏️")],
                [KeyboardButton(text="Баланс 🏦")]
            ],
            resize_keyboard=True
        )

# --- Список админов ---
ADMINS = {216453}

# --- Вспомогательная функция: поиск пользователя по username (никнейму) ---
def get_user_by_username(username):
    username = username.lstrip('@')
    for user in users_cache.values():
        if user.get('Никнейм', '').lower() == username.lower():
            return user
    return None

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    name = message.from_user.full_name
    username = message.from_user.username or ""
    # Быстрый UX: отправляем сообщение о запуске
    thinking_msg = await message.answer("Завожу мотор... 🏎️")
    user = get_user(user_id)
    if not user:
        add_user(user_id, name, username)
        user = get_user(user_id)
        # Инициализируем новые поля сразу
        user['last_checkin_ts'] = ''
        user['last_condition_ts'] = ''
        user['conditions_after_checkin'] = '0'
        update_user(user_id, user)
    else:
        # Обновляем имя и никнейм, если пользователь уже есть
        user['Имя'] = name
        user['Никнейм'] = username
        update_user(user_id, user)
    text = (
        "Привет, друг! 💔\n\n"
        "Здесь за каждый факап — баллы.\n"
        "За баллы — футболка, пицца и место в первом ряду.\n\n"
        "Всё просто. Добро пожаловать!"
    )
    # Удаляем thinking message
    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=thinking_msg.message_id)
    except Exception:
        pass
    await message.answer(text, reply_markup=get_main_kb(user))

@dp.message(Command("чек-ин"))
async def cmd_checkin(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала запусти бота!  /start!", reply_markup=main_kb)
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
        await message.answer("Сначала запусти бота!  /start!", reply_markup=main_kb)
        await state.clear()
        return
    if not message.photo:
        await message.answer("Пожалуйста, пришли именно фото! 📷")
        return
    # Сохраняем фото локально
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    file_path = file.file_path
    local_path = f"checkin_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    await bot.download_file(file_path, local_path)
    # Отправляем быстрое сообщение о том, что бот думает
    thinking_msg = await message.answer("Бот думает... ⌛")
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
    user['last_checkin_ts'] = datetime.now().isoformat()
    user['conditions_after_checkin'] = '0'
    update_user(user_id, user)
    await state.clear()
    # Долгая операция: загрузка фото и обновление ссылки
    await _upload_photo_and_update_user(user_id, local_path)
    # Follow-up сообщение с результатом
    await bot.send_message(
        chat_id=message.chat.id,
        text=f"Чек-ин с селфи засчитан! Спасибо!\n+1 грабля\n\n<b>Всего граблей:</b> {balance}",
        reply_markup=get_main_kb(user),
        parse_mode="HTML"
    )
    # Удаляем сообщение 'Бот думает...'
    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=thinking_msg.message_id)
    except Exception:
        pass

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала запусти бота!  /start!", reply_markup=main_kb)
        return
    conds = get_conditions(user)
    to_next, reward = next_reward(int(user['Баллы']))
    status = "Резидент Граблей! 🎖" if all(conds) else "Почётный гость 💫"
    text = (
        f"<b>Баланс</b>: {user['Баллы']}\n"
        f"{f'<b>До следующей награды</b>: {to_next} — {reward}' if to_next else 'Ты собрал все награды! ❤️'}\n"
        f"<b>Статус</b>: {status}"
    )
    await message.answer(text, reply_markup=get_main_kb(user), parse_mode="HTML")

@dp.message(Command("прогресс"))
async def cmd_progress(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала запусти бота!  /start!", reply_markup=main_kb)
        return
    conds = get_conditions(user)
    text = "\n".join([
        f"{'✅' if done else '❌'} {cond}" for done, cond in zip(conds, CONDITIONS)
    ])
    visits = ", ".join([str(d) for d in parse_visits(user['Даты посещений'])])
    await message.answer(
        "Если хочешь обновить прогресс — сначала зачекинься! ✔️\n\n"
        f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
        reply_markup=get_main_kb(user),
        parse_mode="HTML"
    )

@dp.message(Command("checkin"))
async def cmd_checkin_alias(message: Message):
    await cmd_checkin(message)

@dp.message(Command("balance"))
async def cmd_balance_alias(message: Message):
    await cmd_balance(message)

@dp.message(Command("progress"))
async def cmd_progress_buttons(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала запусти бота!  /start!", reply_markup=main_kb)
        return
    conds = get_conditions(user)
    text = "\n".join([
        f"{'✅' if done else '❌'} {cond}" for done, cond in zip(conds, CONDITIONS)
    ])
    visits = ", ".join([str(d) for d in parse_visits(user['Даты посещений'])])
    # Если не было чек-ина — не показываем остальные кнопки
    if not conds[0]:
        await message.answer(
            "Если хочешь обновить прогресс — сначала зачекинься! ✔️\n\n"
            f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
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
        buttons.append([KeyboardButton(text="← Назад в меню")])
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True) if buttons else get_main_kb(user)
    await message.answer(
        "Если хочешь обновить прогресс — сначала зачекинься! ✔️\n\n"
        f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(lambda m: m.text == "Зачекиниться ✔️")
async def handle_checkin_button(message: Message, state: FSMContext):
    await cmd_checkin(message, state)

@dp.message(lambda m: m.text == "Прогресс ✏️")
async def handle_progress_button(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    text = "\n".join([
        f"{'✅' if done else '❌'} {cond}" for done, cond in zip(conds, CONDITIONS)
    ])
    visits = ", ".join([str(d) for d in parse_visits(user['Даты посещений'])])
    if not conds[0]:
        await message.answer(
            "Если хочешь обновить прогресс — сначала зачекинься! ✔️\n\n"
            f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
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
        buttons.append([KeyboardButton(text="← Назад в меню")])
    kb = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True) if buttons else get_main_kb(user)
    await message.answer(
        f"<b>Твой прогресс</b>:\n{text}\n\n<b>Даты визитов</b>: {visits if visits else '—'}",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.message(lambda m: m.text == "Баланс 🏦")
async def handle_balance_button(message: Message):
    await cmd_balance(message)

@dp.message(lambda m: m.text == "← Назад в меню")
async def handle_back_to_menu(message: Message):
    user = get_user(message.from_user.id)
    await message.answer("Окей, возвращаемся в меню...", reply_markup=get_main_kb(user))

@dp.message(lambda m: m.text == "Привёл друга")
async def handle_friend_brought(message: Message, state: FSMContext):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if conds[1]:
        await message.answer("Уже засчитано! 😕", reply_markup=get_main_kb(user))
        return
    await message.answer("Пришли фото с другом для подтверждения! 🤳", reply_markup=ReplyKeyboardRemove())
    await state.set_state(FriendPhoto.waiting_for_photo)

@dp.message(FriendPhoto.waiting_for_photo)
async def process_friend_photo(message: Message, state: FSMContext):
    if message.text and message.text.startswith("/"):
        await state.clear()
        await dp.feed_update(message)
        return
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала запусти бота!  /start!", reply_markup=get_main_kb(user))
        await state.clear()
        return
    if not message.photo:
        await message.answer("Пожалуйста, пришли именно фото с другом! 🤳")
        return
    # Сохраняем фото локально
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    file_path = file.file_path
    local_path = f"friend_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    await bot.download_file(file_path, local_path)
    # Отправляем быстрое сообщение
    thinking_msg = await message.answer("Бот думает... ⌛")
    # Загрузка фото в Google Drive и получение ссылки
    from google_sheets import upload_photo_to_drive
    drive_link = upload_photo_to_drive(local_path, os.path.basename(local_path))
    os.remove(local_path)
    user = get_user(user_id)
    if user:
        user['Фото с другом'] = drive_link
        user['Баллы'] = int(user['Баллы']) + 1
        user['Привел друга'] = 'yes'
        user['last_condition_ts'] = datetime.now().isoformat()
        user['conditions_after_checkin'] = str(int(user.get('conditions_after_checkin', '0')) + 1)
        update_user(user_id, user)
    await state.clear()
    await bot.send_message(
        chat_id=message.chat.id,
        text="Фото с другом засчитано! +1 грабля 🏅",
        reply_markup=get_main_kb(user)
    )
    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=thinking_msg.message_id)
    except Exception:
        pass

@dp.message(lambda m: m.text == "История из зала")
async def handle_story(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    can_do, msg = can_perform_condition(user)
    if not can_do:
        await message.answer(msg or "Сначала зачекинься!", reply_markup=get_main_kb(user))
        return
    if not conds[2]:
        conds[2] = True
        user['Баллы'] = int(user['Баллы']) + 1
        user['История'] = 'yes'
        user['last_condition_ts'] = datetime.now().isoformat()
        user['conditions_after_checkin'] = str(int(user.get('conditions_after_checkin', '0')) + 1)
        update_user(user_id, user)
        await message.answer("История из зала засчитано! +1 грабля 🏅", reply_markup=get_main_kb(user))
    else:
        await message.answer("Уже засчитано! 😕", reply_markup=get_main_kb(user))

@dp.message(lambda m: m.text == "Подготовленное выступление")
async def handle_performance(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if not conds[3]:
        conds[3] = True
        user['Баллы'] = int(user['Баллы']) + 2
        user['Выступление'] = 'yes'
        user['last_condition_ts'] = datetime.now().isoformat()
        user['conditions_after_checkin'] = str(int(user.get('conditions_after_checkin', '0')) + 1)
        update_user(user_id, user)
        await message.answer("Выступление засчитано! +2 грабли 🎤", reply_markup=get_main_kb(user))
    else:
        await message.answer("Уже засчитано! 😕", reply_markup=get_main_kb(user))

@dp.message(lambda m: m.text == "Фото с табличкой / с другом" or m.text == "Фото с табличкой")
async def handle_photo_with_sign(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    conds = get_conditions(user)
    if not conds[4]:
        conds[4] = True
        user['Баллы'] = int(user['Баллы']) + 1
        user['Фото с табличкой'] = 'yes'
        user['last_condition_ts'] = datetime.now().isoformat()
        user['conditions_after_checkin'] = str(int(user.get('conditions_after_checkin', '0')) + 1)
        update_user(user_id, user)
        await message.answer("Фото с табличкой засчитано! +1 грабля 🏅", reply_markup=get_main_kb(user))
    else:
        await message.answer("Уже засчитано! 😕", reply_markup=get_main_kb(user))

@dp.message(Command("delete"))
async def cmd_delete(message: Message):
    if message.from_user.id not in ADMINS:
        return
    args = message.text.split()
    if len(args) != 2 or not args[1].lstrip("@").isalnum():
        await message.answer("Используй: /delete @username или /delete username")
        return
    username = args[1].lstrip("@")
    # Быстрый ответ админу
    thinking_msg = await message.answer("Бот думает... ⌛")
    user, source = get_user_by_username_anywhere(username)
    if not user:
        await message.answer(f"Пользователь {args[1]} не найден или у него не установлен username.")
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=thinking_msg.message_id)
        except Exception:
            pass
        return
    # Удаляем из кэша и из таблицы
    deleted_cache = delete_user_by_username(username)
    deleted_sheet = False
    if user:
        deleted_sheet = delete_user_by_telegram_id(int(user['Telegram ID']))
    load_users_cache()
    await message.answer(f"Пользователь @{username} удалён из базы (кэш: {deleted_cache}, таблица: {deleted_sheet}). Прогресс сброшен. (Источник: {source})")
    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=thinking_msg.message_id)
    except Exception:
        pass

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMINS:
        return
    text = (
        "<b>Админ-команды:</b>\n\n"
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

@dp.message(Command("add"))
async def cmd_add(message: Message):
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
    user['last_condition_ts'] = datetime.now().isoformat()
    user['conditions_after_checkin'] = '0'
    update_user(user['Telegram ID'], user)
    await message.answer(f"@{username}: +{n} баллов. Теперь {user['Баллы']} баллов. (Источник: {source})")

@dp.message(Command("check"))
async def cmd_check(message: Message):
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

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
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

@dp.message(Command("residentify"))
async def cmd_residentify(message: Message):
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
    user['last_condition_ts'] = datetime.now().isoformat()
    user['conditions_after_checkin'] = '0'
    update_user(user['Telegram ID'], user)
    await message.answer(f"@{username} теперь резидент! (Источник: {source})")

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

# В каждом хендлере условия (кроме чек-ина) — проверяю таймаут
TIMEOUT_MINUTES = 15
TIMEOUT_MSG = "Ой-ой! Ты слишком быстро набираешь баллы, дай себе отдохнуть 😮‍💨\nПопробуй через некоторое время!"

def can_perform_condition(user):
    # Если нет чек-ина — нельзя
    visits = parse_visits(user['Даты посещений'])
    today = datetime.now().date()
    if not (visits and visits[-1] == today):
        return False, None
    # Если после чек-ина не было ни одного условия — можно
    if str(user.get('conditions_after_checkin', '0')) == '0':
        return True, None
    # Если уже было одно условие — проверяем таймаут
    last_ts = user.get('last_condition_ts')
    if not last_ts:
        return True, None
    try:
        last_dt = datetime.fromisoformat(last_ts)
    except Exception:
        return True, None
    if datetime.now() - last_dt < timedelta(minutes=TIMEOUT_MINUTES):
        return False, TIMEOUT_MSG
    return True, None 