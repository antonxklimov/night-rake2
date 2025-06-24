import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict, Any, Optional
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import unicodedata

# Название таблицы и листа
SPREADSHEET_ID = '1pozCL8pbOxpE8lRMA_EDzGkcsutDj6c36UA00TSyS_M'
SHEET_NAME = 'Лист1'
DRIVE_FOLDER_ID = '1rAs1j1KuKOOuQyEp23OTJW6BEfOq2vuc'

# Столбцы таблицы (должны совпадать с первой строкой в Google Sheets)
COLUMNS = [
    'Telegram ID', 'Никнейм', 'Имя', 'Баллы', 'Даты посещений', 'Фото', 'Ссылка на фото', 'Фото с табличкой', 'История', 'Выступление', 'Привел друга', '3 визита подряд', 'Резидент'
]

# Авторизация и подключение к таблице
def get_sheet():
    creds = Credentials.from_service_account_file('/tmp/credentials.json', scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(SHEET_NAME)
    return worksheet

# Авторизация для Google Drive
def get_drive_service():
    creds = Credentials.from_service_account_file('/tmp/credentials.json', scopes=[
        'https://www.googleapis.com/auth/drive',
    ])
    return build('drive', 'v3', credentials=creds)

# Загрузка фото в Google Drive и получение публичной ссылки
def upload_photo_to_drive(local_path: str, filename: str) -> str:
    service = get_drive_service()
    file_metadata = {
        'name': filename,
        'parents': [DRIVE_FOLDER_ID]
    }
    media = MediaFileUpload(local_path, mimetype='image/jpeg')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    # Сделать файл публичным
    service.permissions().create(
        fileId=file['id'],
        body={'role': 'reader', 'type': 'anyone'},
    ).execute()
    link = f'https://drive.google.com/file/d/{file["id"]}/view?usp=sharing'
    return link

# --- Нормализация заголовков ---
def normalize_header(header: str) -> str:
    # Привести к нижнему регистру, убрать пробелы, заменить ё на е, удалить спецсимволы
    header = header.lower().replace('ё', 'е').replace('й', 'и')
    header = ''.join(c for c in header if c.isalnum())
    return header

# --- Получить маппинг: нормализованный_заголовок -> (оригинал, индекс) ---
def get_header_mapping(worksheet) -> dict:
    headers = worksheet.row_values(1)
    mapping = {}
    for idx, h in enumerate(headers):
        norm = normalize_header(h)
        mapping[norm] = (h, idx)
    return mapping

# --- Преобразование строки таблицы в dict по маппингу ---
def row_to_user(row: List[str], header_mapping: dict = None) -> Dict[str, Any]:
    # Если маппинг не передан, получить его из таблицы
    if header_mapping is None:
        ws = get_sheet()
        header_mapping = get_header_mapping(ws)
    user = {}
    for code_key in COLUMNS:
        norm = normalize_header(code_key)
        if norm in header_mapping:
            idx = header_mapping[norm][1]
            user[code_key] = row[idx] if idx < len(row) else ''
        else:
            user[code_key] = ''
    # Привести типы
    user['Баллы'] = int(user.get('Баллы', 0) or 0)
    if user.get('Резидент') not in ('yes', 'no'):
        user['Резидент'] = 'no'
    return user

# Поиск пользователя по Telegram ID
def find_user_row(worksheet, telegram_id: int, header_mapping: dict = None) -> Optional[int]:
    records = worksheet.get_all_values()
    if header_mapping is None:
        header_mapping = get_header_mapping(worksheet)
    id_idx = header_mapping.get(normalize_header('Telegram ID'), (None, 0))[1]
    for idx, row in enumerate(records[1:], start=2):  # первая строка — заголовки
        if str(telegram_id) == row[id_idx]:
            return idx
    return None

# Получить пользователя по Telegram ID
def get_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    ws = get_sheet()
    header_mapping = get_header_mapping(ws)
    records = ws.get_all_values()
    for row in records[1:]:
        id_idx = header_mapping.get(normalize_header('Telegram ID'), (None, 0))[1]
        if str(telegram_id) == row[id_idx]:
            return row_to_user(row, header_mapping)
    return None

# Добавить нового пользователя
def add_user(telegram_id: int, name: str, username: str = ""):
    ws = get_sheet()
    header_mapping = get_header_mapping(ws)
    # Собираем строку по маппингу
    user_data = {
        'Telegram ID': str(telegram_id),
        'Никнейм': username,
        'Имя': name,
        'Баллы': 0,
        'Даты посещений': '',
        'Фото': '',
        'Ссылка на фото': '',
        'Фото с табличкой': '',
        'История': '',
        'Выступление': '',
        'Привел друга': '',
        '3 визита подряд': '',
        'Резидент': 'no',
    }
    row = [''] * len(header_mapping)
    for code_key, value in user_data.items():
        norm = normalize_header(code_key)
        if norm in header_mapping:
            idx = header_mapping[norm][1]
            row[idx] = value
    ws.append_row(row)

# Обновить пользователя (поиск по Telegram ID)
def update_user(telegram_id: int, data: Dict[str, Any]):
    ws = get_sheet()
    header_mapping = get_header_mapping(ws)
    row_idx = find_user_row(ws, telegram_id, header_mapping)
    if not row_idx:
        return
    # Собираем строку по маппингу
    row = [''] * len(header_mapping)
    for code_key in COLUMNS:
        norm = normalize_header(code_key)
        if norm in header_mapping:
            idx = header_mapping[norm][1]
            row[idx] = data.get(code_key, '')
    # Определяем диапазон для обновления
    start_col = gspread.utils.rowcol_to_a1(1, 1)[0]
    end_col = gspread.utils.rowcol_to_a1(1, len(header_mapping))[0]
    ws.update(f'{start_col}{row_idx}:{end_col}{row_idx}', [row])

def delete_user_by_telegram_id(telegram_id: int):
    ws = get_sheet()
    header_mapping = get_header_mapping(ws)
    row_idx = find_user_row(ws, telegram_id, header_mapping)
    if row_idx:
        ws.delete_rows(row_idx)
        return True
    return False 