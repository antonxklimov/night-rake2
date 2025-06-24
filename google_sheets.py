import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict, Any, Optional
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

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
    creds = Credentials.from_service_account_file('credentials.json', scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(SHEET_NAME)
    return worksheet

# Авторизация для Google Drive
def get_drive_service():
    creds = Credentials.from_service_account_file('credentials.json', scopes=[
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

# Преобразование строки таблицы в dict
def row_to_user(row: List[str]) -> Dict[str, Any]:
    return {
        'Telegram ID': row[0],
        'Никнейм': row[1],
        'Имя': row[2],
        'Баллы': int(row[3]),
        'Даты посещений': row[4],
        'Фото': row[5],
        'Ссылка на фото': row[6],
        'Фото с табличкой': row[7] if len(row) > 7 else '',
        'История': row[8] if len(row) > 8 else '',
        'Выступление': row[9] if len(row) > 9 else '',
        'Привел друга': row[10] if len(row) > 10 else '',
        '3 визита подряд': row[11] if len(row) > 11 else '',
        'Резидент': row[12] if len(row) > 12 and row[12] in ('yes', 'no') else 'no',
    }

# Поиск пользователя по Telegram ID
def find_user_row(worksheet, telegram_id: int) -> Optional[int]:
    records = worksheet.get_all_values()
    for idx, row in enumerate(records[1:], start=2):  # первая строка — заголовки
        if str(telegram_id) == row[0]:
            return idx
    return None

# Получить пользователя по Telegram ID
def get_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    ws = get_sheet()
    records = ws.get_all_values()
    for row in records[1:]:
        if str(telegram_id) == row[0]:
            return row_to_user(row)
    return None

# Добавить нового пользователя
def add_user(telegram_id: int, name: str, username: str = ""):
    ws = get_sheet()
    ws.append_row([
        str(telegram_id), username, name, 0, '', '', '', '', '', '', '', '', 'no'
    ])

# Обновить пользователя (поиск по Telegram ID)
def update_user(telegram_id: int, data: Dict[str, Any]):
    ws = get_sheet()
    row_idx = find_user_row(ws, telegram_id)
    if not row_idx:
        return
    values = [
        str(telegram_id),
        data.get('Никнейм', ''),
        data.get('Имя', ''),
        str(data.get('Баллы', 0)),
        data.get('Даты посещений', ''),
        data.get('Фото', ''),
        data.get('Ссылка на фото', ''),
        data.get('Фото с табличкой', ''),
        data.get('История', ''),
        data.get('Выступление', ''),
        data.get('Привел друга', ''),
        data.get('3 визита подряд', ''),
        data.get('Резидент', 'no'),
    ]
    ws.update(f'A{row_idx}:M{row_idx}', [values]) 