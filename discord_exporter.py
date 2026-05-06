# для экселя
import gspread
import csv
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
import psycopg2
import subprocess

# для прочтения файлов и сохранения в бд
import numpy as np
from datetime import datetime, UTC
import os
import re
from pathlib import Path

now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M")

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "файл подключения Google Cloud.json")
project_path = r'XXX'
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/ХХХ' # табл


def run_OSINTCord():
    try:
        # Смена текущей рабочей директории процесса Python
        os.chdir(project_path)
        print(f"Перешли в директорию: {os.getcwd()}")
    
        # Запуск npm start
        # shell=True может потребоваться для работы в Windows
        result = subprocess.run("npm start", shell=True, check=True, text=True, capture_output=True,
            encoding='utf-8',  # Явно указываем кодировку
            errors='replace')
        
        # Вывод результатов
        print("Команда выполнена успешно!")
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
    
    except FileNotFoundError:
        print(f"Ошибка: Директория {project_path} не найдена.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении npm start (код возврата {e.returncode}):")
        # Обрабатываем stderr с правильной кодировкой
        if e.stderr:
            print("STDERR:")
            print(e.stderr)
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
    
def executemany_postgresql(table_name, data):
    cursor = conn.cursor()
    try:
        if not data:
            return  

        columns = ', '.join(data[0].keys())
        placeholders = ', '.join(['%s' for _ in data[0]])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = [tuple(item.values()) for item in data]
        cursor.executemany(query, values)
        conn.commit()
        print(f"Успешно добавлено {len(data)} записей в таблицу {table_name}")
        
    except Exception as e:
        print(f"Ошибка при вставке данных в таблицу {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def process_all_files_in_folder(folder_path, file_pattern="*.txt"):
    """
    Обрабатывает все файлы в указанной папке,
    """
    folder = Path(folder_path)
    files = list(folder.glob(file_pattern))
    
    if not files:
        print(f"Файлы по шаблону '{file_pattern}' не найдены в папке {folder_path}")
        return []
    
    print(f"Найдено файлов для обработки: {len(files)}")
    return files
    
def delete_original_files(files):
    """Удаляет исходные файлы после обработки"""
    for file_path in files:
        try:
            os.remove(file_path)
            print(f"Файл удален: {file_path.name}")
        except Exception as e:
            print(f"Ошибка при удалении файла {file_path.name}: {e}")

    
def extract_server_id_from_path(file_path):
    filename = os.path.basename(file_path)
    match = re.search(r'-(\d+)-', filename)
    if match:
        return match.group(1)
    else:
        parts = filename.split('-')
        if len(parts) >= 2:
            return parts[-2]
        return "unknown_server"
    
def clean_column_values(df, column_name):
    """
    Роли чистит от лишних цифр
    """
    if column_name not in df.columns:
        return df
    
    def clean_value(value):
        if pd.isna(value) or not isinstance(value, str):
            return value
        
        # Разделяем по запятым и очищаем каждую часть
        parts = value.split(',')
        cleaned_parts = []
        
        for part in parts:
            # Удаляем только паттерн "цифры - " в начале
            cleaned_part = re.sub(r'^\d+\s*-\s*', '', part.strip())
            cleaned_parts.append(cleaned_part)
        
        return ', '.join(cleaned_parts)
    
    df[column_name] = df[column_name].apply(clean_value)
    return df    
    
def clean_and_sort_roles(role_string):
    """
    Сортирует роли и удаляет лишнюю
    """
    if pd.isna(role_string):
        return role_string
    # Split into a list, remove '@everyone', strip whitespace
    roles_list = role_string.split(',')
    cleaned_roles = [role.strip() for role in roles_list if role.strip() != '@everyone']
    # Sort the list alphabetically
    cleaned_roles_sorted = sorted(cleaned_roles)
    # Join back into a string
    return ', '.join(cleaned_roles_sorted)
    
def filter_bot_rows(df, flags_column='flags'):
    """
    Удаляет строки, где в колонке flags есть VERIFIED_BOT или BOT_HTTP_INTERACTIONS
    """
    if flags_column not in df.columns:
        print(f"Предупреждение: колонка '{flags_column}' не найдена")
        return df
        
    # Создаем маску для строк, которые НЕ содержат указанные значения
    mask = ~(
        df[flags_column].astype(str).str.contains('VERIFIED_BOT', na=False) |
        df[flags_column].astype(str).str.contains('BOT_HTTP_INTERACTIONS', na=False)
    )
    
    initial_count = len(df)
    df_filtered = df[mask].copy()
    filtered_count = initial_count - len(df_filtered)
    print(f"Удалено строк с ботами: {filtered_count}")
    return df_filtered
    
def replace_all_nan_values(df):
    return df.replace({np.nan: None})

def select_to_dataframe():
    with open("src/user_guild_voice_on_server_one.sql", 'r') as file:
        query = file.read()
        df = pd.read_sql_query(query, conn)
        print(f"Получено {len(df)} записей")
        print(df.head())
        return df
    
def read_table_with_pandas(files, new_columns):
    combined_df = pd.DataFrame()
    for file_path in files:
        try:
            print(f"Обрабатывается файл: {file_path.name}")
            server_id = extract_server_id_from_path(file_path)

            # Читаем с безопасными настройками
            df = pd.read_csv(
                file_path,
                sep='\t',
                engine='python',
                quoting=3,  # QUOTE_NONE — не интерпретировать кавычки
                on_bad_lines='skip',
                encoding='utf-8',
                dtype=str
            )

            if new_columns:
                if len(new_columns) != len(df.columns):
                    raise ValueError("Количество новых заголовков не совпадает с исходным")
                df.columns = new_columns

            df.insert(0, 'datetime', now)
            df.insert(2, 'server_id', server_id)
            if 'user_status' in df.columns and 'user_activity_name' in df.columns:
                df = df.drop(columns=['user_status', 'user_activity_name'])
            
            df = clean_column_values(df, 'user_role_name')
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        except Exception as e:
            print(f"Ошибка при обработке файла {file_path.name}: {e}")
            continue

    if not combined_df.empty:
        delete_original_files(files)

    combined_df = replace_all_nan_values(combined_df)
    combined_df['user_role_name'] = combined_df['user_role_name'].apply(clean_and_sort_roles)
    combined_df = filter_bot_rows(combined_df, 'flags')
    return combined_df.to_dict('records')


def upload_to_google_sheets(df):
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_url(SPREADSHEET_URL)
    worksheet = sh.sheet1
    
    columns_to_convert = [
        'datetime_s2_role',
        'server1_joined_at', 
        'server2_joined_at',
        'min_date_info_server2_at',
        'last_voice',
        'user_id',
        'server2_server_id'
    ]

    df[columns_to_convert] = df[columns_to_convert].astype(str)
    # df = df.astype(str)
    data_for_upload = [df.columns.values.tolist()] + df.values.tolist()
    # worksheet.clear()
    worksheet.batch_clear(['B1:X1000'])
    worksheet.update(data_for_upload, 'B1')
    worksheet.update_title(now)
    print(f'Данные успешно загружены в таблицу: {SPREADSHEET_URL}')

if __name__ == '__main__':
    new_columns = ['user_id', 'user_name', 'user_display_name', 'avatar', 'user_role_name', 'created_at', 'joined_at', 'user_activity_name', 'user_status', 'flags', 'boosting_since']
    run_OSINTCord() 
    LOGS_FOLDER = os.path.join(BASE_DIR, "logs")
    files = process_all_files_in_folder(LOGS_FOLDER)
    #files = process_all_files_in_folder(r'C:\my_files\my_programs\1OSINT\OSINTCord\src\logs')
    df = read_table_with_pandas(files, new_columns)
    executemany_postgresql('user_guild', df) #в postgresql передать собранные данные
    df_to_excel = pd.DataFrame()
    df_to_excel = select_to_dataframe() #запрос к базе select запросом
    upload_to_google_sheets(df_to_excel) #в эксель результат запроса
    conn.close()  
