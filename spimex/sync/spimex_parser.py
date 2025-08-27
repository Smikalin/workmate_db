import re
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from tqdm import tqdm
from models.trading_result import TradingResult
from database import SessionLocal
from constants import (
    EXCEL_ENGINE,
    COLUMN_PATTERNS,
    DATE_FORMAT,
    DATE_FORMAT_SPIMEX,
    BASE_URL,
    PAGE_URL_PATTERN,
    FILE_URL_PATTERN,
    ALL_FILES_PATTERN,
    RECORDS_TO_SAVE,
    METRIC_TON_MARKER,
    NUMERIC_COLUMNS,
    MAX_PAGES_TO_CHECK,
    HTTP_TIMEOUT,
)


PAGE_CACHE: dict = {}


def clear_page_cache():
    """Очищает кэш страниц"""
    global PAGE_CACHE
    PAGE_CACHE.clear()


def get_cache_stats():
    """Возвращает статистику кэша"""
    return {"cached_pages": len(PAGE_CACHE), "pages": list(PAGE_CACHE.keys())}


def extract_metric_ton_data(file_content, date_str):
    """
    Извлекает данные из секции 'Метрическая тонна' Excel файла
    """
    try:
        df_raw = pd.read_excel(
            BytesIO(file_content),
            engine=EXCEL_ENGINE,
            header=None,
        )

        metric_ton_row = None
        for idx, row in tqdm(
            df_raw.iterrows(),
            desc="Поиск метрической тонны",
            total=len(df_raw),
            leave=False,
        ):
            row_str = " ".join([str(cell) for cell in row if pd.notna(cell)])
            if METRIC_TON_MARKER in row_str:
                metric_ton_row = idx
                break

        header_row = metric_ton_row + 1
        df_section = pd.read_excel(
            BytesIO(file_content), engine=EXCEL_ENGINE, header=header_row
        )

        df_section.columns = [
            str(col).replace("\n", " ").strip() for col in df_section.columns
        ]

        column_mapping = find_columns(df_section.columns)

        df_filtered = df_section[list(column_mapping.values())].copy()

        code_col = column_mapping["exchange_product_id"]
        df_filtered = df_filtered[df_filtered[code_col].notna()]
        df_filtered = df_filtered[df_filtered[code_col].astype(str).str.len() > 3]

        df_filtered = df_filtered.rename(
            columns={v: k for k, v in column_mapping.items()}
        )

        numeric_columns = NUMERIC_COLUMNS
        for col in numeric_columns:
            if col in df_filtered.columns:
                df_filtered[col] = pd.to_numeric(
                    df_filtered[col].replace("-", None), errors="coerce"
                )

        df_filtered = df_filtered.dropna(subset=NUMERIC_COLUMNS, how="all")

        df_filtered = df_filtered[df_filtered["exchange_product_id"].notna()]
        df_filtered = df_filtered[
            ~df_filtered["exchange_product_id"]
            .astype(str)
            .str.contains("Итого", na=False)
        ]
        df_filtered = df_filtered[
            df_filtered["exchange_product_id"].astype(str) != "nan"
        ]

        return df_filtered

    except Exception as e:
        print(f"❌ Ошибка извлечения данных метрической тонны: {e}")
        return None


def find_columns(columns):
    """
    Находит нужные столбцы по частичному совпадению названий
    """
    column_mapping = {}

    for target_name, keywords in COLUMN_PATTERNS.items():
        for col in columns:
            col_lower = str(col).lower()
            if all(keyword in col_lower for keyword in keywords):
                column_mapping[target_name] = col
                break
    return column_mapping


def find_url_for_date(target_date: str):
    """
    Ищет правильный URL для скачивания файла на конкретную дату через парсинг
    HTML с пагинацией. Начинает поиск с последней страницы и прекращает поиск
    на странице, если файл оказался первым
    (более новые даты на этой странице быть не могут)
    target_date: строка в формате YYYY-MM-DD
    """
    try:
        date_obj = datetime.strptime(target_date, DATE_FORMAT)
        date_formatted = date_obj.strftime(DATE_FORMAT_SPIMEX)

        file_url, file_response, last_date, first_date = search_in_page(
            BASE_URL, date_formatted, page_num=1
        )
        if file_url:
            return file_url, file_response

        page_range = list(range(MAX_PAGES_TO_CHECK, 1, -1))
        for page_num in tqdm(
            page_range,
            desc="Поиск по страницам",
            leave=False,
        ):
            page_url = BASE_URL + PAGE_URL_PATTERN.format(page_num=page_num)
            file_url, file_response, last_date, first_date = search_in_page(
                page_url, date_formatted, page_num
            )

            if last_date and last_date > date_formatted:
                break

            if first_date and first_date < date_formatted:
                continue

            if file_url:
                return file_url, file_response

        return None, None

    except Exception as e:
        print(f"⚠️ Ошибка при парсинге HTML: {e}")
        return None, None


def search_in_page(page_url: str, date_formatted: str, page_num: int):
    """
    Ищет файл на конкретной странице
    Возвращает (file_url, file_response, last_file_date, first_file_date)
    """
    global PAGE_CACHE

    if page_num in PAGE_CACHE:
        cached_data = PAGE_CACHE[page_num]
        first_file_date = cached_data["first_date"]
        last_file_date = cached_data["last_date"]

        if first_file_date and first_file_date < date_formatted:
            return None, None, last_file_date, first_file_date
    else:
        try:
            response = requests.get(page_url, timeout=HTTP_TIMEOUT)

            if response.status_code != 200:
                return None, None, None, None

            html_content = response.text

            all_files_pattern = ALL_FILES_PATTERN
            all_matches = re.findall(all_files_pattern, html_content)

            first_file_date = None
            last_file_date = None
            if all_matches:
                first_file_date = all_matches[0][1]
                last_file_date = all_matches[-1][1]

                PAGE_CACHE[page_num] = {
                    "first_date": first_file_date,
                    "last_date": last_file_date,
                }
        except Exception:
            return None, None, None, None

        if first_file_date and first_file_date < date_formatted:
            return None, None, last_file_date, first_file_date

    if page_num not in PAGE_CACHE:
        try:
            response = requests.get(page_url, timeout=HTTP_TIMEOUT)
            if response.status_code != 200:
                return None, None, None, None
            html_content = response.text
        except Exception:
            return None, None, None, None
    else:
        try:
            response = requests.get(page_url, timeout=HTTP_TIMEOUT)
            if response.status_code != 200:
                return None, None, first_file_date, last_file_date
            html_content = response.text
        except Exception:
            return None, None, first_file_date, last_file_date

    pattern = FILE_URL_PATTERN.format(date_formatted=date_formatted)
    target_matches = re.findall(pattern, html_content)

    if target_matches:
        file_path = target_matches[0]

        if file_path.startswith("/"):
            file_url = f"https://spimex.com{file_path}"
        else:
            file_url = f"https://spimex.com/{file_path}"

        file_response = requests.get(file_url, timeout=HTTP_TIMEOUT)
        if file_response.status_code == 200:
            return (
                file_url,
                file_response,
                last_file_date,
                first_file_date,
            )

    return None, None, last_file_date, first_file_date


def parse_bulletin_for_date(date_str: str, max_retries=3):
    """
    Парсит бюллетень по итогам торгов для указанной даты
    date_str: строка в формате YYYY-MM-DD
    max_retries: максимальное количество попыток при ошибках сети
    """
    print(f"Обработка даты: {date_str}")

    url, response = find_url_for_date(date_str)

    if not url:
        print(f"❌ Не найден URL для даты {date_str}")
        return

    print(f"📥 Найден URL: {url}")

    df = None
    engine = EXCEL_ENGINE

    try:
        df = pd.read_excel(BytesIO(response.content), engine=engine)
        print(f"✅ Успешно прочитано с движком {engine}")
    except Exception as e:
        print(f"❌ Ошибка с движком {engine}: {e}")
        return

    if df is None:
        print(f"📄 Не удалось прочитать Excel файл на {date_str}")
        return

    df_metric_ton = extract_metric_ton_data(response.content, date_str)

    if df_metric_ton is None or df_metric_ton.empty:
        print(f"ℹ️ Данных по метрической тонне нет на {date_str}")
        return

    df_metric_ton = df_metric_ton[df_metric_ton["count"] > 0]

    if df_metric_ton.empty:
        print(f"ℹ️ Нет записей с количеством договоров > 0 на {date_str}")
        return

    df = df_metric_ton

    df["oil_id"] = df["exchange_product_id"].str[:4]
    df["delivery_basis_id"] = df["exchange_product_id"].str[4:7]
    df["delivery_type_id"] = df["exchange_product_id"].str[-1]

    df["date"] = pd.to_datetime(date_str)
    now = pd.Timestamp.now()
    df["created_on"] = now
    df["updated_on"] = now

    records = df[RECORDS_TO_SAVE].to_dict(orient="records")

    session = SessionLocal()
    try:
        existing_count = (
            session.query(TradingResult)
            .filter(TradingResult.date == pd.to_datetime(date_str).date())
            .count()
        )
        if existing_count > 0:
            print(
                f"ℹ️ Данные за {date_str} уже существуют в БД "
                f"({existing_count} записей), пропускаем"
            )
            return

        session.bulk_insert_mappings(TradingResult.__mapper__, records)
        session.commit()
        print(f"✅ Загружено записей: {len(records)}")
    except Exception as e:
        session.rollback()
        print(f"❌ Ошибка при сохранении в БД: {e}")
    finally:
        session.close()
