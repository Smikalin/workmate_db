from datetime import timedelta, datetime
from tqdm import tqdm
from spimex_parser import parse_bulletin_for_date, clear_page_cache

from constants import DATE_FORMAT


def generate_dates(start: datetime, end: datetime):
    """Генерирует даты в формате YYYY-MM-DD"""
    d = start
    while d <= end:
        yield d.strftime(DATE_FORMAT)
        d += timedelta(days=1)


def sync_main():
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()

    print("🏦 SPIMEX Синхронный Парсер - Массовая загрузка данных")
    print("=" * 70)
    print(
        f"📅 Период: с {start_date.strftime(DATE_FORMAT)} "
        f"по {end_date.strftime(DATE_FORMAT)}"
    )
    print("🔄 Режим: Синхронная обработка")
    print("📊 База данных: spimex_sync_db")
    print("=" * 70)

    clear_page_cache()
    print("🗑️ Кэш страниц очищен")

    processed_count = 0
    success_count = 0

    dates_list = list(generate_dates(start_date, end_date))

    for date_str in tqdm(dates_list, desc="Обработка дат", unit="дата"):
        processed_count += 1

        try:
            parse_bulletin_for_date(date_str)
            success_count += 1
            tqdm.write(f"✅ {date_str} - успешно обработано")
        except Exception as e:
            tqdm.write(f"❌ {date_str} - ошибка: {e}")

    print("\n" + "=" * 70)
    print("🏁 Синхронная загрузка завершена!")
    print("📊 Статистика:")
    print(f"   • Обработано дат: {processed_count}")
    print(f"   • Успешно загружено: {success_count}")
    print(f"   • Ошибок: {processed_count - success_count}")
    print("=" * 70)


if __name__ == "__main__":
    sync_main()
