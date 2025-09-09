import asyncio
from datetime import timedelta, datetime
from spimex_parser import parse_multiple_dates, clear_page_cache
from constants import DATE_FORMAT


def generate_dates(start: datetime, end: datetime):
    """Генерирует даты в формате YYYY-MM-DD"""
    d = start
    while d <= end:
        yield d.strftime(DATE_FORMAT)
        d += timedelta(days=1)


async def async_main():
    """Основная асинхронная функция"""
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()

    print("🚀 SPIMEX Асинхронный Парсер - Массовая загрузка данных")
    print("=" * 70)
    print(
        f"📅 Период: с {start_date.strftime(DATE_FORMAT)} "
        f"по {end_date.strftime(DATE_FORMAT)}"
    )
    print("⚡ Режим: Асинхронная обработка (до 50 запросов параллельно)")
    print("📊 База данных: spimex_async_db")
    print("=" * 70)

    clear_page_cache()
    print("🗑️ Кэш страниц очищен")

    dates_list = list(generate_dates(start_date, end_date))
    total_dates = len(dates_list)

    print(f"📊 Всего дат для обработки: {total_dates}")
    print("🚀 Запуск асинхронной обработки...\n")

    start_time = datetime.now()

    try:
        results = await parse_multiple_dates(dates_list, max_concurrent=50)

        success_count = sum(1 for result in results if result is not None)
        total_records = sum(len(result) for result in results if result is not None)

        end_time = datetime.now()
        processing_time = end_time - start_time

        print("\n" + "=" * 70)
        print("🏁 Асинхронная загрузка завершена!")
        print("📊 Статистика:")
        print(f"   • Обработано дат: {total_dates}")
        print(f"   • Успешно загружено: {success_count}")
        print(f"   • Ошибок: {total_dates - success_count}")
        print(f"   • Всего записей: {total_records}")
        print(f"   • Время обработки: {processing_time}")
        print(
            f"   • Скорость: {total_dates / processing_time.total_seconds():.2f} дат/сек"
        )
        print("=" * 70)

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return


if __name__ == "__main__":
    asyncio.run(async_main())
