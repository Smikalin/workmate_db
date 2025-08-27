import asyncio
import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Tuple


def generate_test_dates(start_date: str, days_count: int) -> List[str]:
    """Генерирует список тестовых дат"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days_count)]


def run_sync_parser(dates: List[str]) -> Tuple[float, int, int]:
    """Запускает синхронный парсер и возвращает время выполнения"""
    print(f"🔄 Запуск синхронного парсера для {len(dates)} дат...")

    sync_path = os.path.join(os.path.dirname(__file__), "sync")
    sys.path.insert(0, sync_path)

    modules_to_clear = ["spimex_parser", "database", "config", "constants"]
    for module in modules_to_clear:
        if module in sys.modules:
            del sys.modules[module]

    try:
        from spimex_parser import parse_bulletin_for_date, clear_page_cache

        clear_page_cache()
        start_time = time.time()

        success_count = 0
        total_records = 0

        for date_str in dates:
            try:
                import io
                from contextlib import redirect_stdout

                f = io.StringIO()
                with redirect_stdout(f):
                    parse_bulletin_for_date(date_str)

                output = f.getvalue()
                if "✅ Загружено записей:" in output:
                    records = int(output.split("✅ Загружено записей:")[-1].strip())
                    total_records += records
                    success_count += 1
                elif "ℹ️ Данные за" in output and "уже существуют" in output:
                    success_count += 1

            except Exception as e:
                print(f"❌ Ошибка для {date_str}: {e}")

        end_time = time.time()
        execution_time = end_time - start_time

        return execution_time, success_count, total_records

    finally:
        if sync_path in sys.path:
            sys.path.remove(sync_path)

        for module in modules_to_clear:
            if module in sys.modules:
                del sys.modules[module]


async def run_async_parser(dates: List[str]) -> Tuple[float, int, int]:
    """Запускает асинхронный парсер и возвращает время выполнения"""
    print(f"⚡ Запуск асинхронного парсера для {len(dates)} дат...")

    async_path = os.path.join(os.path.dirname(__file__), "async")
    sys.path.insert(0, async_path)

    modules_to_clear = ["spimex_parser", "database", "config", "constants"]
    for module in modules_to_clear:
        if module in sys.modules:
            del sys.modules[module]

    try:
        from spimex_parser import parse_multiple_dates, clear_page_cache

        clear_page_cache()
        start_time = time.time()

        results = await parse_multiple_dates(dates, max_concurrent=50)

        end_time = time.time()
        execution_time = end_time - start_time

        success_count = sum(1 for result in results if result is not None)
        total_records = sum(len(result) for result in results if result is not None)

        return execution_time, success_count, total_records

    finally:
        if async_path in sys.path:
            sys.path.remove(async_path)

        for module in modules_to_clear:
            if module in sys.modules:
                del sys.modules[module]


def format_time(seconds: float) -> str:
    """Форматирует время в читаемом виде"""
    if seconds < 60:
        return f"{seconds:.2f}с"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}м {secs:.2f}с"


def calculate_speed(dates_count: int, execution_time: float) -> float:
    """Вычисляет скорость обработки дат/сек"""
    return dates_count / execution_time if execution_time > 0 else 0


async def run_benchmark(test_dates: List[str]) -> None:
    """Запускает бенчмарк и выводит результаты"""

    print("🏁 SPIMEX Парсер - Бенчмарк производительности")
    print("=" * 70)
    print(f"📅 Тестовые даты: {len(test_dates)} дат")
    print(f"📊 Период: {test_dates[0]} - {test_dates[-1]}")
    print("=" * 70)

    results = {}

    try:
        sync_time, sync_success, sync_records = run_sync_parser(test_dates)
        results["sync"] = {
            "time": sync_time,
            "success": sync_success,
            "records": sync_records,
            "speed": calculate_speed(len(test_dates), sync_time),
        }
        print(f"✅ Синхронный парсер: {format_time(sync_time)}")
    except Exception as e:
        print(f"❌ Ошибка синхронного парсера: {e}")
        results["sync"] = None

    print("-" * 50)

    try:
        async_time, async_success, async_records = await run_async_parser(test_dates)
        results["async"] = {
            "time": async_time,
            "success": async_success,
            "records": async_records,
            "speed": calculate_speed(len(test_dates), async_time),
        }
        print(f"✅ Асинхронный парсер: {format_time(async_time)}")
    except Exception as e:
        print(f"❌ Ошибка асинхронного парсера: {e}")
        results["async"] = None

    print("\n" + "=" * 70)
    print("📊 РЕЗУЛЬТАТЫ БЕНЧМАРКА")
    print("=" * 70)

    if results["sync"] and results["async"]:
        sync_r = results["sync"]
        async_r = results["async"]

        print(f"📈 Обработано дат:")
        print(f"   • Синхронный:  {sync_r['success']}/{len(test_dates)}")
        print(f"   • Асинхронный: {async_r['success']}/{len(test_dates)}")

        print(f"\n📊 Время выполнения:")
        print(f"   • Синхронный:  {format_time(sync_r['time'])}")
        print(f"   • Асинхронный: {format_time(async_r['time'])}")

        print(f"\n⚡ Скорость обработки:")
        print(f"   • Синхронный:  {sync_r['speed']:.2f} дат/сек")
        print(f"   • Асинхронный: {async_r['speed']:.2f} дат/сек")

        if sync_r["records"] > 0 or async_r["records"] > 0:
            print(f"\n💾 Загружено записей:")
            print(f"   • Синхронный:  {sync_r['records']:,}")
            print(f"   • Асинхронный: {async_r['records']:,}")

        if sync_r["time"] > 0 and async_r["time"] > 0:
            speedup = sync_r["time"] / async_r["time"]
            if speedup > 1:
                print(f"\n🚀 Ускорение: Асинхронный парсер в {speedup:.2f}x быстрее!")
            else:
                print(f"\n🐌 Синхронный парсер быстрее в {1/speedup:.2f}x")

            print(f"\n💡 Рекомендации:")
            if speedup > 2:
                print("   ✅ Используйте асинхронный парсер для массовой загрузки")
            elif speedup > 1.5:
                print("   ⚡ Асинхронный парсер показывает хорошее ускорение")
            else:
                print("   🤔 Разница в производительности незначительна")

    print("=" * 70)


def main():
    """Главная функция"""
    print("🚀 SPIMEX Бенчмарк - Сравнение производительности парсеров")
    print("=" * 70)
    print("📊 Тестирование 30 дат начиная с 2023-01-25")
    print("=" * 70)

    # Генерируем 30 тестовых дат
    test_dates = generate_test_dates("2023-01-25", 30)

    # Запускаем бенчмарк
    asyncio.run(run_benchmark(test_dates))


if __name__ == "__main__":
    main()
