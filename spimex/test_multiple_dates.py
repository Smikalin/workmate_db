#!/usr/bin/env python3
"""
Тест парсера на нескольких датах
"""

from spimex_parser import parse_bulletin_for_date
import time


def test_dates():
    """Тестирует парсер на нескольких датах"""
    test_dates = [
        "2024-12-20",  # Декабрь 2024
        "2024-12-19",  # Декабрь 2024
        "2024-12-18",  # Декабрь 2024
    ]

    print("🧪 Тестирование парсера на нескольких датах")
    print("=" * 50)

    for date_str in test_dates:
        print(f"\n🔍 Тестирование даты: {date_str}")
        try:
            parse_bulletin_for_date(date_str)
            print(f"✅ Дата {date_str} обработана успешно")
        except Exception as e:
            print(f"❌ Ошибка на дате {date_str}: {e}")

        # Пауза между запросами
        time.sleep(2)

    print("\n" + "=" * 50)
    print("🏁 Тестирование завершено!")


if __name__ == "__main__":
    test_dates()
