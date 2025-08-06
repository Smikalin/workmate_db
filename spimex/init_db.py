#!/usr/bin/env python3
"""
Скрипт для инициализации базы данных SPIMEX парсера
"""

import sys
from database import engine, Base
from models.trading_result import TradingResult


def init_database():
    """Создает все таблицы в базе данных"""
    print("🔧 Создание таблиц в базе данных...")

    try:
        # Создаем все таблицы
        Base.metadata.create_all(bind=engine)
        print("✅ Таблицы успешно созданы!")

        # Выводим информацию о созданных таблицах
        tables = Base.metadata.tables.keys()
        print(f"📋 Созданные таблицы: {list(tables)}")

        # Показываем структуру основной таблицы
        print("\n📊 Структура таблицы spimex_trading_results:")
        for column in TradingResult.__table__.columns:
            print(f"   {column.name}: {column.type}")

    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")
        sys.exit(1)


if __name__ == "__main__":
    init_database()
