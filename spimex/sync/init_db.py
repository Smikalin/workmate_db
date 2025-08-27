import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from config import SQLALCHEMY_DATABASE_URL, DB_NAME
from database import engine, Base
from models.trading_result import TradingResult


def create_database_if_not_exists():
    """Создает базу данных если она не существует"""
    base_url = SQLALCHEMY_DATABASE_URL.rsplit("/", 1)[0]
    postgres_url = f"{base_url}/postgres"

    try:
        temp_engine = create_engine(postgres_url)
        with temp_engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": DB_NAME},
            )

            if not result.fetchone():
                conn.execute(text("COMMIT"))
                conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
                print(f"✅ База данных '{DB_NAME}' создана")
            else:
                print(f"ℹ️ База данных '{DB_NAME}' уже существует")

    except OperationalError as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        print("💡 Убедитесь, что PostgreSQL запущен и настройки подключения корректны")
        return False
    except Exception as e:
        print(f"❌ Ошибка при создании базы данных '{DB_NAME}': {e}")
        return False

    return True


def init_database():
    """Создает базу данных и все таблицы"""
    print("🔧 Инициализация синхронной базы данных...")

    # Сначала создаем базу данных
    if not create_database_if_not_exists():
        print("❌ Не удалось создать базу данных")
        sys.exit(1)

    # Затем создаем таблицы
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ Синхронные таблицы успешно созданы!")

        tables = Base.metadata.tables.keys()
        print(f"📋 Созданные таблицы: {list(tables)}")

        print("\n📊 Структура таблицы spimex_trading_results:")
        for column in TradingResult.__table__.columns:
            print(f"   {column.name}: {column.type}")

    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")
        sys.exit(1)


if __name__ == "__main__":
    init_database()
