import uvicorn
from config import api_settings


def main():
    """Основная функция запуска API"""
    print("🚀 Запуск SPIMEX API из async_parser")
    print("=" * 50)
    print(f"🌐 Host: {api_settings.HOST}")
    print(f"🔌 Port: {api_settings.PORT}")
    print(f"🐛 Debug: {api_settings.DEBUG}")
    print(
        f"📊 Database: {api_settings.DATABASE_URL.split('@')[-1] if '@' in api_settings.DATABASE_URL else 'Local'}"
    )
    print(f"🗄️ Redis: {api_settings.REDIS_URL}")
    print("=" * 50)

    try:
        uvicorn.run(
            "api.main:app",
            host=api_settings.HOST,
            port=api_settings.PORT,
            reload=api_settings.DEBUG,
            log_level=api_settings.LOG_LEVEL.lower(),
            access_log=True,
            loop="asyncio",
            workers=1 if api_settings.DEBUG else 4,
        )
    except KeyboardInterrupt:
        print("\n🔄 Завершение работы по запросу пользователя")
    except Exception as e:
        print(f"\n❌ Ошибка запуска: {e}")


if __name__ == "__main__":
    main()
