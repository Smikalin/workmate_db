from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from datetime import date, datetime
import uvicorn

import sys
from pathlib import Path

async_parser_path = Path(__file__).parent.parent
sys.path.insert(0, str(async_parser_path))

from models.schemas import (
    TradingResultResponse,
    TradingDynamicsResponse,
    LastTradingDatesResponse,
    TradingResultFilter,
    DynamicsFilter,
)
from .services.trading_service import TradingService
from .services.cache_service import CacheService
from config import api_settings as settings

from init_db import init_async_database

app = FastAPI(
    title="SPIMEX Trading Results API",
    description="API для получения данных торгов SPIMEX",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

trading_service = TradingService()
cache_service = CacheService()


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске приложения"""
    await init_async_database()
    await cache_service.init_redis()
    print("✅ API сервис SPIMEX запущен")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка ресурсов при остановке"""
    await cache_service.close()
    print("🔄 API сервис SPIMEX остановлен")


@app.get("/", tags=["Общие"])
async def root():
    """Корневой endpoint с информацией о сервисе"""
    return {
        "service": "SPIMEX Trading Results API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "last_trading_dates": "/api/v1/last-trading-dates",
            "dynamics": "/api/v1/dynamics",
            "trading_results": "/api/v1/trading-results",
        },
        "docs": "/docs",
    }


@app.get("/health", tags=["Общие"])
async def health_check():
    """Проверка состояния сервиса"""
    try:
        redis_status = await cache_service.ping()
        db_status = await trading_service.health_check()

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "database": "ok" if db_status else "error",
                "redis": "ok" if redis_status else "error",
            },
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
        }


@app.get(
    "/api/v1/last-trading-dates",
    response_model=LastTradingDatesResponse,
    tags=["Торговые данные"],
)
async def get_last_trading_dates(
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Количество последних торговых дней (1-100)",
    )
):
    """
    Получить список дат последних торговых дней

    **Параметры:**
    - **limit**: Количество последних торговых дней (обязательный, по умолчанию 10)

    **Обоснование обязательности:**
    - limit - обязательный с значением по умолчанию для предотвращения
      возврата слишком большого объема данных
    """
    try:
        cache_key = f"last_trading_dates:{limit}"

        cached_result = await cache_service.get(cache_key)
        if cached_result:
            return cached_result

        dates = await trading_service.get_last_trading_dates(limit)

        result = LastTradingDatesResponse(dates=dates, count=len(dates))

        await cache_service.set(cache_key, result.dict())

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Ошибка получения данных: {str(e)}"
        )


@app.get(
    "/api/v1/dynamics", response_model=TradingDynamicsResponse, tags=["Торговые данные"]
)
async def get_dynamics(
    start_date: date = Query(
        ...,
        description="Дата начала периода (обязательная)",
    ),
    end_date: date = Query(
        ...,
        description="Дата окончания периода (обязательная)",
    ),
    oil_id: Optional[str] = Query(
        None,
        description="ID нефтепродукта (4 символа)",
    ),
    delivery_type_id: Optional[str] = Query(
        None,
        description="ID типа поставки (1 символ)",
    ),
    delivery_basis_id: Optional[str] = Query(
        None,
        description="ID базиса поставки (3 символа)",
    ),
    limit: int = Query(1000, ge=1, le=10000, description="Лимит записей (1-10000)"),
):
    """
    Получить динамику торгов за заданный период

    **Параметры:**
    - **start_date**: Дата начала периода (обязательная)
    - **end_date**: Дата окончания периода (обязательная)
    - **oil_id**: ID нефтепродукта (опциональный)
    - **delivery_type_id**: ID типа поставки (опциональный)
    - **delivery_basis_id**: ID базиса поставки (опциональный)
    - **limit**: Лимит записей (опциональный, по умолчанию 1000)

    **Обоснование обязательности:**
    - start_date, end_date - обязательные для предотвращения запроса всех данных
    - Остальные параметры опциональные для гибкости фильтрации
    """
    try:
        if start_date > end_date:
            raise HTTPException(
                status_code=400,
                detail="Дата начала не может быть больше даты окончания",
            )

        filter_params = DynamicsFilter(
            start_date=start_date,
            end_date=end_date,
            oil_id=oil_id,
            delivery_type_id=delivery_type_id,
            delivery_basis_id=delivery_basis_id,
            limit=limit,
        )

        cache_key = f"dynamics:{filter_params.cache_key()}"

        cached_result = await cache_service.get(cache_key)
        if cached_result:
            return TradingDynamicsResponse(**cached_result)

        results = await trading_service.get_dynamics(filter_params)

        response = TradingDynamicsResponse(
            results=results,
            count=len(results),
            filter=filter_params.dict(exclude_none=True),
        )

        await cache_service.set(cache_key, response.dict())

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Ошибка получения динамики: {str(e)}"
        )


@app.get(
    "/api/v1/trading-results",
    response_model=TradingResultResponse,
    tags=["Торговые данные"],
)
async def get_trading_results(
    oil_id: Optional[str] = Query(None, description="ID нефтепродукта (4 символа)"),
    delivery_type_id: Optional[str] = Query(
        None,
        description="ID типа поставки (1 символ)",
    ),
    delivery_basis_id: Optional[str] = Query(
        None,
        description="ID базиса поставки (3 символа)",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Лимит записей (1-1000)",
    ),
):
    """
    Получить последние результаты торгов

    **Параметры:**
    - **oil_id**: ID нефтепродукта (опциональный)
    - **delivery_type_id**: ID типа поставки (опциональный)
    - **delivery_basis_id**: ID базиса поставки (опциональный)
    - **limit**: Лимит записей (опциональный, по умолчанию 100)

    **Обоснование обязательности:**
    - Все параметры опциональные для максимальной гибкости
    - limit имеет разумное значение по умолчанию
    - Без фильтров возвращаются последние торги по всем инструментам
    """
    try:
        filter_params = TradingResultFilter(
            oil_id=oil_id,
            delivery_type_id=delivery_type_id,
            delivery_basis_id=delivery_basis_id,
            limit=limit,
        )

        cache_key = f"trading_results:{filter_params.cache_key()}"

        cached_result = await cache_service.get(cache_key)
        if cached_result:
            return TradingResultResponse(**cached_result)

        results = await trading_service.get_trading_results(filter_params)

        response = TradingResultResponse(
            results=results,
            count=len(results),
            filter=filter_params.dict(exclude_none=True),
        )

        await cache_service.set(cache_key, response.dict())

        return response

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Ошибка получения результатов: {str(e)}"
        )


@app.get("/api/v1/cache/stats", tags=["Кэш"])
async def get_cache_stats():
    """Получить статистику кэша"""
    try:
        stats = await cache_service.get_stats()
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Ошибка получения статистики: {str(e)}"
        )


@app.delete("/api/v1/cache/clear", tags=["Кэш"])
async def clear_cache():
    """Очистить весь кэш (для администрирования)"""
    try:
        await cache_service.clear_all()
        return {"message": "Кэш очищен", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка очистки кэша: {str(e)}",
        )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info",
    )
