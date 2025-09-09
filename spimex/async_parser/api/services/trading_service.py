from typing import List
from datetime import date
from sqlalchemy import select, distinct, desc, and_

import sys
from pathlib import Path

async_parser_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(async_parser_path))

from database import AsyncSessionLocal
from models.trading_result import TradingResult
from models.schemas import (
    TradingResultFilter,
    DynamicsFilter,
    TradingResultItem,
)


class TradingService:
    """Сервис для работы с торговыми данными SPIMEX"""

    async def health_check(self) -> bool:
        """Проверка доступности базы данных"""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(select(1))
                return True
        except Exception:
            return False

    async def get_last_trading_dates(self, limit: int = 10) -> List[date]:
        """
        Получить список последних торговых дат

        Args:
            limit: Количество дат для возврата

        Returns:
            Список дат в порядке убывания
        """
        try:
            async with AsyncSessionLocal() as session:
                query = (
                    select(distinct(TradingResult.date))
                    .order_by(desc(TradingResult.date))
                    .limit(limit)
                )

                result = await session.execute(query)
                dates = [row[0] for row in result.fetchall()]

                return dates

        except Exception as e:
            raise Exception(f"Ошибка получения торговых дат: {str(e)}")

    async def get_dynamics(
        self, filter_params: DynamicsFilter
    ) -> List[TradingResultItem]:
        """
        Получить динамику торгов за период

        Args:
            filter_params: Параметры фильтрации

        Returns:
            Список результатов торгов
        """
        try:
            async with AsyncSessionLocal() as session:
                query = select(TradingResult).where(
                    and_(
                        TradingResult.date >= filter_params.start_date,
                        TradingResult.date <= filter_params.end_date,
                    )
                )

                conditions = []

                if filter_params.oil_id:
                    conditions.append(
                        TradingResult.oil_id == filter_params.oil_id
                        )

                if filter_params.delivery_type_id:
                    conditions.append(
                        TradingResult.delivery_type_id
                        == filter_params.delivery_type_id
                    )

                if filter_params.delivery_basis_id:
                    conditions.append(
                        TradingResult.delivery_basis_id
                        == filter_params.delivery_basis_id
                    )

                if conditions:
                    query = query.where(and_(*conditions))

                query = query.order_by(
                    desc(TradingResult.date), desc(TradingResult.id)
                ).limit(filter_params.limit)

                result = await session.execute(query)
                trading_results = result.scalars().all()

                return [
                    TradingResultItem(
                        id=tr.id,
                        exchange_product_id=tr.exchange_product_id,
                        exchange_product_name=tr.exchange_product_name,
                        oil_id=tr.oil_id,
                        delivery_basis_id=tr.delivery_basis_id,
                        delivery_basis_name=tr.delivery_basis_name,
                        delivery_type_id=tr.delivery_type_id,
                        volume=tr.volume,
                        total=tr.total,
                        count=tr.count,
                        date=tr.date,
                        created_on=tr.created_on,
                        updated_on=tr.updated_on,
                    )
                    for tr in trading_results
                ]

        except Exception as e:
            raise Exception(f"Ошибка получения динамики торгов: {str(e)}")

    async def get_trading_results(
        self, filter_params: TradingResultFilter
    ) -> List[TradingResultItem]:
        """
        Получить последние результаты торгов

        Args:
            filter_params: Параметры фильтрации

        Returns:
            Список результатов торгов
        """
        try:
            async with AsyncSessionLocal() as session:
                # Сначала находим последнюю торговую дату
                latest_date_query = (
                    select(TradingResult.date)
                    .order_by(desc(TradingResult.date))
                    .limit(1)
                )
                latest_date_result = await session.execute(latest_date_query)
                latest_date = latest_date_result.scalar()

                if not latest_date:
                    return []

                # Базовый запрос для последней торговой даты
                query = select(TradingResult).where(TradingResult.date == latest_date)

                # Применяем фильтры
                conditions = []

                if filter_params.oil_id:
                    conditions.append(TradingResult.oil_id == filter_params.oil_id)

                if filter_params.delivery_type_id:
                    conditions.append(
                        TradingResult.delivery_type_id == filter_params.delivery_type_id
                    )

                if filter_params.delivery_basis_id:
                    conditions.append(
                        TradingResult.delivery_basis_id
                        == filter_params.delivery_basis_id
                    )

                if conditions:
                    query = query.where(and_(*conditions))

                # Сортировка по объему торгов (от больших к меньшим) и ограничение
                query = query.order_by(
                    desc(TradingResult.total), desc(TradingResult.volume)
                ).limit(filter_params.limit)

                result = await session.execute(query)
                trading_results = result.scalars().all()

                # Преобразуем в Pydantic модели
                return [
                    TradingResultItem(
                        id=tr.id,
                        exchange_product_id=tr.exchange_product_id,
                        exchange_product_name=tr.exchange_product_name,
                        oil_id=tr.oil_id,
                        delivery_basis_id=tr.delivery_basis_id,
                        delivery_basis_name=tr.delivery_basis_name,
                        delivery_type_id=tr.delivery_type_id,
                        volume=tr.volume,
                        total=tr.total,
                        count=tr.count,
                        date=tr.date,
                        created_on=tr.created_on,
                        updated_on=tr.updated_on,
                    )
                    for tr in trading_results
                ]

        except Exception as e:
            raise Exception(f"Ошибка получения результатов торгов: {str(e)}")

    async def get_trading_statistics(self) -> dict:
        """
        Получить общую статистику по торгам

        Returns:
            Словарь со статистикой
        """
        try:
            async with AsyncSessionLocal() as session:
                from sqlalchemy import func

                # Общее количество записей
                total_count_query = select(func.count(TradingResult.id))
                total_count_result = await session.execute(total_count_query)
                total_count = total_count_result.scalar()

                # Количество уникальных торговых дат
                unique_dates_query = select(func.count(distinct(TradingResult.date)))
                unique_dates_result = await session.execute(unique_dates_query)
                unique_dates = unique_dates_result.scalar()

                # Количество уникальных инструментов
                unique_instruments_query = select(
                    func.count(distinct(TradingResult.exchange_product_id))
                )
                unique_instruments_result = await session.execute(
                    unique_instruments_query
                )
                unique_instruments = unique_instruments_result.scalar()

                # Первая и последняя даты торгов
                date_range_query = select(
                    func.min(TradingResult.date).label("first_date"),
                    func.max(TradingResult.date).label("last_date"),
                )
                date_range_result = await session.execute(date_range_query)
                date_range = date_range_result.first()

                return {
                    "total_records": total_count,
                    "unique_trading_dates": unique_dates,
                    "unique_instruments": unique_instruments,
                    "first_trading_date": (
                        date_range.first_date.isoformat()
                        if date_range.first_date
                        else None
                    ),
                    "last_trading_date": (
                        date_range.last_date.isoformat()
                        if date_range.last_date
                        else None
                    ),
                }

        except Exception as e:
            raise Exception(f"Ошибка получения статистики: {str(e)}")
