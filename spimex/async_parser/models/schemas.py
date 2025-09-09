from pydantic import BaseModel, validator
from typing import List, Optional, Dict, Any
from datetime import date, datetime
import hashlib


class TradingResultItem(BaseModel):
    """Элемент результата торгов"""

    id: int
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: float
    total: float
    count: int
    date: date
    created_on: datetime
    updated_on: datetime

    class Config:
        orm_mode = True
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat(),
        }


class TradingResultFilter(BaseModel):
    """Фильтр для получения результатов торгов"""

    oil_id: Optional[str] = None
    delivery_type_id: Optional[str] = None
    delivery_basis_id: Optional[str] = None
    limit: int = 100

    @validator("oil_id")
    def validate_oil_id(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError("oil_id должен содержать только буквы и цифры")
        return v

    @validator("delivery_type_id")
    def validate_delivery_type_id(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError("delivery_type_id должен содержать только буквы и цифры")
        return v

    @validator("delivery_basis_id")
    def validate_delivery_basis_id(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError("delivery_basis_id должен содержать только буквы и цифры")
        return v

    def cache_key(self) -> str:
        """Генерирует ключ для кэширования"""
        params = f"{self.oil_id}:{self.delivery_type_id}:{self.delivery_basis_id}:{self.limit}"
        return hashlib.md5(params.encode()).hexdigest()


class DynamicsFilter(BaseModel):
    """Фильтр для получения динамики торгов"""

    start_date: date
    end_date: date
    oil_id: Optional[str] = None
    delivery_type_id: Optional[str] = None
    delivery_basis_id: Optional[str] = None
    limit: int = 1000

    @validator("oil_id")
    def validate_oil_id(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError("oil_id должен содержать только буквы и цифры")
        return v

    @validator("delivery_type_id")
    def validate_delivery_type_id(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError("delivery_type_id должен содержать только буквы и цифры")
        return v

    @validator("delivery_basis_id")
    def validate_delivery_basis_id(cls, v):
        if v is not None and not v.isalnum():
            raise ValueError("delivery_basis_id должен содержать только буквы и цифры")
        return v

    def cache_key(self) -> str:
        """Генерирует ключ для кэширования"""
        params = f"{self.start_date}:{self.end_date}:{self.oil_id}:{self.delivery_type_id}:{self.delivery_basis_id}:{self.limit}"
        return hashlib.md5(params.encode()).hexdigest()


class TradingResultResponse(BaseModel):
    """Ответ с результатами торгов"""

    results: List[TradingResultItem]
    count: int
    filter: Dict[str, Any]


class TradingDynamicsResponse(BaseModel):
    """Ответ с динамикой торгов"""

    results: List[TradingResultItem]
    count: int
    filter: Dict[str, Any]


class LastTradingDatesResponse(BaseModel):
    """Ответ со списком последних торговых дат"""

    dates: List[date]
    count: int

    class Config:
        json_encoders = {
            date: lambda v: v.isoformat(),
        }


class ErrorResponse(BaseModel):
    """Ответ с ошибкой"""

    error: str
    detail: Optional[str] = None
    timestamp: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class CacheStatsResponse(BaseModel):
    """Ответ со статистикой кэша"""

    total_keys: int
    memory_usage: str
    hits: int
    misses: int
    hit_rate: float
    expires_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }
