import json
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, time, timedelta
import redis.asyncio as aioredis
from redis.asyncio import Redis

import sys
from pathlib import Path

async_parser_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(async_parser_path))

from config import api_settings as settings


class CacheService:
    """Сервис для работы с Redis кэшем с автосбросом в 14:11"""

    def __init__(self):
        self.redis: Optional[Redis] = None
        self.cache_reset_time = time(14, 11)
        self._reset_task: Optional[asyncio.Task] = None
        self._stats = {"hits": 0, "misses": 0}

    async def init_redis(self):
        """Инициализация подключения к Redis"""
        try:
            self.redis = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30,
            )

            await self.redis.ping()
            print("✅ Redis подключен успешно")

            self._reset_task = asyncio.create_task(self._schedule_cache_reset())

        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {e}")
            raise

    async def close(self):
        """Закрытие подключения к Redis"""
        if self._reset_task:
            self._reset_task.cancel()
            try:
                await self._reset_task
            except asyncio.CancelledError:
                pass

        if self.redis:
            await self.redis.close()

    async def ping(self) -> bool:
        """Проверка доступности Redis"""
        try:
            if not self.redis:
                return False
            await self.redis.ping()
            return True
        except Exception:
            return False

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Получить значение из кэша

        Args:
            key: Ключ кэша

        Returns:
            Значение или None если не найдено
        """
        try:
            if not self.redis:
                return None

            cached_data = await self.redis.get(key)
            if cached_data:
                self._stats["hits"] += 1
                return json.loads(cached_data)
            else:
                self._stats["misses"] += 1
                return None

        except Exception as e:
            print(f"❌ Ошибка получения из кэша {key}: {e}")
            self._stats["misses"] += 1
            return None

    async def set(
        self, key: str, value: Dict[str, Any], ttl: Optional[int] = None
    ) -> bool:
        """
        Сохранить значение в кэш

        Args:
            key: Ключ кэша
            value: Значение для сохранения
            ttl: Время жизни в секундах (если не указано, кэш будет сброшен в 14:11)

        Returns:
            True если успешно сохранено
        """
        try:
            if not self.redis:
                return False

            if ttl is None:
                ttl = self._calculate_ttl_until_reset()

            json_data = json.dumps(value, default=str, ensure_ascii=False)
            await self.redis.setex(key, ttl, json_data)
            return True

        except Exception as e:
            print(f"❌ Ошибка сохранения в кэш {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Удалить ключ из кэша

        Args:
            key: Ключ для удаления

        Returns:
            True если удален успешно
        """
        try:
            if not self.redis:
                return False

            result = await self.redis.delete(key)
            return result > 0

        except Exception as e:
            print(f"❌ Ошибка удаления из кэша {key}: {e}")
            return False

    async def clear_all(self) -> bool:
        """
        Очистить весь кэш

        Returns:
            True если очищен успешно
        """
        try:
            if not self.redis:
                return False

            await self.redis.flushdb()
            print("🗑️ Кэш полностью очищен")
            return True

        except Exception as e:
            print(f"❌ Ошибка очистки кэша: {e}")
            return False

    async def get_stats(self) -> Dict[str, Any]:
        """
        Получить статистику кэша

        Returns:
            Словарь со статистикой
        """
        try:
            if not self.redis:
                return {
                    "status": "disconnected",
                    "total_keys": 0,
                    "memory_usage": "0B",
                    "hits": self._stats["hits"],
                    "misses": self._stats["misses"],
                    "hit_rate": 0.0,
                    "expires_at": None,
                }

            info = await self.redis.info()

            db_info = await self.redis.info("keyspace")
            total_keys = 0
            if "db0" in db_info:
                keys_info = db_info["db0"]
                if "keys" in keys_info:
                    total_keys = keys_info["keys"]

            total_requests = self._stats["hits"] + self._stats["misses"]
            hit_rate = (
                (self._stats["hits"] / total_requests * 100)
                if total_requests > 0
                else 0.0
            )

            next_reset = self._get_next_reset_time()

            return {
                "status": "connected",
                "total_keys": total_keys,
                "memory_usage": self._format_bytes(info.get("used_memory", 0)),
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "hit_rate": round(hit_rate, 2),
                "expires_at": next_reset.isoformat() if next_reset else None,
                "redis_version": info.get("redis_version", "unknown"),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            }

        except Exception as e:
            print(f"❌ Ошибка получения статистики кэша: {e}")
            return {
                "status": "error",
                "error": str(e),
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "hit_rate": 0.0,
            }

    def _calculate_ttl_until_reset(self) -> int:
        """
        Рассчитать TTL до следующего сброса кэша в 14:11

        Returns:
            TTL в секундах
        """
        now = datetime.now()
        next_reset = self._get_next_reset_time()

        if next_reset:
            delta = next_reset - now
            return max(int(delta.total_seconds()), 60)

        return 3600

    def _get_next_reset_time(self) -> Optional[datetime]:
        """
        Получить время следующего сброса кэша

        Returns:
            Datetime следующего сброса или None
        """
        now = datetime.now()
        today_reset = datetime.combine(now.date(), self.cache_reset_time)

        if now < today_reset:
            return today_reset
        else:
            tomorrow = now.date() + timedelta(days=1)
            return datetime.combine(tomorrow, self.cache_reset_time)

    async def _schedule_cache_reset(self):
        """
        Планировщик автосброса кэша в 14:11
        """
        while True:
            try:
                next_reset = self._get_next_reset_time()
                if not next_reset:
                    await asyncio.sleep(3600)
                    continue

                now = datetime.now()
                sleep_seconds = (next_reset - now).total_seconds()

                if sleep_seconds > 0:
                    print(
                        f"⏰ Следующий автосброс кэша: {next_reset.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    await asyncio.sleep(sleep_seconds)

                await self.clear_all()
                print(
                    f"🔄 Автосброс кэша выполнен в {datetime.now().strftime('%H:%M:%S')}"
                )

                await asyncio.sleep(60)

            except asyncio.CancelledError:
                print("🔄 Планировщик автосброса кэша остановлен")
                break
            except Exception as e:
                print(f"❌ Ошибка в планировщике автосброса кэша: {e}")
                await asyncio.sleep(300)

    @staticmethod
    def _format_bytes(bytes_value: int) -> str:
        """
        Форматировать байты в человекочитаемый вид

        Args:
            bytes_value: Количество байт

        Returns:
            Отформатированная строка
        """
        for unit in ["B", "KB", "MB", "GB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f}{unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f}TB"
