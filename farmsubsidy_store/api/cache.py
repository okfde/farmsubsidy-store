from typing import Any

from cachelib import redis

from farmsubsidy_store import settings
from farmsubsidy_store.logging import get_logger

log = get_logger(__name__)


class Cache:
    def __init__(self):
        if settings.API_CACHE:
            uri = settings.REDIS_URL
            uri = uri.replace("redis://", "")
            host, *port = uri.rsplit(":", 1)
            port = port[0] if len(port) else 6379
            self.cache = redis.RedisCache(
                host, port, default_timeout=0, key_prefix="fs-api"
            )
        else:
            self.cache = None

    def set(self, key: str, data: Any):
        if self.cache is not None:
            self.cache.set(key, data)

    def get(self, key: str) -> Any:
        if self.cache is not None:
            res = self.cache.get(key)
            if res is not None:
                log.info(f"Cache hit: `{key}`")
                return res


cache = Cache()
