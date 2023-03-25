from typing import Any

import redis
from cachelib.serializers import RedisSerializer

from farmsubsidy_store import settings
from farmsubsidy_store.logging import get_logger

log = get_logger(__name__)


class Cache:
    def __init__(self):
        self.serializer = RedisSerializer()
        if settings.API_CACHE:
            con = redis.from_url(settings.REDIS_URL)
            log.info("Hello redis: %s" % con.ping())
            self.cache = con
        else:
            self.cache = None

    def set(self, key: str, data: Any):
        if self.cache is not None:
            key = f"{settings.DATABASE_TABLE}:{settings.VERSION}:{key}"
            data = self.serializer.dumps(data)
            self.cache.set(key, data)

    def get(self, key: str) -> Any:
        if self.cache is not None:
            key = f"{settings.DATABASE_TABLE}:{settings.VERSION}:{key}"
            res = self.cache.get(key)
            if res is not None:
                log.info(f"Cache hit: `{key}`")
                res = self.serializer.loads(res)
                return res


cache = Cache()
