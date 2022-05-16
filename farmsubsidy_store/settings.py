import os

from .exceptions import ImproperlyConfigured


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DATA_ROOT = get_env("DATA_ROOT", os.path.join(os.getcwd(), "data"))

DEFAULT_DATABASE_URIS = {
    "duckdb": os.path.join(DATA_ROOT, "farmsubsidy.duckdb"),
    "clickhouse": "localhost",
}

SUPPORTED_DRIVERS = DEFAULT_DATABASE_URIS.keys()

DRIVER = get_env("DRIVER", "clickhouse")
if DRIVER == "clickhouse":
    DATABASE_URI = get_env("DATABASE_URI", DEFAULT_DATABASE_URIS["clickhouse"])
elif DRIVER == "duckdb":
    DATABASE_URI = get_env("DATABASE_URI", DEFAULT_DATABASE_URIS["duckdb"])
else:
    raise ImproperlyConfigured(f"Not a supported DB driver: `{DRIVER}`")

DATABASE_TABLE = get_env("DATABASE_TABLE", "farmsubsidy")
LOG_LEVEL = get_env("LOG_LEVEL", "warning")
LRU_QUERY_CACHE_SIZE = 1024 * 1000  # 1MB


API_CACHE_TYPE = get_env("FS_API_CACHE_TYPE", "NullCache")
API_CACHE_DEFAULT_TIMEOUT = get_env("FS_API_CACHE_DEFAULT_TIMEOUT", 60 * 60 * 24)
FLASK_DEBUG = get_env("FLASK_DEBUG") == "1"
REDIS_URL = get_env("REDIS_URL")
