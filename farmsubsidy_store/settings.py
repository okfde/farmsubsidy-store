import os

from banal import as_bool

from .exceptions import ImproperlyConfigured


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DEBUG = as_bool(get_env("DEBUG"))

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

API_CACHE = as_bool(get_env("API_CACHE"))
REDIS_URL = get_env("REDIS_URL", "redis://localhost:6379")
ALLOWED_ORIGIN = get_env("API_ALLOWED_ORIGIN", "https://farmsubsidy.org")
API_KEY = get_env("API_KEY", "secret-api-key")
API_HTPASSWD = get_env("API_HTPASSWD", os.path.join(DATA_ROOT, ".htpasswd"))
API_TOKEN_SECRET = get_env("API_TOKEN_SECRET", "fsscrt")

PUBLIC_YEARS = get_env("API_PUBLIC_YEARS", "2020,2021").split(",")
EXPORT_DIRECTORY = get_env("EXPORT_DIRECTORY", os.path.join(DATA_ROOT, "exports"))
EXPORT_PUBLIC_PATH = get_env("EXPORT_PUBLIC_PATH", "/exports")
