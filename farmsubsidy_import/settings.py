import os

from .exceptions import ImproperlyConfigured


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DRIVER = get_env("DB_DRIVER", "psql")
if DRIVER == "psql":
    DATABASE_URI = get_env("DATABASE_URI", "postgresql:///farmsubsidy")
elif DRIVER == "duckdb":
    DATABASE_URI = get_env(
        "DATABASE_URI", os.path.join(os.getcwd(), "farmsubsidy.duckdb")
    )
else:
    raise ImproperlyConfigured(f"Not a supported DB driver: `{DRIVER}`")

DATA_ROOT = get_env("DATA_ROOT", os.path.join(os.getcwd(), "data"))
LOG_LEVEL = get_env("LOG_LEVEL", "warning")
