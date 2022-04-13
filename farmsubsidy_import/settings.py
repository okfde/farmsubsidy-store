import os


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DUCKDB_PATH = get_env("DUCKDB_PATH", os.path.join(os.getcwd(), "farmsubsidy.duckdb"))
DATA_ROOT = get_env("DATA_ROOT", os.path.join(os.getcwd(), "data"))
LOG_LEVEL = get_env("LOG_LEVEL", "warning")
