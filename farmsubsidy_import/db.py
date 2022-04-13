from functools import lru_cache
from typing import Optional

import duckdb
import pandas as pd
from countrynames import mappings

from .currency_conversion import CURRENCIES
from .logging import get_logger
from .settings import DUCKDB_PATH
from .util import handle_error

log = get_logger(__name__)


@lru_cache(128)
def get_db(read_only=False):
    return duckdb.connect(DUCKDB_PATH, read_only=read_only)


def get_create_enum(name, values):
    values = ", ".join(f"'{v}'" for v in values)
    return f"CREATE TYPE {name} AS ENUM ({values})"


CREATE_ENUM_COUNTRY = get_create_enum("country", mappings.keys())
CREATE_ENUM_YEAR = get_create_enum("year", range(2000, 2030))
CREATE_ENUM_CURRENCY = get_create_enum("currency", CURRENCIES)

CREATE_TABLE = """
CREATE TABLE farmsubsidy(
    pk                      INTEGER DEFAULT NEXTVAL('fs_pk'),
    country                 country,
    year                    year,
    recipient_id            VARCHAR,
    recipient_name          VARCHAR,
    recipient_fingerprint   VARCHAR,
    recipient_address       VARCHAR,
    recipient_country       VARCHAR,
    recipient_url           VARCHAR,
    scheme                  VARCHAR,
    scheme_code             VARCHAR,
    scheme_description      VARCHAR,
    amount                  DECIMAL(18, 2),
    currency                currency,
    amount_original         DECIMAL(18, 2),
    currency_original       currency
)
"""

UNIQUE_INDEX = "CREATE UNIQUE INDEX farmsubsidy_uniq_ix ON farmsubsidy (year, country, recipient_id, scheme, amount)"


def init(recreate: Optional[bool] = False):
    cursor = get_db()
    try:
        if recreate:
            cursor.execute("DROP TABLE IF EXISTS farmsubsidy")
            cursor.execute("DROP SEQUENCE IF EXISTS fs_pk")
            cursor.execute("DROP TYPE IF EXISTS country")
            cursor.execute("DROP TYPE IF EXISTS year")
            cursor.execute("DROP TYPE IF EXISTS currency")
        cursor.execute("CREATE SEQUENCE fs_pk")
        cursor.execute(CREATE_ENUM_COUNTRY)
        cursor.execute(CREATE_ENUM_YEAR)
        cursor.execute(CREATE_ENUM_CURRENCY)
        cursor.execute(CREATE_TABLE)
        cursor.execute(UNIQUE_INDEX)
    except RuntimeError as e:
        if not recreate:
            log.error(
                str(e),
                hint="\nUse the `--recreate` flag to recreate the db from scratch",
            )
        else:
            log.exception(str(e), exception=e)


def insert(
    df: pd.DataFrame, do_raise: Optional[bool] = True, fpath: Optional[str] = None
) -> int:
    df.insert(0, "pk", None)
    cursor = get_db()
    try:
        res = cursor.execute("INSERT INTO farmsubsidy SELECT * FROM df")
        res = res.df()["Count"].sum()
        return res
    except Exception as e:
        handle_error(log, e, do_raise, fpath=fpath)


def index():
    BASE_INDEX = """
    CREATE INDEX idx ON farmsubsidy (recipient_id);
    CREATE INDEX country_ix ON farmsubsidy (country);
    CREATE INDEX year_ix ON farmsubsidy (year);
    """
    FTS_INDEX = """
    PRAGMA create_fts_index(
        'farmsubsidy',
        'pk',
        'recipient_name',
        'recipient_fingerprint',
        'recipient_address',
        stemmer='none',
        stopwords='none',
        strip_accents=1,
        lower=1,
        overwrite=1
    )
    """
    cursor = get_db()
    cursor.execute(BASE_INDEX)
    cursor.execute(FTS_INDEX)
