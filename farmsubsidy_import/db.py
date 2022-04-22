from functools import lru_cache
from typing import Optional

import dataset
import duckdb
import pandas as pd
from countrynames import mappings

from . import settings
from .currency_conversion import CURRENCIES
from .logging import get_logger
from .util import handle_error

log = get_logger(__name__)


@lru_cache(128)
def get_db(driver, as_executor=False, read_only=False):
    if driver == "psql":
        db = dataset.connect(settings.DATABASE_URI)
        if as_executor:
            return db.query
        return db
    if driver == "duckdb":
        db = duckdb.connect(settings.DATABASE_URI, read_only=read_only)
        if as_executor:
            return db.execute
        return db


def get_create_enum(name, values):
    values = ", ".join(f"'{v}'" for v in sorted(values))
    return f"CREATE TYPE {name} AS ENUM ({values})"


CREATE_ENUM_COUNTRY = get_create_enum("country", mappings.keys())
CREATE_ENUM_YEAR = get_create_enum("year", range(2000, 2030))
CREATE_ENUM_CURRENCY = get_create_enum("currency", CURRENCIES)

CREATE_TABLE = """
CREATE TABLE farmsubsidy(
    pk                      INTEGER DEFAULT NEXTVAL('fs_pk') NOT NULL,
    country                 country NOT NULL,
    year                    year NOT NULL,
    recipient_id            VARCHAR NOT NULL,
    recipient_name          VARCHAR NOT NULL,
    recipient_fingerprint   VARCHAR NOT NULL,
    recipient_address       VARCHAR,
    recipient_country       VARCHAR NOT NULL,
    recipient_url           VARCHAR,
    scheme                  VARCHAR,
    scheme_code             VARCHAR,
    scheme_description      VARCHAR,
    amount                  DECIMAL(18, 2) NOT NULL,
    currency                currency NOT NULL,
    amount_original         DECIMAL(18, 2),
    currency_original       currency
)
"""

UNIQUE_INDEX = "CREATE UNIQUE INDEX farmsubsidy_uniq_ix ON farmsubsidy (year, country, recipient_id, scheme, amount)"


def init(driver: str, recreate: Optional[bool] = False):
    db = get_db(driver, as_executor=True)
    try:
        if recreate:
            db("DROP TABLE IF EXISTS farmsubsidy")
            db("DROP SEQUENCE IF EXISTS fs_pk")
            db("DROP TYPE IF EXISTS country")
            db("DROP TYPE IF EXISTS year")
            db("DROP TYPE IF EXISTS currency")
        db("CREATE SEQUENCE fs_pk")
        db(CREATE_ENUM_COUNTRY)
        db(CREATE_ENUM_YEAR)
        db(CREATE_ENUM_CURRENCY)
        db(CREATE_TABLE)
        db(UNIQUE_INDEX)
    except Exception as e:
        if not recreate:
            log.error(
                str(e),
                hint="\nUse the `--recreate` flag to recreate the db from scratch",
            )
        else:
            log.exception(str(e), exception=e)


def insert(
    driver: str,
    df: pd.DataFrame,
    do_raise: Optional[bool] = True,
    fpath: Optional[str] = None,
) -> int:
    # df.insert(0, "pk", None)
    with get_db(driver) as tx:
        try:
            if driver == "psql":
                table = tx["farmsubsidy"]
                table.insert_many([dict(x) for _, x in df.iterrows()], ["pk"])
                res = len(df)
            elif driver == "duckdb":
                res = tx.execute("INSERT INTO farmsubsidy SELECT * FROM df")
                res = res.df()["Count"].sum()
            return res
        except Exception as e:
            handle_error(log, e, do_raise, fpath=fpath)


def index(driver: str):
    BASE_INDEX = """
    CREATE INDEX idx ON farmsubsidy (recipient_id);
    CREATE INDEX country_ix ON farmsubsidy (country);
    CREATE INDEX year_ix ON farmsubsidy (year);
    """
    if driver == "psql":
        FTS_INDEX = None
    elif driver == "duckdb":
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
    db = get_db(driver, as_executor=True)
    db(BASE_INDEX)
    if FTS_INDEX is not None:
        db(FTS_INDEX)


def get_aggregations(driver):
    """print some aggs"""

    AGGS = {
        "meta": """
            SELECT
                year,
                country,
                count(distinct recipient_id) as recipients,
                sum(amount) as amount_sum,
                max(amount) as amount_max,
                min(amount) as amount_min,
                avg(amount) as amount_avg
            FROM farmsubsidy
            WHERE amount > 0
            GROUP BY year, country
            ORDER BY year, country
    """
    }
    db = get_db(driver, as_executor=True, read_only=True)
    res = {k: [dict(x) for x in db(v)] for k, v in AGGS.items()}
    return res
