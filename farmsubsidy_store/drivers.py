from functools import lru_cache
from typing import Iterable, Optional

import duckdb
import pandas as pd
from clickhouse_driver import Client

from . import enums, settings
from .exceptions import ImproperlyConfigured
from .query import Query, RecipientListQuery, SchemeListQuery


# don't show clickhouse numpy warnings:
logging.getLogger("clickhouse_driver.columns.service").setLevel(logging.ERROR)


@lru_cache(128)
def _get_connection(
    driver: str,
    uri: Optional[str] = settings.DATABASE_URI,
    read_only: Optional[bool] = False,
):
    if driver not in settings.SUPPORTED_DRIVERS:
        raise ImproperlyConfigured(f"Not a supported DB driver: `{driver}`")
    if driver == "clickhouse":
        return Client(host=uri, settings={"use_numpy": True})
    if driver == "duckdb":
        return duckdb.connect(uri, read_only=read_only)


class Driver:
    DROP_TABLE = "DROP TABLE IF EXISTS {table}"

    def __init__(
        self,
        table: Optional[str] = settings.DATABASE_TABLE,
        uri: Optional[str] = settings.DATABASE_URI,
        read_only: Optional[bool] = True,
    ):
        self.table = table
        self.uri = uri
        self.read_only = read_only
        self.driver = self.__class__.__name__.lower()

    def __str__(self):
        return self.uri

    def __repr__(self):
        return f"<{self.__class__.__name__} ({self})>"

    @property
    def conn(self):
        return _get_connection(self.driver, self.uri, self.read_only)

    def init(self, recreate: Optional[bool] = False):
        if recreate:
            self.execute(self.drop_statement)
        self.execute(self.create_statement)

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    def select(self, **filters) -> pd.DataFrame:
        query = Query(self.driver, self.table, **filters)
        return self.query(query)

    def select_recipients(self, **filters) -> pd.DataFrame:
        query = RecipientListQuery(self.driver, self.table, **filters)
        if not filters:  # limit results
            query = query.page()
        return self.query(query)

    def select_schemes(self, **filters) -> pd.DataFrame:
        query = SchemeListQuery(self.driver, self.table, **filters)
        return self.query(query)


class Clickhouse(Driver):
    CREATE_TABLE = """
    CREATE TABLE {table}
    (
        `pk`                      FixedString(40) NOT NULL,
        `country`                 {country} NOT NULL,
        `year`                    {year} NOT NULL,
        `recipient_id`            FixedString(40) NOT NULL,
        `recipient_name`          String NOT NULL,
        `recipient_fingerprint`   String NOT NULL,
        `recipient_address`       String,
        `recipient_country`       String NOT NULL,
        `recipient_url`           String,
        `scheme`                  String,
        `scheme_code`             String,
        `scheme_description`      String,
        `amount`                  Decimal(18, 2) NOT NULL,
        `currency`                {currency} NOT NULL,
        `amount_original`         Decimal(18, 2),
        `currency_original`       {currency}
    ) ENGINE = MergeTree()
    ORDER BY (country, year, recipient_fingerprint, recipient_id, amount)
    """

    def get_enum(self, values: Iterable[str]) -> str:
        values = ", ".join(f"'{v}'" for v in sorted(values))
        return f"Enum({values})"

    @property
    def create_statement(self) -> str:
        # implicit enum types
        country = self.get_enum(enums.COUNTRIES)
        currency = self.get_enum(enums.CURRENCIES)
        year = self.get_enum(enums.YEARS)
        return self.CREATE_TABLE.format(
            table=self.table, country=country, currency=currency, year=year
        )

    @property
    def drop_statement(self) -> str:
        return self.DROP_TABLE.format(table=self.table)

    def insert(self, df: pd.DataFrame) -> int:
        res = self.conn.insert_dataframe("INSERT INTO %s VALUES" % self.table, df)
        return res

    def query(self, query: Query) -> pd.DataFrame:
        query = str(query)
        return self.conn.query_dataframe(query)  # noqa


class Duckdb(Driver):
    CREATE_TABLE = """
    CREATE TABLE {table}(
        pk                      CHAR(40) NOT NULL PRIMARY KEY,
        country                 country NOT NULL,
        year                    year NOT NULL,
        recipient_id            CHAR(40) NOT NULL,
        recipient_name          VARCHAR NOT NULL,
        recipient_fingerprint   VARCHAR NOT NULL,
        recipient_address       VARCHAR,
        recipient_country       VARCHAR NOT NULL,
        recipient_url           VARCHAR,
        scheme                  VARCHAR,
        scheme_code             VARCHAR,
        scheme_description      VARCHAR,
        amount                  DECIMAL(18, 2),
        currency                currency NOT NULL,
        amount_original         DECIMAL(18, 2),
        currency_original       currency
    )
    """

    def get_enum(self, name: str, values: Iterable[str]) -> str:
        values = ", ".join(f"'{v}'" for v in sorted(values))
        return f"CREATE TYPE {name} AS ENUM ({values})"

    @property
    def create_statement(self) -> str:
        # create enum types first
        countries = self.get_enum("country", enums.COUNTRIES)
        currencies = self.get_enum("currency", enums.CURRENCIES)
        years = self.get_enum("year", enums.YEARS)
        table = self.CREATE_TABLE.format(table=self.table)

        # create indexes
        indexes = (
            "CREATE INDEX idx ON %s (recipient_id)" % self.table,
            "CREATE INDEX country_ix ON %s (country)" % self.table,
            "CREATE INDEX year_ix ON %s (year)" % self.table,
            "CREATE INDEX fp_ix ON %s (recipient_fingerprint)" % self.table,
        )
        return ";\n".join((countries, currencies, years, table, *indexes))

    @property
    def drop_statement(self) -> str:
        # drop enum types and table
        table = self.DROP_TABLE.format(table=self.table)
        enums = ("DROP TYPE IF EXISTS %s" % t for t in ("country", "currency", "year"))
        return ";\n".join((table, *enums))

    def insert(self, df: pd.DataFrame) -> int:
        res = self.execute("INSERT INTO %s SELECT * FROM df" % self.table)
        return res.df()["Count"].sum()

    def query(self, query: str) -> pd.DataFrame:
        res = self.execute(query)
        return res.df()


@lru_cache(128)
def get_driver(
    driver: Optional[str] = settings.DRIVER,
    uri: Optional[str] = settings.DATABASE_URI,
    table: Optional[str] = settings.DATABASE_TABLE,
    read_only: Optional[bool] = True,
) -> Driver:
    if driver not in settings.SUPPORTED_DRIVERS:
        raise ImproperlyConfigured(f"Not a supported DB driver: `{driver}`")
    if driver == "clickhouse":
        return Clickhouse(table, uri)
    if driver == "duckdb":
        return Duckdb(table, uri, read_only=read_only)


current_driver = get_driver()
