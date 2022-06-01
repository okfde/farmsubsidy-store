import logging
from functools import lru_cache
from typing import Iterable, Iterator, Optional

import duckdb
import pandas as pd
from clickhouse_driver import Client

from . import enums, settings
from .exceptions import ImproperlyConfigured
from .query import Query

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
        host, *port = uri.split(":")
        if not port:
            port = [9000]
        return Client(settings={"use_numpy": True}, host=host, port=port[0])
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
        for stmt in self.create_statement:
            self.execute(stmt)

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    def select(self, query_cls: Optional[Query] = Query, *args, **kwargs) -> Query:
        return query_cls(driver=self, *args, **kwargs)


class Clickhouse(Driver):
    CREATE_TABLE = """
    CREATE TABLE {table}
    (
        `pk`                      FixedString(40) NOT NULL,
        `country`                 {country} NOT NULL,
        `year`                    {year} NOT NULL,
        `recipient_id`            FixedString(40) NOT NULL,
        `recipient_name`          String NULL,
        `recipient_fingerprint`   String NOT NULL,
        `recipient_address`       String NULL,
        `recipient_country`       {country} NOT NULL,
        `recipient_url`           String NULL,
        `scheme_id`               String NULL,
        `scheme`                  String NULL,
        `scheme_code`             String NULL,
        `scheme_description`      String NULL,
        `amount`                  Decimal(18, 2) NULL,
        `currency`                {currency} NULL,
        `amount_original`         Decimal(18, 2) NULL,
        `currency_original`       {currency} NULL,
        INDEX fp_ix (recipient_fingerprint) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
    ) ENGINE = ReplacingMergeTree()
    PRIMARY KEY (country, year, recipient_id)
    ORDER BY (country, year, recipient_id, pk)
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
        create_table = self.CREATE_TABLE.format(
            table=self.table, country=country, currency=currency, year=year
        )
        by_id = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_id (
            SELECT *
            ORDER BY recipient_id,country,year
        )
        """
        by_fingerprint = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_fp (
            SELECT *
            ORDER BY recipient_fingerprint,country,year
        )
        """
        by_name = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_name (
            SELECT *
            ORDER BY recipient_name,country,year
        )
        """
        by_country = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_country (
            SELECT *
            ORDER BY country,year,recipient_fingerprint
        )
        """
        by_year = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_year (
            SELECT *
            ORDER BY year,country,recipient_fingerprint
        )
        """
        by_scheme_id = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_scheme_id (
            SELECT *
            ORDER BY scheme_id,country,year
        )
        """
        by_scheme = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_scheme (
            SELECT *
            ORDER BY scheme,country,year
        )
        """

        yield create_table
        yield by_id
        yield by_fingerprint
        yield by_name
        yield by_country
        yield by_year
        yield by_scheme
        yield by_scheme_id

    @property
    def drop_statement(self) -> str:
        return self.DROP_TABLE.format(table=self.table)

    def insert(self, df: pd.DataFrame) -> int:
        # https://clickhouse-driver.readthedocs.io/en/latest/features.html#numpy-pandas-support
        df = df.applymap(lambda x: None if x == "" else x)
        res = self.conn.insert_dataframe("INSERT INTO %s VALUES" % self.table, df)
        return res

    def query(self, query: Query) -> pd.DataFrame:
        query = str(query)
        return self.conn.query_dataframe(query)


class Duckdb(Driver):
    """this driver is not fully featured for specific queries. It is mainly
    used for importing data and raw sql queries"""

    CREATE_TABLE = """
    CREATE TABLE {table}(
        pk                      CHAR(40) NOT NULL,
        country                 country NOT NULL,
        year                    year NOT NULL,
        recipient_id            CHAR(40) NOT NULL,
        recipient_name          VARCHAR,
        recipient_fingerprint   VARCHAR,
        recipient_address       VARCHAR,
        recipient_country       VARCHAR NOT NULL,
        recipient_url           VARCHAR,
        scheme_id               VARCHAR,
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
    def create_statement(self) -> Iterator[str]:
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
            "CREATE INDEX scheme_ix ON %s (scheme_id)" % self.table,
        )
        yield countries
        yield currencies
        yield years
        yield table
        for ix in indexes:
            yield ix

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
    driver: Optional[str] = None,
    uri: Optional[str] = None,
    table: Optional[str] = None,
    read_only: Optional[bool] = True,
) -> Driver:

    # this allows overwriting settings during runtime (aka tests)
    driver = driver or settings.DRIVER
    uri = uri or settings.DATABASE_URI
    table = table or settings.DATABASE_TABLE

    if driver not in settings.SUPPORTED_DRIVERS:
        raise ImproperlyConfigured(f"Not a supported DB driver: `{driver}`")
    if driver == "clickhouse":
        return Clickhouse(table, uri)
    if driver == "duckdb":
        return Duckdb(table, uri, read_only=read_only)
