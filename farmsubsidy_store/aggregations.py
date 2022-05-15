from functools import lru_cache
from typing import Optional

import pandas as pd

from . import settings
from .drivers import Driver, get_driver
from .query import CountryQuery, Query, YearQuery


@lru_cache(settings.LRU_QUERY_CACHE_SIZE)
def agg_by_country(driver: Optional[Driver] = None, **filters) -> pd.DataFrame:
    filters.pop("country", None)
    driver = driver or get_driver()
    query = CountryQuery(driver=driver).where(**filters)
    return driver.query(query)


@lru_cache(settings.LRU_QUERY_CACHE_SIZE)
def agg_by_year(driver: Optional[Driver] = None, **filters) -> pd.DataFrame:
    filters.pop("year", None)
    driver = driver or get_driver()
    query = YearQuery(driver=driver).where(**filters)
    return driver.query(query)


@lru_cache(settings.LRU_QUERY_CACHE_SIZE)
def amount_sum(driver: Optional[Driver] = None, **filters) -> pd.DataFrame:
    driver = driver or get_driver()
    query = Query(driver=driver).select("sum(amount) as amount_sum").where(**filters)
    df = driver.query(query)
    return df["amount_sum"][0]


AGGREGATIONS = {
    "by_country": agg_by_country,
    "by_year": agg_by_year,
    "amount_sum": amount_sum,
}
