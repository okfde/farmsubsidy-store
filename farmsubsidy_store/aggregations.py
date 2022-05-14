from typing import Optional
from functools import lru_cache

from .drivers import Driver, get_driver
from .query import CountryQuery, YearQuery


@lru_cache(1024 * 1000)  # 1 GB
def agg_by_country(driver: Optional[Driver] = None, **filters):
    driver = driver or get_driver()
    query = CountryQuery(driver=driver).where(**filters)
    return driver.query(query)


@lru_cache(1024 * 1000)  # 1 GB
def agg_by_year(driver: Optional[Driver] = None, **filters):
    driver = driver or get_driver()
    query = YearQuery(driver=driver).where(**filters)
    return driver.query(query)


AGGREGATIONS = {"by_country": agg_by_country, "by_year": agg_by_year}
