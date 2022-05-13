from typing import Optional
from functools import cache, lru_cache

from .drivers import Driver, current_driver
from .query import Query


class CountryAggregation(Query):
    fields = (
        "country",
        "count(distinct recipient_id) as total_recipients",
        "count(*) as total_payments",
        "groupUniqArray(year) as years",
        "sum(amount) as amount_sum",
        "max(amount) as amount_max",
        "min(amount) as amount_min",
        "avg(amount) as amount_avg",
    )
    group_by = ("country",)
    order_by = ("country",)


class YearAggregation(Query):
    fields = (
        "year",
        "count(distinct recipient_id) as total_recipients",
        "count(*) as total_payments",
        "groupUniqArray(country) as countries",
        "sum(amount) as amount_sum",
        "max(amount) as amount_max",
        "min(amount) as amount_min",
        "avg(amount) as amount_avg",
    )
    group_by = ("year",)
    order_by = ("year",)


@lru_cache(1024 * 10)  # 10 MB
def agg_by_country(driver: Optional[Driver] = current_driver, **filters):
    query = CountryAggregation(driver.driver, driver.table, **filters)
    return driver.query(query)


@lru_cache(1024 * 10)  # 10 MB
def agg_by_year(driver: Optional[Driver] = current_driver, **filters):
    query = YearAggregation(driver.driver, driver.table, **filters)
    return driver.query(query)


AGGREGATIONS = {"by_country": agg_by_country, "by_year": agg_by_year}


@cache
def get_cached_aggregations():
    return {name: agg() for name, agg in AGGREGATIONS}
