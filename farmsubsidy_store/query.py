from functools import cached_property, lru_cache
from typing import Any, Iterable, Iterator, Optional, Union

import pandas as pd
from banal import is_listish, as_bool

from . import settings
from .exceptions import InvalidQuery


@lru_cache(settings.LRU_QUERY_CACHE_SIZE)
def _get_cached_query(driver, query: str) -> pd.DataFrame:
    return driver.query(query)


class Query:
    OPERATORS = {
        "like": "LIKE",
        "ilike": "ILIKE",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
        "in": "IN",
        "null": "IS",
    }

    fields = "*"
    group_by_fields = None
    order_by_fields = None

    def __init__(
        self,
        table: Optional[str] = None,
        # driver: Optional["Driver"] = None,
        driver=None,  # FIXME circular imports
        result_cls=None,
        fields: Optional[Iterable[str]] = None,
        group_by_fields: Optional[Iterable[str]] = None,
        order_by_fields: Optional[Iterable[str]] = None,
        order_direction: Optional[str] = "ASC",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_lookup: Optional[dict] = None,
        having_lookup: Optional[dict] = None,
    ):
        if table is None:
            if driver is not None:
                table = driver.table
            else:
                table = settings.DATABASE_TABLE
        self.driver = driver
        self.table = table
        self.result_cls = result_cls
        self.fields = fields or self.fields
        self.group_by_fields = group_by_fields or self.group_by_fields
        self.order_by_fields = order_by_fields or self.order_by_fields
        self.order_direction = order_direction
        self.limit = limit
        self.offset = offset
        self.where_lookup = where_lookup
        self.having_lookup = having_lookup

    def __str__(self) -> str:
        return self.get_query()

    def __iter__(self) -> Iterator[Any]:
        df = self.execute()
        for _, row in df.iterrows():
            yield self.apply_result(row)

    @cached_property
    def count(self):
        """get an optimized query for count()"""
        # FIXME this doesn't cover aggregated cases for `having`
        count_part = "*"
        if self.group_by_fields:
            count_part = f"DISTINCT {', '.join(self.group_by_fields)}"
        query = (
            f"SELECT count({count_part}) AS count FROM {self.table}{self.where_part}"
        )
        df = self.execute(query)
        return int(df["count"][0])

    def execute(self, query: Optional[Union["Query", str]] = None) -> pd.DataFrame:
        """actually return results from `self.driver`"""
        if self.driver is None:
            raise InvalidQuery("No driver for this query.")
        query = query or self.get_query()
        return _get_cached_query(self.driver, str(query))

    def apply_result(self, row) -> Any:
        if self.result_cls is not None:
            return self.result_cls(**row)
        else:
            return row

    def first(self):
        # return the first object
        for res in self:
            return res

    def _chain(self, **kwargs):
        # merge current state
        new_kwargs = self.__dict__.copy()
        new_kwargs.pop("df", None)  # FIXME
        for key, new_value in kwargs.items():
            old_value = new_kwargs[key]
            if old_value is None:
                new_kwargs[key] = new_value
            # overwrite order by
            elif key == "order_by_fields":
                new_kwargs[key] = new_value
            # combine tuples and dicts
            elif isinstance(old_value, tuple):
                new_kwargs[key] = old_value + new_value
            elif isinstance(old_value, dict):
                new_kwargs[key] = {**old_value, **new_value}
            else:  # replace
                new_kwargs[key] = new_value
        return self.__class__(**new_kwargs)

    def select(self, *fields) -> "Query":
        return self._chain(fields=fields)

    def where(self, **filters) -> "Query":
        return self._chain(where_lookup=filters)

    def having(self, **filters) -> "Query":
        return self._chain(having_lookup=filters)

    def group_by(self, *fields) -> "Query":
        return self._chain(group_by_fields=fields)

    def order_by(self, *fields, ascending=True) -> "Query":
        return self._chain(
            order_by_fields=fields, order_direction="ASC" if ascending else "DESC"
        )

    # for slicing
    def __getitem__(self, value) -> "Query":
        if isinstance(value, int):
            if value < 0:
                raise InvalidQuery("Invalid slicing: slice must not be negative.")
            return self._chain(limit=1, offset=value)
        if isinstance(value, slice):
            if value.step is not None:
                raise InvalidQuery("Invalid slicing: steps not allowed.")
            offset = value.start or 0
            if value.stop is not None:
                return self._chain(limit=value.stop - offset, offset=offset)
            return self._chain(offset=offset)
        raise NotImplementedError

    @property
    def fields_part(self) -> str:
        return ", ".join(self.fields or "*")

    def _get_lookup_part(self, lookup: dict) -> str:
        # for where and having
        parts = []
        for field, value in lookup.items():
            field, *operator = field.split("__")
            if operator:
                if len(operator) > 1:
                    raise InvalidQuery(f"Invalid operator: {operator}")
                operator = operator[0]
                if operator not in self.OPERATORS:
                    raise InvalidQuery(f"Invalid operator: {operator}")

                if operator == "in":
                    if not is_listish(value):
                        raise InvalidQuery(f"Invalid value for `IN` operator: {value}")
                    values = ", ".join([f"'{v}'" for v in value])
                    value = f"({values})"
                elif operator == "null":
                    # field__null=True|False
                    value = "NULL" if as_bool(value) else "NOT NULL"
                else:
                    value = f"'{value}'"
                parts.append(" ".join((field, self.OPERATORS[operator], value)))
            else:
                parts.append(f"{field} = '{value}'")
        return " AND ".join(parts)

    @property
    def where_part(self) -> str:
        if not self.where_lookup:
            return ""
        return " WHERE " + self._get_lookup_part(self.where_lookup)

    @property
    def having_part(self) -> str:
        if not self.group_part or not self.having_lookup:
            return ""
        return " HAVING " + self._get_lookup_part(self.having_lookup)

    @property
    def group_part(self) -> str:
        if self.group_by_fields is None:
            return ""
        return " GROUP BY " + ", ".join(self.group_by_fields)

    @property
    def order_part(self) -> str:
        if self.order_by_fields is None:
            return ""
        return (
            " ORDER BY " + ", ".join(self.order_by_fields) + " " + self.order_direction
        )

    @property
    def limit_part(self) -> str:
        if self.limit is None and self.offset is None:
            return ""
        offset = self.offset or 0
        if self.limit:
            if self.limit < 0:
                raise InvalidQuery(f"Limit {self.limit} must not be negative")
            return f" LIMIT {offset}, {self.limit}"
        return f" OFFSET {offset}"

    @property
    def is_filtered(self) -> bool:
        return bool(self.where_part or self.having_part)

    def get_query(self) -> str:
        rest = "".join(
            (
                self.where_part,
                self.group_part,
                self.having_part,
                self.order_part,
                self.limit_part,
            )
        ).strip()
        q = f"SELECT {self.fields_part} FROM {self.table} {rest}"
        return q.strip()


class RecipientQuery(Query):
    fields = (
        "recipient_id AS id",
        "count(*) AS total_payments",
        "groupUniqArray(year) AS years",
        "groupUniqArray(recipient_name) AS name",
        "arrayElement(groupUniqArray(recipient_country), 1) AS recipient_country",
        "groupUniqArray(recipient_address) AS address",
        "sum(amount) AS amount_sum",
        "round(avg(amount), 2) AS amount_avg",
        "max(amount) AS amount_max",
        "min(amount) AS amount_min",
    )
    group_by_fields = ("recipient_id",)
    order_by_fields = ("recipient_id",)


class RecipientBaseQuery(RecipientQuery):
    """simpler query without string aggregation"""

    fields = (
        "recipient_id AS id",
        "count(*) AS total_payments",
        "sum(amount) AS amount_sum",
        "round(avg(amount), 2) AS amount_avg",
        "max(amount) AS amount_max",
        "min(amount) AS amount_min",
    )


class RecipientNameQuery(Query):
    """quick search for names"""

    fields = (
        "distinct recipient_id AS id",
        "recipient_name AS name",
        "recipient_country",
    )


class SchemeQuery(Query):
    fields = (
        "scheme",
        "groupUniqArray(year) AS years",
        "groupUniqArray(country) AS countries",
        "count(*) AS total_payments",
        "count(distinct recipient_id) AS total_recipients",
        "sum(amount) AS amount_sum",
        "round(avg(amount), 2) AS amount_avg",
        "max(amount) AS amount_max",
        "min(amount) AS amount_min",
    )
    group_by_fields = ("scheme",)
    order_by_fields = ("scheme",)


class CountryQuery(Query):
    fields = (
        "country",
        "groupUniqArray(year) AS years",
        "count(*) AS total_payments",
        "count(distinct recipient_id) AS total_recipients",
        "sum(amount) AS amount_sum",
        "round(avg(amount), 2) AS amount_avg",
        "max(amount) AS amount_max",
        "min(amount) AS amount_min",
    )
    group_by_fields = ("country",)
    order_by_fields = ("country",)


class YearQuery(Query):
    fields = (
        "year",
        "groupUniqArray(country) AS countries",
        "count(*) AS total_payments",
        "count(distinct recipient_id) AS total_recipients",
        "sum(amount) AS amount_sum",
        "round(avg(amount), 2) AS amount_avg",
        "max(amount) AS amount_max",
        "min(amount) AS amount_min",
    )
    group_by_fields = ("year",)
    order_by_fields = ("year",)
