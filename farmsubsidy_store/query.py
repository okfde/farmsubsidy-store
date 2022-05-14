from typing import Any, Iterable, Iterator, Optional

from . import settings
from .exceptions import InvalidQuery


class Query:
    OPERATORS = {
        "like": "LIKE",
        "ilike": "ILIKE",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
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
        **filters,
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
        self.filters = filters

    def __str__(self) -> str:
        return self.get_query()

    def __iter__(self) -> Iterator[Any]:
        """actually return results from `self.driver`"""
        if self.driver is None:
            raise InvalidQuery("No driver for this query.")
        df = self.driver.query(self.get_query())
        for _, row in df.iterrows():
            if self.result_cls is not None:
                yield self.result_cls(**row)
            else:
                yield row

    def _chain(self, **kwargs):
        old_kwargs = {
            **{
                "driver": self.driver,
                "table": self.table,
                "result_cls": self.result_cls,
                "fields": self.fields,
                "group_by_fields": self.group_by_fields,
                "order_by_fields": self.order_by_fields,
                "order_direction": self.order_direction,
            },
            **self.filters,
        }
        new_kwargs = {**old_kwargs, **kwargs}
        return self.__class__(**new_kwargs)

    def select(self, *fields) -> "Query":
        return self._chain(fields=fields)

    def where(self, **filters) -> "Query":
        return self._chain(**filters)

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

    @property
    def where_part(self) -> str:
        if not self.filters:
            return ""
        parts = []
        for field, value in self.filters.items():
            field, *operator = field.split("__")
            if operator:
                if len(operator) > 1:
                    raise InvalidQuery(f"Invalid operator: {operator}")
                operator = operator[0]
                if operator not in self.OPERATORS:
                    raise InvalidQuery(f"Invalid operator: {operator}")
                parts.append(f"{field} {self.OPERATORS[operator]} '{value}'")
            else:
                parts.append(f"{field} = '{value}'")
        return " WHERE " + " AND ".join(parts)

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

    def get_query(self) -> str:
        q = f"SELECT {self.fields_part} FROM {self.table}{self.where_part}{self.group_part}{self.order_part}{self.limit_part}"
        return q.strip()


class _RecipientOuterQuery(Query):
    fields = (
        "recipient_id as id",
        "groupUniqArray(year) as years",
        "groupUniqArray(recipient_name) as name",
        "groupUniqArray(recipient_country) as country",
        "groupUniqArray(recipient_address) as address",
        "count(*) as total_payments",
        "sum(amount) as amount_sum",
        "avg(amount) as amount_avg",
        "max(amount) as amount_max",
        "min(amount) as amount_min",
    )
    group_by_fields = ("recipient_id",)
    order_by_fields = ("recipient_id",)


class RecipientQuery(Query):
    fields = ("recipient_id",)
    group_by_fields = ("recipient_id",)
    order_by_fields = ("recipient_id",)

    def get_query(self):
        inner = super().get_query()
        outer = _RecipientOuterQuery()
        return f"SELECT {outer.fields_part} FROM {self.table} WHERE recipient_id IN ({inner}) {self.group_part} {self.order_part}"


class SchemeQuery(Query):
    fields = (
        "scheme",
        "groupUniqArray(year) as years",
        "groupUniqArray(country) as countries",
        "count(*) as total_payments",
        "count(distinct recipient_id) as total_recipients",
        "sum(amount) as amount_sum",
        "avg(amount) as amount_avg",
        "max(amount) as amount_max",
        "min(amount) as amount_min",
    )
    group_by_fields = ("scheme",)
    order_by_fields = ("scheme",)


class CountryQuery(Query):
    fields = (
        "country",
        "groupUniqArray(year) as years",
        "count(*) as total_payments",
        "count(distinct recipient_id) as total_recipients",
        "sum(amount) as amount_sum",
        "avg(amount) as amount_avg",
        "max(amount) as amount_max",
        "min(amount) as amount_min",
    )
    group_by_fields = ("country",)
    order_by_fields = ("country",)


class YearQuery(Query):
    fields = (
        "year",
        "groupUniqArray(country) as countries",
        "count(*) as total_payments",
        "count(distinct recipient_id) as total_recipients",
        "sum(amount) as amount_sum",
        "avg(amount) as amount_avg",
        "max(amount) as amount_max",
        "min(amount) as amount_min",
    )
    group_by_fields = ("year",)
    order_by_fields = ("year",)
