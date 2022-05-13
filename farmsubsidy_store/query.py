from typing import Iterable, Optional

from . import settings


class Query:
    fields = "*"
    group_by = None
    order_by = None
    page_size = 1000

    def __init__(
        self,
        driver: Optional[str] = settings.DRIVER,
        table: Optional[str] = settings.DATABASE_TABLE,
        fields: Optional[Iterable[str]] = None,
        group_by: Optional[Iterable[str]] = None,
        order_by: Optional[Iterable[str]] = None,
        search: Optional[dict] = {},
        **filters,
    ):
        self.driver = driver
        self.table = table
        self.fields = fields or self.fields
        self.group_by = group_by or self.group_by
        self.order_by = order_by or self.order_by
        self.search = search
        self.filters = filters

    def __str__(self) -> str:
        return f"SELECT {self.fields_part} FROM {self.table} {self.where_part} {self.group_part} {self.order_part}"

    @property
    def fields_part(self) -> str:
        return ", ".join(self.fields or "*")

    @property
    def where_part(self) -> str:
        if not self.filters and not self.search:
            return ""
        parts = set()
        for field, value in self.filters.items():
            parts.add(f"{field} = '{value}'")
        for field, value in self.search.items():
            parts.add(f"{field} ILIKE '%{value}%'")
        return "WHERE " + " AND ".join(parts)

    @property
    def group_part(self) -> str:
        if self.group_by is None:
            return ""
        return "GROUP BY " + ", ".join(self.group_by)

    @property
    def order_part(self) -> str:
        if self.order_by is None:
            return ""
        return "ORDER BY " + ", ".join(self.order_by)

    def page(self, page: Optional[int] = 1) -> str:
        q = str(self)
        return q + f" LIMIT {self.page_size * (page-1)}, {self.page_size} WITH TIES"


class RecipientListQuery(Query):
    fields = (
        "recipient_id as id",
        "groupUniqArray(year) as years",
        "groupUniqArray(recipient_name) as name",
        "groupUniqArray(recipient_country) as recipient_country",
        "groupUniqArray(recipient_address) as address",
        "count(*) as total_payments",
        "sum(amount) as sum_amount",
        "avg(amount) as avg_amount",
        "max(amount) as max_amount",
        "min(amount) as min_amount",
    )
    group_by = ("recipient_id",)
    order_by = ("recipient_id",)


class SchemeListQuery(Query):
    fields = (
        "scheme",
        "groupUniqArray(year) as years",
        "groupUniqArray(country) as countries",
        "count(*) as total_payments",
        "count(distinct recipient_id) as total_recipients",
        "sum(amount) as sum_amount",
        "avg(amount) as avg_amount",
        "max(amount) as max_amount",
        "min(amount) as min_amount",
    )
    group_by = ("scheme",)
    order_by = ("scheme",)
