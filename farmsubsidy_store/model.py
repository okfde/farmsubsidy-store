from pydantic import BaseModel

from .drivers import get_driver
from .query import (
    AggregationQuery,
    CountryQuery,
    LocationQuery,
    Nuts1Query,
    Nuts2Query,
    Nuts3Query,
    Query,
    RecipientBaseQuery,
    RecipientNameQuery,
    RecipientQuery,
    SchemeQuery,
    YearQuery,
)
from .schemes import DESCRIPTIONS
from .util import get_country_name


class BaseORM:
    _lookup_field = "pk"
    _query_cls = Query

    @classmethod
    def get(cls, lookup_value: str):
        db = get_driver()
        res = db.select(query_cls=cls._query_cls, result_cls=cls).where(
            **{cls._lookup_field: lookup_value}
        )
        return res.first()

    @classmethod
    def select(cls, *fields) -> Query:
        db = get_driver()
        return db.select(query_cls=cls._query_cls, result_cls=cls, fields=fields)


class Payment(BaseORM, BaseModel):
    pk: str
    country: str
    year: int
    recipient_id: str
    recipient_name: str | None = None  # anonymous recipients
    recipient_fingerprint: str | None = None
    recipient_address: str | None = None
    recipient_country: str
    recipient_url: str | None = None
    scheme_id: str | None = None
    scheme: str | None = None
    scheme_code: str | None = None
    scheme_description: str | None = None
    amount: float | None = None
    currency: str | None = None
    amount_original: float | None = None
    currency_original: str | None = None

    def __str__(self):
        return f"{self.country}-{self.year}-{self.pk}"


class RecipientName(BaseORM, BaseModel):
    """abstraction to quickly get names"""

    _lookup_field = "recipient_fingerprint"
    _query_cls = RecipientNameQuery

    id: str
    name: str
    country: str

    def __init__(self, **data):  # FIXME
        data["country"] = data.pop("recipient_country", data.get("country"))
        super().__init__(**data)


class RecipientBase(BaseORM, BaseModel):
    _lookup_field = "recipient_id"
    _query_cls = RecipientBaseQuery

    id: str
    total_payments: int | None = 0
    amount_sum: float | None = 0
    amount_avg: float | None = 0
    amount_max: float | None = 0
    amount_min: float | None = 0


class Recipient(BaseORM, BaseModel):
    """name, address, country, url are always multi-valued"""

    _lookup_field = "recipient_id"
    _query_cls = RecipientQuery

    id: str
    name: list[str] | None = []  # anonymous recipients
    address: list[str] | None = []
    country: str
    url: list[str] | None = []
    years: list[int] = []
    total_payments: int | None = 0
    amount_sum: float | None = 0
    amount_avg: float | None = 0
    amount_max: float | None = 0
    amount_min: float | None = 0

    def __init__(self, **data):  # FIXME
        data["country"] = data.pop("recipient_country", data.get("country"))
        super().__init__(**data)

    def __str__(self):
        return "; ".join(self.name)


class Scheme(BaseORM, BaseModel):
    _lookup_field = "scheme_id"
    _query_cls = SchemeQuery

    id: str | None = None  # FIXME
    name: str
    years: list[int]
    countries: list[str]
    total_payments: int
    total_recipients: int
    amount_sum: float | None = None
    amount_avg: float | None = None
    amount_max: float | None = None
    amount_min: float | None = None
    description: str | None = None

    def __init__(self, **data):
        data["description"] = DESCRIPTIONS.get(data["name"])
        super().__init__(**data)


class Country(BaseORM, BaseModel):
    _lookup_field = "country"
    _query_cls = CountryQuery

    country: str
    name: str
    total_recipients: int
    total_payments: int
    years: list[int]
    amount_sum: float
    amount_avg: float
    amount_max: float
    amount_min: float

    def __str__(self):
        return self.country

    def __init__(self, **data):
        data["name"] = get_country_name(data["country"])
        super().__init__(**data)


class Year(BaseORM, BaseModel):
    _lookup_field = "year"
    _query_cls = YearQuery

    year: int
    total_recipients: int
    total_payments: int
    countries: list[str]
    amount_sum: float
    amount_avg: float
    amount_max: float
    amount_min: float

    def __str__(self):
        return str(self.year)


class Location(BaseORM, BaseModel):
    _lookup_field = "recipient_address"
    _query_cls = LocationQuery

    location: str | None = None
    years: list[int] | None = []
    countries: list[str] | None = []
    total_recipients: int | None = None
    total_payments: int | None = None
    amount_sum: float | None = None
    amount_avg: float | None = None
    amount_max: float | None = None
    amount_min: float | None = None

    def __str__(self):
        return str(self.location)


class Nuts1(BaseORM, BaseModel):
    _lookup_field = "nuts1"
    _query_cls = Nuts1Query

    level: int | None = 1
    nuts: str
    years: list[int] | None = []
    countries: list[str] | None = []
    total_recipients: int | None = None
    total_payments: int | None = None
    amount_sum: float | None = None
    amount_avg: float | None = None
    amount_max: float | None = None
    amount_min: float | None = None

    def __str__(self):
        return str(self.nuts)


class Nuts2(BaseORM, BaseModel):
    _lookup_field = "nuts2"
    _query_cls = Nuts2Query

    level: int | None = 2
    nuts: str
    years: list[int] | None = []
    countries: list[str] | None = []
    total_recipients: int | None = None
    total_payments: int | None = None
    amount_sum: float | None = None
    amount_avg: float | None = None
    amount_max: float | None = None
    amount_min: float | None = None

    def __str__(self):
        return str(self.nuts)


class Nuts3(BaseORM, BaseModel):
    _lookup_field = "nuts3"
    _query_cls = Nuts3Query

    level: int | None = 3
    nuts: str
    years: list[int] | None = []
    countries: list[str] | None = []
    total_recipients: int | None = None
    total_payments: int | None = None
    amount_sum: float | None = None
    amount_avg: float | None = None
    amount_max: float | None = None
    amount_min: float | None = None

    def __str__(self):
        return str(self.nuts)


class Aggregation(BaseORM, BaseModel):
    _query_cls = AggregationQuery

    total_recipients: int | None = None
    total_payments: int | None = None
    amount_sum: float | None = None
    amount_avg: float | None = None
    amount_max: float | None = None
    amount_min: float | None = None
