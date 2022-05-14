from typing import List, Optional

from pydantic import BaseModel

from .drivers import get_driver
from .query import CountryQuery, Query, RecipientQuery, SchemeQuery, YearQuery


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
    recipient_name: str
    recipient_fingerprint: str
    recipient_address: Optional[str] = None
    recipient_country: str
    recipient_url: Optional[str] = None
    scheme: Optional[str] = None
    scheme_code: Optional[str] = None
    scheme_description: Optional[str] = None
    amount: float
    currency: str
    amount_original: Optional[float] = None
    currency_original: Optional[str] = None

    def get_recipient(self) -> "Recipient":
        return Recipient.get(self.recipient_id)

    def get_scheme(self) -> "Scheme":
        return Scheme.get(self.scheme)

    def get_year(self) -> "Year":
        return Year.get(self.year)

    def get_country(self) -> "Country":
        return Country.get(self.country)


class Recipient(BaseORM, BaseModel):
    """name, address, country, url are always multi-valued"""

    _lookup_field = "recipient_id"
    _query_cls = RecipientQuery

    id: str
    name: List[str]
    address: Optional[List[str]] = []
    country: List[str]
    url: Optional[List[str]] = []
    years: List[int] = []
    total_payments: int
    amount_sum: float
    amount_avg: float
    amount_max: float
    amount_min: float

    @property
    def payments(self) -> Query:
        return Payment.select().where(recipient_id=self.id)

    @property
    def schemes(self) -> Query:
        return Scheme.select().where(recipient_id=self.id)

    @property
    def years(self) -> Query:
        return Year.select().where(recipient_id=self.id)

    @property
    def countries(self) -> Query:
        return Country.select().where(recipient_id=self.id)


class Scheme(BaseORM, BaseModel):
    _lookup_field = "scheme"
    _query_cls = SchemeQuery

    scheme: str
    years: List[int]
    countries: List[str]
    total_payments: int
    total_recipients: int
    amount_sum: float
    amount_avg: float
    amount_max: float
    amount_min: float

    @property
    def recipients(self) -> RecipientQuery:
        return Recipient.select().where(scheme=self.scheme)

    @property
    def payments(self) -> Query:
        return Payment.select().where(scheme=self.scheme)

    @property
    def years(self) -> Query:
        return Year.select().where(scheme=self.scheme)

    @property
    def countries(self) -> Query:
        return Country.select().where(scheme=self.scheme)


class Country(BaseORM, BaseModel):
    _lookup_field = "country"
    _query_cls = CountryQuery

    country: str
    total_recipients: int
    total_payments: int
    years: List[int]
    amount_sum: float
    amount_avg: float
    amount_max: float
    amount_min: float

    @property
    def recipients(self) -> RecipientQuery:
        return Recipient.select().where(country=self.country)

    @property
    def payments(self) -> Query:
        return Payment.select().where(country=self.country)

    @property
    def schemes(self) -> Query:
        return Scheme.select().where(country=self.country)

    @property
    def years(self) -> Query:
        return Year.select().where(country=self.country)


class Year(BaseORM, BaseModel):
    _lookup_field = "year"
    _query_cls = YearQuery

    year: int
    total_recipients: int
    total_payments: int
    countries: List[str]
    amount_sum: float
    amount_avg: float
    amount_max: float
    amount_min: float

    @property
    def recipients(self) -> RecipientQuery:
        return Recipient.select().where(year=self.year)

    @property
    def payments(self) -> Query:
        return Payment.select().where(year=self.year)

    @property
    def schemes(self) -> Query:
        return Scheme.select().where(year=self.year)

    @property
    def countries(self) -> Query:
        return Country.select().where(year=self.year)
