"""Paginated model views that can be used for the api
(see farmsubsidy_store.api) or third party apps"""

from typing import Iterator, Optional, Tuple

from banal import clean_dict
from pydantic import BaseModel, create_model, validator

from farmsubsidy_store.model import (
    Country,
    Payment,
    Recipient,
    RecipientBase,
    Scheme,
    Year,
)
from farmsubsidy_store.query import Query

NULL = ("null",)
STRING_COMPARATORS = NULL + ("like", "ilike")
NUMERIC_COMPARATORS = NULL + ("gt", "gte", "lt", "lte")
BASE_ORDER_BY = ("country", "year", "recipient_name", "scheme", "amount")
BASE_LOOKUPS = {
    "country": STRING_COMPARATORS,
    "year": NUMERIC_COMPARATORS,
    "recipient_id": [],  # only direct lookup
    "recipient_name": STRING_COMPARATORS,
    "recipient_fingerprint": STRING_COMPARATORS,
    "recipient_address": STRING_COMPARATORS,
    "scheme": STRING_COMPARATORS,
    "scheme_code": STRING_COMPARATORS,
    "scheme_description": STRING_COMPARATORS,
    "amount": NUMERIC_COMPARATORS,
}
AGGREGATION_FIELDS = (
    "amount_sum",
    "amount_avg",
    "amount_max",
    "amount_min",
    "total_payments",
    "total_recipients",
)
AGGREGATED_ORDER_BY = BASE_ORDER_BY + AGGREGATION_FIELDS
AGGREGATED_LOOKUPS = {
    **BASE_LOOKUPS,
    **{f: NUMERIC_COMPARATORS for f in AGGREGATION_FIELDS},
}


class BaseParams(BaseModel):
    order_by: Optional[str] = None
    limit: Optional[int] = None
    p: Optional[int] = 1

    @validator("p")
    def validate_p(cls, value):
        if value < 1:
            raise ValueError("Page must be 1 or higher (got `{value}`).")
        return value

    @validator("limit")
    def validate_limit(cls, value):
        if value < 1:
            raise ValueError("Limit must be 1 or higher (got `{value}`).")
        return value

    class Config:
        extra = "forbid"


class BaseListView:
    """this is a bit weird implemented with all the setters on self, but here
    we don't know about a request object and don't want to initialize the class
    with params"""

    max_limit = 1000
    model = None
    order_by = BASE_ORDER_BY
    lookups = BASE_LOOKUPS
    params_cls = BaseParams

    def get_results(self, **params):
        self.apply_params(**params)
        self.query = self.get_query(**params)
        self.data = list(self.query)
        self.has_next = self.query.count >= self.limit
        self.has_prev = self.page > 1
        return self.data

    def get_allowed_params(self) -> Iterator[str]:
        for field, operators in self.lookups.items():
            yield field
            for operator in operators:
                yield f"{field}__{operator}"

    def apply_params(self, **params) -> dict:
        params = self.validate_params(**params)
        self.page = params.pop("p", 1)
        self.order_by = params.pop("order_by", None)
        self.limit = min(self.max_limit, params.pop("limit", self.max_limit))
        self.params = params
        return params

    def validate_params(self, **params) -> dict:
        Params = create_model(
            f"{self.__class__.__name__}Params",
            __base__=self.params_cls,
            **{k: (str, None) for k in self.get_allowed_params()},
        )

        params = Params(**params)
        if params.order_by and params.order_by.lstrip("-") not in self.order_by:
            raise ValueError(f"Order by field `{params.order_by}` invalid.")
        return clean_dict(params.dict())

    def get_slice(self, page: int, limit: int) -> Tuple[int, int]:
        start = (page - 1) * limit
        end = start + limit
        return start, end

    def get_query(self, **params) -> Query:
        if not hasattr(self, "params"):
            self.apply_params(**params)
        query = (
            self.get_initial_query()
            .where(**self.where_params)
            .having(**self.having_params)
        )
        order_by = self.order_by
        if order_by is not None:
            ascending = True
            if self.order_by.startswith("-"):
                order_by = order_by[1:]
                ascending = False
            query = query.order_by(order_by, ascending=ascending)
        start, end = self.get_slice(self.page, self.limit)
        return query[start:end]

    def get_initial_query(self) -> Query:
        return self.model.select()

    @property
    def where_params(self) -> dict:
        params = {}
        for k, v in self.params.items():
            base_k, *_ = k.split("__")
            if base_k in BASE_LOOKUPS:
                params[k] = v
        return params

    @property
    def having_params(self) -> dict:
        params = {}
        for k, v in self.params.items():
            base_k, *_ = k.split("__")
            if base_k in AGGREGATION_FIELDS:
                params[k] = v
        return params


class PaymentListView(BaseListView):
    model = Payment


class RecipientListView(BaseListView):
    order_by = AGGREGATED_ORDER_BY
    lookups = AGGREGATED_LOOKUPS
    model = Recipient


class RecipientBaseView(RecipientListView):
    """improved query performance because no string aggregation happens),
    useful for "get top 5 of country X" but accepts the same parameters as the
    big list view, so sorting for schemes or searching for names/fingerprints
    is still possible!
    """

    model = RecipientBase


class SchemeListView(BaseListView):
    order_by = AGGREGATED_ORDER_BY
    lookups = AGGREGATED_LOOKUPS
    model = Scheme


class CountryListView(BaseListView):
    order_by = AGGREGATED_ORDER_BY
    lookups = AGGREGATED_LOOKUPS
    model = Country


class YearListView(BaseListView):
    order_by = AGGREGATED_ORDER_BY
    lookups = AGGREGATED_LOOKUPS
    model = Year
