"""Paginated model views that can be used for the api
(see farmsubsidy_store.api) or third party apps"""

from enum import Enum
from itertools import chain, product
from typing import Iterable, Optional, Tuple, Union

import pandas as pd
from banal import clean_dict
from fingerprints import generate as generate_fp
from pydantic import BaseModel, create_model, validator

from farmsubsidy_store import model as models
from farmsubsidy_store.query import Query


class NullLookup(BaseModel):
    null: bool = None


class StringLookups(NullLookup):
    like: str = None
    ilike: str = None


class NumericLookups(NullLookup):
    gt: int = None
    gte: int = None
    lt: int = None
    lte: int = None


class BaseFields(BaseModel):
    country: StringLookups = None
    year: NumericLookups = None
    recipient_id: str = None
    recipient_name: StringLookups = None
    recipient_fingerprint: StringLookups = None
    recipient_address: StringLookups = None
    scheme_id: StringLookups = None
    scheme: StringLookups = None
    scheme_code: StringLookups = None
    scheme_description: StringLookups = None
    amount: NumericLookups = None

    def __init__(self, **data):
        # rewrite fingerprint lookups to actual fingerprints
        # but preserve % signs (if any) for LIKE clause
        for key, value in data.items():
            if "fingerprint" in key and value is not None:
                old_value = value.strip("%")
                data[key] = value.replace(old_value, generate_fp(old_value))
        super().__init__(**data)


class AggregatedFields(BaseModel):
    amount_sum: NumericLookups = None
    amount_avg: NumericLookups = None
    amount_min: NumericLookups = None
    amount_max: NumericLookups = None
    total_payments: NumericLookups = None
    total_recipients: NumericLookups = None


class OutputFormat(Enum):
    csv = "csv"
    json = "json"


def _create_model(
    name: str, fields_model: Union[BaseFields, AggregatedFields]
) -> BaseModel:
    def _fields():
        for field, lookups in fields_model.__fields__.items():
            if lookups.type_ in (NumericLookups, StringLookups):
                if lookups.type_ == NumericLookups:
                    yield field, int
                else:
                    yield field, str
                for lookup, lookup_type in lookups.type_.__fields__.items():
                    yield f"{field}__{lookup}", lookup_type.type_
            else:
                yield field, str

    return create_model(
        name,
        **{field: (Optional[type_], None) for field, type_ in _fields()},
        __base__=fields_model,
    )


def _get_enum(name: str, items: Iterable[str]) -> Enum:
    # amount_sum: asc , -amount_sum: desc
    return Enum(name, (("".join(x), "".join(x)) for x in product(("", "-"), items)))


OrderBy = _get_enum("order_by", BaseFields.__fields__)
AggregatedOrderBy = _get_enum(
    "order_by", chain(BaseFields.__fields__, AggregatedFields.__fields__)
)


BaseFieldsParams = _create_model("BaseFields", BaseFields)
AggregatedFieldsParams = _create_model("AggregatedFields", AggregatedFields)


class BaseViewParams(BaseFieldsParams):
    order_by: Optional[OrderBy] = None
    limit: Optional[int] = 1000
    p: Optional[int] = 1
    output: Optional[OutputFormat] = OutputFormat.json

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

    @validator("order_by")
    def validate_order_by(cls, value):
        if isinstance(value, Enum):
            return value.value
        return value

    class Config:
        extra = "forbid"


class AggregatedViewParams(BaseViewParams, AggregatedFieldsParams):
    order_by: Optional[AggregatedOrderBy] = None


class BaseListView:
    """this is a bit weird implemented with all the setters on self, but here
    we don't know about a request object and don't want to initialize the class
    with params"""

    max_limit = 1000
    model = None
    params_cls = BaseViewParams

    def get_results(self, df: pd.DataFrame = None, **params):
        self.apply_params(**params)
        self.query = self.get_query(**params)
        if df is not None:
            self.data = self.get_results_from_df(df)
        else:
            self.data = list(self.query)
        self.has_next = self.query.count >= self.page * self.limit
        self.has_prev = self.page > 1
        return self.data

    def apply_params(self, **params) -> dict:
        params = self.params_cls(**params)
        params = clean_dict(params.dict())
        self.page = params.pop("p", 1)
        self.order_by = params.pop("order_by", None)
        self.limit = min(self.max_limit, params.pop("limit", self.max_limit))
        self.output_format = params.pop("output")
        self.params = params
        return params

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
        if self.order_by is not None:
            order_by = self.order_by
            ascending = True
            if order_by.startswith("-"):
                order_by = order_by[1:]
                ascending = False
            query = query.order_by(order_by, ascending=ascending)
        start, end = self.get_slice(self.page, self.limit)
        return query[start:end]

    def get_initial_query(self) -> Query:
        return self.model.select()

    def get_results_from_df(self, df: pd.DataFrame):
        # use a previously cached df and apply ordering and slicing on it
        if not len(df):
            return []
        if self.order_by is not None:
            order_by = self.order_by
            ascending = True
            if order_by.startswith("-"):
                order_by = order_by[1:]
                ascending = False
            df = df.sort_values(order_by, ascending=ascending)
        start, end = self.get_slice(self.page, self.limit)
        df = df.iloc[start:end]
        return [self.model(**row) for _, row in df.iterrows()]

    @property
    def where_params(self) -> dict:
        params = {}
        for k, v in self.params.items():
            if k in BaseFieldsParams.__fields__:
                params[k] = v
        return params

    @property
    def having_params(self) -> dict:
        params = {}
        for k, v in self.params.items():
            if k in AggregatedFieldsParams.__fields__:
                params[k] = v
        return params

    @classmethod
    def get_params_cls(cls):
        # FIXME
        # fastapi views cannot share same classes for pydantic models !?
        order_by_fields = cls.params_cls.schema()["definitions"]["order_by"]["enum"]
        OrderBy = Enum(
            f"{cls.__name__}{cls.params_cls.__name__}OrderBy",
            ((o, o) for o in order_by_fields),
        )
        return create_model(
            f"{cls.__name__}ViewParams",
            order_by=(OrderBy, None),
            __base__=cls.params_cls,
        )


class PaymentListView(BaseListView):
    model = models.Payment


class RecipientListView(BaseListView):
    params_cls = AggregatedViewParams
    model = models.Recipient


class RecipientBaseView(RecipientListView):
    """improved query performance because no string aggregation happens),
    useful for "get top 5 of country X" but accepts the same parameters as the
    big list view, so sorting for schemes or searching for names/fingerprints
    is still possible!
    """

    model = models.RecipientBase


class RecipientNameView(BaseListView):
    """quick autocomplete view"""

    max_limit = 10
    model = models.RecipientName


class SchemeListView(BaseListView):
    params_cls = AggregatedViewParams
    model = models.Scheme


class CountryListView(BaseListView):
    params_cls = AggregatedViewParams
    model = models.Country


class YearListView(BaseListView):
    params_cls = AggregatedViewParams
    model = models.Year
