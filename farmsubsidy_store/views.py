"""Paginated model views that can be used for the api
(see farmsubsidy_store.api) or third party apps"""

import os
from enum import Enum
from itertools import chain, product
from typing import Iterable, List, Optional, Tuple, Union

import pandas as pd
from banal import clean_dict
from fingerprints import generate as generate_fp
from followthemoney.util import make_entity_id as make_id
from pydantic import BaseModel, create_model, validator

from farmsubsidy_store import model as models
from farmsubsidy_store import settings
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
    export = "export"


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
    limit: Optional[int] = None
    p: Optional[int] = 1
    output: Optional[OutputFormat] = OutputFormat.json
    api_key: Optional[str] = None

    @validator("p")
    def validate_p(cls, value):
        if value < 1:
            raise ValueError("Page must be 1 or higher (got `{value}`).")
        return value

    @validator("limit")
    def validate_limit(cls, value):
        if value is not None:
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

    def get_results(self, df: Optional[pd.DataFrame] = None, **params):
        self.apply_params(**params)
        self.query = self.get_query(**params)
        if df is not None:
            self.data = self.get_results_from_df(df)
        else:
            self.data = list(self.query)
        self.has_next = (
            (self.query.count >= self.page * self.limit)
            if self.limit is not None
            else False
        )
        self.has_prev = self.page > 1
        return self.data

    def apply_params(self, **params) -> dict:
        if hasattr(self, "params"):  # might be already called
            return self.params
        params = self.params_cls(**params)
        params = clean_dict(params.dict())
        self.page = params.pop("p", 1)
        self.order_by = params.pop("order_by", None)
        self.output_format = params.pop("output")
        self.limit = self.get_limit(params.pop("limit", None), **params)
        self.params = params
        return params

    def get_limit(self, limit: Optional[int] = None, **params) -> int:
        # allow override in api view for higher limit based on api key
        return min(self.max_limit, limit or self.max_limit)

    def get_slice(self, page: int, limit: Optional[int] = None) -> Tuple[int, int]:
        if limit is not None:
            start = (page - 1) * limit
            end = start + limit
            return start, end
        return None, None

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

    def get_export(self, **params):
        """generate an exported csv file (or use existing) and return public
        path to it"""
        self.query = self.get_query(**params)
        export_file = make_id("export", str(self.query)) + ".csv"
        export_fpath = os.path.join(settings.EXPORT_DIRECTORY, export_file)
        if not os.path.isfile(export_fpath):
            data = self.get_results(**params)
            self.to_csv(data, export_fpath)
        return settings.EXPORT_PUBLIC_PATH + "/" + export_file

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
    def to_csv(cls, data: List[BaseModel], fpath: Optional[str] = None):
        df = pd.DataFrame(dict(i) for i in data)
        df = df.applymap(
            lambda x: ";".join(sorted(str(i) for i in x)) if isinstance(x, list) else x
        )
        if fpath is None:
            return df.fillna("").to_csv(index=False)
        df.fillna("").to_csv(fpath, index=False)


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


class LocationListView(BaseListView):
    params_cls = AggregatedViewParams
    model = models.Location


class AggregationView(BaseListView):
    params_cls = BaseViewParams
    model = models.Aggregation

    def get_results(self, df: pd.DataFrame = None, **params):
        res = super().get_results(df, **params)
        self.has_next = False
        self.has_prev = False
        return res
