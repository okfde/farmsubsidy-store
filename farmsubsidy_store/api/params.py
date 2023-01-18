from enum import Enum
from itertools import chain, product
from typing import Iterable

from fingerprints import generate as generate_fp
from pydantic import BaseModel, create_model, validator


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
    nuts1: StringLookups = None
    nuts2: StringLookups = None
    nuts3: StringLookups = None
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


def _create_model(name: str, fields_model: BaseFields | AggregatedFields) -> BaseModel:
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
        **{field: (type_ | None, None) for field, type_ in _fields()},
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
    order_by: OrderBy | None = None
    limit: int | None = None
    p: int | None = 1
    output: OutputFormat | None = OutputFormat.json
    api_key: str | None = None

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
    order_by: AggregatedOrderBy | None = None
