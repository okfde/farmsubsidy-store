import re
from functools import lru_cache

import pandas as pd
import shortuuid
from fingerprints import generate as _generate_fingerprint
from followthemoney.util import make_entity_id as _make_entity_id
from ftm_geocode.cache import cache
from normality import collapse_spaces, normalize
from pydantic import BaseModel

from .currency_conversion import CURRENCIES, CURRENCY_LOOKUP, convert_to_euro
from .exceptions import InvalidAmount, InvalidCountry, InvalidCurrency, InvalidSource
from .logging import get_logger
from .schemes import guess_scheme
from .util import clear_lru, get_country_code, get_country_name
from .util import handle_error as _handle_error

log = get_logger(__name__)


def handle_error(log, e, do_raise, **kwargs):
    bulk_errors: set | None = kwargs.pop("bulk_errors", None)
    if bulk_errors is None:
        _handle_error(log, e, do_raise, **kwargs)
        return
    bulk_errors.add(str(e))


UNIQUE = ["year", "country", "recipient_id", "scheme_id", "amount"]


class Row(BaseModel):
    pk: str
    country: str
    year: int
    recipient_id: str
    recipient_name: str
    recipient_fingerprint: str
    recipient_address: str | None = None
    recipient_country: str | None = None
    recipient_url: str | None = None
    scheme_id: str | None = None
    scheme: str | None = None
    scheme_code: str | None = None
    scheme_description: str | None = None
    amount: float
    currency: str
    amount_original: float | None = None
    currency_original: str | None = None
    nuts: str | None = None
    nuts1: str | None = None
    nuts2: str | None = None
    nuts3: str | None = None


REQUIRED_SRC_COLUMNS = (
    # column name, *alternatives
    ("recipient_name",),
    ("recipient_country", "country"),
    # (
    #     "scheme",
    #     "scheme_name",
    #     "scheme_1",
    #     "scheme_2",
    #     "scheme_code_short",
    #     "scheme_description",
    # ),
    ("amount",),
    ("currency",),
    ("year",),
)


ADDRESS_PARTS = (
    "recipient_street",
    "recipient_street1",
    "recipient_street2",
    "recipient_location",
    "recipient_postcode",
    "recipient_country",
)


LRU = 1_000_000


ANONYMOUS = (
    "**",
    "--",
    "private individual",
    "nije primjenjivo",
)


@lru_cache(LRU)
def generate_fingerprint(*parts):
    return _generate_fingerprint(*parts)


def validate_source(
    df: pd.DataFrame,
    bulk_errors: set,
    year: str | None = None,
    country: str | None = None,
    currency: str | None = None,
    fpath: str | None = None,
    do_raise: bool | None = True,
) -> pd.DataFrame:
    """check if source dataframe has at least all required columns"""
    if "year" not in df:
        if year is None:
            handle_error(
                log,
                InvalidSource("Year is missing."),
                do_raise,
                fpath=fpath,
                bulk_errors=bulk_errors,
            )
            return
        df["year"] = year
    if "country" not in df:
        if country is None:
            handle_error(
                log,
                InvalidSource("Country is missing."),
                do_raise,
                fpath=fpath,
                bulk_errors=bulk_errors,
            )
            return
        df["country"] = country
    if "currency" not in df and currency is not None:
        df["currency"] = currency
    for columns in REQUIRED_SRC_COLUMNS:
        if not df.columns.isin(columns).any():
            handle_error(
                log,
                InvalidSource(f"Not any of columns `{columns}` existent"),
                do_raise,
                bulk_errors=bulk_errors,
                fpath=fpath,
                existing_columns=list(df.columns),
            )
    return df


def clean_source(
    df: pd.DataFrame, year: str | None = None, country: str | None = None
) -> pd.DataFrame:
    """do a bit string cleaning, insert year & country data if not present"""

    @lru_cache(LRU)
    def _clean(v):
        if isinstance(v, str):
            return v.strip("\"' ',").strip()
        return v

    if "year" not in df:
        df["year"] = year
    else:
        df["year"] = df["year"].fillna(year).map(lambda x: x or year)

    if "country" not in df:
        df["country"] = country
    else:
        df["country"] = df["country"].fillna(country).map(lambda x: x or country)

    return df.applymap(_clean)


def validate_country(
    value: str, bulk_errors: set, do_raise: bool, fpath: str | None = None
) -> str:
    res = get_country_code(value)
    if res is None:
        handle_error(
            log,
            InvalidCountry(value),
            do_raise,
            value=value,
            fpath=fpath,
            bulk_errors=bulk_errors,
        )
    return res


@lru_cache(LRU)
def _test_name(name: str) -> bool:
    name = name.lower()
    for test in ANONYMOUS:
        if test in name:
            return False
    return generate_fingerprint(name)


@lru_cache(LRU)
def _clean_recipient_name(
    name: str, country: str, ident: str | None = None
) -> str | None:
    if _test_name(name):
        return collapse_spaces(name)
    if generate_fingerprint(ident):
        return " ".join((country, ident))


# don't cache this call!
def clean_recipient_name(row: pd.Series) -> str:
    """generate a cleaned name or, if empty, take a original id (if any) or do a random one"""
    name = row["recipient_name"]
    country = row["country"]
    ident = row.get("recipient_id")  # here we have the original id from source, if any
    name = _clean_recipient_name(name, country, ident)
    if name is not None:
        return name
    return f"Anonymous {shortuuid.uuid()}"


@lru_cache(LRU)
def make_entity_id(*parts) -> str:
    return _make_entity_id(*parts)


def clean_recipient_id(row: pd.Series) -> str:
    """deduplicate recipients via generated id from country, name, address"""
    fp = row["recipient_fingerprint"]
    assert fp is not None and fp != "", dict(row)
    return make_entity_id(
        row["recipient_country"],
        row["recipient_fingerprint"],
        generate_fingerprint(row["recipient_address"]) or shortuuid.uuid(),
    )


address_xpath_pat = re.compile(r".*data='([\w\.\s]+)'.*")
address_postcode_pat = re.compile(r".*\s(\d+\.\d).*")


@lru_cache(LRU)
def _clean_recipient_address(full_address, *parts, country: str) -> str:
    parts = (p.strip() for p in parts if p is not None and p.strip())
    if not full_address:
        full_address = ", ".join(parts)
    else:
        parts = (p for p in parts if p not in full_address)
        full_address = ", ".join([full_address, *parts])

    full_address = full_address.rstrip(", " + country)
    # empty address
    if generate_fingerprint(full_address) is None:
        return get_country_name(country)
    # wtf
    m = re.match(address_postcode_pat, full_address)
    if m is not None:
        res = m.groups()[0]
        full_address = full_address.replace(res, res.split(".", 1)[0])
    if "xpath" in full_address:
        m = re.match(address_xpath_pat, full_address)
        if m is not None:
            full_address = m.groups()[0]
    return full_address


def clean_recipient_address(row: pd.Series) -> str:
    parts = [row.get(p) for p in ADDRESS_PARTS]
    full_address = row.get("recipient_address")
    return _clean_recipient_address(
        full_address, *parts, country=row["recipient_country"]
    )


@lru_cache(LRU)
def get_nuts(address_line: str, country: str) -> dict[str, str | None]:
    nuts: dict[str, str | None] = {}
    address = cache.get(address_line, country=country)
    if address:
        nuts["nuts1"] = address.nuts1_id
        nuts["nuts2"] = address.nuts2_id
        nuts["nuts3"] = address.nuts3_id
    return nuts


def apply_nuts(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {**r, **get_nuts(r["recipient_address"], r["country"])}
        for _, r in df.iterrows()
    )


def clean_recipient_country(
    row: pd.Series, bulk_errors: set, do_raise: bool, fpath: str | None = None
) -> str:
    country = row.get("recipient_country", row.get("country"))
    return validate_country(country, bulk_errors, do_raise, fpath=fpath)


def clean_scheme(row: pd.Series) -> str:
    scheme = row.get("scheme")
    if scheme is None:
        scheme = " ".join(
            (
                row.get("scheme_name", ""),
                row.get("scheme_1", ""),
                row.get("scheme_2", ""),
            )
        ).strip()
    scheme = collapse_spaces(scheme)
    return guess_scheme(scheme)


@lru_cache(LRU)
def clean_scheme_id(scheme: str) -> str:
    return make_entity_id(normalize(scheme))


@lru_cache(LRU)
def to_decimal(value: str, allow_empty: bool | None = True) -> float:
    if value is None and not allow_empty:
        raise ValueError
    if "," in value:
        if len(value.split(",")[-1]) > 2:
            raise ValueError
        value = value.replace(",", ".")
    value = pd.to_numeric(value)
    if value is None and not allow_empty:
        raise ValueError
    return value


def clean_amount(
    row: pd.Series, bulk_errors: set, do_raise: bool, fpath: str | None = None
) -> float:
    """
    Make sure amounts are properly formatted.  Replace "," with "." in amount
    strings but raise an error if decimal places are more than 2.  calculate
    EUR amount if currency != `EUR`
    """
    country, year = row["country"], row["year"]
    currency = row.get("currency_original", CURRENCY_LOOKUP.get(country))
    if currency not in CURRENCIES:
        handle_error(
            log,
            InvalidCurrency(currency),
            do_raise,
            row=dict(row),
            fpath=fpath,
            bulk_errors=bulk_errors,
        )

    amount = row.get("amount_original")
    try:
        amount = to_decimal(amount, allow_empty=False)
        if currency == "EUR":
            return amount
        return convert_to_euro(country, year, amount)
    except ValueError:
        handle_error(
            log,
            InvalidAmount(amount),
            do_raise,
            row=dict(row),
            fpath=fpath,
            bulk_errors=bulk_errors,
        )
    except KeyError as e:
        handle_error(
            log,
            InvalidCurrency(f"No currency conversion for `{e}`"),
            do_raise,
            row=dict(row),
            year=year,
            country=country,
            fpath=fpath,
            bulk_errors=bulk_errors,
        )
    except InvalidCountry as e:
        handle_error(
            log, e, do_raise, row=dict(row), fpath=fpath, bulk_errors=bulk_errors
        )


def drop_header_rows(df):
    """in some source csv there are multiple header rows"""
    df = df[df["recipient_name"] != "recipient_name"]  # FIXME?
    return df


def apply_clean(
    df: pd.DataFrame,
    bulk_errors: set,
    do_raise: bool,
    fpath: str | None = None,
) -> pd.DataFrame:

    funcs = {
        # column: clean_function
        "amount": lambda r: clean_amount(r, bulk_errors, do_raise, fpath),
        "recipient_country": lambda r: clean_recipient_country(
            r, bulk_errors, do_raise, fpath
        ),
        "recipient_address": clean_recipient_address,
        "recipient_id": clean_recipient_id,
        "scheme": clean_scheme,
    }

    # safe original amounts before conversion
    # FIXME
    try:
        df["amount_original"] = df["amount"]
        df["currency_original"] = df["currency"]
        df["currency"] = "EUR"
    except Exception as e:
        handle_error(log, e, do_raise, fpath=fpath, bulk_errors=bulk_errors)

    for col, func in funcs.items():
        df[col] = df.apply(func, axis=1)

    if "schem_code_short" in df:
        df["scheme_code"] = df["scheme_code_short"]
    else:
        df["scheme_code"] = None

    df["scheme_id"] = df["scheme"].map(clean_scheme_id)

    df["amount_original"] = df["amount_original"].map(to_decimal)

    df["pk"] = df.apply(lambda r: make_entity_id(*[r[k] for k in UNIQUE]), axis=1)

    return df


def clean(
    df: pd.DataFrame,
    ignore_errors: bool,
    year: str | None = None,
    country: str | None = None,
    currency: str | None = None,
    fpath: str | None = None,
) -> pd.DataFrame:
    bulk_errors = set()
    df = drop_header_rows(df)
    df = df.fillna("")
    df = validate_source(
        df, bulk_errors, year, country, currency, fpath, do_raise=not ignore_errors
    )
    if df is not None:
        df = clean_source(df, year, country)
        df["country"] = df["country"].map(
            lambda x: validate_country(x, bulk_errors, not ignore_errors, fpath)
        )
        df["recipient_name"] = df.apply(clean_recipient_name, axis=1)
        df["recipient_fingerprint"] = df["recipient_name"].map(generate_fingerprint)
        df = apply_clean(df, bulk_errors, do_raise=not ignore_errors, fpath=fpath)
        df = apply_nuts(df)
        # validate FIXME performance
        df = pd.DataFrame(Row(**r).dict() for _, r in df.fillna("").iterrows())
        df = df.drop_duplicates(subset=("pk",))
    for e in bulk_errors:
        _handle_error(log, e, False, fpath=fpath or "stdin")
    clear_lru()
    return df
