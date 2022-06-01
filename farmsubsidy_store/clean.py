import uuid
from functools import lru_cache
from typing import Optional

import countrynames
import pandas as pd
from fingerprints import generate as _generate_fingerprint
from followthemoney.util import make_entity_id as _make_entity_id
from normality import normalize

from .currency_conversion import CURRENCIES, CURRENCY_LOOKUP, convert_to_euro
from .exceptions import InvalidAmount, InvalidCountry, InvalidCurrency, InvalidSource
from .logging import get_logger
from .util import clear_lru
from .util import handle_error as _handle_error

log = get_logger(__name__)


def handle_error(log, e, do_raise, **kwargs):
    bulk_errors: Optional[set] = kwargs.pop("bulk_errors", None)
    if bulk_errors is None:
        _handle_error(log, e, do_raise, **kwargs)
        return
    bulk_errors.add(str(e))


UNIQUE = ["year", "country", "recipient_id", "scheme", "amount"]


CLEAN_COLUMNS = (
    "pk",
    "country",
    "year",
    "recipient_id",
    "recipient_name",
    "recipient_fingerprint",
    "recipient_address",
    "recipient_country",
    "recipient_url",
    "scheme_id",
    "scheme",
    "scheme_code",
    "scheme_description",
    "amount",
    "currency",
    "amount_original",
    "currency_original",
)


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


LRU = 1024 * 1000


@lru_cache(LRU)
def generate_fingerprint(*parts):
    return _generate_fingerprint(*parts)


def validate_source(
    df: pd.DataFrame,
    bulk_errors: set,
    year: Optional[str] = None,
    country: Optional[str] = None,
    currency: Optional[str] = None,
    fpath: Optional[str] = None,
    do_raise: Optional[bool] = True,
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
    df: pd.DataFrame, year: Optional[str] = None, country: Optional[str] = None
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


@lru_cache(LRU)
def _get_country_code(value):
    return countrynames.to_code(value)


def validate_country(
    value: str, bulk_errors: set, do_raise: bool, fpath: Optional[str] = None
) -> str:
    res = _get_country_code(value)
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
def clean_recipient_name(name: str):
    """fingerprint is None for empty names or names only with special chars (***)"""
    fp = generate_fingerprint(name)
    if fp is None:
        return
    return name


@lru_cache(LRU)
def make_entity_id(*parts) -> str:
    return _make_entity_id(*parts)


def clean_recipient_id(row: pd.Series) -> str:
    """deduplicate recipients via generated id from country, name, address"""
    name = row["recipient_name"]
    if name is None:
        # anonymous
        name = uuid.uuid4()
    address_fp = generate_fingerprint(row["recipient_address"])
    return make_entity_id(
        row["recipient_country"],
        row["recipient_fingerprint"],
        address_fp or uuid.uuid4(),
    )


@lru_cache(LRU)
def _clean_recipient_address(full_address, *parts) -> str:
    parts = (p.strip() for p in parts if p is not None and p.strip())
    if not full_address:
        return ", ".join(parts)

    parts = (p for p in parts if p not in full_address)
    return ", ".join([full_address, *parts])


def clean_recipient_address(row: pd.Series) -> str:
    parts = [row.get(p) for p in ADDRESS_PARTS]
    full_address = row.get("recipient_address")
    return _clean_recipient_address(full_address, *parts)


def clean_recipient_country(
    row: pd.Series, bulk_errors: set, do_raise: bool, fpath: Optional[str] = None
) -> str:
    country = row.get("recipient_country", row.get("country"))
    return validate_country(country, bulk_errors, do_raise, fpath=fpath)


def clean_scheme(row: pd.Series) -> str:
    scheme = row.get("scheme")
    if scheme is not None:
        return scheme
    return " ".join(
        (
            row.get("scheme_name", ""),
            row.get("scheme_1", ""),
            row.get("scheme_2", ""),
        )
    ).strip()


@lru_cache(LRU)
def clean_scheme_id(scheme: str) -> str:
    return make_entity_id(normalize(scheme))


@lru_cache(LRU)
def to_decimal(value: str, allow_empty: Optional[bool] = True) -> float:
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
    row: pd.Series, bulk_errors: set, do_raise: bool, fpath: Optional[str] = None
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


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in CLEAN_COLUMNS:
        if col not in df:
            df[col] = ""
    return df[list(CLEAN_COLUMNS)]


def drop_header_rows(df):
    """in some source csv there are multiple header rows"""
    df = df[df["recipient_name"] != "recipient_name"]  # FIXME?
    return df


def apply_clean(
    df: pd.DataFrame,
    bulk_errors: set,
    do_raise: bool,
    fpath: Optional[str] = None,
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
        "pk": lambda r: make_entity_id(*[r[k] for k in UNIQUE]),
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

    return df


def clean(
    df: pd.DataFrame,
    ignore_errors: bool,
    year: Optional[str] = None,
    country: Optional[str] = None,
    currency: Optional[str] = None,
    fpath: Optional[str] = None,
) -> pd.DataFrame:
    bulk_errors = set()
    df = drop_header_rows(df)
    df = df.fillna("")
    df = validate_source(
        df, bulk_errors, year, country, currency, fpath, do_raise=not ignore_errors
    )
    if df is not None:
        df = clean_source(df, year, country)
        df["recipient_name"] = df["recipient_name"].map(clean_recipient_name)
        df["recipient_fingerprint"] = df["recipient_name"].map(generate_fingerprint)
        df["country"] = df["country"].map(
            lambda x: validate_country(x, bulk_errors, not ignore_errors, fpath)
        )
        df = apply_clean(df, bulk_errors, do_raise=not ignore_errors, fpath=fpath)
        df = ensure_columns(df)
        df = df.drop_duplicates(subset=("pk",))
    for e in bulk_errors:
        _handle_error(log, e, False, fpath=fpath or "stdin")
    clear_lru()
    return df
