from typing import Optional

import countrynames
import pandas as pd
from fingerprints import generate as f
from followthemoney.util import make_entity_id

from .currency_conversion import CURRENCIES, convert_to_euro
from .exceptions import InvalidAmount, InvalidCountry, InvalidCurrency, InvalidSource
from .logging import get_logger
from .util import handle_error

log = get_logger(__name__)


CLEAN_COLUMNS = (
    "country",
    "year",
    "recipient_id",
    "recipient_name",
    "recipient_fingerprint",
    "recipient_address",
    "recipient_country",
    "recipient_url",
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
)


def validate_source(
    df: pd.DataFrame,
    year: Optional[str] = None,
    country: Optional[str] = None,
    fpath: Optional[str] = None,
    do_raise: Optional[bool] = True,
) -> pd.DataFrame:
    """check if source dataframe has at least all required columns"""
    for columns in REQUIRED_SRC_COLUMNS:
        if not df.columns.isin(columns).any():
            handle_error(
                log,
                InvalidSource(f"Not any of columns `{columns}` existent"),
                do_raise,
                fpath=fpath,
                existing_columns=list(df.columns),
            )
    if "year" not in df.columns and year is None:
        handle_error(log, InvalidSource("Year is missing."), do_raise, fpath=fpath)
    if "country" not in df.columns and country is None:
        handle_error(log, InvalidSource("Country is missing."), do_raise, fpath=fpath)
    return df


def clean_source(
    df: pd.DataFrame, year: Optional[str] = None, country: Optional[str] = None
) -> pd.DataFrame:
    """do a bit string cleaning, insert year & country data if not present"""

    def _clean(v):
        if isinstance(v, str):
            return v.strip()
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


def validate_country(value: str, do_raise: bool, fpath: Optional[str] = None) -> str:
    res = countrynames.to_code(value)
    if res is None:
        handle_error(log, InvalidCountry(value), do_raise, value=value, fpath=fpath)
    return res


def clean_recipient_id(row: pd.Series) -> str:
    if row.get("recipient_id"):
        return make_entity_id(row["recipient_id"])
    else:
        return make_entity_id(row["recipient_fingerprint"], row["year"])


def clean_recipient_address(row: pd.Series) -> str:
    if row.get("recipient_address"):
        return row["recipient_address"]
    return " ".join(
        (
            row.get("recipient_location", ""),
            row.get("recipient_postcode", ""),
            row.get("recipient_country", ""),
        )
    )


def clean_recipient_country(
    row: pd.Series, do_raise: bool, fpath: Optional[str] = None
) -> str:
    country = row.get("recipient_country", row.get("country"))
    return validate_country(country, do_raise, fpath=fpath)


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
    )


def clean_scheme_code(row: pd.Series) -> str:
    return row.get("scheme_code_short")


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


def clean_amount(row: pd.Series, do_raise: bool, fpath: Optional[str] = None) -> float:
    """
    Make sure amounts are properly formatted.
    Replace "," with "." in amount strings but raise an error if decimal places are more than 2.
    calculate EUR amount if currency != `EUR`
    """
    currency = row.get("currency_original")
    if currency not in CURRENCIES:
        handle_error(
            log, InvalidCurrency(currency), do_raise, row=dict(row), fpath=fpath
        )

    amount = row.get("amount_original")
    country, year = row["country"], row["year"]
    try:
        amount = to_decimal(amount, allow_empty=False)
        if currency == "EUR":
            return amount
        return convert_to_euro(country, year, amount)
    except ValueError:
        handle_error(log, InvalidAmount(amount), do_raise, row=dict(row), fpath=fpath)
    except InvalidCountry:
        handle_error(
            log,
            InvalidCurrency(f"{country}, {year}"),
            do_raise,
            row=dict(row),
            fpath=fpath,
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
    df: pd.DataFrame, do_raise: bool, fpath: Optional[str] = None
) -> pd.DataFrame:
    funcs = {
        # column: clean_function
        "amount": lambda r: clean_amount(r, do_raise, fpath),
        "recipient_id": clean_recipient_id,
        "recipient_address": clean_recipient_address,
        "recipient_country": lambda r: clean_recipient_country(r, do_raise, fpath),
        "scheme": clean_scheme,
        "scheme_code": clean_scheme_code,
    }

    # safe original amounts before conversion
    # FIXME
    try:
        df["amount_original"] = df["amount"]
        df["currency_original"] = df["currency"]
        df["currency"] = "EUR"
    except Exception as e:
        handle_error(log, e, do_raise, fpath=fpath)

    for col, func in funcs.items():
        df[col] = df.apply(func, axis=1)

    df["amount_original"] = df["amount_original"].map(to_decimal)

    return df


def clean(
    df: pd.DataFrame,
    ignore_errors: bool,
    year: Optional[str] = None,
    country: Optional[str] = None,
    fpath: Optional[str] = None,
) -> pd.DataFrame:
    df = drop_header_rows(df)
    df = df.fillna("")
    df = validate_source(df, year, country, fpath, do_raise=not ignore_errors)
    df = clean_source(df, year, country)
    df["recipient_fingerprint"] = df["recipient_name"].map(f)
    df["country"] = df["country"].map(
        lambda x: validate_country(x, not ignore_errors, fpath)
    )
    df = apply_clean(df, do_raise=not ignore_errors, fpath=fpath)
    df = ensure_columns(df)
    return df
