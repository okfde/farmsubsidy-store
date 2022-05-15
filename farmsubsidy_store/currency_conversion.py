import json
import os

from .exceptions import ImproperlyConfigured, InvalidCountry

CURRENCY_LOOKUP = {
    "AT": "ATS",
    "BE": "BEF",
    "BG": "BGN",
    "CY": "CYP",
    "DE": "DEM",
    "EE": "EEK",
    "ES": "ESP",
    "FI": "FIM",
    "FR": "FRF",
    "GR": "GRD",
    "IE": "IEP",
    "IT": "ITL",
    "LT": "LTL",
    "LU": "LUF",
    "LV": "LVL",
    "MT": "MTL",
    "NL": "NLG",
    "PT": "PTE",
    "SI": "SIT",
    "SK": "SKK",
    "SE": "SEK",
    "CZ": "CZK",
    "HU": "HUF",
    "LI": "CHF",
    "PL": "PLN",
    "RO": "RON",
    "DK": "DKK",
    "HR": "HRK",
    "GB": "GBP",
    "UA": "UAH",
}

CURRENCIES = list(CURRENCY_LOOKUP.values()) + ["EUR"]


try:
    with open(os.path.join(os.path.dirname(__file__), "currency_rates.json")) as f:
        CACHED_RATES = {tuple(k.split("|")): v for k, v in json.load(f).items()}
except FileNotFoundError:
    CACHED_RATES = {}


def convert_to_euro(country: str, year: str, amount: float):
    if not CACHED_RATES:
        raise ImproperlyConfigured("currency_rates.json not found")
    try:
        currency = CURRENCY_LOOKUP[country]
    except KeyError:
        raise InvalidCountry(country)
    key = (currency, year)
    rate = CACHED_RATES[key]
    return rate * amount
