from functools import cache

from countrynames import mappings as country_mappings
from ftm_geocode.nuts import get_nuts_data

from .currency_conversion import CURRENCY_LOOKUP

COUNTRIES = tuple(country_mappings.keys())
CURRENCIES = tuple((*CURRENCY_LOOKUP.values(), "EUR"))
YEARS = tuple(range(2000, 2030))


@cache
def get_nuts_enums():
    df = get_nuts_data()
    df = df[["LEVL_CODE", "NUTS_ID"]]
    return (
        tuple(df[df["LEVL_CODE"] == 1]["NUTS_ID"].unique()),
        tuple(df[df["LEVL_CODE"] == 2]["NUTS_ID"].unique()),
        tuple(df[df["LEVL_CODE"] == 3]["NUTS_ID"].unique()),
    )
