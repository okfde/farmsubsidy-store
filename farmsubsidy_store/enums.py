from countrynames import mappings as country_mappings
from .currency_conversion import CURRENCY_LOOKUP


COUNTRIES = tuple(country_mappings.keys())
CURRENCIES = tuple((*CURRENCY_LOOKUP.values(), "EUR"))
YEARS = tuple(range(2000, 2030))
