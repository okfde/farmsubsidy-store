import re

from fastapi import Depends, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from followthemoney.util import make_entity_id as make_id

from farmsubsidy_store import settings
from farmsubsidy_store.drivers import get_driver
from farmsubsidy_store.logging import get_logger

from . import auth
from .cache import cache
from .params import OutputFormat
from .util import to_csv
from .views import (
    AggregationView,
    CountryView,
    LocationView,
    Nuts1View,
    Nuts2View,
    Nuts3View,
    PaymentView,
    RecipientBaseView,
    RecipientNameView,
    RecipientView,
    SchemeView,
    YearView,
)

CSV = OutputFormat.csv
EXPORT = OutputFormat.export


origins = [
    "http://localhost:3000",
    settings.ALLOWED_ORIGIN,
]
app = FastAPI(
    debug=settings.DEBUG,
    version=settings.VERSION,
    title=settings.API_TITLE,
    contact=settings.API_CONTACT,
    description=settings.API_DESCRIPTION,
    redoc_url="/",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["OPTIONS", "GET"],
    allow_headers=["Authorization"],
    allow_credentials=True,
)

log = get_logger(__name__)


if settings.DEBUG:
    # serve csv exports directily, for production use e.g. nginx
    app.mount(
        settings.EXPORT_PUBLIC_PATH,
        StaticFiles(directory=settings.EXPORT_DIRECTORY),
        name="exports",
    )


@app.get("/payments", response_model=PaymentView.get_response_model())
async def payments(
    request: Request,
    response: Response,
    params: PaymentView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(auth.get_authenticated),
):
    """
    Get a list of `Payment` object based on filters.

    Example queries:
    - [All payments for Slovakia in 2022 ordered by amount (descending)](/payments?country=SK&year=2022&order_by=-amount)
    - [All payments related to climate schemes in 2022](/payments?year=2022&scheme__ilike=%climate%)
    - [Get all payments for a specific recipient via `recipient_id`](/payments?recipient_id=2f5812af62c20b884ed5dbddbacaaba362525110)

    Example return data for `Payment` model:

    ```json
    {
      "pk": "9ed2157fe62b2c7d6cacee8a8bd018679dc5b404",
      "country": "HU",
      "year": 2014,
      "recipient_id": "2f5812af62c20b884ed5dbddbacaaba362525110",
      "recipient_name": "ATEV Fehérjefeldolgozó  Zrt.",
      "recipient_fingerprint": "atev feherjefeldolgozo zrt",
      "recipient_address": "Illatos út 23., Budapest, 1097, HU",
      "recipient_country": "HU",
      "recipient_url": null,
      "scheme": "Állati hulla ártalmatlanítása (Nemzeti, Nemzeti)",
      "scheme_code": null,
      "scheme_description": null,
      "amount": 6759434.1,
      "currency": "EUR",
      "amount_original": 2005766797,
      "currency_original": "HUF"
    }
    ```
    """
    view = PaymentView(params, is_authenticated)
    return view.get(request, response)


@app.get("/recipients", response_model=RecipientView.get_response_model())
async def recipients(
    request: Request,
    response: Response,
    params: RecipientView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(auth.get_authenticated),
):
    """
    Get a list of `Recipient` objects based on filters.

    There are two kinds of filters:

    1. fitler criteria on the payments of individual recipients
    2. filter criteria on the payments aggegration on  each recipient


    The payments are filtered **before** the numbers for recipients
    are aggregated, so if you filter for specific payment criteria,
    the `amount_sum`, `amount_avg`... properties fo the returned recipients
    are the aggregations for the filtered subset of their payments, not for
    all their payments.

    Example:

    **To get recipients that have single payments with at least 100.000 €:**

    `amount__gte=100000` *(the returned numbers are aggregated on these filtered payments)*

    **To get recipients that have received at least 100.000 €:**

    `amount_sum__gte=100000` *(Here the aggregated numbers are on all of the recipients payments)*

    **Search for recipients**

    The `recipient_fingerprint` field is a normalized value of the recipient's name.
    Lookups via the sql `LIKE` clause are possible via: (no need for `ILIKE` as it's always case insensitive)

    This applies to all other api views that can filter by `recipient_fingerprint`

    `recipient_fingerprint__like=%nestle%`

    Example return data for `Recipient` model:

    ```json
    {
      "id": "e03c4b6034def644b096a70148e9cdeaa25f8702",
      "name": [
        "Landesbetr. f.Hochwasserschutz u. Wasserwirtsch. (LHW)"
      ],
      "address": [
        "Magdeburg, Landeshauptstadt, DE-39104, DE"
      ],
      "country": "DE",
      "url": [],
      "years": [
        2022,
        2015,
        2017,
        2019,
        2016,
        2018,
        2014
      ],
      "total_payments": 13,
      "amount_sum": 86110951.26,
      "amount_avg": 6623919.32,
      "amount_max": 18899165.05,
      "amount_min": 25600
    }
    ```
    """
    view = RecipientView(params, is_authenticated)
    return view.get(request, response)


@app.get("/recipients/base", response_model=RecipientBaseView.get_response_model())
async def recipients_base(
    request: Request,
    response: Response,
    params: RecipientBaseView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(auth.get_authenticated),
):
    """
    A stripped down version of `Recipients` but only returning recipients `id`,
    `total_payments` and the aggregated values for `amount`.

    This is faster much faster as the api endpoint with the full data.

    This is useful to get a list of ids for recipients to query for more
    metadata (names, ...) in subsequent calls.

    Although in the returned objects the "string fields" name, address, scheme, ...
    are missing, they are still filterable (see query parameters below), e.g. searching
    via `recipient_fingerprint` (see example above)

    Example return data for `RecipientBase` model:

    ```json
    {
      "id": "e03c4b6034def644b096a70148e9cdeaa25f8702",
      "total_payments": 13,
      "amount_sum": 86110951.26,
      "amount_avg": 6623919.32,
      "amount_max": 18899165.05,
      "amount_min": 25600
    }
    ```
    """
    view = RecipientBaseView(params, is_authenticated)
    return view.get(request, response)


@app.get("/schemes", response_model=SchemeView.get_response_model())
async def schemes(
    request: Request,
    response: Response,
    params: SchemeView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for schemes based on filter criteria.

    Currently, schemes are not very clean (aka de-duplicated) across countries.

    As well they mostly lack more detailed descriptions.

    ```json
    {
      "scheme": "\"FEGA\" \"Plata unica pe suprafata - R.73/09, art.122\"",
      "years": [
        2014
      ],
      "countries": [
        "RO"
      ],
      "total_payments": 753800,
      "total_recipients": 713239,
      "amount_sum": 941990883.68,
      "amount_avg": 1249.66,
      "amount_max": 6980579.2,
      "amount_min": 0.04
    }
    ```
    """
    view = SchemeView(params)
    return view.get(request, response)


@app.get("/countries", response_model=CountryView.get_response_model())
async def countries(
    request: Request,
    response: Response,
    params: CountryView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for countries based on filter criteria.

    Of course, you can filter by `country` to
    [get a single country](/countries?country=SK).

    There are two kinds of filters:

    1. fitler criteria on the payments of individual recipients
    2. filter criteria on the payments aggegration on each country

    The payments are filtered **before** the numbers for countries
    are aggregated, so if you filter for specific payment criteria,
    the `amount_sum`, `amount_avg`... properties fo the returned countries
    are the aggregations for the filtered subset of their payments, not for
    all their payments.

    Example return data for `Country` model:

    ```json
    {
      "country": "SK",
      "total_recipients": 30276,
      "total_payments": 264854,
      "years": [
        2022,
        2015,
        2017,
        2016,
        2019,
        2014
      ],
      "amount_sum": 3650826849.16,
      "amount_avg": 13784.29,
      "amount_max": 19038160,
      "amount_min": -430471.33
    }
    ```
    """
    view = CountryView(params)
    return view.get(request, response)


@app.get("/years", response_model=YearView.get_response_model())
async def years(
    request: Request,
    response: Response,
    params: YearView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for years based on filter criteria.

    Of course, you can filter by `years` to
    [get a single year](/years?year=2022).

    There are two kinds of filters:

    1. fitler criteria on the payments of individual recipients
    2. filter criteria on the payments aggegration on each year

    The payments are filtered **before** the numbers for years
    are aggregated, so if you filter for specific payment criteria,
    the `amount_sum`, `amount_avg`... properties fo the returned years
    are the aggregations for the filtered subset of their payments, not for
    all their payments.

    Example return data for `Year` model:

    ```json
    {
      "year": 2022,
      "total_recipients": 4300168,
      "total_payments": 18301312,
      "countries": [
        "SK",
        "IE",
        "PT",
        "DE",
        "SE",
        "GB",
        "CY",
        "MT",
        "FR",
        "DK",
        "LU",
        "CZ",
        "BE",
        "LT",
        "HR",
        "IT",
        "GR",
        "RO",
        "NL",
        "AT",
        "BG",
        "SI",
        "PL",
        "ES",
        "EE",
        "LV"
      ],
      "amount_sum": 55454516829.99,
      "amount_avg": 3030.08,
      "amount_max": 43400289.79,
      "amount_min": -46089007.35
    }
    ```
    """
    view = YearView(params)
    return view.get(request, response)


@app.get("/locations", response_model=LocationView.get_response_model())
async def locations(
    request: Request,
    response: Response,
    params: LocationView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for locations (`recipient_address`) based on
    filter criteria.

    Of course, you can filter by `recipient_address` to
    [get a single location](/locations?recipient_address="Sevilla, Sevilla, 41091, ES").

    There are two kinds of filters:

    1. fitler criteria on the payments of individual recipients
    2. filter criteria on the payments aggegration on each location

    The payments are filtered **before** the numbers for locations
    are aggregated, so if you filter for specific payment criteria,
    the `amount_sum`, `amount_avg`... properties fo the returned locations
    are the aggregations for the filtered subset of their payments, not for
    all their payments.

    Example return data for `Location` model:

    ```json
    {
      "location": "ADMONT, 8913, AT",
      "years": [
        2022,
        2021,
        2020,
        2019,
        2018,
        2017,
        2016,
        2015,
        2014
      ],
      "countries": [
        "AT"
      ],
      "total_recipients": 37,
      "total_payments": 543,
      "amount_sum": 4861766.56,
      "amount_avg": 8953.53,
      "amount_max": 754827.8,
      "amount_min": -667.9
    }
    ```
    """
    view = LocationView(params)
    return view.get(request, response)


@app.get("/nuts1", response_model=Nuts1View.get_response_model())
async def nuts1(
    request: Request, response: Response, params: Nuts1View.get_params_cls() = Depends()
):
    """
    EU NUTS1 regions
    """
    view = Nuts1View(params)
    return view.get(request, response)


@app.get("/nuts2", response_model=Nuts2View.get_response_model())
async def nuts2(
    request: Request, response: Response, params: Nuts2View.get_params_cls() = Depends()
):
    """
    EU NUTS2 regions
    """
    view = Nuts2View(params)
    return view.get(request, response)


@app.get("/nuts3", response_model=Nuts3View.get_response_model())
async def nuts3(
    request: Request, response: Response, params: Nuts3View.get_params_cls() = Depends()
):
    """
    EU NUTS3 regions
    """
    view = Nuts3View(params)
    return view.get(request, response)


@app.get(
    "/recipients/autocomplete", response_model=RecipientNameView.get_response_model()
)
async def recipients_autocomplete(
    request: Request,
    response: Response,
    params: RecipientNameView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(auth.get_authenticated),
):
    """
    Search for recipient names and get first matching results
    for autocompleting purposes

    Lookup by parameter `recipient_name__ilike='foo%'`

    Optional filtering by common filters (see below)

    Example result:

    ```json
    {
      "id": "7907b1ad3ff464b302715d4f7a90b3c938a916bc",
      "name": "Jansen, Sven Olaf",
      "country": "DE"
    }
    ```
    """
    view = RecipientNameView(params, is_authenticated)
    return view.get(request, response)


@app.get("/agg", response_model=AggregationView.get_response_model())
async def aggregation(
    request: Request,
    response: Response,
    params: AggregationView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(auth.get_authenticated),
):
    """
    Aggregate numbers for whatever filter criterion

    The returned results list contains always 1 result (the aggregation)

    Example result for `Aggregation` model:

    ```json
    {
      "total_recipients": 12092563,
      "total_payments": 90437860,
      "amount_sum": 371207322754.9,
      "amount_avg": 4108.96,
      "amount_max": 131798746.58,
      "amount_min": -214470170.97
    }
    ```
    """
    view = AggregationView(params, is_authenticated)
    return view.get(request, response)


# raw sql
def check_query(query: str = ""):
    query = query.replace(";", "").strip("\"' ")
    if re.match(r"^select\s.*\sfrom\sfarmsubsidy.*", query, re.IGNORECASE):
        return query


@app.get("/sql")
async def raw_sql(
    response: Response,
    query: str = Depends(check_query),
    is_authenticated: bool = Depends(auth.require_authenticated),
):
    """
    Execute raw sql queries. Returns csv data.

    Only "SELECT ..." queries are allowed.
    """
    if query is not None:
        try:
            cache_key = make_id("raw-sql", str(query))
            cached_results = cache.get(cache_key)
            if cached_results:
                return Response(content=cached_results, media_type="text/csv")
            driver = get_driver()
            data = to_csv(driver.query(query))
            cache.set(cache_key, data)
            return Response(content=data, media_type="text/csv")
        except Exception:
            response.status_code = 500
            return {"error": "Server error"}
    response.status_code = 400
    return {"error": "Invalid query"}


@app.get("/token", response_model=auth.Token)
async def login_token(
    credentials: auth.HTTPBasicCredentials = Depends(auth.basic_auth),
):
    """
    Send username & password via `Authorization` header (basic auth) and
    retrieve a [JWT](https://jwt.io/) token to use for the api.
    """
    return auth.login_for_access_token(credentials)


@app.get("/authenticated", response_model=auth.Authenticated)
async def auth_status(is_authenticated: bool = Depends(auth.get_authenticated)):
    return auth.Authenticated(status=is_authenticated)
