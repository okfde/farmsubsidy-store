import re
from typing import List

from cachelib import redis
from fastapi import Depends, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from followthemoney.util import make_entity_id as make_id
from furl import furl
from pydantic import BaseModel, create_model

from farmsubsidy_store import search, settings, views
from farmsubsidy_store.drivers import get_driver
from farmsubsidy_store.logging import get_logger

app = FastAPI(title="Farmsubsidy.org API", redoc_url="/")

origins = [
    "http://localhost:3000",
    settings.ALLOWED_ORIGIN,
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

log = get_logger(__name__)


if settings.API_CACHE:
    uri = settings.REDIS_URL
    uri = uri.replace("redis://", "")
    host, *port = uri.rsplit(":", 1)
    port = port[0] if len(port) else 6379
    cache = redis.RedisCache(host, port, default_timeout=0, key_prefix="fs-api")
else:
    cache = None


class ApiResultMeta(BaseModel):
    page: int = 1
    item_count: int = 0
    next_url: str = None
    prev_url: str = None


class ApiView(views.BaseListView):
    def get_page_url(self, url: str, change: int):
        new_page = self.page + change
        url = furl(url)
        url.args["p"] = new_page
        return str(url)

    def get(self, request, **params):
        query = self.get_query(**params)

        if cache is not None:
            cache_key = make_id(str(query))
            cached_results = cache.get(cache_key)
            if cached_results:
                log.info(f"Cache hit for `{cache_key}`")
                return cached_results

        self.get_results(**params)
        results = {
            "page": self.page,
            "item_count": self.query.count,
            "next_url": self.get_page_url(request.url, 1) if self.has_next else None,
            "prev_url": self.get_page_url(request.url, -1) if self.has_prev else None,
            "results": [i.dict() for i in self.data],
        }
        if cache is not None:
            cache.set(cache_key, results)
        return results

    @classmethod
    def get_response_model(cls):
        return create_model(
            f"{cls.__name__}{cls.model.__name__}ApiResult",
            results=(List[cls.model], []),
            __base__=ApiResultMeta,
        )


class PaymentApiView(views.PaymentListView, ApiView):
    endpoint = "/payments"


class RecipientApiView(views.RecipientListView, ApiView):
    endpoint = "/recipients"


class RecipientBaseApiView(views.RecipientBaseView, ApiView):
    endpoint = "/recipients/base"


class RecipientSearchApiView(search.RecipientSearchView, ApiView):
    endpoint = "/recipients/search"


class SchemeApiView(views.SchemeListView, ApiView):
    endpoint = "/schems"


class SchemeSearchApiView(search.SchemeSearchView, ApiView):
    endpoint = "/schemes/search"


class CountryApiView(views.CountryListView, ApiView):
    endpoint = "/countries"


class YearApiView(views.YearListView, ApiView):
    endpoint = "/years"


# model views
@app.get(PaymentApiView.endpoint, response_model=PaymentApiView.get_response_model())
async def payments(
    request: Request,
    commons: PaymentApiView.get_params_cls() = Depends(),
):
    """
    Get a list of `Payment` object based on filters.

    Example queries:
    - [All payments for Slovakia in 2020 ordered by amount (descending)](/payments?country=SK&year=2020&order_by=-amount)
    - [All payments related to climate schemes in 2020](/payments?year=2020&scheme__ilike=%climate%)
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
    return PaymentApiView().get(request, **commons.dict())


@app.get(
    RecipientApiView.endpoint, response_model=RecipientApiView.get_response_model()
)
async def recipients(
    request: Request,
    commons: RecipientApiView.get_params_cls() = Depends(),
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
      "country": [
        "DE"
      ],
      "url": [],
      "years": [
        2020,
        2015,
        2017,
        2019,
        2016,
        2018,
        2014
      ],
      "total_payments": 13,
      "amount_sum": 86110951.26,
      "amount_avg": 6623919.327692308,
      "amount_max": 18899165.05,
      "amount_min": 25600
    }
    ```
    """
    return RecipientApiView().get(request, **commons.dict())


@app.get(
    RecipientBaseApiView.endpoint,
    response_model=RecipientBaseApiView.get_response_model(),
)
async def recipients_base(
    request: Request,
    commons: RecipientBaseApiView.get_params_cls() = Depends(),
):
    """
    A stripped down version of `Recipients` but only returning recipients `id`,
    `total_payments` and the aggregated values for `amount`.

    This is faster much faster as the api endpoint with the full data.

    This is useful to get a list of ids for recipients to query for more
    metadata (names, ...) in subsequent calls.

    Although in the returned objects the "string fields" name, address, scheme, ...
    are missing, they are still filterable (see query parameters below)

    Example return data for `RecipientBase` model:

    ```json
    {
      "id": "e03c4b6034def644b096a70148e9cdeaa25f8702",
      "total_payments": 13,
      "amount_sum": 86110951.26,
      "amount_avg": 6623919.327692308,
      "amount_max": 18899165.05,
      "amount_min": 25600
    }
    ```
    """
    return RecipientBaseApiView().get(request, **commons.dict())


@app.get(CountryApiView.endpoint, response_model=CountryApiView.get_response_model())
async def countries(
    request: Request,
    commons: CountryApiView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for countries based on filter criteria.

    Of course, you can filter by `country` to
    [get a single country](http://localhost:8000/countries?country=SK).

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
        2020,
        2015,
        2017,
        2016,
        2019,
        2014
      ],
      "amount_sum": 3650826849.16,
      "amount_avg": 13784.299459928867,
      "amount_max": 19038160,
      "amount_min": -430471.33
    }
    ```
    """
    return CountryApiView().get(request, **commons.dict())


@app.get(YearApiView.endpoint, response_model=YearApiView.get_response_model())
async def years(
    request: Request,
    commons: YearApiView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for years based on filter criteria.

    Of course, you can filter by `years` to
    [get a single years](http://localhost:8000/years?year=2020).

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
      "year": 2020,
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
      "amount_avg": 3030.084227294196,
      "amount_max": 43400289.79,
      "amount_min": -46089007.35
    }
    ```
    """
    return YearApiView().get(request, **commons.dict())


# search views
@app.get(
    RecipientSearchApiView.endpoint,
    response_model=RecipientSearchApiView.get_response_model(),
)
async def recipients_search(
    request: Request,
    commons: RecipientSearchApiView.get_params_cls() = Depends(),
):
    """
    Search for recipients by search strings and additional filters.

    Search is always case insensitive.

    The `q` parameter for searching recipient names works like the sql "LIKE" clause,
    so to search for a recipient with a name *containing* "nestle", the search string
    (`q`) would be "%nestle%"

    The returned objects are `Payment` instances as described above.
    """
    return RecipientSearchApiView().get(request, **commons.dict())


@app.get(
    SchemeSearchApiView.endpoint,
    response_model=SchemeSearchApiView.get_response_model(),
)
async def scheme_search(
    request: Request,
    commons: SchemeSearchApiView.get_params_cls() = Depends(),
):
    """
    Search for schemes by search strings and additional filters.

    Search is always case insensitive.

    The `q` parameter for searching schemes works like the sql "LIKE" clause,
    so e.g. to search for the EU scheme starting with "I/V.5", the search string
    (`q`) would be "I/V.5%"

    The returned objects are `Scheme` instances as described above.
    """
    return SchemeSearchApiView().get(request, **commons.dict())


# raw sql
def check_query(query: str = ""):
    query = query.replace(";", "").strip("\"' ")
    if re.match(r"^select\s.*\sfrom\sfarmsubsidy.*", query, re.IGNORECASE):
        return query


@app.get("/sql")
async def raw_sql(response: Response, query: str = Depends(check_query)):
    """
    Execute raw sql queries. Returns csv data.

    Only "SELECT ..." queries are allowed.
    """
    if query is not None:
        try:
            if cache is not None:
                cache_key = make_id("raw-sql", str(query))
                cached_results = cache.get(cache_key)
                if cached_results:
                    log.info(f"Cache hit for `{cache_key}`")
                    return cached_results
            driver = get_driver()
            df = driver.query(query)
            data = df.to_csv(index=False)
            if cache is not None:
                cache.set(cache_key, data)
            return Response(content=data, media_type="text/csv")
        except Exception:
            response.status_code = 503
            return {"error": "Server error"}
    response.status_code = 400
    return {"error": "Invalid query"}
