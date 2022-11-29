import re
import secrets
from enum import Enum
from typing import List, Optional, Union

from cachelib import redis
from fastapi import Depends, FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from followthemoney.util import make_entity_id as make_id
from furl import furl
from pydantic import BaseModel, create_model

from farmsubsidy_store import settings, views
from farmsubsidy_store.drivers import get_driver
from farmsubsidy_store.logging import get_logger

CSV = views.OutputFormat.csv
EXPORT = views.OutputFormat.export

optional_auth = HTTPBasic(auto_error=False)
strict_auth = HTTPBasic(auto_error=True)


def get_authenticated(
    credentials: Optional[HTTPBasicCredentials] = Depends(optional_auth),
) -> bool:
    if credentials is None:
        return False
    username, password = settings.API_BASIC_AUTH.split(":")
    return secrets.compare_digest(
        username, credentials.username
    ) and secrets.compare_digest(password, credentials.password)


def require_authenticated(
    credentials: HTTPBasicCredentials = Depends(strict_auth),
) -> bool:
    return get_authenticated(credentials)


origins = [
    "http://localhost:3000",
    settings.ALLOWED_ORIGIN,
]
app = FastAPI(
    title="Farmsubsidy.org API",
    redoc_url="/",
    dependencies=[Depends(get_authenticated)],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["OPTIONS", "GET"],
    allow_headers=["*"],
    allow_credentials=True,
    expose_headers=["*"],
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


if settings.DEBUG:
    # serve csv exports directily, for production use e.g. nginx
    app.mount(
        settings.EXPORT_PUBLIC_PATH,
        StaticFiles(directory=settings.EXPORT_DIRECTORY),
        name="exports",
    )


class ApiResultMeta(BaseModel):
    authenticated: bool = False
    page: int = 1
    item_count: int = 0
    url: str
    export_url: str = None
    query: dict = {}
    next_url: str = None
    prev_url: str = None
    error: str = None
    limit: int = None


class ApiView(views.BaseListView):
    def get(
        self,
        request: Request,
        response: Response,
        is_authenticated: Optional[bool] = False,
        **params,
    ):
        log.info("Auth", authenticated=is_authenticated)
        try:
            query = self.get_query(is_authenticated=is_authenticated, **params)
            df = None

            if self.output_format == EXPORT:
                export_path = self.get_export(**params)
                export_url = furl(request.base_url) / export_path
                return {
                    "authenticated": is_authenticated,
                    "item_count": self.query.count,
                    "limit": self.limit,
                    "url": str(request.url),
                    "query": self.params,
                    "export_url": str(export_url),
                }

            if cache is not None:
                cache_prefix = "csv" if self.output_format == CSV else "view"
                cache_key = make_id(cache_prefix, str(query), int(is_authenticated))
                cached_results = cache.get(cache_key)
                if cached_results:
                    log.info(
                        f"Cache hit for `{cache_key}` ({self.output_format.value})"
                    )
                    return self.ensure_response(request, cached_results)

                if self.output_format == CSV:
                    # maybe we have the query cached already as json
                    cache_key_json = make_id("view", str(query))
                    cached_results = cache.get(cache_key_json)
                    if cached_results:
                        log.info(f"Cache hit for `{cache_key}` (json)")
                        result = self.to_csv(cached_results["results"])
                        cache.set(cache_key, result)
                        return self.ensure_response(request, result)

                # as for filtered queries limiting and sorting (windowing) is
                # as expensive as the whole, we cache the full query for fast
                # api pagination/ordering
                full_query = query._chain(limit=None, offset=None, order_by_fields=None)
                full_cache_key = make_id("precached-window", str(full_query))
                full_cache_skip = make_id("skip", full_cache_key)
                skip = cache.get(full_cache_skip)
                if skip is None:
                    df = cache.get(full_cache_key)
                    if df is not None:
                        log.info(f"Cache hit for `{cache_key}` (precached window)")
                    else:
                        # don't cache too big queries
                        if full_query.count <= 100_000:
                            df = full_query.execute()
                            cache.set(full_cache_key, df)
                        else:
                            cache.set(full_cache_skip, True)

            self.get_results(df, **params)

            # csv response
            if self.output_format == CSV:
                result = self.to_csv(self.data)
                if cache is not None:
                    cache.set(cache_key, result)
                return self.ensure_response(request, result)

            # json response
            results = {
                "authenticated": is_authenticated,
                "page": self.page,
                "limit": self.limit,
                "item_count": self.query.count,
                "url": str(request.url),
                "next_url": self.get_page_url(self.page, request.url, 1)
                if self.has_next
                else None,
                "prev_url": self.get_page_url(self.page, request.url, -1)
                if self.has_prev
                else None,
                "query": self.params,
                "results": [i.dict() for i in self.data],
            }
            if cache is not None:
                cache.set(cache_key, results)
            return results

        except Exception as e:
            response.status_code = 400
            return {
                "error": str(e),
                "url": str(request.url),
                "query": self.params,
                "authenticated": is_authenticated,
            }

    def get_limit(self, limit, **params):
        # allow higher for secret api key (used by nextjs server side calls)
        api_key = params.pop("api_key", "")
        if secrets.compare_digest(api_key, settings.API_KEY):
            return limit
        return super().get_limit(limit)

    def ensure_response(self, request: Request, result: Union[dict, str]) -> dict:
        if self.output_format == CSV:
            return Response(content=result, media_type="text/csv")

        # request urls should always rewritten in returned cached payload as
        # someone could put anything in get parameters (they are ignored by the
        # api, though) that would then be cached and returned to other users as
        # well, this seems not to be a security risk but feels weird
        url = str(request.url)
        result["url"] = url
        if result["next_url"] is not None:
            result["next_url"] = self.get_page_url(result["page"], url, 1)
        if result["prev_url"] is not None:
            result["prev_url"] = self.get_page_url(result["page"], url, -1)
        return result

    @classmethod
    def get_response_model(cls):
        return create_model(
            f"{cls.__name__}{cls.model.__name__}ApiResult",
            results=(List[cls.model], []),
            __base__=ApiResultMeta,
        )

    @classmethod
    def get_page_url(cls, page: int, url: str, change: int):
        new_page = page + change
        url = furl(url)
        url.args["p"] = new_page
        return str(url)

    @classmethod
    def get_params_cls(cls):
        # FIXME
        # fastapi views cannot share same classes for pydantic models !?
        order_by_fields = cls.params_cls.schema()["definitions"]["order_by"]["enum"]
        OrderBy = Enum(
            f"{cls.__name__}{cls.params_cls.__name__}OrderBy",
            ((o, o) for o in order_by_fields),
        )
        return create_model(
            f"{cls.__name__}ViewParams",
            order_by=(OrderBy, None),
            __base__=cls.params_cls,
        )


class PaymentApiView(views.PaymentListView, ApiView):
    endpoint = "/payments"


class RecipientApiView(views.RecipientListView, ApiView):
    endpoint = "/recipients"


class RecipientBaseApiView(views.RecipientBaseView, ApiView):
    endpoint = "/recipients/base"


class RecipientAutocompleteApiView(views.RecipientNameView, ApiView):
    endpoint = "/recipients/autocomplete"


class SchemeApiView(views.SchemeListView, ApiView):
    endpoint = "/schemes"


class CountryApiView(views.CountryListView, ApiView):
    endpoint = "/countries"


class YearApiView(views.YearListView, ApiView):
    endpoint = "/years"


class LocationApiView(views.LocationListView, ApiView):
    endpoint = "/locations"


class AggregationApiView(views.AggregationView, ApiView):
    endpoint = "/agg"


# model views
@app.get(PaymentApiView.endpoint, response_model=PaymentApiView.get_response_model())
async def payments(
    request: Request,
    response: Response,
    commons: PaymentApiView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(get_authenticated),
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
    return PaymentApiView().get(request, response, is_authenticated, **commons.dict())


@app.get(
    RecipientApiView.endpoint, response_model=RecipientApiView.get_response_model()
)
async def recipients(
    request: Request,
    response: Response,
    commons: RecipientApiView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(get_authenticated),
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
      "amount_avg": 6623919.32,
      "amount_max": 18899165.05,
      "amount_min": 25600
    }
    ```
    """
    return RecipientApiView().get(request, response, is_authenticated, **commons.dict())


@app.get(
    RecipientBaseApiView.endpoint,
    response_model=RecipientBaseApiView.get_response_model(),
)
async def recipients_base(
    request: Request,
    response: Response,
    commons: RecipientBaseApiView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(get_authenticated),
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
    return RecipientBaseApiView().get(
        request, response, is_authenticated, **commons.dict()
    )


@app.get(SchemeApiView.endpoint, response_model=SchemeApiView.get_response_model())
async def schemes(
    request: Request,
    response: Response,
    commons: SchemeApiView.get_params_cls() = Depends(),
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
    return SchemeApiView().get(request, response, **commons.dict())


@app.get(CountryApiView.endpoint, response_model=CountryApiView.get_response_model())
async def countries(
    request: Request,
    response: Response,
    commons: CountryApiView.get_params_cls() = Depends(),
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
        2020,
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
    return CountryApiView().get(request, response, **commons.dict())


@app.get(YearApiView.endpoint, response_model=YearApiView.get_response_model())
async def years(
    request: Request,
    response: Response,
    commons: YearApiView.get_params_cls() = Depends(),
):
    """
    Return aggregated values for years based on filter criteria.

    Of course, you can filter by `years` to
    [get a single year](/years?year=2020).

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
      "amount_avg": 3030.08,
      "amount_max": 43400289.79,
      "amount_min": -46089007.35
    }
    ```
    """
    return YearApiView().get(request, response, **commons.dict())


@app.get(LocationApiView.endpoint, response_model=LocationApiView.get_response_model())
async def locations(
    request: Request,
    response: Response,
    commons: LocationApiView.get_params_cls() = Depends(),
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
        2020,
        2015,
        2017,
        2016,
        2019,
        2018,
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
    return LocationApiView().get(request, response, **commons.dict())


@app.get(
    RecipientAutocompleteApiView.endpoint,
    response_model=RecipientAutocompleteApiView.get_response_model(),
)
async def recipients_autocomplete(
    request: Request,
    response: Response,
    commons: RecipientAutocompleteApiView.get_params_cls() = Depends(),
    is_authenticated: bool = Depends(get_authenticated),
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
    return RecipientAutocompleteApiView().get(
        request, response, is_authenticated, **commons.dict()
    )


@app.get(
    AggregationApiView.endpoint,
    response_model=AggregationApiView.get_response_model(),
)
async def aggregation(
    request: Request,
    response: Response,
    commons: AggregationApiView.get_params_cls() = Depends(),
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
    return AggregationApiView().get(request, response, **commons.dict())


# raw sql
def check_query(query: str = ""):
    query = query.replace(";", "").strip("\"' ")
    if re.match(r"^select\s.*\sfrom\sfarmsubsidy.*", query, re.IGNORECASE):
        return query


@app.get("/sql")
async def raw_sql(
    response: Response,
    query: str = Depends(check_query),
    is_authenticated: bool = Depends(require_authenticated),
):
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
                    return Response(content=cached_results, media_type="text/csv")
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


class LoginResponse(BaseModel):
    authenticated: bool = False


@app.get("/login", response_model=LoginResponse)
async def login(
    is_authenticated: bool = Depends(require_authenticated),
    next_url: Optional[str] = None,
):
    """
    Log in via basic auth credentials.
    Optionally redirect to `next_url` (no matter if login is successful or not!)
    Otherwise return authenticated status.
    """
    if next_url is not None:
        return RedirectResponse(url=next_url, status_code=status.HTTP_302_FOUND)
    return {"authenticated": is_authenticated}
