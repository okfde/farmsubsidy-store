"""Paginated and cached model views for the api"""

import os
import secrets
from enum import Enum
from functools import cached_property
from typing import Any

import pandas as pd
from banal import clean_dict
from fastapi import Request, Response
from followthemoney.util import make_entity_id as make_id
from furl import furl
from pydantic import BaseModel, create_model

from farmsubsidy_store import model as models
from farmsubsidy_store import settings
from farmsubsidy_store.logging import get_logger
from farmsubsidy_store.query import Query

from .cache import cache
from .params import (
    AggregatedFieldsParams,
    AggregatedViewParams,
    BaseFieldsParams,
    BaseViewParams,
    OutputFormat,
)
from .util import get_page_url, get_slice, to_csv

log = get_logger(__name__)


class ApiResult(BaseModel):
    authenticated: bool = False
    url: str
    page: int = 1
    limit: int = None
    item_count: int = 0
    export_url: str = None
    query: dict = {}
    next_url: str = None
    prev_url: str = None
    error: str = None
    results: list[dict[str, Any]] | None = []


class BaseView:
    max_limit = 1000
    model = None
    params_cls = BaseViewParams

    def __init__(self, params: BaseViewParams, is_authenticated: bool | None = False):
        self.params = params
        self.is_authenticated = is_authenticated
        self.ensure_limit()

    def get(self, request: Request, response: Response) -> ApiResult:
        log.debug("Auth", authenticated=self.is_authenticated)
        result = ApiResult(
            authenticated=self.is_authenticated,
            url=str(request.url),
            query=clean_dict(self.params.dict()),
            item_count=self.query.count,
            limit=self.params.limit,
            page=self.params.p,
        )
        try:
            result_data = self.get_data()
        except Exception as e:
            raise e
            response.status_code = 400
            result.error = str(e)
            return result
        if self.params.output == OutputFormat.csv:
            return Response(content=result_data, media_type="text/csv")
        if self.params.output == OutputFormat.export:
            result.export_url = str(furl(request.base_url) / result_data)
            return result

        # normal json response
        has_next = (
            (self.query.count >= self.params.p * self.params.limit)
            if self.params.limit is not None
            else False
        )
        has_prev = self.params.p > 1
        result.next_url = (
            get_page_url(self.params.p, request.url, 1) if has_next else None
        )
        result.prev_url = (
            get_page_url(self.params.p, request.url, -1) if has_prev else None
        )
        result.results = [i.dict() for i in result_data]
        return result

    def get_data(self) -> Any:
        """
        try to get results from cache or compute new results, store in cache and return
        """
        if self.params.output == OutputFormat.export:
            return self.get_export_path()

        cached_results = self.get_results_from_cache()
        if cached_results is not None:
            return cached_results

        result = list(self.query)
        cache.set(self.get_cache_key(), result)
        return result

    def get_query(self) -> Query:
        query = (
            self.model.select().where(**self.where_params).having(**self.having_params)
        )

        if self.params.order_by is not None:
            order_by = self.params.order_by
            ascending = True
            if order_by.startswith("-"):
                order_by = order_by[1:]
                ascending = False
            query = query.order_by(order_by, ascending=ascending)
        start, end = get_slice(self.params.p, self.params.limit)
        return query[start:end]

    def ensure_limit(self) -> None:
        # allow higher for secret api key (used by nextjs build calls or export)
        if not secrets.compare_digest(self.params.api_key or "", settings.API_KEY):
            if self.is_authenticated and self.params.output in (
                OutputFormat.csv,
                OutputFormat.export,
            ):  # user exports
                self.params.limit = min(
                    settings.EXPORT_LIMIT, self.params.limit or settings.EXPORT_LIMIT
                )
            else:
                self.params.limit = min(
                    self.max_limit, self.params.limit or self.max_limit
                )

    def get_results_from_cache(self) -> Any:
        # 1. try direct cache key
        key = self.get_cache_key()
        res = cache.get(key)
        if res is not None:
            return res

        # 2. maybe we have the query cached already as json
        if self.params.output == OutputFormat.csv:
            key = self.get_cache_key("csv")
            res = cache.get(key)
            if res is not None:
                res = to_csv(res)
                # store as csv for next time
                cache.set(key, res)
                return res

        # 3.
        # as for filtered queries limiting and sorting (windowing) is
        # as expensive as the whole, we cache the full query for fast
        # api pagination/ordering
        full_query = self.query._chain(limit=None, offset=None, order_by_fields=None)
        full_cache_key = make_id("precached-window", str(full_query))
        full_cache_skip = make_id("skip", full_cache_key)
        skip = cache.get(full_cache_skip)
        if skip is None:
            df = cache.get(full_cache_key)
            if df is not None:
                results = self.get_results_from_df(df)
                cache.set(self.get_cache_key(), results)
                return results
            else:
                # don't cache too big queries
                if full_query.count <= 100_000:
                    df = full_query.execute()
                    cache.set(full_cache_key, df)
                    return self.get_results_from_df(df)
                else:
                    cache.set(full_cache_skip, True)

    def get_results_from_df(self, df: pd.DataFrame):
        """use a previously cached df and apply ordering and slicing on it"""
        if not len(df):
            return []
        if self.params.order_by is not None:
            order_by = self.params.order_by
            ascending = True
            if order_by.startswith("-"):
                order_by = order_by[1:]
                ascending = False
            df = df.sort_values(order_by, ascending=ascending)
        start, end = get_slice(self.params.p, self.params.limit)
        df = df.iloc[start:end]
        return [self.model(**row) for _, row in df.iterrows()]

    def get_export_path(self):
        """generate an exported csv file (or use existing) and return public
        path to it"""
        export_file = make_id("export", str(self.query)) + ".csv"
        export_fpath = os.path.join(settings.EXPORT_DIRECTORY, export_file)
        if not os.path.isfile(export_fpath):
            df = self.query.execute()
            to_csv(df, export_fpath)
        return settings.EXPORT_PUBLIC_PATH + "/" + export_file

    def get_cache_key(self, prefix: str | None = None) -> str:
        prefix = prefix or self.params.output
        return make_id(prefix, int(self.is_authenticated), str(self.query))

    @cached_property
    def query(self) -> Query:
        return self.get_query()

    @cached_property
    def where_params(self) -> dict[str, Any]:
        params = {}
        for k, v in self.params:
            if k in BaseFieldsParams.__fields__:
                params[k] = v
        return clean_dict(params)

    @cached_property
    def having_params(self) -> dict[str, Any]:
        params = {}
        for k, v in self.params:
            if k in AggregatedFieldsParams.__fields__:
                params[k] = v
        return clean_dict(params)

    @classmethod
    def get_response_model(cls):
        return create_model(
            f"{cls.__name__}{cls.model.__name__}ApiResult",
            results=(list[cls.model], []),
            __base__=ApiResult,
        )

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


class RestrictedView(BaseView):
    """
    restrict query results to last two years due to legal (lex farmsubsidy)
    """

    def get_query(self, *args, **kwargs) -> Query:
        query = super().get_query(*args, **kwargs)
        if not self.is_authenticated:
            query = query.where(year__in=settings.PUBLIC_YEARS)
        return query


class PaymentView(RestrictedView):
    model = models.Payment


class RecipientView(RestrictedView):
    params_cls = AggregatedViewParams
    model = models.Recipient


class RecipientBaseView(RestrictedView):
    """
    improved query performance because no string aggregation happens, useful
    for "get top 5 of country X" but accepts the same parameters as the big
    list view, so sorting for schemes or searching for names/fingerprints is
    still possible even though these values don't appear in the results
    """

    params_cls = AggregatedViewParams
    model = models.RecipientBase


class RecipientNameView(RestrictedView):
    """quick autocomplete view"""

    max_limit = 10
    model = models.RecipientName


class SchemeView(BaseView):
    params_cls = AggregatedViewParams
    model = models.Scheme


class CountryView(BaseView):
    params_cls = AggregatedViewParams
    model = models.Country


class YearView(BaseView):
    params_cls = AggregatedViewParams
    model = models.Year


class LocationView(BaseView):
    params_cls = AggregatedViewParams
    model = models.Location


class AggregationView(RestrictedView):  # FIXME
    params_cls = BaseViewParams
    model = models.Aggregation

    def get(self, *args, **kwargs):
        data = super().get(*args, **kwargs)
        data.next_url = None
        data.prev_url = None
        return data
