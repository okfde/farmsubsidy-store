from typing import Optional

import fingerprints

from .exceptions import InvalidSearch
from .model import Recipient, Scheme
from .views import AggregatedViewParams, RecipientListView, SchemeListView
from .query import Query


class Search:
    field = None
    model = None

    def __init__(self, q: str, base_query: Optional[Query] = None):
        self.q = (q or "").strip()
        if not self.q:
            raise InvalidSearch("Search string is empty")
        self.base_query = base_query

    def get_search_string(self) -> str:
        return self.q.strip()

    def get_query(self) -> Query:
        q = self.get_search_string()
        query = self.base_query or self.model.select()
        search = {f"{self.field}__ilike": f"%{q}%"}
        return query.where(**search)


class RecipientNameSearch(Search):
    model = Recipient
    field = "recipient_fingerprint"

    def get_search_string(self) -> str:
        return fingerprints.generate(self.q)


class SchemeSearch(Search):
    model = Scheme
    field = "scheme"


# search views


class SearchParams(AggregatedViewParams):
    q: Optional[str] = None


class BaseSearchView:
    params_cls = SearchParams
    search_cls = None

    def apply_params(self, **params) -> dict:
        params = super().apply_params(**params)
        self.q = params.pop("q", None)
        if not self.q and not params:
            raise InvalidSearch("Result too large. Please set search params")
        return params

    def get_query(self, **params) -> Query:
        base_query = super().get_query(**params)
        if self.q is None:
            return base_query
        s = self.search_cls(self.q, base_query)
        start, end = self.get_slice(self.page, self.limit)
        return s.get_query()[start:end]


class RecipientSearchView(BaseSearchView, RecipientListView):
    search_cls = RecipientNameSearch

    def get_initial_query(self) -> Query:
        return super().get_initial_query().where(recipient_name__null=False)


class SchemeSearchView(BaseSearchView, SchemeListView):
    search_cls = SchemeSearch

    def get_initial_query(self) -> Query:
        return super().get_initial_query().where(scheme_name__null=False)
