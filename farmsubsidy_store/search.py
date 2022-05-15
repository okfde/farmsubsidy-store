from typing import Optional

import fingerprints

from .exceptions import InvalidSearch
from .model import Recipient, Scheme
from .views import BaseParams, RecipientListView, SchemeListView
from .query import Query


class Search:
    field = None
    model = None

    def __init__(self, q: str, base_query: Optional[Query] = None):
        self.q = q
        self.base_query = base_query

    def get_search_string(self) -> str:
        return self.q.strip()

    def query(self) -> Query:
        q = self.get_search_string()
        if not q:
            raise InvalidSearch("Search string is empty")
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


class SearchParams(BaseParams):
    q: Optional[str] = None


class BaseSearchView:
    params_cls = SearchParams
    search_cls = None

    def apply_params(self, **params):
        params = super().apply_params(**params)
        self.q = params.pop("q", None)
        return params

    def get_query(self):
        base_query = super().get_query()
        if self.q is None:
            return base_query
        s = self.search_cls(self.q, base_query)
        start, end = self.get_slice(self.page, self.limit)
        return s.query()[start:end]


class RecipientSearchView(BaseSearchView, RecipientListView):
    search_cls = RecipientNameSearch


class SchemeSearchView(BaseSearchView, SchemeListView):
    search_cls = SchemeSearch
