from typing import Optional

import fingerprints

from .exceptions import InvalidSearch
from .model import Recipient, Scheme
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
