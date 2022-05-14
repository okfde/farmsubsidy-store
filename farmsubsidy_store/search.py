import fingerprints

from .exceptions import InvalidSearch
from .model import Recipient, Scheme
from .query import Query


class Search:
    field = None
    model = None

    def __init__(self, q, **filters):
        self.q = q
        self.filters = filters

    def get_search_string(self) -> str:
        return self.q.strip()

    def query(self) -> Query:
        q = self.get_search_string()
        if not q:
            raise InvalidSearch("Search string is empty")
        where = {**{f"{self.field}__ilike": f"%{q}%"}, **self.filters}
        return self.model.select().where(**where)


class RecipientNameSearch(Search):
    model = Recipient
    field = "recipient_fingerprint"

    def get_search_string(self):
        return fingerprints.generate(self.q)


class SchemeSearch(Search):
    model = Scheme
    field = "scheme"
