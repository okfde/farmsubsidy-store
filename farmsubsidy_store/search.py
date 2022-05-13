from typing import Iterator, Optional

import fingerprints
import pandas as pd

from .drivers import Driver, current_driver
from .exceptions import InvalidSearch
from .model import Recipient, Scheme
from .query import Query, RecipientListQuery, SchemeListQuery


class Search:
    field = None
    query = Query
    result_class = None

    def __init__(
        self,
        q,
        driver: Optional[Driver] = current_driver,
        **filters,
    ):
        self.q = q
        self.driver = driver
        self.table = driver.table
        self.filters = filters

    def get_search_string(self) -> str:
        return self.q.strip()

    def get_query(self) -> Query:
        q = self.get_search_string()
        if not q:
            raise InvalidSearch("Search string is empty")
        search = {self.field: q}
        return self.query(self.driver.driver, self.table, search=search, **self.filters)

    def _execute(self) -> pd.DataFrame:
        query = str(self.get_query())
        return self.driver.query(query)

    def execute(self) -> Iterator:
        """return iterator of result models"""
        df = self._execute()
        for _, row in df.iterrows():
            yield self.result_class(**row)


class RecipientNameSearch(Search):
    query = RecipientListQuery
    field = "recipient_fingerprint"
    result_class = Recipient

    def get_search_string(self):
        return fingerprints.generate(self.q)


class SchemeSearch(Search):
    query = SchemeListQuery
    field = "scheme"
    result_class = Scheme
