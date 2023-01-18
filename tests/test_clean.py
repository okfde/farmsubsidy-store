from unittest import TestCase

from farmsubsidy_store.clean import clean
from farmsubsidy_store.util import read_csv


class CleanTestCase(TestCase):
    def test_clean(self):
        df = read_csv("./fixtures/cz_2015.csv.gz")
        df = clean(df)
        import ipdb

        ipdb.set_trace()
