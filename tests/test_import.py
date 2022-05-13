import os
from unittest import TestCase

from farmsubsidy_store.drivers import get_driver
from farmsubsidy_store.util import read_csv


class ImportTestCase(TestCase):
    fixtures = os.path.join(os.getcwd(), "tests", "fixtures")
    duckdb_uri = os.path.join(os.getcwd(), "tests", "duckdb.tmp")

    def tearDown(self):
        try:
            os.remove(self.duckdb_uri)
        except FileNotFoundError:
            pass

        try:
            driver = get_driver("clickhouse", table="farmsubsidy_test")
            driver.execute(driver.drop_statement)
        except Exception:
            pass

    def _get_df(self, path):
        return read_csv(os.path.join(self.fixtures, path))

    def _import(self, driver, uri):
        driver = get_driver(driver, uri=uri, table="farmsubsidy_test", read_only=False)
        driver.init()
        df = self._get_df("cz_2015.csv.cleaned.csv.gz")
        res = driver.insert(df)
        self.assertEqual(res, len(df))

        df = self._get_df("lu_2019.csv.cleaned.csv.gz")
        res = driver.insert(df)
        self.assertEqual(res, len(df))

        return driver

    def test_duckdb_import(self):
        driver = self._import("duckdb", self.duckdb_uri)
        # foreign key constraint
        with self.assertRaisesRegex(Exception, "Constraint Error: duplicate key"):
            df = self._get_df("lu_2019.csv.cleaned.csv.gz")
            driver.insert(df)

    def test_clickhouse_import(self):
        self._import("clickhouse", "localhost")
        # FIXME check unique constraint
