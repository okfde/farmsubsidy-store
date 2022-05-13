import os
from unittest import TestCase

from farmsubsidy_store.drivers import get_driver


class DuckdbDriverTestCase(TestCase):
    uri = os.path.join(os.getcwd(), "tests", "duckdb.tmp")

    def tearDown(self):
        # cleanup databases
        try:
            os.remove(self.uri)
        except FileNotFoundError:
            pass

        try:
            driver = get_driver("clickhouse", table="farmsubsidy_test")
            driver.execute(driver.drop_statement)
        except Exception:
            pass

    def test_duckdb_init(self):
        # does not exist for default read only mode
        with self.assertRaisesRegex(Exception, "database does not exist"):
            driver = get_driver("duckdb", uri=self.uri)
            driver.init()

        # explicitly pass read_only=False
        driver = get_driver("duckdb", uri=self.uri, read_only=False)
        driver.init()
        self.assertEqual(driver.uri, self.uri)

        # without recreate
        with self.assertRaisesRegex(Exception, "already exists"):
            driver.init()

        # ok
        driver.init(recreate=True)

    def test_clickhouse_init(self):
        driver = get_driver("clickhouse", table="farmsubsidy_test")
        driver.init()

        # without recreate
        with self.assertRaisesRegex(Exception, "already exists"):
            driver.init()

        # ok
        driver.init(recreate=True)
