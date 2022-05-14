import os
from unittest import TestCase

from farmsubsidy_store.drivers import get_driver
from farmsubsidy_store.util import read_csv
from farmsubsidy_store import settings


def configure_clickhouse_settings():
    settings.DRIVER = "clickhouse"
    settings.DATABSE_URL = "localhost"
    settings.DATABASE_TABLE = "farmsubsidy_test"


def get_clickhouse_test_driver():
    configure_clickhouse_settings()
    driver = get_driver()
    assert driver.driver == "clickhouse"
    assert driver.table == "farmsubsidy_test"
    return driver


class ClickhouseTestCase(TestCase):
    data = (
        os.path.join(os.getcwd(), "tests", "fixtures", "cz_2015.csv.cleaned.csv.gz"),
        os.path.join(os.getcwd(), "tests", "fixtures", "lu_2019.csv.cleaned.csv.gz"),
    )

    @classmethod
    def setUpClass(cls):
        driver = get_clickhouse_test_driver()
        driver.init()
        for fpath in cls.data:
            df = read_csv(fpath)
            driver.insert(df)

    @classmethod
    def tearDownClass(cls):
        driver = get_clickhouse_test_driver()
        driver.execute(driver.drop_statement)
