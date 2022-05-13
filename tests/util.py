import os
from unittest import TestCase

from farmsubsidy_store.drivers import get_driver
from farmsubsidy_store.util import read_csv


class ClickhouseTestCase(TestCase):
    driver = get_driver("clickhouse", table="farmsubsidy_test")
    data = (
        os.path.join(os.getcwd(), "tests", "fixtures", "cz_2015.csv.cleaned.csv.gz"),
        os.path.join(os.getcwd(), "tests", "fixtures", "lu_2019.csv.cleaned.csv.gz"),
    )

    @classmethod
    def setUpClass(cls):
        cls.driver.init()
        for fpath in cls.data:
            df = read_csv(fpath)
            cls.driver.insert(df)

    @classmethod
    def tearDownClass(cls):
        cls.driver.execute(cls.driver.drop_statement)
