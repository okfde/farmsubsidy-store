from tests.util import ClickhouseTestCase

from farmsubsidy_store.exceptions import InvalidSearch
from farmsubsidy_store.search import RecipientNameSearch, SchemeSearch


class ClikhouseSearchTestCase(ClickhouseTestCase):
    def test_clickhouse_search_recipients(self):
        s = RecipientNameSearch("ANDY BRISBOIS", self.driver)
        res = s.execute()
        res = list(res)
        self.assertEqual(len(res), 1)
        res = res[0]
        self.assertDictEqual(
            res.dict(),
            {
                "id": "077ad7bcdbd53b5026cef7da79122b3560a805a0",
                "name": ["ANDY BRISBOIS"],
                "address": ["Walferdange  "],
                "country": None,
                "url": [],
                "payments": [],
                "years": [2019],
                "total_payments": 8,
                "sum_amount": 185330.92,
                "avg_amount": 23166.365,
                "max_amount": 88983.4,
                "min_amount": 498.6,
            },
        )

        # additional filter
        s = RecipientNameSearch("ANDY BRISBOIS", self.driver, year=2019)
        res = s.execute()
        res = list(res)
        self.assertEqual(len(res), 1)
        res = res[0]
        self.assertDictEqual(
            res.dict(),
            {
                "id": "077ad7bcdbd53b5026cef7da79122b3560a805a0",
                "name": ["ANDY BRISBOIS"],
                "address": ["Walferdange  "],
                "country": None,
                "url": [],
                "payments": [],
                "years": [2019],
                "total_payments": 8,
                "sum_amount": 185330.92,
                "avg_amount": 23166.365,
                "max_amount": 88983.4,
                "min_amount": 498.6,
            },
        )

        # no result
        s = RecipientNameSearch("ANDY BRISBOIS", self.driver, year=2018)
        res = s.execute()
        res = list(res)
        self.assertEqual(len(res), 0)

    def test_clickhouse_search_scheme(self):
        s = SchemeSearch("IV/A.15", self.driver)
        res = s.execute()
        res = next(res)
        self.assertDictEqual(
            res.dict(),
            {
                "scheme": "IV/A.15",
                "years": [2019],
                "countries": ["LU"],
                "total_payments": 1398,
                "total_recipients": 1396,
                "sum_amount": 19685401.62,
                "avg_amount": 14081.11703862661,
                "max_amount": 193141.24,
                "min_amount": -961.27,
                "recipients": [],
            },
        )

        s = SchemeSearch("IV/A.", self.driver)
        res = s.execute()
        res = list(res)
        self.assertEqual(len(res), 8)
        self.assertSetEqual(
            set(
                [
                    "IV/A.15",
                    "IV/A.16",
                    "IV/A.17",
                    "IV/A.18",
                    "IV/A.24",
                    "IV/A.25",
                    "IV/A.4",
                    "IV/A.6",
                ]
            ),
            set([s.scheme for s in res]),
        )

        s = SchemeSearch("IV/A.15", self.driver, country="LU")
        res = s.execute()
        res = list(res)
        self.assertEqual(len(res), 1)

        s = SchemeSearch("IV/A.15", self.driver, country="AT")
        res = s.execute()
        res = list(res)
        self.assertEqual(len(res), 0)

    def test_clickhouse_search_invalid_q(self):
        with self.assertRaisesRegex(InvalidSearch, "empty"):
            s = RecipientNameSearch(" ", self.driver)
            next(s.execute())
            s = SchemeSearch(" ", self.driver)
            next(s.execute())