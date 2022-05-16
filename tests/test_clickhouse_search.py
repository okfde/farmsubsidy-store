from tests.util import ClickhouseTestCase

from farmsubsidy_store.exceptions import InvalidSearch
from farmsubsidy_store.search import RecipientNameSearch, SchemeSearch


class ClikhouseSearchTestCase(ClickhouseTestCase):
    def test_clickhouse_search_recipients(self):
        s = RecipientNameSearch("ANDY BRISBOIS")
        query = s.get_query()
        res = list(query)
        self.assertEqual(len(res), 1)
        res = res[0]
        self.assertDictEqual(
            res.dict(),
            {
                "id": "077ad7bcdbd53b5026cef7da79122b3560a805a0",
                "name": ["ANDY BRISBOIS"],
                "address": ["Walferdange  "],
                "country": ["LU"],
                "url": [],
                "years": [2019],
                "total_payments": 8,
                "amount_sum": 185330.92,
                "amount_avg": 23166.365,
                "amount_max": 88983.4,
                "amount_min": 498.6,
            },
        )

        # additional filter
        s = RecipientNameSearch("ANDY BRISBOIS")
        res = s.get_query().where(year=2019)
        res = list(res)
        self.assertEqual(len(res), 1)
        res = res[0]
        self.assertDictEqual(
            res.dict(),
            {
                "id": "077ad7bcdbd53b5026cef7da79122b3560a805a0",
                "name": ["ANDY BRISBOIS"],
                "address": ["Walferdange  "],
                "country": ["LU"],
                "url": [],
                "years": [2019],
                "total_payments": 8,
                "amount_sum": 185330.92,
                "amount_avg": 23166.365,
                "amount_max": 88983.4,
                "amount_min": 498.6,
            },
        )

        # no result
        s = RecipientNameSearch("ANDY BRISBOIS")
        res = s.get_query().where(year=2018)
        res = list(res)
        self.assertEqual(len(res), 0)

    def test_clickhouse_search_scheme(self):
        s = SchemeSearch("IV/A.15")
        res = s.get_query()
        res = list(res)[0]
        self.assertDictEqual(
            res.dict(),
            {
                "scheme": "IV/A.15",
                "years": [2019],
                "countries": ["LU"],
                "total_payments": 1398,
                "total_recipients": 1396,
                "amount_sum": 19685401.62,
                "amount_avg": 14081.11703862661,
                "amount_max": 193141.24,
                "amount_min": -961.27,
            },
        )

        s = SchemeSearch("IV/A.")
        res = s.get_query()
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

        s = SchemeSearch("IV/A.15")
        res = s.get_query()
        res = list(res)
        self.assertEqual(len(res), 1)

        s = SchemeSearch("IV/A.15")
        res = s.get_query().where(country="AT")
        res = list(res)
        self.assertEqual(len(res), 0)

    def test_clickhouse_search_invalid_q(self):
        with self.assertRaisesRegex(InvalidSearch, "empty"):
            s = RecipientNameSearch(" ")
            next(s.get_query())
            s = SchemeSearch(" ")
            next(s.get_query())
