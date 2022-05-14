from unittest import TestCase

from farmsubsidy_store import settings
from farmsubsidy_store.exceptions import InvalidQuery
from farmsubsidy_store.query import Query


class QueryTestCase(TestCase):
    def setUp(self):
        settings.DATABASE_TABLE = "farmsubsidy"

    def test_query(self):
        q = Query()
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy")

        q = Query().select()
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy")

        q = Query("farmsubsidy_test")
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy_test")

        q = Query().select("recipient_id", "recipient_name")
        self.assertEqual(str(q), "SELECT recipient_id, recipient_name FROM farmsubsidy")

        q = Query().select("recipient_id").where(country="de")
        self.assertEqual(
            str(q), "SELECT recipient_id FROM farmsubsidy WHERE country = 'de'"
        )

        q = Query().select("recipient_id").where(country="de", year=2019)
        self.assertEqual(
            str(q),
            "SELECT recipient_id FROM farmsubsidy WHERE country = 'de' AND year = '2019'",
        )

        q = Query().group_by("country", "year")
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy GROUP BY country, year")

        q = Query().order_by("year")
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy ORDER BY year ASC")

        q = Query().order_by("year", "country", ascending=False)
        self.assertEqual(
            str(q), "SELECT * FROM farmsubsidy ORDER BY year, country DESC"
        )

    def test_query_where_operators(self):
        q = Query().where(recipient_fingerprint__like="nestle")
        self.assertEqual(
            str(q),
            "SELECT * FROM farmsubsidy WHERE recipient_fingerprint LIKE 'nestle'",
        )

        q = Query().where(recipient_fingerprint__ilike="nestle")
        self.assertEqual(
            str(q),
            "SELECT * FROM farmsubsidy WHERE recipient_fingerprint ILIKE 'nestle'",
        )

        q = Query().where(amount__gt=10)
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy WHERE amount > '10'")

        q = Query().where(amount__gte=10)
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy WHERE amount >= '10'")

        q = Query().where(amount__lt=10)
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy WHERE amount < '10'")

        q = Query().where(amount__lte=10)
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy WHERE amount <= '10'")

        q = Query().where(foo__in=(1, 2, 3))
        self.assertEqual(
            str(q), "SELECT * FROM farmsubsidy WHERE foo IN ('1', '2', '3')"
        )

    def test_query_slice(self):
        q = Query()[:100]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy LIMIT 0, 100")

        q = Query()[100:200]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy LIMIT 100, 100")

        q = Query()[100:]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy OFFSET 100")

        q = Query()[17]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy LIMIT 17, 1")

    def test_query_having(self):
        q = (
            Query()
            .select("country", "sum(amount) as amount_sum")
            .where(year=2019)
            .group_by("country")
            .having(amount_sum__gte=100)
        )
        self.assertEqual(
            str(q),
            "SELECT country, sum(amount) as amount_sum FROM farmsubsidy WHERE year = '2019' GROUP BY country HAVING amount_sum >= '100'",
        )

        # no having if no group by
        q = Query().having(foo="bar")
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy")
        self.assertEqual(
            str(q.group_by("foo")),
            "SELECT * FROM farmsubsidy GROUP BY foo HAVING foo = 'bar'",
        )

    def test_query_correct_update(self):
        q = Query().select("a").where(foo="bar").select("b", "c").where(d=1, e="f")
        self.assertEqual(
            str(q),
            "SELECT a, b, c FROM farmsubsidy WHERE foo = 'bar' AND d = '1' AND e = 'f'",
        )

        # group by should be combined
        q = Query().group_by("a").group_by("b")
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy GROUP BY a, b")

        # order by should be overwritten!
        q = Query().order_by("a").order_by("b")
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy ORDER BY b ASC")

    def test_query_invalid(self):
        with self.assertRaisesRegex(InvalidQuery, "must not be negative"):
            q = Query()[-1]
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "must not be negative"):
            q = Query()[100:50]
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "steps not allowed"):
            q = Query()[100:50:2]
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "Invalid operator"):
            q = Query().where(recipient_id__invalid_op=0)
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "Invalid operator"):
            q = Query().where(recipient_id__invalid__op=0)
            str(q)
