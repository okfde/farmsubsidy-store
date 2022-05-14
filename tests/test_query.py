from unittest import TestCase

from farmsubsidy_store.exceptions import InvalidQuery
from farmsubsidy_store.query import Query


class QueryTestCase(TestCase):
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

    def test_query_slice(self):
        q = Query()[:100]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy LIMIT 0, 100")

        q = Query()[100:200]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy LIMIT 100, 100")

        q = Query()[100:]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy OFFSET 100")

        q = Query()[17]
        self.assertEqual(str(q), "SELECT * FROM farmsubsidy LIMIT 17, 1")

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
