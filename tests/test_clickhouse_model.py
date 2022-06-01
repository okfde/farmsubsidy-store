from farmsubsidy_store.model import Country, Payment, Recipient, Scheme, Year
from farmsubsidy_store.query import (
    CountryQuery,
    Query,
    RecipientQuery,
    SchemeQuery,
    YearQuery,
)
from tests.util import ClickhouseTestCase


class ClickhouseModelTestCase(ClickhouseTestCase):
    def test_clickhouse_model_payment_detail(self):
        payment = Payment.get("293f7602da9ae71ce109391790e3c3502ef9f25f")
        self.assertIsInstance(payment, Payment)
        self.assertDictEqual(
            payment.dict(),
            {
                "pk": "293f7602da9ae71ce109391790e3c3502ef9f25f",
                "country": "LU",
                "year": 2019,
                "recipient_id": "005e71502e2563a45712b6746281304ba7850a0e",
                "recipient_name": "ANDRE SCHOLTES",
                "recipient_fingerprint": "andre scholtes",
                "recipient_address": "Weiswampach, LU",
                "recipient_country": "LU",
                "recipient_url": None,
                "scheme_id": "76a11191619ecfa3f6db476a0fb17606ad4f29cc",
                "scheme": "II.1",
                "scheme_code": None,
                "scheme_description": None,
                "amount": 4475.54,
                "currency": "EUR",
                "amount_original": 4475.54,
                "currency_original": "EUR",
            },
        )

        self.assertIsInstance(payment.get_recipient(), Recipient)
        self.assertEqual(payment.get_recipient().id, payment.recipient_id)
        self.assertIsInstance(payment.get_scheme(), Scheme)
        self.assertEqual(payment.get_scheme().name, payment.scheme)
        self.assertIsInstance(payment.get_year(), Year)
        self.assertEqual(payment.get_year().year, payment.year)
        self.assertIsInstance(payment.get_country(), Country)
        self.assertEqual(payment.get_country().country, payment.country)

    def test_clickhouse_model_payment_list(self):
        self.assertIsInstance(Payment.select(), Query)

        payments = Payment.select()
        payments = list(payments)
        self.assertIsInstance(payments[0], Payment)
        self.assertEqual(len(payments), 13352)

    def test_clickhouse_model_recipient_detail(self):
        recipient = Recipient.get("4262d50d6d8095a89895b8740208018da19e140a")
        self.assertIsInstance(recipient, Recipient)
        self.assertDictEqual(
            recipient.dict(),
            {
                "id": "4262d50d6d8095a89895b8740208018da19e140a",
                "name": ["LUC HOFFMANN"],
                "address": ["(B) Burg-Reuland, LU"],
                "country": "LU",
                "url": [],
                "years": [2019],
                "total_payments": 3,
                "amount_sum": 4294.58,
                "amount_avg": 1431.53,
                "amount_max": 3461.15,
                "amount_min": 29.15,
            },
        )

        self.assertIsInstance(recipient.get_payments(), Query)
        payments = list(recipient.get_payments())
        self.assertEqual(len(payments), 3)
        self.assertEqual(recipient.amount_sum, 4294.58)
        self.assertEqual(recipient.total_payments, 3)
        payments = list(recipient.get_payments().where(amount__gt=1000))
        self.assertEqual(len(payments), 1)

        self.assertIsInstance(recipient.get_payments(), Query)
        self.assertIsInstance(recipient.get_schemes(), SchemeQuery)
        self.assertIsInstance(recipient.get_years(), YearQuery)
        self.assertIsInstance(recipient.get_countries(), CountryQuery)

    def test_clickhouse_model_recipient_list(self):
        self.assertIsInstance(Recipient.select(), RecipientQuery)

        recipients = list(Recipient.select())
        self.assertEqual(len(recipients), 3032)

        recipients = list(Recipient.select()[:100])
        self.assertEqual(len(recipients), 100)

        # proper slicing
        recipient = list(Recipient.select()[100])[0]
        recipients = list(Recipient.select()[100:])
        self.assertDictEqual(recipient.dict(), recipients[0].dict())

        recipients = list(Recipient.select().where(country="LU"))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1526)

        recipients = list(Recipient.select().where(country="LU", year=2019))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1526)

        # no results
        recipients = list(Recipient.select().where(country="LU", year=2018))
        self.assertEqual(len(recipients), 0)

        # check if data is consistent
        payments = Payment.select().where(country="LU")
        total_payments = len(list(payments))
        recipients_payments = 0
        actual_payments = 0
        for recipient in Recipient.select().where(country="LU"):
            recipients_payments += recipient.total_payments
            actual_payments += len(list(recipient.get_payments()))

        self.assertEqual(total_payments, recipients_payments)
        self.assertEqual(total_payments, actual_payments)

    def test_clickhouse_model_scheme_detail(self):
        scheme = Scheme.get("02ad3dc4be4e81a0afcfe2732fce5c859e620d59")
        self.assertIsInstance(scheme, Scheme)
        self.assertEqual(scheme.total_payments, 109)
        self.assertEqual(scheme.total_recipients, 109)

        self.assertDictEqual(
            scheme.dict(),
            {
                "id": "02ad3dc4be4e81a0afcfe2732fce5c859e620d59",
                "name": "II.6",
                "years": [2019],
                "countries": ["LU"],
                "total_payments": 109,
                "total_recipients": 109,
                "amount_sum": 536547.34,
                "amount_avg": 4922.45,
                "amount_max": 8973.78,
                "amount_min": 88.48,
            },
        )

        self.assertIsInstance(scheme.get_payments(), Query)
        self.assertEqual(len(list(scheme.get_payments())), scheme.total_payments)

        self.assertIsInstance(scheme.get_recipients(), RecipientQuery)
        self.assertEqual(len(list(scheme.get_recipients())), scheme.total_recipients)

        self.assertIsInstance(scheme.get_payments(), Query)
        self.assertIsInstance(scheme.get_recipients(), RecipientQuery)
        self.assertIsInstance(scheme.get_years(), YearQuery)
        self.assertIsInstance(scheme.get_countries(), CountryQuery)

    def test_clickhouse_model_scheme_list(self):
        self.assertIsInstance(Scheme.select(), SchemeQuery)

        schemes = list(Scheme.select().where(country="LU"))
        self.assertIsInstance(schemes[0], Scheme)
        self.assertEqual(len(schemes), 19)

        schemes = list(Scheme.select().where(country="LU", year=2019))
        self.assertEqual(len(schemes), 19)

        schemes = list(Scheme.select().where(country="LU", year=2018))
        self.assertEqual(len(schemes), 0)

    def test_clickhouse_model_country_detail(self):
        country = Country.get("LU")
        self.assertIsInstance(country, Country)
        self.assertDictEqual(
            country.dict(),
            {
                "country": "LU",
                "name": "Luxembourg",
                "years": [2019],
                "total_recipients": 1526,
                "total_payments": 7718,
                "amount_sum": 80915089.85,
                "amount_avg": 10483.95,
                "amount_max": 690400.51,
                "amount_min": -99562.84,
            },
        )

        self.assertIsInstance(country.get_payments(), Query)
        self.assertIsInstance(country.get_recipients(), RecipientQuery)
        self.assertIsInstance(country.get_schemes(), SchemeQuery)
        self.assertIsInstance(country.get_years(), YearQuery)

    def test_clickhouse_model_country_list(self):
        self.assertIsInstance(Country.select(), CountryQuery)

        countries = list(Country.select())
        self.assertIsInstance(countries[0], Country)
        self.assertEqual(len(countries), 2)

        countries = list(Country.select().where(year=2019))
        self.assertEqual(len(countries), 1)

    def test_clickhouse_model_year_detail(self):
        year = Year.get(2019)
        self.assertIsInstance(year, Year)
        self.assertDictEqual(
            year.dict(),
            {
                "year": 2019,
                "countries": ["LU"],
                "total_recipients": 1526,
                "total_payments": 7718,
                "amount_sum": 80915089.85,
                "amount_avg": 10483.95,
                "amount_max": 690400.51,
                "amount_min": -99562.84,
            },
        )

        self.assertIsInstance(year.get_payments(), Query)
        self.assertIsInstance(year.get_recipients(), RecipientQuery)
        self.assertIsInstance(year.get_schemes(), SchemeQuery)
        self.assertIsInstance(year.get_countries(), CountryQuery)

    def test_clickhouse_model_year_list(self):
        self.assertIsInstance(Year.select(), YearQuery)

        years = list(Year.select())
        self.assertIsInstance(years[0], Year)
        self.assertEqual(len(years), 2)

        years = list(Year.select().where(country="LU"))
        self.assertEqual(len(years), 1)

    def test_clickhouse_model_reverse_aggs_integrity(self):
        # this is more for data integrity test than functional (which is covered above)

        models = (
            (Payment, "get_payments"),
            (Recipient, "get_recipients"),
            (Scheme, "get_schemes"),
            (Year, "get_years"),
            (Country, "get_countries"),
        )

        def _test(cls, value):
            field = cls._lookup_field
            instance = cls.get(value)
            for model, getter in models:
                if model != cls:
                    self.assertEqual(
                        len(list(getattr(instance, getter)())),
                        len(list(model.select().where(**{field: value}))),
                    )

        _test(Year, 2019)
        _test(Country, "LU")
        _test(Scheme, "02ad3dc4be4e81a0afcfe2732fce5c859e620d59")
        _test(Recipient, "4262d50d6d8095a89895b8740208018da19e140a")

    def test_clickhouse_model_aggregated_lookups(self):
        # the country with most recipients
        res = Country.select().order_by("total_recipients", ascending=False)[0].first()
        self.assertEqual(res.country, "LU")

        # the country with highest amount sum
        res = Country.select().order_by("amount_sum", ascending=False)[0].first()
        self.assertEqual(res.country, "CZ")

        # the top 5 recipients for CZ
        res = (
            Recipient.select()
            .where(country="CZ")
            .order_by("amount_sum", ascending=False)[:5]
        )
        self.assertListEqual(
            [r.id for r in res],
            [
                "05e03e6b126fbfd92d741fde3d836485053590ec",
                "28b7c19a6192bee7993522d9ab34222caebd535f",
                "ce0ed1b807ba5912f2dfbd333351e4b144f61508",
                "e421b72afd80bf9b5ca040ca85bb482c8e5dfb15",
                "bd738818419703f77bd2f355a8048b91e4cf0ce7",
            ],
        )

        # the top recipient for LU in 2019
        recipient = (
            Recipient.select()
            .where(country="LU", year=2019)
            .order_by("amount_sum", ascending=False)
            .first()
        )
        self.assertEqual(recipient.id, "ebcc24f4e75fa9cde8ddfd436ea8d33e2cc17416")

        # get the recipient from LU in 2019 that has the lowest but positive income:
        recipient = (
            Recipient.select()
            .where(country="LU", year=2019)
            .having(amount_sum__gt=0)
            .order_by("amount_sum")
            .first()
        )
        self.assertTrue(recipient.amount_sum > 0)
        self.assertEqual(recipient.id, "d3e89e4b8d6fff3157229ac61ff7ae0dcb604ba3")
