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
        payment = Payment.get("60ec9ed0ebb8097eb37da4899254cec0357e001a")
        self.assertIsInstance(payment, Payment)
        self.assertDictEqual(
            payment.dict(),
            {
                "pk": "60ec9ed0ebb8097eb37da4899254cec0357e001a",
                "country": "CZ",
                "year": 2015,
                "recipient_id": "8dcd46e1010938d370d4f49477ce50b71c1a1f3e",
                "recipient_name": "AGRO Chomutice a.s.",
                "recipient_fingerprint": "agro as chomutice",
                "recipient_address": "Chomutice, district Jičín  ",
                "recipient_country": "CZ",
                "recipient_url": "http://www.szif.cz/irj/portal/eng/list_of_beneficiaries?ji=1000002098&opatr=&year=2015&portalAction=detail",
                "scheme": "SSP",
                "scheme_code": "",
                "scheme_description": "",
                "amount": 110923.77,
                "currency": "EUR",
                "amount_original": 3070241.87,
                "currency_original": "CZK",
            },
        )

        self.assertIsInstance(payment.get_recipient(), Recipient)
        self.assertEqual(payment.get_recipient().id, payment.recipient_id)
        self.assertIsInstance(payment.get_scheme(), Scheme)
        self.assertEqual(payment.get_scheme().scheme, payment.scheme)
        self.assertIsInstance(payment.get_year(), Year)
        self.assertEqual(payment.get_year().year, payment.year)
        self.assertIsInstance(payment.get_country(), Country)
        self.assertEqual(payment.get_country().country, payment.country)

    def test_clickhouse_model_payment_list(self):
        self.assertIsInstance(Payment.select(), Query)

        payments = Payment.select()
        payments = list(payments)
        self.assertIsInstance(payments[0], Payment)
        self.assertEqual(len(payments), 13361)

    def test_clickhouse_model_recipient_detail(self):
        recipient = Recipient.get("20e1978de9f56d8d39ee78315b83339cf9c6e620")
        self.assertIsInstance(recipient, Recipient)
        self.assertDictEqual(
            recipient.dict(),
            {
                "id": "20e1978de9f56d8d39ee78315b83339cf9c6e620",
                "name": ["LUC HOFFMANN"],
                "address": ["(B) Burg-Reuland  "],
                "country": ["LU"],
                "url": [],
                "years": [2019],
                "total_payments": 3,
                "amount_sum": 4294.58,
                "amount_avg": 1431.5266666666666,
                "amount_max": 3461.15,
                "amount_min": 29.15,
            },
        )

        self.assertIsInstance(recipient.get_payments(), Query)
        payments = list(recipient.get_payments())
        self.assertEqual(len(payments), 3)
        self.assertEqual(recipient.amount_sum, 4294.58)
        self.assertEqual(recipient.total_payments, 3)
        self.assertEqual(recipient.name, ["LUC HOFFMANN"])
        self.assertEqual(recipient.country, ["LU"])
        payments = list(recipient.get_payments().where(amount__gt=1000))
        self.assertEqual(len(payments), 1)

        self.assertIsInstance(recipient.get_payments(), Query)
        self.assertIsInstance(recipient.get_schemes(), SchemeQuery)
        self.assertIsInstance(recipient.get_years(), YearQuery)
        self.assertIsInstance(recipient.get_countries(), CountryQuery)

    def test_clickhouse_model_recipient_list(self):
        self.assertIsInstance(Recipient.select(), RecipientQuery)

        recipients = list(Recipient.select())
        self.assertEqual(len(recipients), 3202)

        recipients = list(Recipient.select()[:100])
        self.assertEqual(len(recipients), 100)

        # proper slicing
        recipient = list(Recipient.select()[100])[0]
        recipients = list(Recipient.select()[100:])
        self.assertDictEqual(recipient.dict(), recipients[0].dict())

        recipients = list(Recipient.select().where(country="LU"))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1683)

        recipients = list(Recipient.select().where(country="LU", year=2019))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1683)

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
        scheme = Scheme.get("IV/A.18")
        self.assertIsInstance(scheme, Scheme)
        self.assertEqual(scheme.total_payments, 1206)
        self.assertEqual(scheme.total_recipients, 1204)

        self.assertDictEqual(
            scheme.dict(),
            {
                "scheme": "IV/A.18",
                "years": [2019],
                "countries": ["LU"],
                "total_payments": 1206,
                "total_recipients": 1204,
                "amount_sum": 13384487.96,
                "amount_avg": 11098.24872305141,
                "amount_max": 54243.39,
                "amount_min": 270.16,
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
                "years": [2019],
                "total_recipients": 1683,
                "total_payments": 7718,
                "amount_sum": 80915089.85,
                "amount_avg": 10483.945303187353,
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
                "total_recipients": 1683,
                "total_payments": 7718,
                "amount_sum": 80915089.85,
                "amount_avg": 10483.945303187353,
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

        def _test(cls, value, field=None):
            field = field or cls.__name__.lower()
            instance = cls.get(value)
            for model, getter in models:
                if model != cls:
                    self.assertEqual(
                        len(list(getattr(instance, getter)())),
                        len(list(model.select().where(**{field: value}))),
                    )

        _test(Year, 2019)
        _test(Country, "LU")
        _test(Scheme, "IV/A.18")
        _test(
            Recipient, "20e1978de9f56d8d39ee78315b83339cf9c6e620", field="recipient_id"
        )

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
                "1a8f3fe35e2133d54cd358d9ec1ca9ec63733417",
                "5f94bb2400314f95210416da60a162e69a4df896",
                "ff5b4056d8874a4318c68418fba6c36f95c56f88",
                "8bd89aeeb10f99deb18eccc2e67093f2c39ef016",
                "51fb847f17983024b1196afc91027e35fa2029f9",
            ],
        )

        # the top recipient for LU in 2019
        recipient = (
            Recipient.select()
            .where(country="LU", year=2019)
            .order_by("amount_sum", ascending=False)
            .first()
        )
        self.assertEqual(recipient.id, "7804e76ad6a464153745e7277b453c9980f2191c")

        # get the recipient from LU in 2019 that has the lowest but positive income:
        recipient = (
            Recipient.select()
            .where(country="LU", year=2019)
            .having(amount_sum__gt=0)
            .order_by("amount_sum")
            .first()
        )
        self.assertTrue(recipient.amount_sum > 0)
        self.assertEqual(recipient.id, "a52d8abe7134d380447c0fd68ab88ba973e3563c")
