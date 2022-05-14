from tests.util import ClickhouseTestCase

from farmsubsidy_store.model import Recipient, Scheme, Payment
from farmsubsidy_store.query import Query, RecipientQuery, SchemeQuery


class ClickhouseModelTestCase(ClickhouseTestCase):
    def test_clickhouse_model_payment_detail(self):
        payment = Payment.get("60ec9ed0ebb8097eb37da4899254cec0357e001a", self.driver)
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

    def test_clickhouse_model_payment_list(self):
        self.assertIsInstance(Payment.select(), Query)

        payments = Payment.select(driver=self.driver)
        payments = list(payments)
        self.assertIsInstance(payments[0], Payment)
        self.assertEqual(len(payments), 13361)

    def test_clickhouse_model_recipient_detail(self):
        recipient = Recipient.get(
            "20e1978de9f56d8d39ee78315b83339cf9c6e620", self.driver
        )
        self.assertIsInstance(recipient, Recipient)
        self.assertIsInstance(recipient.payments, Query)
        payments = list(recipient.payments)
        self.assertEqual(len(payments), 3)
        self.assertEqual(recipient.amount_sum, 4294.58)
        self.assertEqual(recipient.total_payments, 3)
        self.assertEqual(recipient.name, ["LUC HOFFMANN"])
        self.assertEqual(recipient.country, ["LU"])
        payments = list(recipient.payments.where(amount__gt=1000))
        self.assertEqual(len(payments), 1)

    def test_clickhouse_model_recipient_list(self):
        self.assertIsInstance(Recipient.select(), RecipientQuery)

        recipients = list(Recipient.select(self.driver))
        self.assertEqual(len(recipients), 3202)

        recipients = list(Recipient.select(self.driver)[:100])
        self.assertEqual(len(recipients), 100)

        # proper slicing
        recipient = list(Recipient.select(self.driver)[100])[0]
        recipients = list(Recipient.select(self.driver)[100:])
        self.assertDictEqual(recipient.dict(), recipients[0].dict())

        recipients = list(Recipient.select(self.driver).where(country="LU"))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1683)

        recipients = list(Recipient.select(self.driver).where(country="LU", year=2019))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1683)

        # no results
        recipients = list(Recipient.select(self.driver).where(country="LU", year=2018))
        self.assertEqual(len(recipients), 0)

        # check if data is consistent
        payments = Payment.select(self.driver).where(country="LU")
        total_payments = len(list(payments))
        recipients_payments = 0
        actual_payments = 0
        for recipient in Recipient.select(self.driver).where(country="LU"):
            recipients_payments += recipient.total_payments
            actual_payments += len(list(recipient.payments))

        self.assertEqual(total_payments, recipients_payments)
        self.assertEqual(total_payments, actual_payments)

    def test_clickhouse_model_scheme_detail(self):
        scheme = Scheme.get("IV/A.18", self.driver)
        self.assertIsInstance(scheme, Scheme)
        self.assertEqual(scheme.total_payments, 1206)
        self.assertEqual(scheme.total_recipients, 1204)

        self.assertIsInstance(scheme.payments, Query)
        self.assertEqual(len(list(scheme.payments)), scheme.total_payments)

        self.assertIsInstance(scheme.recipients, RecipientQuery)
        self.assertEqual(len(list(scheme.recipients)), scheme.total_recipients)

    def test_clickhouse_model_scheme_list(self):
        self.assertIsInstance(Scheme.select(), SchemeQuery)

        schemes = list(Scheme.select(self.driver).where(country="LU"))
        self.assertIsInstance(schemes[0], Scheme)
        self.assertEqual(len(schemes), 19)

        schemes = list(Scheme.select(self.driver).where(country="LU", year=2019))
        self.assertIsInstance(schemes[0], Scheme)
        self.assertEqual(len(schemes), 19)

        schemes = list(Scheme.select(self.driver).where(country="LU", year=2018))
        self.assertEqual(len(schemes), 0)
