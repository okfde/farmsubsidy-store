from tests.util import ClickhouseTestCase

from farmsubsidy_store.model import Recipient, Scheme


class ClikhouseModelTestCase(ClickhouseTestCase):
    def test_clickhouse_model_recipient(self):
        recipient = Recipient.get(
            "20e1978de9f56d8d39ee78315b83339cf9c6e620", self.driver
        )
        self.assertIsInstance(recipient, Recipient)
        self.assertEqual(len(recipient.payments), 3)
        self.assertEqual(recipient.sum_amount, 4294.58)
        self.assertEqual(recipient.total_payments, 3)
        self.assertEqual(recipient.name, ["LUC HOFFMANN"])

    def test_clickhouse_model_recipient_list(self):
        recipients = list(Recipient.select(self.driver, country="LU"))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1683)

        recipients = list(Recipient.select(self.driver, country="LU", year=2019))
        self.assertIsInstance(recipients[0], Recipient)
        self.assertEqual(len(recipients), 1683)

        # no results
        recipients = list(Recipient.select(self.driver, country="LU", year=2018))
        self.assertEqual(len(recipients), 0)

        # limit 1000 when no filters set
        recipients = list(Recipient.select(self.driver))
        self.assertEqual(len(recipients), 1000)

    def test_clickhouse_model_scheme(self):
        scheme = Scheme.get("IV/A.18", self.driver)
        self.assertIsInstance(scheme, Scheme)
        self.assertEqual(scheme.total_payments, 1206)
        self.assertEqual(scheme.total_recipients, 1204)

    def test_clickhouse_model_scheme_list(self):
        schemes = list(Scheme.select(self.driver, country="LU"))
        self.assertIsInstance(schemes[0], Scheme)
        self.assertEqual(len(schemes), 19)

        schemes = list(Scheme.select(self.driver, country="LU", year=2019))
        self.assertIsInstance(schemes[0], Scheme)
        self.assertEqual(len(schemes), 19)

        schemes = list(Scheme.select(self.driver, country="LU", year=2018))
        self.assertEqual(len(schemes), 0)
