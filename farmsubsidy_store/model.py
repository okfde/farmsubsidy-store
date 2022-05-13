from typing import List, Optional

from pydantic import BaseModel

from .drivers import Driver, current_driver


class Payment(BaseModel):
    pk: str
    country: str
    year: int
    recipient_id: str
    recipient_name: str
    recipient_fingerprint: str
    recipient_address: Optional[str] = None
    recipient_country: str
    recipient_url: Optional[str] = None
    scheme: Optional[str] = None
    scheme_code: Optional[str] = None
    scheme_description: Optional[str] = None
    amount: float
    currency: str
    amount_original: Optional[float] = None
    currency_original: Optional[str] = None


class Recipient(BaseModel):
    """name, address, country, url are always multi-valued"""

    id: str
    name: List[str]
    address: Optional[List[str]] = []
    country: Optional[List[str]] = None  # FIXME
    url: Optional[List[str]] = []
    payments: Optional[List[Payment]] = []
    years: List[int] = []
    total_payments: int
    sum_amount: float
    avg_amount: float
    max_amount: float
    min_amount: float

    @classmethod
    def get(
        cls, recipient_id: str, driver: Optional[Driver] = current_driver
    ) -> "Recipient":
        """return `Recipient` with `recipient_id` including all payments"""
        df = driver.select(recipient_id=recipient_id)
        return cls._aggregate(df, recipient_id)

    @classmethod
    def select(
        cls, driver: Optional[Driver] = current_driver, **filters
    ) -> List["Recipient"]:
        """return generator of `Recipient` instances"""
        df = driver.select_recipients(**filters)
        for _, row in df.iterrows():
            # FIXME country vs. recipient_country
            row["country"] = row["recipient_country"]
            yield cls(**row)

    @classmethod
    def _aggregate(cls, df, recipient_id) -> "Recipient":
        data = {
            "id": recipient_id,
            "name": list(df["recipient_name"].unique()),
            "address": list(df["recipient_address"].unique()),
            "years": list(df["year"].unique()),
            "country": list(df["recipient_country"].unique()),
            "url": list(df["recipient_url"].unique()),
            "total_payments": len(df),
            "sum_amount": df["amount"].sum(),
            "avg_amount": df["amount"].mean(),
            "min_amount": df["amount"].min(),
            "max_amount": df["amount"].max(),
            "payments": [Payment(**row) for _, row in df.iterrows()],
        }
        return cls(**data)


class Scheme(BaseModel):
    scheme: str
    years: List[int]
    countries: List[str]
    total_payments: int
    total_recipients: int
    sum_amount: float
    avg_amount: float
    max_amount: float
    min_amount: float
    recipients: Optional[List[Recipient]] = []

    @classmethod
    def get(cls, scheme: str, driver: Optional[Driver] = current_driver) -> "Scheme":
        """return `Scheme` with `scheme` including all recipients and their payments"""
        df = driver.select(scheme=scheme)
        return cls._aggregate(df, scheme)

    @classmethod
    def select(
        cls, driver: Optional[Driver] = current_driver, **filters
    ) -> List["Scheme"]:
        """return generator of `Scheme` instances"""
        df = driver.select_schemes(**filters)
        for _, row in df.iterrows():
            yield cls(**row)

    @classmethod
    def _aggregate(cls, df, scheme) -> "Scheme":
        recipients = []
        for recipient_id in df["recipient_id"].unique():
            _df = df[df["recipient_id"] == recipient_id]
            recipients.append(Recipient._aggregate(_df, recipient_id))
        data = {
            "scheme": scheme,
            "years": list(df["year"].unique()),
            "countries": list(df["country"].unique()),
            "total_payments": len(df),
            "total_recipients": len(df["recipient_id"].unique()),
            "sum_amount": df["amount"].sum(),
            "avg_amount": df["amount"].mean(),
            "min_amount": df["amount"].min(),
            "max_amount": df["amount"].max(),
            "recipients": recipients,
        }
        return cls(**data)
