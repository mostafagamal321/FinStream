import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pandas as pd

from src.producers.producer_utils import new_event_id, utc_now_iso


IEEE_REFERENCE_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


def stable_hash(*values: Any, prefix: str) -> str:
    raw = "|".join("" if value is None else str(value) for value in values)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def derive_event_time(transaction_dt: int) -> str:
    event_time = IEEE_REFERENCE_TIME + timedelta(seconds=int(transaction_dt))
    return event_time.isoformat()


def map_ieee_row_to_transaction_event(
    row: Dict[str, Any],
    split: str,
) -> Dict[str, Any]:
    transaction_id = str(int(row["TransactionID"]))
    transaction_dt = int(row["TransactionDT"])

    card1 = clean_value(row.get("card1"))
    card2 = clean_value(row.get("card2"))
    card3 = clean_value(row.get("card3"))
    card4 = clean_value(row.get("card4"))
    card5 = clean_value(row.get("card5"))
    card6 = clean_value(row.get("card6"))

    addr1 = clean_value(row.get("addr1"))
    addr2 = clean_value(row.get("addr2"))
    payer_email_domain = clean_value(row.get("P_emaildomain"))
    receiver_email_domain = clean_value(row.get("R_emaildomain"))

    product_cd = clean_value(row.get("ProductCD"))

    card_id = stable_hash(card1, card2, card3, card4, card5, card6, prefix="card")
    customer_id = stable_hash(
        card1,
        card2,
        card3,
        card4,
        card5,
        card6,
        addr1,
        addr2,
        payer_email_domain,
        prefix="cust",
    )
    merchant_id = stable_hash(product_cd, receiver_email_domain, prefix="merch")

    is_fraud = clean_value(row.get("isFraud"))
    if is_fraud is not None:
        is_fraud = int(is_fraud)

    amount = clean_value(row.get("TransactionAmt"))
    if amount is None:
        amount = 0.0

    return {
        "event_id": new_event_id(),
        "transaction_id": transaction_id,

        "event_time": derive_event_time(transaction_dt),
        "ingestion_time": utc_now_iso(),
        "source": f"ieee_cis_{split}",

        "split": split,
        "transaction_dt": transaction_dt,

        "amount": float(amount),
        "currency": "USD",
        "product_cd": product_cd,

        "customer_id": customer_id,
        "card_id": card_id,
        "merchant_id": merchant_id,

        "card_brand": card4,
        "card_type": card6,
        "addr1": float(addr1) if addr1 is not None else None,
        "addr2": float(addr2) if addr2 is not None else None,

        "payer_email_domain": payer_email_domain,
        "receiver_email_domain": receiver_email_domain,

        "device_type": clean_value(row.get("DeviceType")),
        "device_info": clean_value(row.get("DeviceInfo")),

        "is_fraud": is_fraud,
    }