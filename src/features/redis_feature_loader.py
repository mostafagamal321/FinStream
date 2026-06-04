import os
from typing import Iterable

import redis
import requests
from dotenv import load_dotenv


load_dotenv()

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "finstream")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "finstream123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "finstream")

REDIS_HOST = os.getenv("REDIS_HOST_LOCAL", os.getenv("REDIS_HOST", "redis"))
REDIS_PORT = int(os.getenv("REDIS_PORT_LOCAL", os.getenv("REDIS_PORT", "6379")))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

FEATURE_TTL_SECONDS = int(os.getenv("FEATURE_TTL_SECONDS", str(7 * 24 * 60 * 60)))

# Bayesian smoothing prevents tiny groups from getting extreme fraud rates.
# Example: if a merchant has 1 fraud out of 1 txn, raw rate = 1.0,
# but smoothed rate stays more conservative.
PRIOR_FRAUD_COUNT = float(os.getenv("PRIOR_FRAUD_COUNT", "1"))
PRIOR_TOTAL_COUNT = float(os.getenv("PRIOR_TOTAL_COUNT", "100"))


def clickhouse_query(query: str) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"database": CLICKHOUSE_DB},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        data=query.encode("utf-8"),
        timeout=120,
    )
    response.raise_for_status()
    return response.text


def iter_tsv_rows(raw_text: str) -> Iterable[list[str]]:
    for line in raw_text.strip().splitlines():
        if line.strip():
            yield line.split("\t")


def flush_feature_keys(redis_client: redis.Redis) -> int:
    """
    Deletes only feature-store keys, not the whole Redis DB.
    """
    patterns = [
        "merchant:*",
        "customer:*",
        "card:*",
        "email:*",
    ]

    deleted = 0

    for pattern in patterns:
        keys = list(redis_client.scan_iter(match=pattern, count=1000))
        if keys:
            deleted += redis_client.delete(*keys)

    return deleted


def seed_merchant_features(redis_client: redis.Redis) -> int:
    """
    Stores merchant features based on real label actual_is_fraud, not our rule decision.

    Keys:
    - merchant:fraud_rate:{merchant_id}
    - merchant:fraud_rate_smoothed:{merchant_id}
    - merchant:fraud_count:{merchant_id}
    - merchant:txn_count:{merchant_id}
    - merchant:avg_amount:{merchant_id}
    """

    query = f"""
    SELECT
        merchant_id,
        count() AS total_txns,
        countIf(actual_is_fraud = 1) AS fraud_txns,
        round(fraud_txns / total_txns, 8) AS fraud_rate,
        round((fraud_txns + {PRIOR_FRAUD_COUNT}) / (total_txns + {PRIOR_TOTAL_COUNT}), 8) AS fraud_rate_smoothed,
        round(avg(amount), 4) AS avg_amount
    FROM finstream.fraud_scores_rt_latest
    WHERE merchant_id != ''
      AND actual_is_fraud IS NOT NULL
    GROUP BY merchant_id
    HAVING total_txns >= 5 OR fraud_txns > 0
    FORMAT TSV
    """

    rows = clickhouse_query(query)
    loaded = 0

    for merchant_id, total_txns, fraud_txns, fraud_rate, fraud_rate_smoothed, avg_amount in iter_tsv_rows(rows):
        redis_client.setex(f"merchant:fraud_rate:{merchant_id}", FEATURE_TTL_SECONDS, fraud_rate)
        redis_client.setex(f"merchant:fraud_rate_smoothed:{merchant_id}", FEATURE_TTL_SECONDS, fraud_rate_smoothed)
        redis_client.setex(f"merchant:fraud_count:{merchant_id}", FEATURE_TTL_SECONDS, fraud_txns)
        redis_client.setex(f"merchant:txn_count:{merchant_id}", FEATURE_TTL_SECONDS, total_txns)
        redis_client.setex(f"merchant:avg_amount:{merchant_id}", FEATURE_TTL_SECONDS, avg_amount)
        loaded += 1

    return loaded


def seed_customer_features(redis_client: redis.Redis) -> int:
    """
    Stores customer features based on actual_is_fraud.

    Keys:
    - customer:fraud_rate:{customer_id}
    - customer:fraud_rate_smoothed:{customer_id}
    - customer:fraud_count:{customer_id}
    - customer:txn_count:{customer_id}
    - customer:avg_amount:{customer_id}
    """

    query = f"""
    SELECT
        customer_id,
        count() AS total_txns,
        countIf(actual_is_fraud = 1) AS fraud_txns,
        round(fraud_txns / total_txns, 8) AS fraud_rate,
        round((fraud_txns + {PRIOR_FRAUD_COUNT}) / (total_txns + {PRIOR_TOTAL_COUNT}), 8) AS fraud_rate_smoothed,
        round(avg(amount), 4) AS avg_amount
    FROM finstream.fraud_scores_rt_latest
    WHERE customer_id != ''
      AND actual_is_fraud IS NOT NULL
    GROUP BY customer_id
    HAVING total_txns >= 2 OR fraud_txns > 0
    FORMAT TSV
    """

    rows = clickhouse_query(query)
    loaded = 0

    for customer_id, total_txns, fraud_txns, fraud_rate, fraud_rate_smoothed, avg_amount in iter_tsv_rows(rows):
        redis_client.setex(f"customer:fraud_rate:{customer_id}", FEATURE_TTL_SECONDS, fraud_rate)
        redis_client.setex(f"customer:fraud_rate_smoothed:{customer_id}", FEATURE_TTL_SECONDS, fraud_rate_smoothed)
        redis_client.setex(f"customer:fraud_count:{customer_id}", FEATURE_TTL_SECONDS, fraud_txns)
        redis_client.setex(f"customer:txn_count:{customer_id}", FEATURE_TTL_SECONDS, total_txns)
        redis_client.setex(f"customer:avg_amount:{customer_id}", FEATURE_TTL_SECONDS, avg_amount)
        loaded += 1

    return loaded


def seed_card_features(redis_client: redis.Redis) -> int:
    """
    Stores card-level fraud statistics.

    Keys:
    - card:fraud_rate:{card_id}
    - card:fraud_rate_smoothed:{card_id}
    - card:fraud_count:{card_id}
    - card:txn_count:{card_id}
    - card:avg_amount:{card_id}
    """

    query = f"""
    SELECT
        card_id,
        count() AS total_txns,
        countIf(actual_is_fraud = 1) AS fraud_txns,
        round(fraud_txns / total_txns, 8) AS fraud_rate,
        round((fraud_txns + {PRIOR_FRAUD_COUNT}) / (total_txns + {PRIOR_TOTAL_COUNT}), 8) AS fraud_rate_smoothed,
        round(avg(amount), 4) AS avg_amount
    FROM finstream.fraud_scores_rt_latest
    WHERE card_id != ''
      AND actual_is_fraud IS NOT NULL
    GROUP BY card_id
    HAVING total_txns >= 2 OR fraud_txns > 0
    FORMAT TSV
    """

    rows = clickhouse_query(query)
    loaded = 0

    for card_id, total_txns, fraud_txns, fraud_rate, fraud_rate_smoothed, avg_amount in iter_tsv_rows(rows):
        redis_client.setex(f"card:fraud_rate:{card_id}", FEATURE_TTL_SECONDS, fraud_rate)
        redis_client.setex(f"card:fraud_rate_smoothed:{card_id}", FEATURE_TTL_SECONDS, fraud_rate_smoothed)
        redis_client.setex(f"card:fraud_count:{card_id}", FEATURE_TTL_SECONDS, fraud_txns)
        redis_client.setex(f"card:txn_count:{card_id}", FEATURE_TTL_SECONDS, total_txns)
        redis_client.setex(f"card:avg_amount:{card_id}", FEATURE_TTL_SECONDS, avg_amount)
        loaded += 1

    return loaded


def seed_email_domain_features(redis_client: redis.Redis) -> int:
    """
    Current ClickHouse serving table does not store payer/receiver email domain directly.
    So this remains static until we add email domain fields to the scored table
    or compute them from Silver/raw transactions.
    """

    static_email_risk = {
        "gmail.com": "0.05",
        "yahoo.com": "0.08",
        "hotmail.com": "0.10",
        "anonymous.com": "0.30",
        "missing": "0.20",
    }

    loaded = 0

    for domain, risk in static_email_risk.items():
        redis_client.setex(f"email:risk:{domain}", FEATURE_TTL_SECONDS, risk)
        loaded += 1

    return loaded


def main() -> None:
    print("Starting Redis feature loader...", flush=True)
    print(f"ClickHouse URL: {CLICKHOUSE_URL}", flush=True)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}", flush=True)
    print(f"Feature TTL seconds: {FEATURE_TTL_SECONDS}", flush=True)
    print(f"Smoothing prior: fraud={PRIOR_FRAUD_COUNT}, total={PRIOR_TOTAL_COUNT}", flush=True)

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
    )

    redis_client.ping()
    print("Redis connection OK.", flush=True)

    deleted = flush_feature_keys(redis_client)
    print(f"Deleted old feature keys: {deleted}", flush=True)

    merchant_count = seed_merchant_features(redis_client)
    customer_count = seed_customer_features(redis_client)
    card_count = seed_card_features(redis_client)
    email_count = seed_email_domain_features(redis_client)

    print("Done seeding Redis feature store.", flush=True)
    print(f"merchant feature groups loaded: {merchant_count}", flush=True)
    print(f"customer feature groups loaded: {customer_count}", flush=True)
    print(f"card feature groups loaded: {card_count}", flush=True)
    print(f"email:risk:* loaded: {email_count}", flush=True)


if __name__ == "__main__":
    main()