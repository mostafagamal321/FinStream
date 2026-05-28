import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
IEEE_FRAUD_DIR = RAW_DATA_DIR / "ieee_fraud"

SCHEMA_DIR = PROJECT_ROOT / "schemas"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092",
)

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8081",
)

TOPIC_TRANSACTIONS_RAW = os.getenv(
    "TOPIC_TRANSACTIONS_RAW",
    "transactions_raw",
)

TOPIC_MARKET_TICKS_RAW = os.getenv(
    "TOPIC_MARKET_TICKS_RAW",
    "market_ticks_raw",
)

TOPIC_NEWS_RAW = os.getenv(
    "TOPIC_NEWS_RAW",
    "news_raw",
)

TOPIC_FX_RATES_RAW = os.getenv(
    "TOPIC_FX_RATES_RAW",
    "fx_rates_raw",
)

TOPIC_DEAD_LETTER_QUEUE = os.getenv(
    "TOPIC_DEAD_LETTER_QUEUE",
    "dead_letter_queue",
)

TRAIN_TRANSACTION_PATH = Path(
    os.getenv(
        "TRAIN_TRANSACTION_PATH",
        str(IEEE_FRAUD_DIR / "train_transaction.csv"),
    )
)

TRAIN_IDENTITY_PATH = Path(
    os.getenv(
        "TRAIN_IDENTITY_PATH",
        str(IEEE_FRAUD_DIR / "train_identity.csv"),
    )
)

TEST_TRANSACTION_PATH = Path(
    os.getenv(
        "TEST_TRANSACTION_PATH",
        str(IEEE_FRAUD_DIR / "test_transaction.csv"),
    )
)

TEST_IDENTITY_PATH = Path(
    os.getenv(
        "TEST_IDENTITY_PATH",
        str(IEEE_FRAUD_DIR / "test_identity.csv"),
    )
)

HISTORICAL_MARKET_PATH = Path(
    os.getenv(
        "HISTORICAL_MARKET_PATH",
        str(RAW_DATA_DIR / "historical_market.csv"),
    )
)

HISTORICAL_NEWS_PATH = Path(
    os.getenv(
        "HISTORICAL_NEWS_PATH",
        str(RAW_DATA_DIR / "historical_news.csv"),
    )
)

REPLAY_DELAY_SECONDS = float(
    os.getenv(
        "REPLAY_DELAY_SECONDS",
        "1",
    )
)

TRANSACTION_SCHEMA_PATH = SCHEMA_DIR / "transactions" / "transaction_event.avsc"
MARKET_TICK_SCHEMA_PATH = SCHEMA_DIR / "market" / "stock_tick_event.avsc"
NEWS_SCHEMA_PATH = SCHEMA_DIR / "news" / "news_event.avsc"
FX_RATE_SCHEMA_PATH = SCHEMA_DIR / "fx" / "fx_rate_event.avsc"
DLQ_SCHEMA_PATH = SCHEMA_DIR / "dlq" / "dead_letter_event.avsc"


FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "TSLA", "AMZN", "NVDA"]