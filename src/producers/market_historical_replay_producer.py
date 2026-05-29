import json
import time
from typing import Any, Dict, List

import pandas as pd

from src.producers.config import (
    HISTORICAL_MARKET_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_TICK_SCHEMA_PATH,
    REPLAY_DELAY_SECONDS,
    SCHEMA_REGISTRY_URL,
    TOPIC_DEAD_LETTER_QUEUE,
    TOPIC_MARKET_TICKS_RAW,
)
from src.producers.mappers.market_mapper import (
    map_historical_market_row_to_market_event,
)
from src.producers.producer_utils import (
    create_avro_producer,
    delivery_report,
    send_to_dlq,
)


# Must match the order of the wide CSV columns:
# Close, Close.1, Close.2, Close.3, Close.4
HISTORICAL_MARKET_SYMBOLS = ["AAPL", "MSFT", "TSLA", "AMZN", "NVDA"]


def _is_null(value: Any) -> bool:
    return value is None or pd.isna(value) or value == ""


def _to_float(value: Any) -> float | None:
    if _is_null(value):
        return None
    return float(value)


def _to_epoch_millis(value: Any) -> int:
    """
    Avro schema expects event_time as integer.
    Converts date/string/timestamp into epoch milliseconds.
    """
    if _is_null(value):
        raise ValueError("Missing event_time/date value")

    parsed = pd.to_datetime(value, errors="coerce", utc=True)

    if pd.isna(parsed):
        raise ValueError(f"Invalid event_time/date value: {value}")

    return int(parsed.timestamp() * 1000)


def _has_symbol_or_ticker(row: pd.Series) -> bool:
    for column in ["symbol", "ticker", "Symbol", "Ticker"]:
        if column in row and not _is_null(row[column]):
            return True
    return False


def _get_value(row: pd.Series, key: str) -> Any:
    if key in row:
        return row[key]
    return None


def _is_header_or_metadata_row(row: pd.Series) -> bool:
    """
    Skips the extra yfinance-style rows that appear after pandas reads the CSV:
      row 0: Price=AAPL, Close=MSFT, ...
      row 1: Price=Date
    """
    price_value = str(_get_value(row, "Price")).strip()

    if price_value in {"AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "Date"}:
        return True

    parsed = pd.to_datetime(price_value, errors="coerce", utc=True)
    return pd.isna(parsed)


def map_wide_historical_market_row_to_market_events(row: pd.Series) -> List[Dict[str, Any]]:
    """
    Converts a wide historical market CSV row into multiple normalized events.

    Input columns:
      Price, Close, Close.1, Close.2, Close.3, Close.4,
      High, High.1, ...
      Low, Low.1, ...
      Open, Open.1, ...
      Volume, Volume.1, ...

    Output:
      one event per symbol.
    """

    if _is_header_or_metadata_row(row):
        return []

    event_date = (
        _get_value(row, "Price")
        or _get_value(row, "Date")
        or _get_value(row, "date")
        or _get_value(row, "Datetime")
        or _get_value(row, "timestamp")
    )

    event_time = _to_epoch_millis(event_date)

    events: List[Dict[str, Any]] = []

    for symbol_index, symbol in enumerate(HISTORICAL_MARKET_SYMBOLS):
        suffix = "" if symbol_index == 0 else f".{symbol_index}"

        close_key = f"Close{suffix}"
        open_key = f"Open{suffix}"
        high_key = f"High{suffix}"
        low_key = f"Low{suffix}"
        volume_key = f"Volume{suffix}"

        if close_key not in row.index:
            continue

        close_price = _to_float(_get_value(row, close_key))

        if close_price is None:
            continue

        open_price = _to_float(_get_value(row, open_key))
        high_price = _to_float(_get_value(row, high_key))
        low_price = _to_float(_get_value(row, low_key))
        volume = _to_float(_get_value(row, volume_key))

        event = {
            "event_id": f"hist-{symbol}-{event_time}",
            "event_time": event_time,
            "symbol": symbol,
            "price": close_price,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,
            "source": "historical_market_replay",
            "raw_payload": json.dumps(row.dropna().to_dict(), default=str),
        }

        events.append(event)

    if not events:
        raise ValueError(
            f"No valid market events produced from wide row: {row.dropna().to_dict()}"
        )

    return events


def map_market_row_to_events(row: pd.Series) -> List[Dict[str, Any]]:
    """
    Supports:
      1. Normalized rows with symbol/ticker
      2. Wide Yahoo/yfinance rows without symbol/ticker
    """

    if _has_symbol_or_ticker(row):
        return [map_historical_market_row_to_market_event(row)]

    return map_wide_historical_market_row_to_market_events(row)


def main() -> None:
    print("[HISTORICAL MARKET REPLAY] Starting historical market replay producer")
    print(f"[HISTORICAL MARKET REPLAY] CSV path: {HISTORICAL_MARKET_PATH}")
    print(f"[HISTORICAL MARKET REPLAY] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[HISTORICAL MARKET REPLAY] Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"[HISTORICAL MARKET REPLAY] Topic: {TOPIC_MARKET_TICKS_RAW}")
    print(f"[HISTORICAL MARKET REPLAY] Schema path: {MARKET_TICK_SCHEMA_PATH}")
    print(f"[HISTORICAL MARKET REPLAY] Replay delay seconds: {REPLAY_DELAY_SECONDS}")
    print(f"[HISTORICAL MARKET REPLAY] Wide CSV symbol order: {HISTORICAL_MARKET_SYMBOLS}")

    if not HISTORICAL_MARKET_PATH.exists():
        raise FileNotFoundError(
            f"Historical market CSV file not found: {HISTORICAL_MARKET_PATH}"
        )

    df = pd.read_csv(HISTORICAL_MARKET_PATH)

    if df.empty:
        raise ValueError(
            f"Historical market CSV file is empty: {HISTORICAL_MARKET_PATH}"
        )

    print(f"[HISTORICAL MARKET REPLAY] Loaded rows: {len(df)}")
    print(f"[HISTORICAL MARKET REPLAY] Columns: {list(df.columns)}")

    producer = create_avro_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=MARKET_TICK_SCHEMA_PATH,
    )

    produced_count = 0
    failed_count = 0
    skipped_count = 0

    for index, row in df.iterrows():
        try:
            events = map_market_row_to_events(row)

            if not events:
                skipped_count += 1
                print(f"[HISTORICAL MARKET REPLAY] Skipped metadata/header row={index}")
                continue

            for event in events:
                producer.produce(
                    topic=TOPIC_MARKET_TICKS_RAW,
                    key=event["symbol"],
                    value=event,
                    on_delivery=delivery_report,
                )

                produced_count += 1

                print(
                    "[HISTORICAL MARKET REPLAY] Produced "
                    f"row={index} "
                    f"symbol={event['symbol']} "
                    f"event_time={event['event_time']} "
                    f"price={event['price']} "
                    f"volume={event.get('volume')} "
                    f"source={event['source']}"
                )

            producer.poll(0)

            if REPLAY_DELAY_SECONDS > 0:
                time.sleep(REPLAY_DELAY_SECONDS)

        except Exception as exc:
            failed_count += 1

            print(
                "[HISTORICAL MARKET REPLAY ERROR] "
                f"row={index}, error={exc}"
            )

            send_to_dlq(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                dlq_topic=TOPIC_DEAD_LETTER_QUEUE,
                failed_record=row.dropna().to_dict(),
                error_message=str(exc),
                source="historical_market_replay",
            )

    producer.flush()

    print("[HISTORICAL MARKET REPLAY] Finished")
    print(f"[HISTORICAL MARKET REPLAY] Produced records: {produced_count}")
    print(f"[HISTORICAL MARKET REPLAY] Skipped rows: {skipped_count}")
    print(f"[HISTORICAL MARKET REPLAY] Failed rows: {failed_count}")


if __name__ == "__main__":
    main()