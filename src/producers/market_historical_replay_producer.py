import time

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


def main() -> None:
    print("[HISTORICAL MARKET REPLAY] Starting historical market replay producer")
    print(f"[HISTORICAL MARKET REPLAY] CSV path: {HISTORICAL_MARKET_PATH}")
    print(f"[HISTORICAL MARKET REPLAY] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[HISTORICAL MARKET REPLAY] Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"[HISTORICAL MARKET REPLAY] Topic: {TOPIC_MARKET_TICKS_RAW}")
    print(f"[HISTORICAL MARKET REPLAY] Schema path: {MARKET_TICK_SCHEMA_PATH}")
    print(f"[HISTORICAL MARKET REPLAY] Replay delay seconds: {REPLAY_DELAY_SECONDS}")

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

    for index, row in df.iterrows():
        try:
            event = map_historical_market_row_to_market_event(row)

            producer.produce(
                topic=TOPIC_MARKET_TICKS_RAW,
                key=event["symbol"],
                value=event,
                on_delivery=delivery_report,
            )

            producer.poll(0)
            produced_count += 1

            print(
                "[HISTORICAL MARKET REPLAY] Produced "
                f"row={index} "
                f"symbol={event['symbol']} "
                f"price={event['price']} "
                f"volume={event.get('volume')} "
                f"source={event['source']}"
            )

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
    print(f"[HISTORICAL MARKET REPLAY] Failed records: {failed_count}")


if __name__ == "__main__":
    main()