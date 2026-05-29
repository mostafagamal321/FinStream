import time

import pandas as pd

from src.producers.config import (
    HISTORICAL_NEWS_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    NEWS_SCHEMA_PATH,
    REPLAY_DELAY_SECONDS,
    SCHEMA_REGISTRY_URL,
    TOPIC_DEAD_LETTER_QUEUE,
    TOPIC_NEWS_RAW,
)
from src.producers.mappers.news_mapper import (
    map_historical_news_row_to_news_event,
)
from src.producers.producer_utils import (
    create_avro_producer,
    delivery_report,
    send_to_dlq,
)


def main() -> None:
    print("[HISTORICAL NEWS REPLAY] Starting historical news replay producer")
    print(f"[HISTORICAL NEWS REPLAY] CSV path: {HISTORICAL_NEWS_PATH}")
    print(f"[HISTORICAL NEWS REPLAY] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[HISTORICAL NEWS REPLAY] Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"[HISTORICAL NEWS REPLAY] Topic: {TOPIC_NEWS_RAW}")
    print(f"[HISTORICAL NEWS REPLAY] Schema path: {NEWS_SCHEMA_PATH}")
    print(f"[HISTORICAL NEWS REPLAY] Replay delay seconds: {REPLAY_DELAY_SECONDS}")

    if not HISTORICAL_NEWS_PATH.exists():
        raise FileNotFoundError(
            f"Historical news CSV file not found: {HISTORICAL_NEWS_PATH}"
        )

    df = pd.read_csv(HISTORICAL_NEWS_PATH)

    if df.empty:
        raise ValueError(
            f"Historical news CSV file is empty: {HISTORICAL_NEWS_PATH}"
        )

    print(f"[HISTORICAL NEWS REPLAY] Loaded rows: {len(df)}")
    print(f"[HISTORICAL NEWS REPLAY] Columns: {list(df.columns)}")

    producer = create_avro_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=NEWS_SCHEMA_PATH,
    )

    produced_count = 0
    failed_count = 0

    for index, row in df.iterrows():
        try:
            event = map_historical_news_row_to_news_event(row)

            producer.produce(
                topic=TOPIC_NEWS_RAW,
                key=event["event_id"],
                value=event,
                on_delivery=delivery_report,
            )

            producer.poll(0)
            produced_count += 1

            print(
                "[HISTORICAL NEWS REPLAY] Produced "
                f"row={index} "
                f"title={event['title'][:90]} "
                f"source={event['source']}"
            )

            if REPLAY_DELAY_SECONDS > 0:
                time.sleep(REPLAY_DELAY_SECONDS)

        except Exception as exc:
            failed_count += 1

            print(
                "[HISTORICAL NEWS REPLAY ERROR] "
                f"row={index}, error={exc}"
            )

            send_to_dlq(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                dlq_topic=TOPIC_DEAD_LETTER_QUEUE,
                failed_record=row.dropna().to_dict(),
                error_message=str(exc),
                source="historical_news_replay",
            )

    producer.flush()

    print("[HISTORICAL NEWS REPLAY] Finished")
    print(f"[HISTORICAL NEWS REPLAY] Produced records: {produced_count}")
    print(f"[HISTORICAL NEWS REPLAY] Failed records: {failed_count}")


if __name__ == "__main__":
    main()