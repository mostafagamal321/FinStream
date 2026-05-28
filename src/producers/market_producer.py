import time

from src.producers.config import (
    DEFAULT_SYMBOLS,
    FINNHUB_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_TICK_SCHEMA_PATH,
    SCHEMA_REGISTRY_URL,
    TOPIC_DEAD_LETTER_QUEUE,
    TOPIC_MARKET_TICKS_RAW,
)
from src.producers.finnhub_client import FinnhubClient
from src.producers.mappers.market_mapper import (
    map_finnhub_rest_quote_to_market_event,
)
from src.producers.producer_utils import (
    create_avro_producer,
    delivery_report,
    send_to_dlq,
)


def main() -> None:
    print("[MARKET PRODUCER] Starting Finnhub REST market producer")
    print(f"[MARKET PRODUCER] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[MARKET PRODUCER] Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"[MARKET PRODUCER] Topic: {TOPIC_MARKET_TICKS_RAW}")
    print(f"[MARKET PRODUCER] Schema path: {MARKET_TICK_SCHEMA_PATH}")
    print(f"[MARKET PRODUCER] Symbols: {DEFAULT_SYMBOLS}")

    client = FinnhubClient(FINNHUB_API_KEY)

    producer = create_avro_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=MARKET_TICK_SCHEMA_PATH,
    )

    produced_count = 0
    failed_count = 0

    for symbol in DEFAULT_SYMBOLS:
        try:
            quote = client.get_quote(symbol)

            event = map_finnhub_rest_quote_to_market_event(
                symbol=symbol,
                quote=quote,
            )

            producer.produce(
                topic=TOPIC_MARKET_TICKS_RAW,
                key=event["symbol"],
                value=event,
                on_delivery=delivery_report,
            )

            producer.poll(0)
            produced_count += 1

            print(
                "[MARKET PRODUCER] Produced "
                f"symbol={event['symbol']} "
                f"price={event['price']} "
                f"source={event['source']}"
            )

            time.sleep(1)

        except Exception as exc:
            failed_count += 1

            print(
                "[MARKET PRODUCER ERROR] "
                f"symbol={symbol}, error={exc}"
            )

            send_to_dlq(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                dlq_topic=TOPIC_DEAD_LETTER_QUEUE,
                failed_record={
                    "symbol": symbol,
                    "source": "finnhub_rest_quote",
                },
                error_message=str(exc),
                source="market_producer",
            )

    producer.flush()

    print("[MARKET PRODUCER] Finished")
    print(f"[MARKET PRODUCER] Produced records: {produced_count}")
    print(f"[MARKET PRODUCER] Failed records: {failed_count}")


if __name__ == "__main__":
    main()