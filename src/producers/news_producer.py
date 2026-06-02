from src.producers.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    NEWS_API_KEY,
    NEWS_SCHEMA_PATH,
    SCHEMA_REGISTRY_URL,
    TOPIC_DEAD_LETTER_QUEUE,
    TOPIC_NEWS_RAW,
)
from src.producers.newsapi_client import NewsApiClient
from src.producers.mappers.news_mapper import (
    map_newsapi_article_to_news_event,
)
from src.producers.producer_utils import (
    create_avro_producer,
    delivery_report,
    send_to_dlq,
)


def main() -> None:
    print("[NEWS PRODUCER] Starting NewsAPI producer")
    print(f"[NEWS PRODUCER] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[NEWS PRODUCER] Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"[NEWS PRODUCER] Topic: {TOPIC_NEWS_RAW}")
    print(f"[NEWS PRODUCER] Schema path: {NEWS_SCHEMA_PATH}")

    client = NewsApiClient(NEWS_API_KEY)

    producer = create_avro_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
        schema_path=NEWS_SCHEMA_PATH,
    )

    queries = [
        "stock market",
        "Federal Reserve",
        "inflation",
        "Apple stock",
        "Tesla stock",
        "Nvidia stock",
    ]

    produced_count = 0
    failed_count = 0

    for query in queries:
        try:
            articles = client.get_market_news(
                query=query,
                page_size=5,
            )

            print(
                f"[NEWS PRODUCER] query='{query}' "
                f"articles_count={len(articles)}"
            )

            for article in articles:
                try:
                    event = map_newsapi_article_to_news_event(
                        article=article,
                        symbol_query=query,
                    )

                    producer.produce(
                        topic=TOPIC_NEWS_RAW,
                        key=event["event_id"],
                        value=event,
                        on_delivery=delivery_report,
                    )

                    producer.poll(0)
                    produced_count += 1

                    print(
                        "[NEWS PRODUCER] Produced "
                        f"title={event['title'][:90]} "
                        f"source={event['source']}"
                    )

                except Exception as article_exc:
                    failed_count += 1

                    print(
                        "[NEWS PRODUCER ERROR] "
                        f"query={query}, error={article_exc}"
                    )

                    send_to_dlq(
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        dlq_topic=TOPIC_DEAD_LETTER_QUEUE,
                        failed_record={
                            "query": query,
                            "article": article,
                            "source": "newsapi",
                        },
                        error_message=str(article_exc),
                        source="news_producer",
                    )

        except Exception as query_exc:
            failed_count += 1

            print(
                "[NEWS PRODUCER ERROR] "
                f"query={query}, error={query_exc}"
            )

            send_to_dlq(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                dlq_topic=TOPIC_DEAD_LETTER_QUEUE,
                failed_record={
                    "query": query,
                    "source": "newsapi",
                },
                error_message=str(query_exc),
                source="news_producer",
            )

    producer.flush()

    print("[NEWS PRODUCER] Finished")
    print(f"[NEWS PRODUCER] Produced records: {produced_count}")
    print(f"[NEWS PRODUCER] Failed records: {failed_count}")


if __name__ == "__main__":
    main()