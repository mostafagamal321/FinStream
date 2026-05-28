import json
import time

import websocket

from src.producers.config import (
    DEFAULT_SYMBOLS,
    FINNHUB_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_TICK_SCHEMA_PATH,
    SCHEMA_REGISTRY_URL,
    TOPIC_MARKET_TICKS_RAW,
)
from src.producers.mappers.market_mapper import (
    map_finnhub_trade_to_market_event,
)
from src.producers.producer_utils import (
    create_avro_producer,
    delivery_report,
)


producer = create_avro_producer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    schema_registry_url=SCHEMA_REGISTRY_URL,
    schema_path=MARKET_TICK_SCHEMA_PATH,
)


def on_open(ws) -> None:
    print("[FINNHUB STREAM] WebSocket opened")

    for symbol in DEFAULT_SYMBOLS:
        subscribe_message = {
            "type": "subscribe",
            "symbol": symbol,
        }

        ws.send(json.dumps(subscribe_message))
        print(f"[FINNHUB STREAM] Subscribed to {symbol}")


def on_message(ws, message: str) -> None:
    try:
        payload = json.loads(message)

        if payload.get("type") != "trade":
            print(f"[FINNHUB STREAM] Non-trade message: {payload}")
            return

        trades = payload.get("data", [])

        for trade in trades:
            event = map_finnhub_trade_to_market_event(trade)

            producer.produce(
                topic=TOPIC_MARKET_TICKS_RAW,
                key=event["symbol"],
                value=event,
                on_delivery=delivery_report,
            )

            producer.poll(0)

            print(
                "[FINNHUB STREAM] Produced trade "
                f"symbol={event['symbol']} "
                f"price={event['price']} "
                f"volume={event['volume']} "
                f"source={event['source']}"
            )

    except Exception as exc:
        print(f"[FINNHUB STREAM ERROR] Failed to process message: {exc}")
        print(f"[FINNHUB STREAM ERROR] Raw message: {message}")


def on_error(ws, error) -> None:
    print(f"[FINNHUB STREAM ERROR] {error}")


def on_close(ws, close_status_code, close_msg) -> None:
    producer.flush()

    print(
        "[FINNHUB STREAM] WebSocket closed "
        f"status={close_status_code}, message={close_msg}"
    )


def main() -> None:
    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY is missing. Add it to your .env file.")

    print("[FINNHUB STREAM] Starting real-time Finnhub WebSocket producer")
    print(f"[FINNHUB STREAM] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[FINNHUB STREAM] Schema Registry URL: {SCHEMA_REGISTRY_URL}")
    print(f"[FINNHUB STREAM] Topic: {TOPIC_MARKET_TICKS_RAW}")
    print(f"[FINNHUB STREAM] Schema path: {MARKET_TICK_SCHEMA_PATH}")
    print(f"[FINNHUB STREAM] Symbols: {DEFAULT_SYMBOLS}")

    socket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    retry_seconds = 10

    while True:
        try:
            print("[FINNHUB STREAM] Connecting to Finnhub WebSocket...")

            ws = websocket.WebSocketApp(
                socket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except KeyboardInterrupt:
            print("[FINNHUB STREAM] Stopped by user")
            producer.flush()
            break

        except Exception as exc:
            print(f"[FINNHUB STREAM FATAL ERROR] {exc}")

        print(f"[FINNHUB STREAM] Reconnecting in {retry_seconds} seconds...")
        time.sleep(retry_seconds)


if __name__ == "__main__":
    main()