import json
import signal
import time
from typing import Dict, Any

import websocket

from src.producers.config import (
    DEFAULT_SYMBOLS,
    FINNHUB_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_TICK_SCHEMA_PATH,
    SCHEMA_REGISTRY_URL,
    TOPIC_MARKET_TICKS_RAW,
)
from src.producers.mappers.market_mapper import map_finnhub_trade_to_market_event
from src.producers.producer_utils import create_avro_producer, delivery_report


producer = create_avro_producer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    schema_registry_url=SCHEMA_REGISTRY_URL,
    schema_path=MARKET_TICK_SCHEMA_PATH,
)

running = True
last_trade_time = 0.0


def shutdown_handler(signum, frame) -> None:
    global running
    running = False
    print("[FINNHUB PRODUCER] Shutdown requested", flush=True)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def produce_event(event: Dict[str, Any]) -> None:
    """
    Publish one normalized market trade event to Kafka using Avro serialization.
    """

    producer.produce(
        topic=TOPIC_MARKET_TICKS_RAW,
        key=event["symbol"],
        value=event,
        on_delivery=delivery_report,
    )

    producer.poll(0)

    print(
        "[FINNHUB PRODUCER] Produced "
        f"symbol={event.get('symbol')} "
        f"price={event.get('price')} "
        f"volume={event.get('volume')} "
        f"event_time={event.get('event_time')} "
        f"source={event.get('source')}",
        flush=True,
    )


def on_open(ws) -> None:
    """
    Subscribe to all configured symbols once the WebSocket connection opens.
    """

    print("[FINNHUB STREAM] WebSocket opened", flush=True)

    for symbol in DEFAULT_SYMBOLS:
        subscribe_message = {
            "type": "subscribe",
            "symbol": symbol,
        }

        ws.send(json.dumps(subscribe_message))
        print(f"[FINNHUB STREAM] Subscribed to {symbol}", flush=True)


def on_message(ws, message: str) -> None:
    """
    Handle incoming Finnhub WebSocket messages.

    Expected trade message shape:
    {
        "type": "trade",
        "data": [
            {
                "s": "AAPL",
                "p": 123.45,
                "t": 1717000000123,
                "v": 100,
                "c": [...]
            }
        ]
    }
    """

    global last_trade_time

    try:
        payload = json.loads(message)

        message_type = payload.get("type")

        if message_type == "ping":
            print("[FINNHUB STREAM] Ping received", flush=True)
            return

        if message_type != "trade":
            print(f"[FINNHUB STREAM] Non-trade message: {payload}", flush=True)
            return

        trades = payload.get("data", [])

        if not trades:
            print("[FINNHUB STREAM] Trade message received with empty data", flush=True)
            return

        for trade in trades:
            event = map_finnhub_trade_to_market_event(trade)

            if not event:
                print(f"[FINNHUB STREAM] Mapper returned empty event for trade={trade}", flush=True)
                continue

            if not event.get("symbol"):
                print(f"[FINNHUB STREAM] Skipping trade with missing symbol: {trade}", flush=True)
                continue

            if float(event.get("price") or 0.0) <= 0:
                print(f"[FINNHUB STREAM] Skipping trade with invalid price: {trade}", flush=True)
                continue

            produce_event(event)
            last_trade_time = time.time()

        producer.flush(0)

    except Exception as exc:
        print(f"[FINNHUB STREAM ERROR] Failed to process message: {exc}", flush=True)
        print(f"[FINNHUB STREAM ERROR] Raw message: {message}", flush=True)


def on_error(ws, error) -> None:
    print(f"[FINNHUB STREAM ERROR] {error}", flush=True)


def on_close(ws, close_status_code, close_msg) -> None:
    producer.flush()
    print(
        "[FINNHUB STREAM] WebSocket closed "
        f"status={close_status_code}, message={close_msg}",
        flush=True,
    )


def main() -> None:
    global running

    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY is missing. Add it to your .env file.")

    print("[FINNHUB PRODUCER] Starting Finnhub WebSocket producer", flush=True)
    print(f"[FINNHUB PRODUCER] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    print(f"[FINNHUB PRODUCER] Schema Registry URL: {SCHEMA_REGISTRY_URL}", flush=True)
    print(f"[FINNHUB PRODUCER] Topic: {TOPIC_MARKET_TICKS_RAW}", flush=True)
    print(f"[FINNHUB PRODUCER] Symbols: {DEFAULT_SYMBOLS}", flush=True)

    socket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    retry_seconds = 10

    while running:
        try:
            print("[FINNHUB STREAM] Connecting to Finnhub WebSocket...", flush=True)

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
            print("[FINNHUB PRODUCER] Stopped by user", flush=True)
            running = False
            break

        except Exception as exc:
            print(f"[FINNHUB STREAM FATAL ERROR] {exc}", flush=True)

        if running:
            print(
                "[FINNHUB PRODUCER] WebSocket disconnected. "
                f"Reconnecting WebSocket in {retry_seconds} seconds...",
                flush=True,
            )
            time.sleep(retry_seconds)

    print("[FINNHUB PRODUCER] Flushing producer before shutdown", flush=True)
    producer.flush()
    print("[FINNHUB PRODUCER] Stopped", flush=True)


if __name__ == "__main__":
    main()