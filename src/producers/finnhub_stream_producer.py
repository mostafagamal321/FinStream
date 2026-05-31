import json
import time
import urllib.request
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

last_trade_time = time.time()


def produce_event(event: Dict[str, Any]) -> None:
    producer.produce(
        topic=TOPIC_MARKET_TICKS_RAW,
        key=event["symbol"],
        value=event,
        on_delivery=delivery_report,
    )
    producer.poll(0)
    print(
        "[FINNHUB PRODUCER] Produced "
        f"symbol={event['symbol']} "
        f"price={event['price']} "
        f"volume={event['volume']} "
        f"event_time={event['event_time']} "
        f"source={event['source']}",
        flush=True,
    )


def quote_to_market_event(symbol: str, quote: Dict[str, Any]) -> Dict[str, Any]:
    """
    Finnhub quote response:
      c  = current price
      h  = high price of the day
      l  = low price of the day
      o  = open price of the day
      pc = previous close
      t  = quote timestamp in epoch seconds
    """
    now_ms = int(time.time() * 1000)

    price = float(quote.get("c") or 0.0)
    open_price = float(quote.get("o") or price)
    high_price = float(quote.get("h") or price)
    low_price = float(quote.get("l") or price)
    close_price = float(quote.get("pc") or price)

    # Use current ingestion time for live partitioning.
    # Finnhub quote 't' can be delayed or stale depending on market/session/API plan.
    event_time_ms = now_ms

    return {
        "event_id": f"finnhub-rest-{symbol}-{event_time_ms}",
        "event_time": event_time_ms,
        "symbol": symbol,
        "price": price,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "volume": 0.0,
        "source": "finnhub_rest_quote",
        "raw_payload": json.dumps(quote),
    }


def fetch_quote(symbol: str) -> Dict[str, Any]:
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}"
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def run_rest_fallback_loop(poll_seconds: int = 10, cycles: int = 6) -> None:
    """
    Poll REST quotes for a short period after WebSocket disconnects.
    This guarantees fresh Kafka records even when WebSocket is unstable.
    """
    print("[FINNHUB REST] Starting REST fallback polling", flush=True)

    for _ in range(cycles):
        for symbol in DEFAULT_SYMBOLS:
            try:
                quote = fetch_quote(symbol)
                event = quote_to_market_event(symbol, quote)

                if event["price"] <= 0:
                    print(f"[FINNHUB REST] Skipping {symbol}: invalid price quote={quote}", flush=True)
                    continue

                produce_event(event)

            except Exception as exc:
                print(f"[FINNHUB REST ERROR] symbol={symbol} error={exc}", flush=True)

        producer.flush()
        time.sleep(poll_seconds)


def on_open(ws) -> None:
    print("[FINNHUB STREAM] WebSocket opened", flush=True)

    for symbol in DEFAULT_SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"[FINNHUB STREAM] Subscribed to {symbol}", flush=True)


def on_message(ws, message: str) -> None:
    global last_trade_time

    try:
        payload = json.loads(message)

        if payload.get("type") != "trade":
            print(f"[FINNHUB STREAM] Non-trade message: {payload}", flush=True)
            return

        trades = payload.get("data", [])

        for trade in trades:
            event = map_finnhub_trade_to_market_event(trade)
            produce_event(event)
            last_trade_time = time.time()

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
    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY is missing. Add it to your .env file.")

    print("[FINNHUB PRODUCER] Starting Finnhub producer", flush=True)
    print(f"[FINNHUB PRODUCER] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
    print(f"[FINNHUB PRODUCER] Schema Registry URL: {SCHEMA_REGISTRY_URL}", flush=True)
    print(f"[FINNHUB PRODUCER] Topic: {TOPIC_MARKET_TICKS_RAW}", flush=True)
    print(f"[FINNHUB PRODUCER] Symbols: {DEFAULT_SYMBOLS}", flush=True)

    socket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    retry_seconds = 10

    while True:
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
            producer.flush()
            break

        except Exception as exc:
            print(f"[FINNHUB STREAM FATAL ERROR] {exc}", flush=True)

        print("[FINNHUB PRODUCER] WebSocket disconnected. Running REST fallback.", flush=True)
        run_rest_fallback_loop(poll_seconds=10, cycles=6)

        print(f"[FINNHUB PRODUCER] Reconnecting WebSocket in {retry_seconds} seconds...", flush=True)
        time.sleep(retry_seconds)


if __name__ == "__main__":
    main()