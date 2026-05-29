import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def get_first_available(
    source: dict | pd.Series,
    candidates: list[str],
    default: Any = None,
) -> Any:
    """
    Return the first non-null value from a source object using possible column/key names.
    Works with both dicts and pandas Series.
    """
    for key in candidates:
        if key in source:
            value = source[key]

            if pd.notna(value):
                return value

    return default


def map_finnhub_rest_quote_to_market_event(
    symbol: str,
    quote: dict,
) -> dict:
    """
    Maps Finnhub REST /quote response to the canonical StockTickEvent Avro schema.

    Finnhub quote example:
    {
      "c": 189.2,
      "h": 190.1,
      "l": 187.5,
      "o": 188.0,
      "pc": 187.7,
      "t": 1716900000
    }
    """

    current_price = quote.get("c")

    if current_price is None or float(current_price) <= 0:
        raise ValueError(f"Invalid Finnhub quote for symbol={symbol}: {quote}")

    event_time = quote.get("t")

    if event_time:
        event_time_ms = int(event_time) * 1000
    else:
        event_time_ms = now_ms()

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time_ms,
        "symbol": str(symbol),
        "price": float(current_price),

        "open_price": float(quote["o"]) if quote.get("o") is not None else None,
        "high_price": float(quote["h"]) if quote.get("h") is not None else None,
        "low_price": float(quote["l"]) if quote.get("l") is not None else None,
        "close_price": float(current_price),

        "volume": None,
        "source": "finnhub_rest_quote",
        "raw_payload": json.dumps(quote, default=str),
    }


def map_finnhub_trade_to_market_event(trade: dict) -> dict:
    """
    Maps Finnhub WebSocket trade message to the canonical StockTickEvent Avro schema.

    Finnhub trade example:
    {
      "s": "AAPL",
      "p": 189.2,
      "v": 100,
      "t": 1716900000000
    }
    """

    symbol = trade.get("s")
    price = trade.get("p")

    if not symbol:
        raise ValueError(f"Finnhub trade has no symbol: {trade}")

    if price is None or float(price) <= 0:
        raise ValueError(f"Finnhub trade has invalid price: {trade}")

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": int(trade.get("t", now_ms())),
        "symbol": str(symbol),
        "price": float(price),


        "open_price": None,
        "high_price": None,
        "low_price": None,
        "close_price": None,

        "volume": float(trade.get("v", 0)),
        "source": "finnhub_websocket",
        "raw_payload": json.dumps(trade, default=str),
    }


def map_historical_market_row_to_market_event(row: pd.Series) -> dict:
    """
    Maps one historical market CSV row to the canonical StockTickEvent Avro schema.

    Supports common CSV column names:
    Date, Symbol, Open, High, Low, Close, Volume
    symbol, date, open, high, low, close, volume
    """

    symbol = get_first_available(
        row,
        ["symbol", "ticker", "Symbol", "Ticker"],
    )

    open_price = get_first_available(
        row,
        ["open", "Open", "o"],
        default=None,
    )

    high_price = get_first_available(
        row,
        ["high", "High", "h"],
        default=None,
    )

    low_price = get_first_available(
        row,
        ["low", "Low", "l"],
        default=None,
    )

    close_price = get_first_available(
        row,
        ["close", "Close", "price", "Price", "c", "last_price"],
        default=None,
    )

    volume = get_first_available(
        row,
        ["volume", "Volume", "v"],
        default=None,
    )

    original_time = get_first_available(
        row,
        ["event_time", "timestamp", "datetime", "date", "Date", "t"],
        default=None,
    )

    if symbol is None:
        raise ValueError(f"Missing symbol/ticker in historical market row: {row.to_dict()}")

    if close_price is None:
        raise ValueError(f"Missing close/price in historical market row: {row.to_dict()}")

    close_price = float(close_price)

    if close_price <= 0:
        raise ValueError(f"Invalid close/price={close_price} in row: {row.to_dict()}")

    parsed_event_time = now_ms()


    raw_payload = row.dropna().to_dict()

    if original_time is not None:
        raw_payload["_original_event_time"] = str(original_time)

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": parsed_event_time,
        "symbol": str(symbol),
        "price": close_price,

        "open_price": float(open_price) if open_price is not None else None,
        "high_price": float(high_price) if high_price is not None else None,
        "low_price": float(low_price) if low_price is not None else None,
        "close_price": close_price,

        "volume": float(volume) if volume is not None else None,
        "source": "historical_market_replay",
        "raw_payload": json.dumps(raw_payload, default=str),
    }