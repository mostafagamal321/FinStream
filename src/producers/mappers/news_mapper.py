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
    for key in candidates:
        if key in source:
            value = source[key]

            if pd.notna(value):
                return value

    return default


def map_newsapi_article_to_news_event(
    article: dict,
    symbol_query: str | None = None,
) -> dict:
    """
    Maps NewsAPI article JSON to canonical NewsEvent Avro schema.
    """

    title = article.get("title")

    if not title:
        raise ValueError(f"NewsAPI article has no title: {article}")

    source_obj = article.get("source") or {}

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": now_ms(),
        "published_at": article.get("publishedAt"),
        "source_name": source_obj.get("name"),
        "author": article.get("author"),
        "title": str(title),
        "description": article.get("description"),
        "url": article.get("url"),
        "symbol_query": symbol_query,
        "source": "newsapi",
        "raw_payload": json.dumps(article, default=str),
    }


def map_historical_news_row_to_news_event(row: pd.Series) -> dict:
    """
    Maps one historical news CSV row to canonical NewsEvent Avro schema.

    Supports common CSV columns:
    title, headline, published_at, date, source, author, description, url, symbol
    """

    title = get_first_available(
        row,
        ["title", "Title", "headline", "Headline"],
    )

    if title is None:
        raise ValueError(f"Missing title/headline in historical news row: {row.to_dict()}")

    published_at = get_first_available(
        row,
        ["published_at", "publishedAt", "date", "Date", "datetime", "timestamp"],
        default=None,
    )

    source_name = get_first_available(
        row,
        ["source_name", "source", "Source", "publisher"],
        default=None,
    )

    author = get_first_available(
        row,
        ["author", "Author"],
        default=None,
    )

    description = get_first_available(
        row,
        ["description", "Description", "summary", "Summary", "content"],
        default=None,
    )

    url = get_first_available(
        row,
        ["url", "URL", "link"],
        default=None,
    )

    symbol_query = get_first_available(
        row,
        ["symbol", "ticker", "query", "symbol_query"],
        default=None,
    )

    raw_payload = row.dropna().to_dict()

    if published_at is not None:
        raw_payload["_original_published_at"] = str(published_at)

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": now_ms(),
        "published_at": str(published_at) if published_at is not None else None,
        "source_name": str(source_name) if source_name is not None else None,
        "author": str(author) if author is not None else None,
        "title": str(title),
        "description": str(description) if description is not None else None,
        "url": str(url) if url is not None else None,
        "symbol_query": str(symbol_query) if symbol_query is not None else None,
        "source": "historical_news_replay",
        "raw_payload": json.dumps(raw_payload, default=str),
    }