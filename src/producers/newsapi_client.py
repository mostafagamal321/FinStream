import requests


class NewsApiClient:
    """
    Small wrapper around NewsAPI.

    This is used to extract market-related news articles and send them
    to Kafka through news_producer.py.
    """

    BASE_URL = "https://newsapi.org/v2"

    def __init__(self, api_key: str | None):
        if not api_key:
            raise ValueError(
                "NEWS_API_KEY is missing. Add it to your .env file."
            )

        self.api_key = api_key

    def get_market_news(
        self,
        query: str = "stock market",
        page_size: int = 10,
        language: str = "en",
    ) -> list[dict]:
        """
        Search for market-related news articles.
        """
        response = requests.get(
            f"{self.BASE_URL}/everything",
            params={
                "q": query,
                "language": language,
                "sortBy": "publishedAt",
                "pageSize": page_size,
                "apiKey": self.api_key,
            },
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        if payload.get("status") != "ok":
            raise RuntimeError(f"NewsAPI returned error payload: {payload}")

        return payload.get("articles", [])