import requests


class FinnhubClient:
    """
    Small wrapper around Finnhub REST API.

    This client is used by market_producer.py for simple quote extraction.
    The WebSocket producer will connect directly to Finnhub's WebSocket API.
    """

    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self, api_key: str | None):
        if not api_key:
            raise ValueError(
                "FINNHUB_API_KEY is missing. Add it to your .env file."
            )

        self.api_key = api_key

    def get_quote(self, symbol: str) -> dict:
        """
        Get latest quote for one stock symbol.

        Finnhub quote fields usually include:
        c = current price
        d = change
        dp = percent change
        h = high price of the day
        l = low price of the day
        o = open price of the day
        pc = previous close price
        t = timestamp
        """
        response = requests.get(
            f"{self.BASE_URL}/quote",
            params={
                "symbol": symbol,
                "token": self.api_key,
            },
            timeout=15,
        )

        response.raise_for_status()
        return response.json()