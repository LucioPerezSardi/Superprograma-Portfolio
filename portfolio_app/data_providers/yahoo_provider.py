from typing import Dict, Iterable, Optional
import yfinance as yf


class YahooPriceProvider:
    """Simple price provider using yfinance."""

    def __init__(self, currency_fallback: str = "USD") -> None:
        self.currency_fallback = currency_fallback

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Return latest close price for symbol; None if unavailable."""
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d")
            if data.empty:
                return None
            return float(data["Close"].iloc[-1])
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Could not fetch price for {symbol}: {exc}")
            return None

    def get_bulk_prices(self, symbols: Iterable[str]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for symbol in symbols:
            price = self.get_latest_price(symbol)
            if price is not None:
                prices[symbol] = price
        return prices
