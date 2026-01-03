from typing import Dict
from portfolio_app.models.operation import OperationType
from portfolio_app.services.portfolio_service import PortfolioService


class MetricsService:
    """Computes portfolio-level and per-asset metrics."""

    def __init__(self, portfolio_service: PortfolioService, price_provider) -> None:
        self.portfolio_service = portfolio_service
        self.price_provider = price_provider

    def compute_metrics(self) -> Dict[str, float]:
        positions = self.portfolio_service.get_positions()
        prices = self.price_provider.get_bulk_prices(positions.keys())

        market_value = 0.0
        invested = 0.0
        asset_returns: Dict[str, float] = {}

        # Net invested cash (buys positive, sells negative)
        for op in self.portfolio_service.store.get_operations():
            sign = 1 if op.op_type == OperationType.BUY else -1
            invested += sign * op.quantity * op.price + (sign * op.fees)

        # Mark-to-market and P&L per asset
        for symbol, qty in positions.items():
            price = prices.get(symbol, 0.0)
            market_value += qty * price
            asset_invested = sum(
                (1 if op.op_type == OperationType.BUY else -1) * op.quantity * op.price
                for op in self.portfolio_service.store.get_operations()
                if op.asset_symbol == symbol
            )
            asset_returns[symbol] = qty * price - asset_invested

        pnl = market_value - invested
        roi = (pnl / invested) * 100 if invested else 0.0

        return {
            "market_value": market_value,
            "invested": invested,
            "pnl": pnl,
            "roi_percent": roi,
            "asset_returns": asset_returns,
        }
