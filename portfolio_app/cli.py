from datetime import date
from portfolio_app.models.asset import Asset, AssetType
from portfolio_app.models.operation import Operation, OperationType
from portfolio_app.storage.in_memory import InMemoryStore
from portfolio_app.services.portfolio_service import PortfolioService
from portfolio_app.services.metrics_service import MetricsService
from portfolio_app.data_providers.yahoo_provider import YahooPriceProvider


def main() -> None:
    store = InMemoryStore()
    portfolio_service = PortfolioService(store)
    price_provider = YahooPriceProvider()
    metrics_service = MetricsService(portfolio_service, price_provider)

    # Register assets
    portfolio_service.register_asset(
        Asset(symbol="AAPL", asset_type=AssetType.STOCK, name="Apple Inc.", currency="USD")
    )
    portfolio_service.register_asset(
        Asset(symbol="BTC-USD", asset_type=AssetType.CRYPTO, name="Bitcoin", currency="USD")
    )

    # Register operations
    portfolio_service.register_operation(
        Operation(
            asset_symbol="AAPL",
            op_type=OperationType.BUY,
            quantity=5,
            price=150,
            date=date(2024, 1, 10),
            fees=1,
        )
    )
    portfolio_service.register_operation(
        Operation(
            asset_symbol="AAPL",
            op_type=OperationType.BUY,
            quantity=3,
            price=160,
            date=date(2024, 3, 5),
            fees=1,
        )
    )
    portfolio_service.register_operation(
        Operation(
            asset_symbol="BTC-USD",
            op_type=OperationType.BUY,
            quantity=0.1,
            price=40000,
            date=date(2024, 2, 1),
            fees=0,
        )
    )

    metrics = metrics_service.compute_metrics()

    print("Market value:", round(metrics["market_value"], 2))
    print("Invested:", round(metrics["invested"], 2))
    print("P&L:", round(metrics["pnl"], 2))
    print("ROI %:", round(metrics["roi_percent"], 2))
    print("P&L by asset:", {k: round(v, 2) for k, v in metrics["asset_returns"].items()})


if __name__ == "__main__":
    main()
