from typing import Dict
from portfolio_app.models.asset import Asset
from portfolio_app.models.operation import Operation, OperationType
from portfolio_app.storage.in_memory import InMemoryStore


class PortfolioService:
    """Handles asset registration and position calculations."""

    def __init__(self, store: InMemoryStore) -> None:
        self.store = store

    def register_asset(self, asset: Asset) -> None:
        self.store.add_asset(asset)

    def register_operation(self, op: Operation) -> None:
        if op.asset_symbol not in self.store.assets:
            raise ValueError(f"Asset {op.asset_symbol} is not registered.")
        self.store.add_operation(op)

    def get_positions(self) -> Dict[str, float]:
        positions: Dict[str, float] = {}
        for op in self.store.get_operations():
            positions.setdefault(op.asset_symbol, 0.0)
            if op.op_type == OperationType.BUY:
                positions[op.asset_symbol] += op.quantity
            else:
                positions[op.asset_symbol] -= op.quantity
        return positions
