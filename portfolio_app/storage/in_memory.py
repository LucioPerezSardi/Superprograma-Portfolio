from typing import Dict, List
from portfolio_app.models.asset import Asset
from portfolio_app.models.operation import Operation


class InMemoryStore:
    """Lightweight in-memory store; swap with CSV/DB later."""

    def __init__(self) -> None:
        self.assets: Dict[str, Asset] = {}
        self.operations: List[Operation] = []

    def add_asset(self, asset: Asset) -> None:
        self.assets[asset.symbol] = asset

    def add_operation(self, op: Operation) -> None:
        self.operations.append(op)

    def get_assets(self) -> Dict[str, Asset]:
        return self.assets

    def get_operations(self) -> List[Operation]:
        return self.operations
