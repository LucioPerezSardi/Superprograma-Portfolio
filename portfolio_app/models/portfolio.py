from typing import Dict, List
from pydantic import BaseModel, Field
from portfolio_app.models.asset import Asset
from portfolio_app.models.operation import Operation


class Portfolio(BaseModel):
    name: str
    assets: Dict[str, Asset] = Field(default_factory=dict)
    operations: List[Operation] = Field(default_factory=list)
