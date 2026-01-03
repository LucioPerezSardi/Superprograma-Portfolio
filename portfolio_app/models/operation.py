from datetime import date
from enum import Enum
from pydantic import BaseModel, Field


class OperationType(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Operation(BaseModel):
    asset_symbol: str
    op_type: OperationType
    quantity: float = Field(gt=0)
    price: float = Field(gt=0)
    date: date
    fees: float = 0.0
