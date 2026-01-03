from enum import Enum
from pydantic import BaseModel


class AssetType(str, Enum):
    STOCK = "stock"
    CEDEAR = "cedear"
    BOND = "bond"
    CRYPTO = "crypto"
    ETF = "etf"


class Asset(BaseModel):
    symbol: str
    asset_type: AssetType
    name: str
    currency: str = "USD"
