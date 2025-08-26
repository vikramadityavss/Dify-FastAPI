from pydantic import BaseModel, Field
from typing import Literal, Optional

class HedgeInceptionInstruction(BaseModel):
    instruction_type: Literal["I", "U"] = Field(..., description="'I' for inception, 'U' for utilisation")
    order_id: str = Field(..., description="Order ID from FPM")
    sub_order_id: str = Field(..., description="Sub-order ID if used by FPM")
    exposure_currency: str = Field(..., min_length=3, max_length=3, description="Exposure currency from FPM")
    hedge_amount_order: float = Field(..., gt=0, description="Amount to be hedged from FPM")
    hedge_method: Literal["COH", "MT"] = Field(..., description="FPM accounting method (COH/MT)")
    nav_type: Optional[str] = Field(None, description="NAV type filter (COI/RE)")
    currency_type: Optional[str] = Field(None, description="Currency type filter")

    class Config:
        schema_extra = {
            "example": {
                "instruction_type": "I",
                "order_id": "ORD_001",
                "sub_order_id": "SUB_001",
                "exposure_currency": "HKD",
                "hedge_amount_order": 5000000.0,
                "hedge_method": "COH",
                "nav_type": "COI",
                "currency_type": "MAJOR"
            }
        }
