from pydantic import BaseModel, Field, validator
from typing import Literal, Optional
from datetime import datetime

class HedgeInceptionInstruction(BaseModel):
    instruction_type: Literal["I", "U"] = Field(..., description="'I' for inception, 'U' for utilisation")
    order_id: str = Field(..., description="Order ID from FPM")
    sub_order_id: str = Field(..., description="Sub-order ID if used by FPM")
    exposure_currency: str = Field(..., min_length=3, max_length=3, description="Exposure currency from FPM")
    hedge_amount_order: float = Field(..., gt=0, description="Amount to be hedged from FPM")
    hedge_method: Literal["COH", "MT"] = Field(..., description="FPM accounting method (COH/MT)")
    nav_type: Optional[Literal["COI", "RE", "RE_Reserve"]] = Field(None, description="NAV type filter (COI/RE/RE_Reserve)")
    currency_type: Optional[Literal["Matched", "Mismatched", "Mismatched_with_Proxy"]] = Field(None, description="Currency type filter")
    
    @validator('exposure_currency')
    def validate_exposure_currency(cls, v):
        """Validate exposure currency is uppercase"""
        return v.upper()
    
    @validator('hedge_amount_order')
    def validate_hedge_amount(cls, v):
        """Validate hedge amount is positive and reasonable"""
        if v <= 0:
            raise ValueError('Hedge amount must be positive')
        if v > 1000000000:  # 1 billion limit
            raise ValueError('Hedge amount exceeds maximum limit')
        return v
    
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
                "currency_type": "Matched"
            }
        }

class EntityPositionInfo(BaseModel):
    """Model for entity position information"""
    entity_id: str
    entity_name: str
    entity_type: str
    exposure_currency: str
    currency_type: Optional[str]
    car_exemption: str
    parent_child_nav_link: bool
    positions: list

class USDPBCheck(BaseModel):
    """Model for USD PB deposit check results"""
    total_usd_equivalent: float
    threshold: float
    status: Literal["PASS", "FAIL"]
    excess_amount: float

class ValidationResults(BaseModel):
    """Model for validation results"""
    entity_check: bool
    currency_config_check: bool
    usd_pb_check: bool
    booking_model_check: bool
    murex_books_check: bool
    warnings: list[str]
    errors: list[str]

class HedgeInceptionResponse(BaseModel):
    """Complete response model for hedge inception validation"""
    status: Literal["success", "error"]
    hedgeinfo: dict
    payload: dict
    validation_results: Optional[ValidationResults] = None
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)
