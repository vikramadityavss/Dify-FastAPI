from pydantic import BaseModel, Field, validator
from typing import Literal, Optional, List, Dict, Any
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
        json_schema_extra = {
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

# New comprehensive response models

class HedgingState(BaseModel):
    """Detailed hedging state for an entity position"""
    already_hedged_amount: float
    available_for_hedging: float
    calculated_available_amount: float
    hedge_utilization_pct: float
    hedging_status: Literal["Available", "Fully_Hedged", "Partially_Hedged", "Not_Available"]
    car_amount_distribution: float
    manual_overlay_amount: float
    buffer_amount: float
    buffer_percentage: float
    framework_type: str
    car_exemption_flag: str
    framework_compliance: str
    last_allocation_date: Optional[str]
    waterfall_priority: Optional[int]
    allocation_sequence: Optional[int]
    allocation_status: str
    active_hedge_count: int
    total_hedge_notional: float

class PositionInfo(BaseModel):
    """Enhanced position information with hedging state"""
    nav_type: str
    current_position: float
    computed_total_nav: float
    optimal_car_amount: float
    buffer_percentage: float
    buffer_amount: float
    manual_overlay: float
    allocation_status: str
    hedging_state: HedgingState
    allocation_data: List[Dict[str, Any]]
    hedge_relationships: List[Dict[str, Any]]
    framework_rule: Dict[str, Any]
    buffer_rule: Dict[str, Any]
    car_data: Dict[str, Any]

class EntityGroup(BaseModel):
    """Complete entity information with positions and hedging state"""
    entity_id: str
    entity_name: str
    entity_type: str
    exposure_currency: str
    currency_type: Optional[str]
    car_exemption: str
    parent_child_nav_link: bool
    positions: List[PositionInfo]

class Stage1AConfig(BaseModel):
    """Stage 1A configuration data"""
    buffer_configuration: List[Dict[str, Any]]
    waterfall_logic: Dict[str, List[Dict[str, Any]]]  # opening, closing
    overlay_configuration: List[Dict[str, Any]]
    hedging_framework: List[Dict[str, Any]]
    system_configuration: List[Dict[str, Any]]
    threshold_configuration: Dict[str, Any]

class Stage1BData(BaseModel):
    """Stage 1B allocation and hedge data"""
    current_allocations: List[Dict[str, Any]]
    hedge_instructions_history: List[Dict[str, Any]]
    active_hedge_events: List[Dict[str, Any]]
    car_master_data: List[Dict[str, Any]]

class Stage2Config(BaseModel):
    """Stage 2 booking and execution configuration"""
    booking_model_config: List[Dict[str, Any]]
    murex_books: List[Dict[str, Any]]
    hedge_instruments: List[Dict[str, Any]]
    hedge_effectiveness: List[Dict[str, Any]]

class StageValidation(BaseModel):
    """Validation results for a specific stage"""
    entity_check: Optional[bool] = None
    buffer_config_check: Optional[bool] = None
    waterfall_config_check: Optional[bool] = None
    hedging_framework_check: Optional[bool] = None
    usd_pb_check: Optional[bool] = None
    system_config_check: Optional[bool] = None
    allocation_data_check: Optional[bool] = None
    hedge_history_check: Optional[bool] = None
    car_data_check: Optional[bool] = None
    active_events_check: Optional[bool] = None
    booking_model_check: Optional[bool] = None
    murex_books_check: Optional[bool] = None
    hedge_instruments_check: Optional[bool] = None
    hedge_effectiveness_check: Optional[bool] = None

class ComprehensiveValidationResults(BaseModel):
    """Complete validation results across all stages"""
    stage_1a: StageValidation
    stage_1b: StageValidation
    stage_2: StageValidation
    warnings: List[str]
    errors: List[str]

class DataCompleteness(BaseModel):
    """Data completeness scores for each stage"""
    stage_1a_completeness: float
    stage_1b_completeness: float
    stage_2_completeness: float
    overall_completeness: float
    total_entities: int
    currency_data_complete: bool
    rates_data_complete: bool

class CompleteHedgeData(BaseModel):
    """Complete hedge data structure covering all stages"""
    entity_groups: List[EntityGroup]
    stage_1a_config: Stage1AConfig
    stage_1b_data: Stage1BData
    stage_2_config: Stage2Config
    risk_monitoring: List[Dict[str, Any]]
    currency_configuration: List[Dict[str, Any]]
    currency_rates: List[Dict[str, Any]]
    proxy_configuration: List[Dict[str, Any]]
    additional_rates: List[Dict[str, Any]]

class ComprehensiveHedgeInceptionResponse(BaseModel):
    """Complete response model for comprehensive hedge inception validation"""
    status: Literal["success", "error"]
    complete_data: CompleteHedgeData
    payload: Dict[str, Any]
    validation_results: ComprehensiveValidationResults
    data_completeness: DataCompleteness
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)

# Legacy models for backward compatibility
class EntityPositionInfo(BaseModel):
    """Legacy model for entity position information"""
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
    """Legacy validation results model"""
    entity_check: bool
    currency_config_check: bool
    usd_pb_check: bool
    booking_model_check: bool
    murex_books_check: bool
    warnings: List[str]
    errors: List[str]

class HedgeInceptionResponse(BaseModel):
    """Legacy response model for hedge inception validation"""
    status: Literal["success", "error"]
    hedgeinfo: dict
    payload: dict
    validation_results: Optional[ValidationResults] = None
    message: str
    timestamp: datetime = Field(default_factory=datetime.now)