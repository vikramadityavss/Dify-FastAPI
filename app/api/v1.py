from fastapi import APIRouter, HTTPException
from app.models.payloads import HedgeInceptionInstruction
from app.services.hedge_data import fetch_complete_hedge_data

router = APIRouter()

@router.post("/hedge/inception/validate-book")
def validate_and_book_hedge_inception(payload: HedgeInceptionInstruction):
    """
    Complete hedge inception validation covering Stages 1A, 1B, and 2 data requirements
    
    This endpoint performs comprehensive validation:
    - Stage 1A: Entity eligibility, buffer rules, waterfall logic, thresholds
    - Stage 1B: Allocation state, hedge relationships, CAR data
    - Stage 2: Booking model configuration, Murex books, hedge instruments
    """
    try:
        complete_hedge_data = fetch_complete_hedge_data(
            exposure_currency=payload.exposure_currency,
            hedge_method=payload.hedge_method,
            hedge_amount_order=payload.hedge_amount_order,
            order_id=payload.order_id,
            nav_type=payload.nav_type,
            currency_type=payload.currency_type
        )
        
        # Check if there was an error in data retrieval
        if "error" in complete_hedge_data:
            return {
                "status": "error",
                "complete_data": complete_hedge_data,
                "payload": payload.dict(),
                "message": f"Complete data retrieval failed: {complete_hedge_data['error']}"
            }
        
        # Perform comprehensive validations across all stages
        validation_results = perform_comprehensive_validations(complete_hedge_data, payload)
        
        # Calculate data completeness scores
        data_completeness = calculate_data_completeness(complete_hedge_data)
        
        return {
            "status": "success",
            "complete_data": complete_hedge_data,
            "payload": payload.dict(),
            "validation_results": validation_results,
            "data_completeness": data_completeness,
            "message": "Complete hedge data retrieval succeeded across all stages."
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

def perform_comprehensive_validations(complete_data: dict, payload: HedgeInceptionInstruction) -> dict:
    """
    Perform validations across Stages 1A, 1B, and 2
    """
    validations = {
        # Stage 1A validations
        "stage_1a": {
            "entity_check": False,
            "buffer_config_check": False,
            "waterfall_config_check": False,
            "hedging_framework_check": False,
            "usd_pb_check": False,
            "system_config_check": False
        },
        # Stage 1B validations
        "stage_1b": {
            "allocation_data_check": False,
            "hedge_history_check": False,
            "car_data_check": False,
            "active_events_check": False
        },
        # Stage 2 validations
        "stage_2": {
            "booking_model_check": False,
            "murex_books_check": False,
            "hedge_instruments_check": False,
            "hedge_effectiveness_check": False
        },
        "warnings": [],
        "errors": []
    }
    
    # Stage 1A Validations
    if complete_data.get("entity_groups"):
        validations["stage_1a"]["entity_check"] = True
    else:
        validations["errors"].append(f"No entities found for currency {payload.exposure_currency}")
    
    stage_1a_config = complete_data.get("stage_1a_config", {})
    if stage_1a_config.get("buffer_configuration"):
        validations["stage_1a"]["buffer_config_check"] = True
    else:
        validations["warnings"].append("No buffer configuration found")
    
    if stage_1a_config.get("waterfall_logic"):
        validations["stage_1a"]["waterfall_config_check"] = True
    else:
        validations["warnings"].append("No waterfall logic configuration found")
    
    if stage_1a_config.get("hedging_framework"):
        validations["stage_1a"]["hedging_framework_check"] = True
    else:
        validations["warnings"].append("No hedging framework configuration found")
    
    usd_pb_check = stage_1a_config.get("threshold_configuration", {}).get("usd_pb_check", {})
    if usd_pb_check.get("status") == "PASS":
        validations["stage_1a"]["usd_pb_check"] = True
    else:
        validations["warnings"].append(f"USD PB deposit check: {usd_pb_check.get('status', 'UNKNOWN')}")
    
    if stage_1a_config.get("system_configuration"):
        validations["stage_1a"]["system_config_check"] = True
    
    # Stage 1B Validations
    stage_1b_data = complete_data.get("stage_1b_data", {})
    if stage_1b_data.get("current_allocations"):
        validations["stage_1b"]["allocation_data_check"] = True
    else:
        validations["warnings"].append("No current allocation data found")
    
    if stage_1b_data.get("hedge_instructions_history"):
        validations["stage_1b"]["hedge_history_check"] = True
    
    if stage_1b_data.get("car_master_data"):
        validations["stage_1b"]["car_data_check"] = True
    else:
        validations["warnings"].append("No CAR master data found")
    
    if stage_1b_data.get("active_hedge_events"):
        validations["stage_1b"]["active_events_check"] = True
    
    # Stage 2 Validations
    stage_2_config = complete_data.get("stage_2_config", {})
    if stage_2_config.get("booking_model_config"):
        validations["stage_2"]["booking_model_check"] = True
    else:
        validations["warnings"].append("No booking model configuration found")
    
    if stage_2_config.get("murex_books"):
        validations["stage_2"]["murex_books_check"] = True
    else:
        validations["warnings"].append("No active Murex books found")
    
    if stage_2_config.get("hedge_instruments"):
        validations["stage_2"]["hedge_instruments_check"] = True
    
    if stage_2_config.get("hedge_effectiveness"):
        validations["stage_2"]["hedge_effectiveness_check"] = True
    
    return validations

def calculate_data_completeness(complete_data: dict) -> dict:
    """
    Calculate completeness scores for each stage
    """
    stage_1a_tables = ["buffer_configuration", "waterfall_logic", "hedging_framework", "system_configuration", "overlay_configuration"]
    stage_1b_tables = ["current_allocations", "hedge_instructions_history", "active_hedge_events", "car_master_data"]
    stage_2_tables = ["booking_model_config", "murex_books", "hedge_instruments", "hedge_effectiveness"]
    
    def calculate_stage_completeness(config_data, required_tables):
        if not config_data:
            return 0.0
        present_tables = sum(1 for table in required_tables if config_data.get(table))
        return (present_tables / len(required_tables)) * 100
    
    stage_1a_score = calculate_stage_completeness(complete_data.get("stage_1a_config", {}), stage_1a_tables)
    stage_1b_score = calculate_stage_completeness(complete_data.get("stage_1b_data", {}), stage_1b_tables)
    stage_2_score = calculate_stage_completeness(complete_data.get("stage_2_config", {}), stage_2_tables)
    
    overall_score = (stage_1a_score + stage_1b_score + stage_2_score) / 3
    
    return {
        "stage_1a_completeness": round(stage_1a_score, 1),
        "stage_1b_completeness": round(stage_1b_score, 1), 
        "stage_2_completeness": round(stage_2_score, 1),
        "overall_completeness": round(overall_score, 1),
        "total_entities": len(complete_data.get("entity_groups", [])),
        "currency_data_complete": bool(complete_data.get("currency_configuration")),
        "rates_data_complete": bool(complete_data.get("currency_rates"))
    }