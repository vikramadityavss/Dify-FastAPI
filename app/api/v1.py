from fastapi import APIRouter, HTTPException
from app.models.payloads import HedgeInceptionInstruction
from app.services.hedge_data import fetch_entity_and_nav_info

router = APIRouter()

@router.post("/hedge/inception/validate-book")
def validate_and_book_hedge_inception(payload: HedgeInceptionInstruction):
    """
    Validate and process hedge inception instruction
    
    This endpoint performs Stage 1A validation:
    - Entity eligibility and hierarchy validation
    - Position and NAV data retrieval
    - USD PB deposit threshold checking
    - Currency configuration validation
    - Booking model configuration retrieval
    """
    try:
        hedgeinfo = fetch_entity_and_nav_info(
            exposure_currency=payload.exposure_currency,
            hedge_method=payload.hedge_method,
            hedge_amount_order=payload.hedge_amount_order,
            order_id=payload.order_id,
            nav_type=payload.nav_type,
            currency_type=payload.currency_type
        )
        
        # Check if there was an error in data retrieval
        if "error" in hedgeinfo:
            return {
                "status": "error",
                "hedgeinfo": hedgeinfo,
                "payload": payload.dict(),
                "message": f"Supabase data retrieval failed: {hedgeinfo['error']}"
            }
        
        # Perform basic validations
        validation_results = perform_basic_validations(hedgeinfo, payload)
        
        return {
            "status": "success",
            "hedgeinfo": hedgeinfo,
            "payload": payload.dict(),
            "validation_results": validation_results,
            "message": "Supabase async data retrieval succeeded."
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

def perform_basic_validations(hedgeinfo: dict, payload: HedgeInceptionInstruction) -> dict:
    """
    Perform basic validations on the retrieved data
    """
    validations = {
        "entity_check": False,
        "currency_config_check": False,
        "usd_pb_check": False,
        "booking_model_check": False,
        "murex_books_check": False,
        "warnings": [],
        "errors": []
    }
    
    # Check if entities were found
    if hedgeinfo.get("entity_groups"):
        validations["entity_check"] = True
    else:
        validations["errors"].append(f"No entities found for currency {payload.exposure_currency}")
    
    # Check currency configuration
    if hedgeinfo.get("currency_configuration"):
        validations["currency_config_check"] = True
    else:
        validations["warnings"].append(f"No currency configuration found for {payload.exposure_currency}")
    
    # Check USD PB status
    usd_pb_check = hedgeinfo.get("usd_pb_check", {})
    if usd_pb_check.get("status") == "PASS":
        validations["usd_pb_check"] = True
    else:
        validations["warnings"].append(f"USD PB deposit check: {usd_pb_check.get('status', 'UNKNOWN')}")
    
    # Check booking model configuration
    if hedgeinfo.get("booking_model_config"):
        validations["booking_model_check"] = True
    else:
        validations["warnings"].append("No booking model configuration found")
    
    # Check murex books
    if hedgeinfo.get("murex_books"):
        validations["murex_books_check"] = True
    else:
        validations["warnings"].append("No active Murex books found")
    
    return validations
