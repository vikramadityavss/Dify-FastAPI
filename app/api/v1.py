from fastapi import APIRouter
from app.models.payloads import HedgeInceptionInstruction
from app.services.hedge_data import fetch_entity_and_nav_info

router = APIRouter()

@router.post("/hedge/inception/validate-book")
def validate_and_book_hedge_inception(payload: HedgeInceptionInstruction):
    hedgeinfo = fetch_entity_and_nav_info(
        exposure_currency=payload.exposure_currency,
        hedge_method=payload.hedge_method,
        hedge_amount_order=payload.hedge_amount_order,
        order_id=payload.order_id
    )
    return {
        "status": "received",
        "hedgeinfo": hedgeinfo,
        "payload": payload.dict(),
        "message": "Supabase async data retrieval succeeded." if "error" not in hedgeinfo else f"Error: {hedgeinfo['error']}"
    }
