from collections import defaultdict
from app.services.supabase_client import get_supabase

def fetch_entity_and_nav_info(
    exposure_currency: str,
    hedge_method: str,
    hedge_amount_order: float,
    order_id: str
):
    supabase = get_supabase()
    try:
        # Entities and positions (COI, RE, etc.)
        entities_q = supabase.table("entity_master").select("*").eq("currency_code", exposure_currency)
        positions_q = supabase.table("position_nav_master").select("*").eq("currency_code", exposure_currency)
        usd_pb_q = supabase.table("usd_pb_deposit").select("*").eq("currency_code", exposure_currency)
        
        # Currency config for all in-scope currencies (exact columns)
        currency_config_q = supabase.table("currency_configuration").select("*").or_(
            f"currency_code.eq.{exposure_currency},proxy_currency.eq.{exposure_currency},base_currency.eq.{exposure_currency}"
        )
        
        # Rates for exposure currency in both directions (fix column name to 'effective_date')
        currency_rates_q = supabase.table("currency_rates").select("*").or_(
            f"currency_pair.eq.{exposure_currency}-SGD,currency_pair.eq.SGD-{exposure_currency}"
        ).order("effective_date", desc=True)
        
        # Additional rates for proxy/base currencies from config, FIX column name to 'effective_date'
        currency_config = currency_config_q.execute()
        currency_config_rows = getattr(currency_config, "data", [])
        proxy_currencies = set()
        for c in currency_config_rows:
            if c.get("proxy_currency"):
                proxy_currencies.add(c["proxy_currency"])
            if c.get("base_currency"):
                proxy_currencies.add(c["base_currency"])
        additional_rates_rows = []
        for proxy_ccy in proxy_currencies:
            if proxy_ccy and proxy_ccy != exposure_currency:
                rate_query = supabase.table("currency_rates").select("*").or_(
                    f"currency_pair.eq.{proxy_ccy}-SGD,currency_pair.eq.SGD-{proxy_ccy}"
                ).order("effective_date", desc=True)
                rate_result = rate_query.execute()
                additional_rates_rows += getattr(rate_result, "data", [])

        # Model and books (for all types)
        booking_model_q = supabase.table("instruction_event_config").select("*").eq("instruction_event", "Inception")
        murex_books_q = supabase.table("murex_book_config").select("*").eq("active_flag", True)

        # Execute all queries
        entities = entities_q.execute()
        positions = positions_q.execute()
        usd_pb = usd_pb_q.execute()
        currency_rates = currency_rates_q.execute()
        booking_models = booking_model_q.execute()
        murex_books = murex_books_q.execute()

        entities_rows = getattr(entities, "data", [])
        positions_rows = getattr(positions, "data", [])
        total_usd_pb_deposits_rows = getattr(usd_pb, "data", [])
        currency_rates_rows = getattr(currency_rates, "data", [])
        booking_model_config_rows = getattr(booking_models, "data", [])
        murex_books_rows = getattr(murex_books, "data", [])

        return structured_response(
            entities_rows, positions_rows, total_usd_pb_deposits_rows,
            currency_config_rows, currency_rates_rows, booking_model_config_rows,
            murex_books_rows, additional_rates_rows
        )

    except Exception as e:
        print("============================")
        print("Supabase Fetch Error:", str(e))
        print("============================\n")
        return {
            "entity_groups": [],
            "usd_pb_check": {},
            "currency_configuration": [],
            "currency_rates": [],
            "booking_model_config": [],
            "murex_books": [],
            "additional_rates": [],
            "error": str(e)
        }

def structured_response(
    entities_rows, positions_rows, total_usd_pb_deposits_rows,
    currency_config_rows, currency_rates_rows, booking_model_config_rows,
    murex_books_rows, additional_rates_rows
):
    USD_PB_THRESHOLD = 135000

    # Entities grouped with all COI/RE positions
    entity_info_lookup = {e["entity_id"]: e for e in entities_rows}
    grouped = defaultdict(list)
    for pos in positions_rows:
        grouped[pos["entity_id"]].append({
            "nav_type": pos.get("nav_type", ""),
            "current_position": pos.get("current_position", 0),
            "coi_amount": pos.get("coi_amount", 0),
            "re_amount": pos.get("re_amount", 0),
            "buffer_pct": pos.get("buffer_pct", 0),
            "buffer_amount": pos.get("buffer_amount", 0),
        })
    entity_groups = []
    for entity_id, navs in grouped.items():
        entity = entity_info_lookup.get(entity_id, {})
        entity_groups.append({
            "entity_id": entity_id,
            "entity_type": entity.get("entity_type", ""),
            "exposure_currency": entity.get("currency_code", ""),
            "car_exemption": entity.get("car_exemption_flag", ""),
            "positions": navs
        })

    # USD PB Check
    total_usd_pb = 0
    for row in total_usd_pb_deposits_rows:
        amount = row.get("usd_pb_amount") or row.get("amount") or 0
        try:
            total_usd_pb += float(amount)
        except Exception:
            pass
    usd_pb_check = {
        "total_usd_equivalent": total_usd_pb,
        "threshold": USD_PB_THRESHOLD,
        "status": "FAIL" if total_usd_pb > USD_PB_THRESHOLD else "PASS",
        "excess_amount": max(0, total_usd_pb - USD_PB_THRESHOLD)
    }

    # Compose and return all groups as requested
    return {
        "entity_groups": entity_groups,
        "usd_pb_check": usd_pb_check,
        "currency_configuration": currency_config_rows,
        "currency_rates": currency_rates_rows,
        "booking_model_config": booking_model_config_rows,
        "murex_books": murex_books_rows,
        "additional_rates": additional_rates_rows
    }
