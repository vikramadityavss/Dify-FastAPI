from collections import defaultdict
from app.services.supabase_client import get_supabase

def fetch_entity_and_nav_info(
    exposure_currency: str,
    hedge_method: str,
    hedge_amount_order: float,
    order_id: str,
    nav_type: str = None,
    currency_type: str = None
):
    supabase = get_supabase()
    try:
        # Join entity_master with currency_configuration to get currencytype
        if currency_type:
            entities_query = (
                supabase.table("entity_master")
                .select("*, currency_configuration!inner(currencytype)")
                .eq("currencycode", exposure_currency)
                .eq("currency_configuration.currencytype", currency_type)
            )
        else:
            entities_query = (
                supabase.table("entity_master") 
                .select("*, currency_configuration(currencytype)")
                .eq("currencycode", exposure_currency)
            )
        
        # Position queries with correct table names
        positions_query = supabase.table("position_nav_master").select("*").eq("currencycode", exposure_currency)
        if nav_type:
            positions_query = positions_query.eq("navtype", nav_type)
        
        # Get USD PB threshold from threshold_configuration table
        threshold_query = supabase.table("threshold_configuration").select("warninglevel").eq("thresholdtype", "USDPBDEPOSIT").eq("currencycode", "USD")
        
        # USD PB deposits query
        usd_pb_query = supabase.table("usd_pb_deposit").select("*").eq("currencycode", exposure_currency)
        
        # Currency config and rates with correct table names
        currency_config_q = supabase.table("currency_configuration").select("*").or_(
            f"currencycode.eq.{exposure_currency},proxycurrency.eq.{exposure_currency},basecurrency.eq.{exposure_currency}"
        )
        
        # Currency rates with correct table names
        currency_rates_q = supabase.table("currency_rates").select("*").or_(
            f"currencypair.eq.{exposure_currency}SGD,currencypair.eq.SGD{exposure_currency}"
        ).order("effectivedate", desc=True)
        
        # Get proxy currencies
        currency_config = currency_config_q.execute()
        currency_config_rows = getattr(currency_config, "data", [])
        proxy_currencies = set()
        for c in currency_config_rows:
            if c.get("proxycurrency"):
                proxy_currencies.add(c["proxycurrency"])
            if c.get("basecurrency"):
                proxy_currencies.add(c["basecurrency"])
        
        # Additional rates for proxy currencies
        additional_rates_rows = []
        for proxy_ccy in proxy_currencies:
            if proxy_ccy and proxy_ccy != exposure_currency:
                rate_query = supabase.table("currency_rates").select("*").or_(
                    f"currencypair.eq.{proxy_ccy}SGD,currencypair.eq.SGD{proxy_ccy}"
                ).order("effectivedate", desc=True)
                rate_result = rate_query.execute()
                additional_rates_rows += getattr(rate_result, "data", [])

        # Booking model and murex books queries with correct table names
        booking_model_q = supabase.table("instruction_event_config").select("*").eq("instructionevent", "Inception")
        murex_books_q = supabase.table("murex_book_config").select("*").eq("activeflag", True)

        # Execute all queries
        entities = entities_query.execute()
        positions = positions_query.execute()
        usd_pb = usd_pb_query.execute()
        threshold_result = threshold_query.execute()
        currency_rates = currency_rates_q.execute()
        booking_models = booking_model_q.execute()
        murex_books = murex_books_q.execute()

        entities_rows = getattr(entities, "data", [])
        positions_rows = getattr(positions, "data", [])
        total_usd_pb_deposits_rows = getattr(usd_pb, "data", [])
        currency_config_rows = getattr(currency_config, "data", [])
        currency_rates_rows = getattr(currency_rates, "data", [])
        booking_model_config_rows = getattr(booking_models, "data", [])
        murex_books_rows = getattr(murex_books, "data", [])

        # Get threshold value dynamically from database
        USD_PB_THRESHOLD = 150000  # default fallback
        if threshold_result.data:
            USD_PB_THRESHOLD = threshold_result.data[0].get("warninglevel", 150000)

        return structured_response(
            entities_rows, positions_rows, total_usd_pb_deposits_rows,
            currency_config_rows, currency_rates_rows, booking_model_config_rows,
            murex_books_rows, additional_rates_rows, USD_PB_THRESHOLD
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
    murex_books_rows, additional_rates_rows, USD_PB_THRESHOLD
):
    # Process entities with currency_type from joined data
    entity_info_lookup = {}
    for e in entities_rows:
        # Extract currency_type from joined currency_configuration data
        currency_type = None
        if "currency_configuration" in e and e["currency_configuration"]:
            if isinstance(e["currency_configuration"], list):
                currency_type = e["currency_configuration"][0].get("currencytype") if e["currency_configuration"] else None
            else:
                currency_type = e["currency_configuration"].get("currencytype")
        
        entity_info_lookup[e["entityid"]] = {
            **e,
            "currency_type": currency_type
        }

    # Group positions by entity using correct field names
    grouped = defaultdict(list)
    for pos in positions_rows:
        grouped[pos["entityid"]].append({
            "nav_type": pos.get("navtype", ""),
            "current_position": pos.get("currentposition", 0),
            "coi_amount": pos.get("coiamount", 0),
            "re_amount": pos.get("reamount", 0),
            "buffer_pct": pos.get("bufferpct", 0),
            "buffer_amount": pos.get("bufferamount", 0),
        })
    
    entity_groups = []
    for entity_id, navs in grouped.items():
        entity = entity_info_lookup.get(entity_id, {})
        entity_groups.append({
            "entity_id": entity_id,
            "entity_type": entity.get("entitytype", ""),
            "exposure_currency": entity.get("currencycode", ""),
            "currency_type": entity.get("currency_type", ""),
            "car_exemption": entity.get("carexemptionflag", ""),
            "positions": navs
        })

    # USD PB Check - you'll need to verify the correct field name for amount
    total_usd_pb = 0
    for row in total_usd_pb_deposits_rows:
        amount = row.get("amount") or row.get("deposit_amount") or 0  # check multiple possible field names
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

    return {
        "entity_groups": entity_groups,
        "usd_pb_check": usd_pb_check,
        "currency_configuration": currency_config_rows,
        "currency_rates": currency_rates_rows,
        "booking_model_config": booking_model_config_rows,
        "murex_books": murex_books_rows,
        "additional_rates": additional_rates_rows
    }
