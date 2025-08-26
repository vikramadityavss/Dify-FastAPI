from collections import defaultdict
from datetime import date
from app.services.supabase_client import get_supabase

def fetch_complete_hedge_data(
    exposure_currency: str,
    hedge_method: str,
    hedge_amount_order: float,
    order_id: str,
    nav_type: str = None,
    currency_type: str = None
):
    supabase = get_supabase()
    try:
        # ===== CORE ENTITY AND POSITION DATA =====
        if currency_type:
            entities_query = (
                supabase.table("entity_master")
                .select("*, currency_configuration!inner(currency_type)")
                .eq("currency_code", exposure_currency)
                .eq("currency_configuration.currency_type", currency_type)
            )
        else:
            entities_query = (
                supabase.table("entity_master")
                .select("*, currency_configuration(currency_type)")
                .eq("currency_code", exposure_currency)
            )

        positions_query = (
            supabase.table("position_nav_master")
            .select("*")
            .eq("currency_code", exposure_currency)
        )
        if nav_type:
            positions_query = positions_query.eq("nav_type", nav_type)

        entities = entities_query.execute()
        positions = positions_query.execute()
        entities_rows = getattr(entities, "data", []) or []
        positions_rows = getattr(positions, "data", []) or []
        entity_ids = {e["entity_id"] for e in entities_rows if e.get("entity_id")} or {
            p["entity_id"] for p in positions_rows if p.get("entity_id")
        }

        # ===== STAGE 1A: CONFIGURATION TABLES =====
        buffer_config_query = (
            supabase.table("buffer_configuration")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )

        waterfall_config_query = (
            supabase.table("waterfall_logic_configuration")
            .select("*")
            .eq("active_flag", "Y")
            .order("waterfall_type")
            .order("priority_level")
        )

        overlay_config_query = (
            supabase.table("overlay_configuration")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )

        hedging_framework_query = (
            supabase.table("hedging_framework")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )

        system_config_query = supabase.table("system_configuration").select("*").eq("active_flag", "Y")

        # ===== STAGE 1A & 1B: ALLOCATION AND HEDGE DATA =====
        allocation_query = (
            supabase.table("allocation_engine")
            .select("*")
            .eq("currency_code", exposure_currency)
            .order("created_date", desc=True)
            .limit(100)
        )

        hedge_instructions_query = (
            supabase.table("hedge_instructions")
            .select("*")
            .eq("exposure_currency", exposure_currency)
            .order("instruction_date", desc=True)
            .order("created_date", desc=True)
            .limit(50)
        )

        # hedge_business_events (schema-aligned)
        hedge_events_query = supabase.table("hedge_business_events").select("*").limit(50)
        if entity_ids:
            hedge_events_query = hedge_events_query.in_("entity_id", list(entity_ids))
        if nav_type:
            hedge_events_query = hedge_events_query.eq("nav_type", nav_type)
        hedge_events_query = hedge_events_query.order("trade_date", desc=True).order("created_date", desc=True)

        # car_master (schema-aligned)
        car_master_query = (
            supabase.table("car_master")
            .select("*")
            .eq("currency_code", exposure_currency)
            .order("reporting_date", desc=True)
        )

        # ===== STAGE 1A: THRESHOLD AND MONITORING =====
        threshold_query = (
            supabase.table("threshold_configuration")
            .select("*")
            .eq("threshold_type", "USD_PB_DEPOSIT")
            .eq("active_flag", "Y")
        )
        usd_pb_query = supabase.table("usd_pb_deposit").select("*")

        # risk_monitoring (schema-aligned)
        risk_monitoring_query = (
            supabase.table("risk_monitoring")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("resolution_status", "Open")
            .order("measurement_timestamp", desc=True)
        )

        # ===== CURRENCY AND RATES DATA =====
        currency_config_q = supabase.table("currency_configuration").select("*").or_(
            f"currency_code.eq.{exposure_currency},proxy_currency.eq.{exposure_currency}"
        )

        currency_rates_q = (
            supabase.table("currency_rates")
            .select("*")
            .or_(f"currency_pair.eq.{exposure_currency}SGD,currency_pair.eq.SGD{exposure_currency}")
            .order("effective_date", desc=True)
            .limit(20)
        )

        # proxy_configuration (schema-aligned)
        today = date.today().isoformat()
        proxy_config_query = (
            supabase.table("proxy_configuration")
            .select("*")
            .eq("exposure_currency", exposure_currency)
            .eq("active_flag", "Y")
            .lte("effective_date", today)
            .order("effective_date", desc=True)
        )

        # ===== STAGE 2: BOOKING AND EXECUTION =====
        booking_model_q = (
            supabase.table("instruction_event_config")
            .select("*")
            .eq("instruction_event", "Initiation")
        )
        if nav_type:
            booking_model_q = booking_model_q.eq("nav_type", nav_type)
        if currency_type:
            booking_model_q = booking_model_q.eq("currency_type", currency_type)

        murex_books_q = (
            supabase.table("murex_book_config")
            .select("*")
            .eq("active_flag", True)   # boolean per schema
        )

        # ===== hedge_instruments (FIXED to your schema) =====
        hi_q = supabase.table("hedge_instruments").select("*")

        # Active and effective
        hi_q = (
            hi_q
            .eq("active_flag", "Y")
            .lte("effective_date", today)
        )

        # Match instrument to the exposure currency: in pair OR as base/quote
        hi_q = hi_q.or_(
            f"base_currency.eq.{exposure_currency},quote_currency.eq.{exposure_currency},currency_pair.cs.{exposure_currency}"
        )

        # Match currency classification if provided (Matched/Mismatched/…)
        if currency_type:
            hi_q = hi_q.eq("currency_classification", currency_type)

        # Match nav_type_applicable (Both or exact)
        if nav_type:
            hi_q = hi_q.in_("nav_type_applicable", ["Both", nav_type])
        else:
            hi_q = hi_q.in_("nav_type_applicable", ["Both", "COI", "RE"])

        # Match accounting_method_supported (Both or exact)
        if hedge_method:
            hi_q = hi_q.in_("accounting_method_supported", ["Both", hedge_method])
        else:
            hi_q = hi_q.in_("accounting_method_supported", ["Both", "COH", "MTM"])

        # Prefer most recent instruments first
        hedge_instruments_query = hi_q.order("effective_date", desc=True)

        hedge_effectiveness_query = (
            supabase.table("hedge_effectiveness")
            .select("*")
            .eq("currency_code", exposure_currency)
            .order("effectiveness_date", desc=True)
            .limit(10)
        )

        # ===== PROXY CURRENCIES HANDLING =====
        currency_config = currency_config_q.execute()
        currency_config_rows = getattr(currency_config, "data", []) or []
        proxy_currencies = {c.get("proxy_currency") for c in currency_config_rows if c.get("proxy_currency")}
        proxy_currencies.discard(exposure_currency)

        additional_rates_rows = []
        for proxy_ccy in proxy_currencies:
            rate_result = (
                supabase.table("currency_rates")
                .select("*")
                .or_(f"currency_pair.eq.{proxy_ccy}SGD,currency_pair.eq.SGD{proxy_ccy}")
                .order("effective_date", desc=True)
                .limit(10)
                .execute()
            )
            additional_rates_rows += getattr(rate_result, "data", []) or []

        # ===== EXECUTE REMAINING QUERIES =====
        buffer_config = buffer_config_query.execute()
        waterfall_config = waterfall_config_query.execute()
        overlay_config = overlay_config_query.execute()
        hedging_framework = hedging_framework_query.execute()
        system_config = system_config_query.execute()

        allocations = allocation_query.execute()
        hedge_instructions = hedge_instructions_query.execute()
        hedge_events = hedge_events_query.execute()
        car_master = car_master_query.execute()

        threshold_result = threshold_query.execute()
        usd_pb = usd_pb_query.execute()
        risk_monitoring = risk_monitoring_query.execute()

        currency_rates = currency_rates_q.execute()
        proxy_config = proxy_config_query.execute()

        booking_models = booking_model_q.execute()
        murex_books = murex_books_q.execute()
        hedge_instruments = hedge_instruments_query.execute()
        hedge_effectiveness = hedge_effectiveness_query.execute()

        # ===== EXTRACT DATA =====
        buffer_config_rows = getattr(buffer_config, "data", []) or []
        waterfall_config_rows = getattr(waterfall_config, "data", []) or []
        overlay_config_rows = getattr(overlay_config, "data", []) or []
        hedging_framework_rows = getattr(hedging_framework, "data", []) or []
        system_config_rows = getattr(system_config, "data", []) or []

        allocations_rows = getattr(allocations, "data", []) or []
        hedge_instructions_rows = getattr(hedge_instructions, "data", []) or []
        hedge_events_rows = getattr(hedge_events, "data", []) or []
        car_master_rows = getattr(car_master, "data", []) or []

        total_usd_pb_deposits_rows = getattr(usd_pb, "data", []) or []
        risk_monitoring_rows = getattr(risk_monitoring, "data", []) or []
        currency_rates_rows = getattr(currency_rates, "data", []) or []
        proxy_config_rows = getattr(proxy_config, "data", []) or []
        booking_model_config_rows = getattr(booking_models, "data", []) or []
        murex_books_rows = getattr(murex_books, "data", []) or []
        hedge_instruments_rows = getattr(hedge_instruments, "data", []) or []
        hedge_effectiveness_rows = getattr(hedge_effectiveness, "data", []) or []

        # USD PB threshold
        USD_PB_THRESHOLD = 150000
        if getattr(threshold_result, "data", None):
            USD_PB_THRESHOLD = threshold_result.data[0].get("warning_level", 150000)

        return complete_structured_response(
            # Core data
            entities_rows, positions_rows, currency_config_rows,
            # Stage 1A Configuration
            buffer_config_rows, waterfall_config_rows, overlay_config_rows,
            hedging_framework_rows, system_config_rows,
            # Allocation and hedge data
            allocations_rows, hedge_instructions_rows, hedge_events_rows, car_master_rows,
            # Thresholds and monitoring
            total_usd_pb_deposits_rows, risk_monitoring_rows, USD_PB_THRESHOLD,
            # Currency and rates
            currency_rates_rows, proxy_config_rows, additional_rates_rows,
            # Stage 2 booking
            booking_model_config_rows, murex_books_rows, hedge_instruments_rows,
            hedge_effectiveness_rows
        )

    except Exception as e:
        print("============================")
        print("Complete Data Fetch Error:", str(e))
        print("============================\n")
        return {
            "entity_groups": [],
            "stage_1a_config": {},
            "stage_1b_data": {},
            "stage_2_config": {},
            "hedging_state": {},
            "risk_monitoring": {},
            "usd_pb_check": {},
            "error": str(e)
        }

def complete_structured_response(
    entities_rows, positions_rows, currency_config_rows,
    buffer_config_rows, waterfall_config_rows, overlay_config_rows,
    hedging_framework_rows, system_config_rows,
    allocations_rows, hedge_instructions_rows, hedge_events_rows, car_master_rows,
    total_usd_pb_deposits_rows, risk_monitoring_rows, USD_PB_THRESHOLD,
    currency_rates_rows, proxy_config_rows, additional_rates_rows,
    booking_model_config_rows, murex_books_rows, hedge_instruments_rows,
    hedge_effectiveness_rows
):
    # Entities with currency_type
    entity_info_lookup = {}
    for e in entities_rows:
        c_type = None
        if "currency_configuration" in e and e["currency_configuration"]:
            if isinstance(e["currency_configuration"], list):
                c_type = e["currency_configuration"][0].get("currency_type") if e["currency_configuration"] else None
            else:
                c_type = e["currency_configuration"].get("currency_type")
        if e.get("entity_id"):
            entity_info_lookup[e["entity_id"]] = {**e, "currency_type": c_type}

    # Allocations by entity
    allocation_lookup = defaultdict(list)
    for alloc in allocations_rows:
        eid = alloc.get("entity_id")
        if eid:
            allocation_lookup[eid].append(alloc)

    # Hedge events by entity
    hedge_relationships = defaultdict(list)
    for event in hedge_events_rows:
        eid = event.get("entity_id")
        if eid:
            hedge_relationships[eid].append(event)

    # Framework rules by entity
    framework_rules = {}
    for rule in hedging_framework_rows:
        eid = rule.get("entity_id")
        if eid:
            framework_rules[eid] = rule

    # Buffer rules by entity
    buffer_rules = {}
    for br in buffer_config_rows:
        eid = br.get("entity_id")
        if eid:
            buffer_rules[eid] = br

    # CAR latest per entity (already ordered desc)
    car_data = {}
    for car in car_master_rows:
        eid = car.get("entity_id")
        if eid and eid not in car_data:
            car_data[eid] = car

    # Group positions + compute state
    grouped = defaultdict(list)
    for pos in positions_rows:
        eid = pos.get("entity_id")
        if not eid:
            continue

        entity_allocations = allocation_lookup.get(eid, [])
        latest_allocation = entity_allocations[0] if entity_allocations else {}
        entity_hedge_relationships = hedge_relationships.get(eid, [])
        framework_rule = framework_rules.get(eid, {})
        buffer_rule = buffer_rules.get(eid, {})
        car_info = car_data.get(eid, {})

        hedging_state = calculate_complete_hedging_state(
            pos, latest_allocation, entity_hedge_relationships, framework_rule, buffer_rule, car_info
        )

        grouped[eid].append({
            "nav_type": pos.get("nav_type", ""),
            "current_position": pos.get("current_position", 0),
            "computed_total_nav": pos.get("computed_total_nav", 0),
            "optimal_car_amount": pos.get("optimal_car_amount", 0),
            "buffer_percentage": pos.get("buffer_percentage", 0),
            "buffer_amount": pos.get("buffer_amount", 0),
            "manual_overlay": pos.get("manual_overlay", 0),
            "allocation_status": pos.get("allocation_status", "Pending"),
            "hedging_state": hedging_state,
            "allocation_data": entity_allocations,
            "hedge_relationships": entity_hedge_relationships,
            "framework_rule": framework_rule,
            "buffer_rule": buffer_rule,
            "car_data": car_info
        })

    # Entity groups
    entity_groups = []
    for eid, navs in grouped.items():
        entity = entity_info_lookup.get(eid, {})
        entity_groups.append({
            "entity_id": eid,
            "entity_name": entity.get("entity_name", ""),
            "entity_type": entity.get("entity_type", ""),
            "exposure_currency": entity.get("currency_code", ""),
            "currency_type": entity.get("currency_type", ""),
            "car_exemption": entity.get("car_exemption_flag", ""),
            "parent_child_nav_link": entity.get("parent_child_nav_link", False),
            "positions": navs
        })

    # USD PB Check
    total_usd_pb = 0.0
    for row in total_usd_pb_deposits_rows:
        try:
            total_usd_pb += float(row.get("total_usd_deposits", 0) or 0)
        except Exception:
            pass

    usd_pb_check = {
        "total_usd_equivalent": total_usd_pb,
        "threshold": USD_PB_THRESHOLD,
        "status": "FAIL" if total_usd_pb > USD_PB_THRESHOLD else "PASS",
        "excess_amount": max(0.0, total_usd_pb - USD_PB_THRESHOLD)
    }

    # Waterfall split
    waterfall_rules = {
        "opening": [w for w in (waterfall_config_rows or []) if w.get("waterfall_type") == "Opening"],
        "closing": [w for w in (waterfall_config_rows or []) if w.get("waterfall_type") == "Closing"]
    }

    return {
        "entity_groups": entity_groups,
        "stage_1a_config": {
            "buffer_configuration": buffer_config_rows,
            "waterfall_logic": waterfall_rules,
            "overlay_configuration": overlay_config_rows,
            "hedging_framework": hedging_framework_rows,
            "system_configuration": system_config_rows,
            "threshold_configuration": {
                "usd_pb_threshold": USD_PB_THRESHOLD,
                "usd_pb_check": usd_pb_check
            }
        },
        "stage_1b_data": {
            "current_allocations": allocations_rows,
            "hedge_instructions_history": hedge_instructions_rows,
            "active_hedge_events": hedge_relationships,  # keyed by entity_id
            "car_master_data": car_master_rows
        },
        "stage_2_config": {
            "booking_model_config": booking_model_config_rows,
            "murex_books": murex_books_rows,
            "hedge_instruments": hedge_instruments_rows,
            "hedge_effectiveness": hedge_effectiveness_rows
        },
        "risk_monitoring": risk_monitoring_rows,
        "currency_configuration": currency_config_rows,
        "currency_rates": currency_rates_rows,
        "proxy_configuration": proxy_config_rows,
        "additional_rates": additional_rates_rows
    }

def calculate_complete_hedging_state(position, allocation, hedge_relationships, framework_rule, buffer_rule, car_info):
    """Comprehensive hedging state for an entity position"""
    current_position = float(position.get("current_position", 0) or 0)
    _ = float(allocation.get("hedge_amount_allocation", 0) or 0)  # placeholder for future logic
    available_for_hedging = float(allocation.get("available_amount_for_hedging", 0) or 0)
    hedged_position = float(allocation.get("hedged_position", 0) or 0)
    car_amount = float(allocation.get("car_amount_distribution", 0) or 0)
    manual_overlay = float(allocation.get("manual_overlay_amount", 0) or 0)
    buffer_amount = float(allocation.get("buffer_amount", 0) or 0)

    hedge_utilization_pct = 0.0
    if current_position > 0:
        hedge_utilization_pct = (hedged_position / current_position) * 100.0

    if hedged_position >= current_position:
        hedging_status = "Fully_Hedged"
    elif hedged_position > 0:
        hedging_status = "Partially_Hedged"
    elif available_for_hedging <= 0:
        hedging_status = "Not_Available"
    else:
        hedging_status = "Available"

    framework_type = framework_rule.get("framework_type", "Not_Defined")
    buffer_percentage = buffer_rule.get("buffer_percentage", position.get("buffer_percentage", 0))
    car_exemption = framework_rule.get("car_exemption_flag", framework_rule.get("car_exemption_override", "N"))

    # Available = Position - CAR + Overlay - Buffer - Hedged
    calculated_available = current_position - car_amount + manual_overlay - buffer_amount - hedged_position

    return {
        "already_hedged_amount": hedged_position,
        "available_for_hedging": available_for_hedging,
        "calculated_available_amount": calculated_available,
        "hedge_utilization_pct": round(hedge_utilization_pct, 2),
        "hedging_status": hedging_status,
        "car_amount_distribution": car_amount,
        "manual_overlay_amount": manual_overlay,
        "buffer_amount": buffer_amount,
        "buffer_percentage": buffer_percentage,
        "framework_type": framework_type,
        "car_exemption_flag": car_exemption,
        "framework_compliance": framework_type,
        "last_allocation_date": allocation.get("created_date"),
        "waterfall_priority": allocation.get("waterfall_priority"),
        "allocation_sequence": allocation.get("allocation_sequence"),
        "allocation_status": allocation.get("allocation_status", "Pending"),
        "active_hedge_count": len(hedge_relationships or []),
        "total_hedge_notional": sum(float(h.get("notional_amount", 0) or 0) for h in (hedge_relationships or []))
    }
