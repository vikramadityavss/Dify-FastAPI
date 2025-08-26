from collections import defaultdict
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
        # Join entity_master with currency_configuration to get currency_type
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
        
        # Position and NAV data
        positions_query = supabase.table("position_nav_master").select("*").eq("currency_code", exposure_currency)
        if nav_type:
            positions_query = positions_query.eq("nav_type", nav_type)
        
        # ===== STAGE 1A: CONFIGURATION TABLES =====
        
        # Buffer Configuration - CRITICAL for Stage 1A
        buffer_config_query = (
            supabase.table("buffer_configuration")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )
        
        # Waterfall Logic Configuration - CRITICAL for Stage 1A & 1B
        waterfall_config_query = (
            supabase.table("waterfall_logic_configuration")
            .select("*")
            .eq("active_flag", "Y")
            .order("waterfall_type", "priority_level")
        )
        
        # Overlay Configuration - for manual adjustments
        overlay_config_query = (
            supabase.table("overlay_configuration")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )
        
        # Hedging Framework - entity hedging rules
        hedging_framework_query = (
            supabase.table("hedging_framework")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )
        
        # System Configuration - dynamic parameters
        system_config_query = (
            supabase.table("system_configuration")
            .select("*")
            .eq("active_flag", "Y")
        )
        
        # ===== STAGE 1A & 1B: ALLOCATION AND HEDGE DATA =====
        
        # Allocation Engine - current allocations
        allocation_query = (
            supabase.table("allocation_engine")
            .select("*")
            .eq("currency_code", exposure_currency)
            .order("created_date", desc=True)
            .limit(100)
        )
        
        # Hedge Instructions - existing hedge history
        hedge_instructions_query = (
            supabase.table("hedge_instructions")
            .select("*")
            .eq("exposure_currency", exposure_currency)
            .order("created_date", desc=True)
            .limit(50)
        )
        
        # Hedge Business Events - active hedge relationships
        hedge_events_query = (
            supabase.table("hedge_business_events")
            .select("*")
            .eq("exposure_currency", exposure_currency)
            .order("event_date", desc=True)
            .limit(50)
        )
        
        # CAR Master - capital adequacy ratios
        car_master_query = (
            supabase.table("car_master")
            .select("*")
            .eq("currency_code", exposure_currency)
            .order("effective_date", desc=True)
        )
        
        # ===== STAGE 1A: THRESHOLD AND MONITORING =====
        
        # USD PB threshold
        threshold_query = (
            supabase.table("threshold_configuration")
            .select("*")
            .eq("threshold_type", "USD_PB_DEPOSIT")
            .eq("active_flag", "Y")
        )
        
        # USD PB deposits
        usd_pb_query = supabase.table("usd_pb_deposit").select("*")
        
        # Risk Monitoring
        risk_monitoring_query = (
            supabase.table("risk_monitoring")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("monitoring_status", "Active")
        )
        
        # ===== CURRENCY AND RATES DATA =====
        
        # Currency configuration
        currency_config_q = supabase.table("currency_configuration").select("*").or_(
            f"currency_code.eq.{exposure_currency},proxy_currency.eq.{exposure_currency}"
        )
        
        # Currency rates
        currency_rates_q = supabase.table("currency_rates").select("*").or_(
            f"currency_pair.eq.{exposure_currency}SGD,currency_pair.eq.SGD{exposure_currency}"
        ).order("effective_date", desc=True).limit(20)
        
        # Proxy Configuration
        proxy_config_query = (
            supabase.table("proxy_configuration")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )
        
        # ===== STAGE 2: BOOKING AND EXECUTION =====
        
        # Booking model configuration
        booking_model_q = (
            supabase.table("instruction_event_config")
            .select("*")
            .eq("instruction_event", "Initiation")
        )
        
        if nav_type:
            booking_model_q = booking_model_q.eq("nav_type", nav_type)
        if currency_type:
            booking_model_q = booking_model_q.eq("currency_type", currency_type)
            
        # Murex book configuration
        murex_books_q = (
            supabase.table("murex_book_config")
            .select("*")
            .eq("active_flag", True)
        )
        
        # Hedge Instruments
        hedge_instruments_query = (
            supabase.table("hedge_instruments")
            .select("*")
            .eq("currency_code", exposure_currency)
            .eq("active_flag", "Y")
        )
        
        # Hedge Effectiveness
        hedge_effectiveness_query = (
            supabase.table("hedge_effectiveness")
            .select("*")
            .eq("currency_code", exposure_currency)
            .order("effectiveness_date", desc=True)
            .limit(10)
        )
        
        # ===== PROXY CURRENCIES HANDLING =====
        
        # Get proxy currencies from currency_configuration
        currency_config = currency_config_q.execute()
        currency_config_rows = getattr(currency_config, "data", [])
        proxy_currencies = set()
        for c in currency_config_rows:
            if c.get("proxy_currency"):
                proxy_currencies.add(c["proxy_currency"])
        
        # Additional rates for proxy currencies
        additional_rates_rows = []
        for proxy_ccy in proxy_currencies:
            if proxy_ccy and proxy_ccy != exposure_currency:
                rate_query = supabase.table("currency_rates").select("*").or_(
                    f"currency_pair.eq.{proxy_ccy}SGD,currency_pair.eq.SGD{proxy_ccy}"
                ).order("effective_date", desc=True).limit(10)
                rate_result = rate_query.execute()
                additional_rates_rows += getattr(rate_result, "data", [])

        # ===== EXECUTE ALL QUERIES =====
        
        # Core data
        entities = entities_query.execute()
        positions = positions_query.execute()
        
        # Stage 1A Configuration
        buffer_config = buffer_config_query.execute()
        waterfall_config = waterfall_config_query.execute()
        overlay_config = overlay_config_query.execute()
        hedging_framework = hedging_framework_query.execute()
        system_config = system_config_query.execute()
        
        # Allocation and Hedge data
        allocations = allocation_query.execute()
        hedge_instructions = hedge_instructions_query.execute()
        hedge_events = hedge_events_query.execute()
        car_master = car_master_query.execute()
        
        # Thresholds and monitoring
        threshold_result = threshold_query.execute()
        usd_pb = usd_pb_query.execute()
        risk_monitoring = risk_monitoring_query.execute()
        
        # Currency and rates
        currency_rates = currency_rates_q.execute()
        proxy_config = proxy_config_query.execute()
        
        # Stage 2 booking
        booking_models = booking_model_q.execute()
        murex_books = murex_books_q.execute()
        hedge_instruments = hedge_instruments_query.execute()
        hedge_effectiveness = hedge_effectiveness_query.execute()
        
        # ===== EXTRACT DATA =====
        
        entities_rows = getattr(entities, "data", [])
        positions_rows = getattr(positions, "data", [])
        buffer_config_rows = getattr(buffer_config, "data", [])
        waterfall_config_rows = getattr(waterfall_config, "data", [])
        overlay_config_rows = getattr(overlay_config, "data", [])
        hedging_framework_rows = getattr(hedging_framework, "data", [])
        system_config_rows = getattr(system_config, "data", [])
        allocations_rows = getattr(allocations, "data", [])
        hedge_instructions_rows = getattr(hedge_instructions, "data", [])
        hedge_events_rows = getattr(hedge_events, "data", [])
        car_master_rows = getattr(car_master, "data", [])
        total_usd_pb_deposits_rows = getattr(usd_pb, "data", [])
        risk_monitoring_rows = getattr(risk_monitoring, "data", [])
        currency_rates_rows = getattr(currency_rates, "data", [])
        proxy_config_rows = getattr(proxy_config, "data", [])
        booking_model_config_rows = getattr(booking_models, "data", [])
        murex_books_rows = getattr(murex_books, "data", [])
        hedge_instruments_rows = getattr(hedge_instruments, "data", [])
        hedge_effectiveness_rows = getattr(hedge_effectiveness, "data", [])

        # Get threshold value dynamically
        USD_PB_THRESHOLD = 150000  # default fallback
        if threshold_result.data:
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
    # Process entities with currency_type from joined data
    entity_info_lookup = {}
    for e in entities_rows:
        currency_type = None
        if "currency_configuration" in e and e["currency_configuration"]:
            if isinstance(e["currency_configuration"], list):
                currency_type = e["currency_configuration"][0].get("currency_type") if e["currency_configuration"] else None
            else:
                currency_type = e["currency_configuration"].get("currency_type")
        
        entity_info_lookup[e["entity_id"]] = {
            **e,
            "currency_type": currency_type
        }

    # Process allocation data by entity
    allocation_lookup = defaultdict(list)
    for alloc in allocations_rows:
        allocation_lookup[alloc["entity_id"]].append(alloc)

    # Process hedge events by entity
    hedge_relationships = defaultdict(list)
    for event in hedge_events_rows:
        entity_id = event.get("entity_id")
        if entity_id:
            hedge_relationships[entity_id].append(event)

    # Process framework rules by entity
    framework_rules = {}
    for rule in hedging_framework_rows:
        entity_id = rule.get("entity_id")
        if entity_id:
            framework_rules[entity_id] = rule

    # Process buffer configuration by entity
    buffer_rules = {}
    for buffer in buffer_config_rows:
        entity_id = buffer.get("entity_id")
        if entity_id:
            buffer_rules[entity_id] = buffer

    # Process CAR data by entity
    car_data = {}
    for car in car_master_rows:
        entity_id = car.get("entity_id")
        if entity_id:
            car_data[entity_id] = car

    # Group positions by entity with complete hedging state
    grouped = defaultdict(list)
    for pos in positions_rows:
        entity_id = pos["entity_id"]
        
        # Get all related data for this entity
        entity_allocations = allocation_lookup.get(entity_id, [])
        latest_allocation = entity_allocations[0] if entity_allocations else {}
        entity_hedge_relationships = hedge_relationships.get(entity_id, [])
        framework_rule = framework_rules.get(entity_id, {})
        buffer_rule = buffer_rules.get(entity_id, {})
        car_info = car_data.get(entity_id, {})
        
        # Calculate complete hedging state
        hedging_state = calculate_complete_hedging_state(
            pos, latest_allocation, entity_hedge_relationships, 
            framework_rule, buffer_rule, car_info
        )
        
        grouped[entity_id].append({
            # Position data
            "nav_type": pos.get("nav_type", ""),
            "current_position": pos.get("current_position", 0),
            "computed_total_nav": pos.get("computed_total_nav", 0),
            "optimal_car_amount": pos.get("optimal_car_amount", 0),
            "buffer_percentage": pos.get("buffer_percentage", 0),
            "buffer_amount": pos.get("buffer_amount", 0),
            "manual_overlay": pos.get("manual_overlay", 0),
            "allocation_status": pos.get("allocation_status", "Pending"),
            # Complete hedging state
            "hedging_state": hedging_state,
            "allocation_data": entity_allocations,
            "hedge_relationships": entity_hedge_relationships,
            "framework_rule": framework_rule,
            "buffer_rule": buffer_rule,
            "car_data": car_info
        })
    
    # Build entity groups
    entity_groups = []
    for entity_id, navs in grouped.items():
        entity = entity_info_lookup.get(entity_id, {})
        entity_groups.append({
            "entity_id": entity_id,
            "entity_name": entity.get("entity_name", ""),
            "entity_type": entity.get("entity_type", ""),
            "exposure_currency": entity.get("currency_code", ""),
            "currency_type": entity.get("currency_type", ""),
            "car_exemption": entity.get("car_exemption_flag", ""),
            "parent_child_nav_link": entity.get("parent_child_nav_link", False),
            "positions": navs
        })

    # USD PB Check
    total_usd_pb = 0
    for row in total_usd_pb_deposits_rows:
        amount = row.get("total_usd_deposits", 0)
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

    # Organize waterfall configuration by type
    waterfall_rules = {
        "opening": [w for w in waterfall_config_rows if w.get("waterfall_type") == "Opening"],
        "closing": [w for w in waterfall_config_rows if w.get("waterfall_type") == "Closing"]
    }

    return {
        # Core entity and position data
        "entity_groups": entity_groups,
        
        # Stage 1A Configuration Data
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
        
        # Stage 1B Allocation Data
        "stage_1b_data": {
            "current_allocations": allocations_rows,
            "hedge_instructions_history": hedge_instructions_rows,
            "active_hedge_events": hedge_events_rows,
            "car_master_data": car_master_rows
        },
        
        # Stage 2 Booking Configuration
        "stage_2_config": {
            "booking_model_config": booking_model_config_rows,
            "murex_books": murex_books_rows,
            "hedge_instruments": hedge_instruments_rows,
            "hedge_effectiveness": hedge_effectiveness_rows
        },
        
        # Risk and Monitoring
        "risk_monitoring": risk_monitoring_rows,
        
        # Currency and Rates
        "currency_configuration": currency_config_rows,
        "currency_rates": currency_rates_rows,
        "proxy_configuration": proxy_config_rows,
        "additional_rates": additional_rates_rows
    }

def calculate_complete_hedging_state(position, allocation, hedge_relationships, framework_rule, buffer_rule, car_info):
    """Calculate comprehensive hedging state for an entity position"""
    current_position = float(position.get("current_position", 0))
    allocated_amount = float(allocation.get("hedge_amount_allocation", 0))
    available_for_hedging = float(allocation.get("available_amount_for_hedging", 0))
    hedged_position = float(allocation.get("hedged_position", 0))
    car_amount = float(allocation.get("car_amount_distribution", 0))
    manual_overlay = float(allocation.get("manual_overlay_amount", 0))
    buffer_amount = float(allocation.get("buffer_amount", 0))
    
    # Calculate hedge utilization
    hedge_utilization_pct = 0
    if current_position > 0:
        hedge_utilization_pct = (hedged_position / current_position) * 100
    
    # Determine hedging status
    hedging_status = "Available"
    if hedged_position >= current_position:
        hedging_status = "Fully_Hedged"
    elif hedged_position > 0:
        hedging_status = "Partially_Hedged"
    elif available_for_hedging <= 0:
        hedging_status = "Not_Available"
    
    # Framework compliance
    framework_type = framework_rule.get("framework_type", "Not_Defined")
    buffer_percentage = buffer_rule.get("buffer_percentage", 0)
    car_exemption = framework_rule.get("car_exemption_flag", "N")
    
    # Calculate available amount formula
    # Available Amount = SFX Position - CAR Amount + Manual Overlay - Buffer Amount - Hedged Position
    calculated_available = current_position - car_amount + manual_overlay - buffer_amount - hedged_position
    
    return {
        # Current hedge state
        "already_hedged_amount": hedged_position,
        "available_for_hedging": available_for_hedging,
        "calculated_available_amount": calculated_available,
        "hedge_utilization_pct": round(hedge_utilization_pct, 2),
        "hedging_status": hedging_status,
        
        # Allocation details
        "car_amount_distribution": car_amount,
        "manual_overlay_amount": manual_overlay,
        "buffer_amount": buffer_amount,
        "buffer_percentage": buffer_percentage,
        
        # Framework and compliance
        "framework_type": framework_type,
        "car_exemption_flag": car_exemption,
        "framework_compliance": framework_type,
        
        # Allocation metadata
        "last_allocation_date": allocation.get("created_date"),
        "waterfall_priority": allocation.get("waterfall_priority"),
        "allocation_sequence": allocation.get("allocation_sequence"),
        "allocation_status": allocation.get("allocation_status", "Pending"),
        
        # Hedge relationships
        "active_hedge_count": len(hedge_relationships),
        "total_hedge_notional": sum(float(h.get("notional_amount", 0)) for h in hedge_relationships)
    }