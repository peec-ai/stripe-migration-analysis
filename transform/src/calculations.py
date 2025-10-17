import pandas as pd

# --- 1. Constants and Pricing Plans ---

GUARDRAIL_ORG_COUNT = False

# Using dictionaries for plans. Prices are in cents, matching the source data.
BRAND_PLANS = {
    "starter": {
        "price": 89,
        "credits": 3560,
        "price_per_credit": 89 / 3560,
        "min_amount": 1,
        "max_org_count": 1,
    },
    "pro": {
        "price": 199,
        "credits": 14925,
        "price_per_credit": 199 / 14925,
        "min_amount": 1,
        "max_org_count": 3,
    },
    "enterprise": {
        "price": 499,
        "credits": 49900,
        "price_per_credit": 499 / 49900,
        "min_amount": 1,
        "max_org_count": 5,
    },
}

AGENCY_PLANS = {
    "intro": {
        "price": 89,
        "credits": 2250,
        "price_per_credit": 89 / 2250,
        "min_amount": 1,
        "max_org_count": 10,
    },
    "growth": {
        "price": 199,
        "credits": 12935,
        "price_per_credit": 199 / 12935,
        "min_amount": 1,
        "max_org_count": 30,
    },
    "scale": {
        "price": 499,
        "credits": 37425,
        "price_per_credit": 499 / 37425,
        "min_amount": 1,
        "max_org_count": 50,
    },
}

MODEL_ID_PRICE_MAP = {
    "gpt-4o": 1,
    "chatgpt": 1,
    "sonar": 1,
    "google-ai-overview": 1,
    "llama-3-3-70b-instruct": 0.5,
    "gpt-4o-search": 1,
    "claude-sonnet-4": 2,
    "claude-3-5-haiku": 2,
    "gemini-1-5-flash": 1,
    "deepseek-r1": 1,
    "gemini-2-5-flash": 2,
    "google-ai-mode": 1,
    "grok-2-1212": 2,
    "gpt-3-5-turbo": 1,
}

# --- 2. Core Calculation Logic ---


def calculate_scenarios_for_company(company_data: pd.Series) -> pd.Series:
    """
    Calculates migration scenarios for a single company.
    Designed to be applied to a row of a Pandas DataFrame.
    """
    # Extract inputs
    company_type = company_data["type"]
    current_mrr = company_data["current_mrr"]
    credits_capacity = company_data["credits_capacity"]
    orgs_count = company_data["orgs_count"]

    all_plans = BRAND_PLANS if company_type == "IN_HOUSE" else AGENCY_PLANS

    # Filter plans based on the number of organizations
    plans = {
        name: p for name, p in all_plans.items() if not GUARDRAIL_ORG_COUNT or p["max_org_count"] >= orgs_count
    }

    # If no plan can support the org count, find the one with the highest org count capacity.
    if not plans:
        largest_plan_name = max(all_plans, key=lambda k: all_plans[k]["max_org_count"])
        plans = {largest_plan_name: all_plans[largest_plan_name]}

    # --- Least Cost Scenario ---
    options = []
    for name, plan in plans.items():
        extra_credits_needed = max(0, credits_capacity - plan["credits"])
        if extra_credits_needed > 0 and extra_credits_needed < plan["min_amount"]:
            extra_credits_needed = plan["min_amount"]

        cost = plan["price"] + extra_credits_needed * plan["price_per_credit"]
        total_credits = plan["credits"] + extra_credits_needed

        options.append(
            {
                "plan_name": f"{name} ({plan['credits']})",
                "cost": cost,
                "extra_credits": extra_credits_needed,
                "surplus_credits": total_credits - credits_capacity,
            }
        )

    best_least_cost = min(options, key=lambda x: x["cost"])

    # --- Compile Results ---
    result = {
        "plan_name": best_least_cost["plan_name"],
        "mrr": int(best_least_cost["cost"]),
        "mrr_change": int(best_least_cost["cost"] - current_mrr),
        "arr_change": int((best_least_cost["cost"] - current_mrr) * 12),
        "extra_credits_purchased": int(best_least_cost["extra_credits"]),
        "surplus_credits": int(best_least_cost["surplus_credits"]),
    }
    return pd.Series(result)


def calculate_credits_usage(row: pd.Series) -> int:
    """
    Calculates the required credits for a given row (organization) based on model usage and run frequency.
    """
    runs_per_month = 30

    model_prices = [MODEL_ID_PRICE_MAP.get(mid, 0) for mid in row["model_ids"]]
    return int(sum(model_prices) * row["prompts_count"] * runs_per_month)

def calculate_credits_capacity(row: pd.Series) -> int:
    """
    Calculates the required credits for a given row (organization) based on prompt capacity and run frequency.
    """
    runs_per_month = 30

    model_prices = [MODEL_ID_PRICE_MAP.get(mid, 0) for mid in row["model_ids"]]
    return int(sum(model_prices) * row["prompt_limit"] * runs_per_month)

def calculate_coupon_multiplier(coupon_ids: list, coupons_map: dict) -> tuple[float, int, int]:
    """
    Calculate the discount multiplier for a list of coupon IDs.
    Only accounts for coupons that are forever or repeating for 12+ months.
    Returns a tuple of (multiplier, long_term_count, total_count) where:
    - multiplier: the multiplier to apply to the price (e.g., 0.8 for 20% off)
    - long_term_count: the number of long-term discounts applied (forever or >=12 months)
    - total_count: the total number of valid discounts (including short-term)
    """
    if not coupon_ids:
        return 1.0, 0, 0
    
    total_percent_off = 0.0
    total_amount_off = 0.0
    long_term_discount_count = 0
    total_discount_count = 0
    
    for coupon_id in coupon_ids:
        coupon = coupons_map.get(coupon_id)
        if not coupon:
            continue
        
        # Count this as a valid discount
        total_discount_count += 1
        
        # Only account for long-term discounts (forever or repeating >= 12 months)
        duration = coupon.get('duration')
        duration_in_months = coupon.get('duration_in_months')
        
        should_account = (
            duration == 'forever' or 
            (duration == 'repeating' and duration_in_months and duration_in_months >= 12)
        )
        
        if not should_account:
            continue
        
        long_term_discount_count += 1
        
        # Apply percentage discount
        if coupon.get('percent_off'):
            total_percent_off += coupon['percent_off']
        
        # Amount off is handled separately (we'll need to know the price)
        if coupon.get('amount_off'):
            total_amount_off += coupon['amount_off']
    
    # Calculate multiplier (assuming no amount_off for now, as it's price-dependent)
    # Percent discounts stack additively (e.g., 10% + 20% = 30% off)
    multiplier = 1.0 - (total_percent_off / 100.0)
    
    return max(0.0, multiplier), long_term_discount_count, total_discount_count