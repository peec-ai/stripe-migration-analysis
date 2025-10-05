import pandas as pd
import math

# --- 1. Constants and Pricing Plans ---

# Using dictionaries for plans. Prices are in cents, matching the source data.
BRAND_PLANS = {
    "starter": {
        "price": 89,
        "credits": 4450,
        "price_per_credit": 89 / 4450,
        "min_amount": 1000,
        "max_org_count": 1,
    },
    "pro": {
        "price": 199,
        "credits": 14925,
        "price_per_credit": 199 / 14925,
        "min_amount": 1000,
        "max_org_count": 3,
    },
    "enterprise": {
        "price": 650,
        "credits": 65000,
        "price_per_credit": 650 / 65000,
        "min_amount": 1000,
        "max_org_count": 5,
    },
}

AGENCY_PLANS = {
    "intro": {
        "price": 89,
        "credits": 4450,
        "price_per_credit": 89 / 4450,
        "min_amount": 1000,
        "max_org_count": 10,
    },
    "growth": {
        "price": 199,
        "credits": 14925,
        "price_per_credit": 199 / 14925,
        "min_amount": 1000,
        "max_org_count": 30,
    },
    "scale": {
        "price": 499,
        "credits": 49900,
        "price_per_credit": 499 / 49900,
        "min_amount": 1000,
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
    required_credits = company_data["required_credits"]
    orgs_count = company_data["orgs_count"]

    all_plans = BRAND_PLANS if company_type == "IN_HOUSE" else AGENCY_PLANS

    # Filter plans based on the number of organizations
    plans = {
        name: p for name, p in all_plans.items() if p["max_org_count"] >= orgs_count
    }

    # If no plan can support the org count, find the one with the highest org count capacity.
    if not plans:
        largest_plan_name = max(all_plans, key=lambda k: all_plans[k]["max_org_count"])
        plans = {largest_plan_name: all_plans[largest_plan_name]}

    # --- Least Cost Scenario ---
    least_cost_options = []
    for name, plan in plans.items():
        extra_credits_needed = max(0, required_credits - plan["credits"])
        if extra_credits_needed > 0 and extra_credits_needed < plan["min_amount"]:
            extra_credits_needed = plan["min_amount"]

        cost = plan["price"] + extra_credits_needed * plan["price_per_credit"]
        total_credits = plan["credits"] + extra_credits_needed

        least_cost_options.append(
            {
                "plan_name": name,
                "cost": cost,
                "extra_credits": extra_credits_needed,
                "surplus_credits": total_credits - required_credits,
            }
        )

    best_least_cost = min(least_cost_options, key=lambda x: x["cost"])

    # --- Matched MRR Scenario ---
    match_mrr_plan_name = None
    match_mrr_monthly_revenue = 0.0
    match_mrr_extra_credits_purchased = 0
    match_mrr_surplus_credits = 0.0

    best_match_base_plan = None

    suitable_plans = [
        (name, p) for name, p in plans.items() if p["price"] < (current_mrr)
    ]

    if suitable_plans:
        # If there are plans cheaper than current MRR, pick the most expensive of them.
        best_match_base_plan_name, best_match_base_plan = max(
            suitable_plans, key=lambda x: x[1]["price"]
        )
    elif plans:
        # Otherwise, if no plan is cheaper, pick the absolute cheapest plan available.
        best_match_base_plan_name, best_match_base_plan = min(
            plans.items(), key=lambda x: x[1]["price"]
        )

    if best_match_base_plan:
        plan_price = best_match_base_plan["price"]
        plan_credits = best_match_base_plan["credits"]
        remaining_mrr_cents = (current_mrr) - plan_price

        if remaining_mrr_cents > 0:
            # If current MRR is higher than the plan, buy extra credits to match MRR.
            extra_credits_to_match = math.ceil(
                remaining_mrr_cents / best_match_base_plan["price_per_credit"]
            )
            final_monthly_revenue_cents = plan_price + (
                extra_credits_to_match * best_match_base_plan["price_per_credit"]
            )
        else:
            # If the plan is more expensive than MRR, no extra credits are purchased.
            extra_credits_to_match = 0
            final_monthly_revenue_cents = plan_price

        match_mrr_plan_name = best_match_base_plan_name
        match_mrr_monthly_revenue = final_monthly_revenue_cents
        match_mrr_extra_credits_purchased = extra_credits_to_match
        match_mrr_surplus_credits = (
            plan_credits + extra_credits_to_match
        ) - required_credits

    # --- Compile Results ---
    result = {
        "least_cost_plan_name": best_least_cost["plan_name"],
        "least_cost_mrr": int(best_least_cost["cost"]),
        "least_cost_mrr_change": int(best_least_cost["cost"] - current_mrr),
        "least_cost_arr_change": int((best_least_cost["cost"] - current_mrr) * 12),
        "least_cost_extra_credits_purchased": int(best_least_cost["extra_credits"]),
        "least_cost_surplus_credits": int(best_least_cost["surplus_credits"]),
        "match_mrr_plan_name": match_mrr_plan_name,
        "match_mrr_mrr": int(match_mrr_monthly_revenue),
        "match_mrr_extra_credits_purchased": match_mrr_extra_credits_purchased,
        "match_mrr_surplus_credits": int(match_mrr_surplus_credits),
    }
    return pd.Series(result)


def calculate_credits(row: pd.Series) -> int:
    """
    Calculates the required credits for a given row (organization) based on model usage and run frequency.
    """
    # Determine the number of runs per day based on the interval.
    # Default to 1 run per day (every 24 hours) if interval is 0 or not specified.
    interval_hours = row.get("chat_interval_in_hours", 24)
    if interval_hours == 0:
        interval_hours = 24
    
    runs_per_day = 24 / interval_hours
    
    # Approximate days in a month
    days_in_month = 30
    
    runs_per_month = runs_per_day * days_in_month

    model_prices = [MODEL_ID_PRICE_MAP.get(mid, 0) for mid in row["model_ids"]]
    return int(sum(model_prices) * row["prompt_limit"] * runs_per_month)
