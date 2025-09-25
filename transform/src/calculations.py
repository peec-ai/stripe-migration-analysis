import pandas as pd
import math

# --- 1. Constants and Pricing Plans ---

# Using dictionaries for plans. Prices are in cents, matching the source data.
BRAND_PLANS = {
    "starter": {
        "price": 8900,
        "credits": 4450,
        "price_per_credit": 8900 / 4450,
        "min_amount": 1000
    },
    "pro": {
        "price": 24900,
        "credits": 18675,
        "price_per_credit": 24900 / 18675,
    },
    "enterprise": {
        "price": 49900,
        "credits": 49900,
        "price_per_credit": 49900 / 49900,
    },
}

AGENCY_PLANS = {
    "intro": {
        "price": 29900,
        "credits": 14950,
        "price_per_credit": 29900 / 14950,
    },
    "growth": {
        "price": 49900,
        "credits": 37425,
        "price_per_credit": 49900 / 37425,
    },
    "scale": {
        "price": 60000,
        "credits": 60000,
        "price_per_credit": 60000 / 60000,
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
    current_monthly_revenue = company_data["current_monthly_revenue"]
    required_credits = company_data["required_credits"]

    plans = BRAND_PLANS if company_type == "IN_HOUSE" else AGENCY_PLANS

    # --- Least Cost Scenario ---
    least_cost_options = []
    for name, plan in plans.items():
        extra_credits_needed = max(0, required_credits - plan["credits"])
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
        (name, p)
        for name, p in plans.items()
        if p["price"] < (current_monthly_revenue * 100)
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
        remaining_mrr_cents = (current_monthly_revenue * 100) - plan_price

        if remaining_mrr_cents > 0:
            # If current MRR is higher than the plan, buy extra credits to match MRR.
            extra_credits_to_match = math.ceil(
                remaining_mrr_cents / best_match_base_plan["price_per_credit"]
            )
            final_monthly_revenue_cents = (
                plan_price
                + (extra_credits_to_match * best_match_base_plan["price_per_credit"])
            )
        else:
            # If the plan is more expensive than MRR, no extra credits are purchased.
            extra_credits_to_match = 0
            final_monthly_revenue_cents = plan_price

        match_mrr_plan_name = best_match_base_plan_name
        match_mrr_monthly_revenue = final_monthly_revenue_cents / 100
        match_mrr_extra_credits_purchased = extra_credits_to_match
        match_mrr_surplus_credits = (
            plan_credits + extra_credits_to_match
        ) - required_credits

    # --- Compile Results ---
    result = {
        "least_cost_plan_name": best_least_cost["plan_name"],
        "least_cost_monthly_revenue": best_least_cost["cost"] / 100,
        "least_cost_mrr_change": (best_least_cost["cost"] / 100)
        - current_monthly_revenue,
        "least_cost_extra_credits_purchased": int(best_least_cost["extra_credits"]),
        "least_cost_surplus_credits": int(best_least_cost["surplus_credits"]),
        "match_arr_plan_name": match_mrr_plan_name,
        "match_mrr_monthly_revenue": match_mrr_monthly_revenue,
        "match_mrr_extra_credits_purchased": match_mrr_extra_credits_purchased,
        "match_mrr_surplus_credits": match_mrr_surplus_credits,
    }
    return pd.Series(result)


def calculate_credits(row: pd.Series) -> float:
    """
    Calculates the required credits for a given row (organization) based on model usage.
    """
    model_prices = [MODEL_ID_PRICE_MAP.get(mid, 0) for mid in row["model_ids"]]
    return sum(model_prices) * row["prompts_count"] * 30
