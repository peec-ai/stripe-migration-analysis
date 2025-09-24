import pandas as pd
import math
from pathlib import Path

from .models import Company, Organization, SubscriptionItem, MigrationOutput

# --- 1. Constants and Pricing Plans ---

# Using dictionaries for plans. Prices are in cents, matching the source data.
BRAND_PLANS = {
    "starter": {
        "price": 8900,
        "credits": 4450,
        "price_per_credit": 8900 / 4450,
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
    "claude-sonnet-4": 3,
    "claude-3-5-haiku": 3,
    "gemini-1-5-flash": 1,
    "deepseek-r1": 1,
    "gemini-2-5-flash": 3,
    "google-ai-mode": 1,
    "grok-2-1212": 3,
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
    current_annual_revenue = company_data["current_annual_revenue"]
    required_credits_annual = company_data["required_credits_annual"]

    plans = BRAND_PLANS if company_type == "IN_HOUSE" else AGENCY_PLANS

    # --- Least Cost Scenario ---
    least_cost_options = []
    for name, plan in plans.items():
        annual_plan_credits = plan["credits"] * 12
        annual_plan_price = plan["price"] * 12

        extra_credits_needed = max(0, required_credits_annual - annual_plan_credits)
        cost = annual_plan_price + extra_credits_needed * plan["price_per_credit"]
        total_credits = annual_plan_credits + extra_credits_needed

        least_cost_options.append(
            {
                "plan_name": name,
                "cost": cost,
                "extra_credits": extra_credits_needed,
                "surplus_credits": total_credits - required_credits_annual,
            }
        )

    best_least_cost = min(least_cost_options, key=lambda x: x["cost"])

    # --- Matched ARR Scenario ---
    match_arr_plan_name = None
    match_arr_annual_revenue = 0.0
    match_arr_extra_credits_purchased = 0
    match_arr_surplus_credits = 0.0

    suitable_plans = [
        (name, p)
        for name, p in plans.items()
        if (p["price"] * 12) < (current_annual_revenue * 100)
    ]

    if suitable_plans:
        # Find the most expensive plan that is still cheaper than the current ARR
        best_match_base_plan_name, best_match_base_plan = max(
            suitable_plans, key=lambda x: x[1]["price"]
        )

        remaining_arr_cents = (current_annual_revenue * 100) - (
            best_match_base_plan["price"] * 12
        )
        extra_credits_to_match = math.ceil(
            remaining_arr_cents / best_match_base_plan["price_per_credit"]
        )

        match_arr_plan_name = best_match_base_plan_name
        match_arr_annual_revenue = (
            (best_match_base_plan["price"] * 12)
            + (extra_credits_to_match * best_match_base_plan["price_per_credit"])
        ) / 100
        match_arr_extra_credits_purchased = extra_credits_to_match
        match_arr_surplus_credits = (
            (best_match_base_plan["credits"] * 12) + extra_credits_to_match
        ) - required_credits_annual

    # --- Compile Results ---
    result = {
        "least_cost_plan_name": best_least_cost["plan_name"],
        "least_cost_annual_revenue": best_least_cost["cost"] / 100,
        "least_cost_arr_change": (best_least_cost["cost"] / 100)
        - current_annual_revenue,
        "least_cost_extra_credits_purchased": int(best_least_cost["extra_credits"]),
        "least_cost_surplus_credits": int(best_least_cost["surplus_credits"]),
        "match_arr_plan_name": match_arr_plan_name,
        "match_arr_annual_revenue": match_arr_annual_revenue,
        "match_arr_extra_credits_purchased": match_arr_extra_credits_purchased,
        "match_arr_surplus_credits": match_arr_surplus_credits,
    }
    return pd.Series(result)


# --- 3. Main ETL and Execution Block ---


def etl_pipeline():
    """
    Main function to run the ETL pipeline.
    """
    # Define paths relative to the script location
    base_path = Path(__file__).parent.parent.parent
    data_path = base_path / "data"
    output_path = data_path / "migrate.csv"

    def load(file_path: Path):
        """Helper to load, clean NaN values, and validate data."""
        df_raw = pd.read_json(file_path)
        # Replace NaN with None, which Pydantic understands as a valid optional value
        records = df_raw.replace({float("nan"): None}).to_dict("records")
        return records

    # Load data using the robust helper
    print("Loading source data...")
    companies = load(data_path / "processed_companies.json")
    orgs = load(data_path / "processed_organizations.json")
    subs = load(data_path / "stripe_subscription_items.json")

    # Filter
    companiesSC = [
        c for c in companies if c["stripeSubscriptionId"] and c["stripeCustomerId"]
    ]

    # Validate
    companies = [Company.model_validate(c) for c in companiesSC]
    orgs = [Organization.model_validate(o) for o in orgs]
    subs = [SubscriptionItem.model_validate(s) for s in subs]

    print(
        f"Loaded {len(companies)} companies, {len(orgs)} organizations, {len(subs)} subscription items."
    )

    # Convert to Pandas DataFrames
    companies_df = pd.DataFrame([c.model_dump() for c in companies])
    orgs_df = pd.DataFrame([o.model_dump() for o in orgs])
    subs_df = pd.DataFrame([s.model_dump() for s in subs])

    # --- Data Transformation ---
    print("Transforming and merging data...")

    # Calculate required credits per organization
    def calculate_credits(row):
        model_prices = [MODEL_ID_PRICE_MAP.get(mid, 0) for mid in row["model_ids"]]
        return sum(model_prices) * row["prompts_count"]

    orgs_df["required_credits_annual"] = orgs_df.apply(calculate_credits, axis=1)

    # Aggregate credits by company
    company_credits = (
        orgs_df.groupby("company_id")["required_credits_annual"].sum().reset_index()
    )

    # Calculate current ARR per customer
    customer_arr = subs_df.groupby("customer_id")["mrr_cents"].sum().reset_index()
    customer_arr["current_annual_revenue"] = customer_arr["mrr_cents"] * 12 / 100

    # Merge all data into a single DataFrame
    merged_df = pd.merge(
        companies_df, company_credits, left_on="id", right_on="company_id", how="inner"
    )
    merged_df = pd.merge(
        merged_df,
        customer_arr,
        left_on="stripe_customer_id",
        right_on="customer_id",
        how="inner",
    )

    # Fill missing values for companies with no subs or orgs
    merged_df["required_credits_annual"] = merged_df["required_credits_annual"].fillna(
        0
    )
    merged_df["current_annual_revenue"] = merged_df["current_annual_revenue"].fillna(0)

    # export csv
    merged_df.to_csv(data_path / "merged_df.csv", index=False)

    # --- Apply Calculation Logic ---
    print("Calculating migration scenarios for each company...")
    scenarios_df = merged_df.apply(calculate_scenarios_for_company, axis=1)


    # Combine initial data with calculated scenarios
    final_df = pd.concat([merged_df, scenarios_df], axis=1)

    # Select and rename columns to match our final desired output
    final_df = final_df.rename(
        columns={
            "name": "company_name",
            "domain": "company_domain",
            "type": "company_type",
        }
    )

    # Use the Pydantic model to define the final column order and selection
    output_columns = list(MigrationOutput.model_fields.keys())
    final_df = final_df[output_columns]

    # --- Save to CSV ---
    print(f"Saving final CSV to {output_path}...")
    final_df.to_csv(output_path, index=False)

    print("Migration analysis complete.")


def data_integrity_checks():
    """
    Perform data integrity checks on the DataFrame.
    """
    base_path = Path(__file__).parent.parent.parent
    data_path = base_path / "data"

    def load_df(file_path: Path) -> pd.DataFrame:
        """Helper to load JSON into a DataFrame."""
        return pd.read_json(file_path)

    # Load data using the robust helper
    print("Loading source data for integrity checks...")
    companies_df = load_df(data_path / "processed_companies.json")

    # --- Perform Checks ---
    print("\n--- Data Integrity Checks ---")

    # Companies with a stripeSubscriptionId
    companies_with_sub = companies_df[companies_df["stripeSubscriptionId"].notna()]
    print(f"Companies with a Stripe Subscription ID: {len(companies_with_sub)}")

    # Companies with a stripeCustomerId
    companies_with_cust = companies_df[companies_df["stripeCustomerId"].notna()]
    print(f"Companies with a Stripe Customer ID: {len(companies_with_cust)}")

    # Companies with both
    companies_with_both = companies_df[
        companies_df["stripeSubscriptionId"].notna()
        & companies_df["stripeCustomerId"].notna()
    ]
    print(f"Companies with both IDs: {len(companies_with_both)}")

    # Companies with neither
    companies_with_neither = companies_df[
        companies_df["stripeSubscriptionId"].isna()
        & companies_df["stripeCustomerId"].isna()
    ]
    print(f"Companies with neither ID: {len(companies_with_neither)}")

    # Companies with a stripeSubscriptionId but no stripeCustomerId
    sub_not_cust = companies_df[
        companies_df["stripeSubscriptionId"].notna()
        & companies_df["stripeCustomerId"].isna()
    ]
    print(
        "Companies with Subscription ID but no Customer ID: "
        f"\n{len(sub_not_cust)}"
    )

    # Companies with a stripeCustomerId but no stripeSubscriptionId
    cust_not_sub = companies_df[
        companies_df["stripeCustomerId"].notna()
        & companies_df["stripeSubscriptionId"].isna()
    ]
    print(
        "Companies with Customer ID but no Subscription ID: "
        f"{len(cust_not_sub)}"
    )

    # Non-unique stripeCustomerIds
    # We only consider non-null customer IDs for duplication checks
    non_null_customer_ids = companies_with_cust["stripeCustomerId"]
    duplicated_customer_ids = non_null_customer_ids[
        non_null_customer_ids.duplicated(keep=False)
    ]
    if not duplicated_customer_ids.empty:
        print("\nFound non-unique Stripe Customer IDs:")
        print(duplicated_customer_ids.value_counts())
    else:
        print("\nAll Stripe Customer IDs are unique.")

    # Non-unique stripeSubscriptionIds
    # We only consider non-null subscription IDs for duplication checks
    non_null_subscription_ids = companies_with_sub["stripeSubscriptionId"]
    duplicated_subscription_ids = non_null_subscription_ids[
        non_null_subscription_ids.duplicated(keep=False)
    ]
    if not duplicated_subscription_ids.empty:
        print("\nFound non-unique Stripe Subscription IDs:")
        print(duplicated_subscription_ids.value_counts())
        print(duplicated_subscription_ids)
    else:
        print("\nAll Stripe Subscription IDs are unique.")

    print("\n--- Integrity Checks Complete ---\n")


def main():
    """Main"""
    # data_integrity_checks()
    etl_pipeline()


if __name__ == "__main__":
    main()
