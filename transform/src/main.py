import pandas as pd
import math
from pathlib import Path

from .models import Company, Organization, SubscriptionItem, MigrationOutput
from .calculations import (
    calculate_scenarios_for_company,
    calculate_credits,
)


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
    orgs_df["required_credits"] = orgs_df.apply(calculate_credits, axis=1)

    # Aggregate credits by company
    company_credits = (
        orgs_df.groupby("company_id")["required_credits"].sum().reset_index()
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
    merged_df["required_credits"] = merged_df["required_credits"].fillna(
        0
    )
    merged_df["current_annual_revenue"] = merged_df["current_annual_revenue"].fillna(0)

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
