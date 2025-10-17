import pandas as pd
from pathlib import Path

from .models import (
    Company,
    Organization,
    SubscriptionItem,
    MigrationOutput,
)
from .calculations import (
    calculate_coupon_multiplier,
    calculate_scenarios_for_company,
    calculate_credits_capacity,
    calculate_credits_usage,
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
    coupons = load(data_path / "stripe_coupons.json")
    prices = load(data_path / "stripe_prices.json")
    products = load(data_path / "stripe_products.json")

    # Filter
    companiesSC = [
        c
        for c in companies
        if c["stripeSubscriptionId"]
        and c["stripeCustomerId"]
        and c["stripeSubscriptionStatus"] == "active"
    ]

    # Validate
    companies = [Company.model_validate(c) for c in companiesSC]
    orgs = [Organization.model_validate(o) for o in orgs]
    subs = [SubscriptionItem.model_validate(s) for s in subs]

    # Create coupon lookup dictionary
    coupons_map = {c["id"]: c for c in coupons}

    print(
        f"Loaded {len(companies)} companies, {len(orgs)} organizations, {len(subs)} subscription items, {len(coupons)} coupons, {len(prices)} prices, {len(products)} products."
    )

    # Convert to Pandas DataFrames
    companies_df = pd.DataFrame([c.model_dump() for c in companies])
    orgs_df = pd.DataFrame([o.model_dump() for o in orgs])
    subs_df = pd.DataFrame([s.model_dump() for s in subs])
    prices_df = pd.DataFrame(prices)
    products_df = pd.DataFrame(products)

    # --- Data Transformation ---
    print("Transforming and merging data...")

    # Calculate credits per organization
    orgs_df["credits_usage"] = orgs_df.apply(calculate_credits_usage, axis=1)
    orgs_df["credits_capacity"] = orgs_df.apply(calculate_credits_capacity, axis=1)

    # Join subscription items with prices and products to get product metadata
    subs_with_product = subs_df.merge(
        prices_df[["id", "product"]],
        left_on="plan_id",
        right_on="id",
        how="left",
        suffixes=("", "_price"),
    )

    subs_with_product = subs_with_product.merge(
        products_df[["id", "metadata"]],
        left_on="product",
        right_on="id",
        how="left",
        suffixes=("", "_product"),
    )

    # Extract metadata fields and filter for WORKSPACE type products
    def extract_prompt_limit(row):
        metadata = row.get("metadata", {})
        if not isinstance(metadata, dict):
            return 0

        product_type = metadata.get("type", "")
        if product_type != "WORKSPACE":
            return 0

        prompt_limit_str = metadata.get("promptLimit", "0")
        try:
            return int(prompt_limit_str)
        except (ValueError, TypeError):
            return 0

    subs_with_product["prompt_limit_per_item"] = subs_with_product.apply(
        extract_prompt_limit, axis=1
    )
    subs_with_product["total_prompt_limit"] = (
        subs_with_product["prompt_limit_per_item"] * subs_with_product["quantity"]
    )

    # Calculate prompt_capacity per customer from workspace subscription items
    customer_prompt_capacity = (
        subs_with_product.groupby("customer_id")
        .agg(prompt_capacity=("total_prompt_limit", "sum"))
        .reset_index()
    )

    # Aggregate credits and usage by company
    company_credits = (
        orgs_df.groupby("company_id")
        .agg(
            prompt_usage=("prompts_count", "sum"),
            credits_capacity=("credits_capacity", "sum"),
            credits_usage=("credits_usage", "sum"),
        )
        .reset_index()
    )

    # count orgs per company
    orgs_count_df = orgs_df.groupby("company_id").size().reset_index(name="orgs_count")

    # Count high-frequency orgs (more than once a day)
    high_freq_orgs_df = (
        orgs_df[
            (orgs_df["chat_interval_in_hours"] < 24)
            & (orgs_df["chat_interval_in_hours"] > 0)
        ]
        .groupby("company_id")
        .size()
        .reset_index(name="orgs_count_hf")
    )

    # Calculate current MRR per customer, taking quantity and discounts into account
    # First, calculate the base MRR without discounts
    subs_df["base_mrr_cents"] = subs_df["mrr_cents"] * subs_df["quantity"]

    # Apply item-level discounts (these are specific to each item)
    def apply_item_discount(row):
        item_discounts = row["discounts"]
        multiplier, long_term_count, total_count = calculate_coupon_multiplier(
            item_discounts, coupons_map
        )
        return pd.Series(
            {
                "mrr_after_item_discounts": row["base_mrr_cents"] * multiplier,
                "item_discount_long_term_count": long_term_count,
                "item_discount_total_count": total_count,
            }
        )

    item_discount_results = subs_df.apply(apply_item_discount, axis=1)
    subs_df["mrr_after_item_discounts"] = item_discount_results[
        "mrr_after_item_discounts"
    ]
    subs_df["item_discount_long_term_count"] = item_discount_results[
        "item_discount_long_term_count"
    ]
    subs_df["item_discount_total_count"] = item_discount_results[
        "item_discount_total_count"
    ]

    # For subscription-level discounts, we need to calculate the multiplier once per subscription
    # and apply it to the total of all items (to avoid denormalization issues)

    # First, get unique subscription discounts per customer
    subscription_discounts_df = (
        subs_df.groupby("customer_id")["subscription_discounts"]
        .first()  # All items in same subscription have same subscription_discounts
        .reset_index()
    )

    # Calculate subscription-level discount multiplier and count
    def get_subscription_multiplier(row):
        sub_discounts = row["subscription_discounts"]
        multiplier, long_term_count, total_count = calculate_coupon_multiplier(
            sub_discounts, coupons_map
        )
        return pd.Series(
            {
                "sub_discount_multiplier": multiplier,
                "sub_discount_long_term_count": long_term_count,
                "sub_discount_total_count": total_count,
            }
        )

    sub_discount_results = subscription_discounts_df.apply(
        get_subscription_multiplier, axis=1
    )
    subscription_discounts_df["sub_discount_multiplier"] = sub_discount_results[
        "sub_discount_multiplier"
    ]
    subscription_discounts_df["sub_discount_long_term_count"] = sub_discount_results[
        "sub_discount_long_term_count"
    ]
    subscription_discounts_df["sub_discount_total_count"] = sub_discount_results[
        "sub_discount_total_count"
    ]

    # Merge subscription multiplier and count back to items
    subs_df = pd.merge(
        subs_df,
        subscription_discounts_df[
            [
                "customer_id",
                "sub_discount_multiplier",
                "sub_discount_long_term_count",
                "sub_discount_total_count",
            ]
        ],
        on="customer_id",
        how="left",
    )

    # Apply subscription-level discounts on top of item-level discounts
    subs_df["discounted_mrr_cents"] = (
        subs_df["mrr_after_item_discounts"] * subs_df["sub_discount_multiplier"]
    )

    # Aggregate by customer
    customer_mrr = (
        subs_df.groupby("customer_id")
        .agg(
            {
                "base_mrr_cents": "sum",
                "discounted_mrr_cents": "sum",
                "item_discount_long_term_count": "sum",  # Sum item-level long-term discount counts
                "item_discount_total_count": "sum",  # Sum item-level total discount counts
                "sub_discount_long_term_count": "first",  # Subscription-level counts are the same for all items
                "sub_discount_total_count": "first",
            }
        )
        .reset_index()
    )

    # Calculate total discount counts (item + subscription level)
    customer_mrr["applied_discounts"] = (
        customer_mrr["item_discount_long_term_count"]
        + customer_mrr["sub_discount_long_term_count"]
    ).astype(int)

    customer_mrr["total_discounts"] = (
        customer_mrr["item_discount_total_count"]
        + customer_mrr["sub_discount_total_count"]
    ).astype(int)

    # Format as "applied (total)"
    customer_mrr["discounts_formatted"] = customer_mrr.apply(
        lambda row: f"{row['applied_discounts']} ({row['total_discounts']})", axis=1
    )

    customer_mrr["current_mrr"] = customer_mrr["discounted_mrr_cents"] / 100
    customer_mrr["current_arr"] = customer_mrr["current_mrr"] * 12

    # Calculate discount percentage: (1 - discounted/base) * 100
    customer_mrr["discount_pct"] = (
        (
            (
                1
                - (
                    customer_mrr["discounted_mrr_cents"]
                    / customer_mrr["base_mrr_cents"]
                )
            )
            * 100
        )
        .fillna(0)
        .round(0)
        .astype(int)
    )

    # --- Interval Formatting ---
    # We only care about the interval of the highest MRR item per customer
    # This is a simplification, but for now it's a good heuristic
    main_subscription = subs_df.loc[
        subs_df.groupby("customer_id")["base_mrr_cents"].idxmax()
    ]

    def format_interval(row):
        if row["interval_count"] != 1:
            return f"{row['interval']} ({row['interval_count']})"
        return row["interval"]

    main_subscription["interval"] = main_subscription.apply(format_interval, axis=1)

    customer_interval = main_subscription[["customer_id", "interval"]]

    # Merge all data into a single DataFrame
    merged_df = pd.merge(
        companies_df, company_credits, left_on="id", right_on="company_id", how="inner"
    )
    merged_df = pd.merge(
        merged_df,
        orgs_count_df,
        on="company_id",
        how="inner",
    )
    merged_df = pd.merge(
        merged_df,
        high_freq_orgs_df,
        on="company_id",
        how="left",  # Use left merge to keep all companies
    )
    merged_df = pd.merge(
        merged_df,
        customer_mrr[
            [
                "customer_id",
                "current_mrr",
                "current_arr",
                "discount_pct",
                "discounts_formatted",
            ]
        ],
        left_on="stripe_customer_id",
        right_on="customer_id",
        how="inner",
    )
    merged_df = pd.merge(
        merged_df,
        customer_interval,
        on="customer_id",
        how="inner",
    )
    merged_df = pd.merge(
        merged_df,
        customer_prompt_capacity,
        on="customer_id",
        how="left",  # Use left merge to keep all companies even if no workspace subscriptions
    )

    # Fill missing values for companies with no subs or orgs
    merged_df["credits_capacity"] = merged_df["credits_capacity"].fillna(0)
    merged_df["credits_usage"] = merged_df["credits_usage"].fillna(0)
    merged_df["current_mrr"] = merged_df["current_mrr"].fillna(0)
    merged_df["current_arr"] = merged_df["current_arr"].fillna(0)
    merged_df["discount"] = merged_df["discount_pct"].fillna(0)
    merged_df["discounts"] = merged_df["discounts_formatted"].fillna("0 (0)")
    merged_df["prompt_usage"] = merged_df["prompt_usage"].fillna(0)
    merged_df["prompt_capacity"] = merged_df["prompt_capacity"].fillna(0)
    merged_df["orgs_count"] = merged_df["orgs_count"].fillna(0)
    merged_df["orgs_count_hf"] = merged_df["orgs_count_hf"].fillna(0)

    # Cast to int
    merged_df["credits_capacity"] = merged_df["credits_capacity"].astype(int)
    merged_df["credits_usage"] = merged_df["credits_usage"].astype(int)
    merged_df["current_mrr"] = merged_df["current_mrr"].astype(int)
    merged_df["current_arr"] = merged_df["current_arr"].astype(int)
    merged_df["discount"] = merged_df["discount_pct"].astype(int)
    # discounts is already a string, no conversion needed
    merged_df["prompt_usage"] = merged_df["prompt_usage"].astype(int)
    merged_df["prompt_capacity"] = merged_df["prompt_capacity"].astype(int)
    merged_df["orgs_count"] = merged_df["orgs_count"].astype(int)
    merged_df["orgs_count_hf"] = merged_df["orgs_count_hf"].astype(int)

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

    # Print sum of arr_change
    print(f"Sum of arr_change: {final_df['arr_change'].sum()}")

    # Use the Pydantic model to define the final column order and selection
    output_columns = list(MigrationOutput.model_fields.keys())
    final_df = final_df[output_columns]

    # --- Save to CSV ---
    print(f"Saving final CSV to {output_path}...")
    final_df.to_csv(output_path, index=False)

    print("Migration analysis complete.")


def main():
    """Main"""
    etl_pipeline()


if __name__ == "__main__":
    main()
