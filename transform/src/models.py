from typing import List, Optional, Literal
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


# This is a base configuration we can reuse.
# It tells Pydantic to automatically convert camelCase JSON fields
# to snake_case Python attributes.
class CamelCaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )


# --- Input Models (from data/ files) ---


class Organization(CamelCaseModel):
    id: str
    company_id: str
    model_ids: List[str]
    prompt_limit: int
    prompts_count: int
    chat_interval_in_hours: int


class Company(CamelCaseModel):
    id: str
    name: str
    type: Literal["IN_HOUSE", "AGENCY"]
    domain: Optional[str] = None
    stripe_customer_id: str
    stripe_subscription_id: str


class SubscriptionItem(CamelCaseModel):
    customer_id: str
    mrr_cents: float
    quantity: int


# --- Output Model (for our final CSV) ---


# We use the improved, clearer names we discussed.
# No camel case conversion is needed here as we define the names.
class MigrationOutput(BaseModel):
    company_name: str
    company_domain: Optional[str]
    company_type: Literal["IN_HOUSE", "AGENCY"]
    orgs_count: int

    current_mrr: int
    current_arr: int

    total_prompts_capacity: int
    total_prompts: int
    required_credits: int

    least_cost_plan_name: str
    least_cost_mrr: int
    least_cost_mrr_change: int
    least_cost_arr_change: int
    least_cost_extra_credits_purchased: int
    least_cost_surplus_credits: int

    match_mrr_plan_name: Optional[str] = None
    match_mrr_mrr: int
    match_mrr_extra_credits_purchased: int
    match_mrr_surplus_credits: int
