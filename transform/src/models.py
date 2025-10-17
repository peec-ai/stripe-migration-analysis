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
    type: Literal["IN_HOUSE", "AGENCY", "PARTNER"]
    domain: Optional[str] = None
    stripe_customer_id: str
    stripe_subscription_id: str

class StripeCoupon(CamelCaseModel):
    id: str
    name: Optional[str] = None
    amount_off: Optional[int] = None
    percent_off: Optional[float] = None
    duration: str
    duration_in_months: Optional[int] = None


class SubscriptionItem(CamelCaseModel):
    customer_id: str
    plan_id: str
    mrr_cents: float
    quantity: int
    interval: str
    interval_count: int
    subscription_discounts: List[str]
    discounts: List[str]


# --- Output Model (for our final CSV) ---


class MigrationOutput(BaseModel):
    company_name: str
    company_domain: Optional[str]
    company_type: Literal["IN_HOUSE", "AGENCY", "PARTNER"]
    orgs_count: int
    orgs_count_hf: int

    current_mrr: int
    current_arr: int
    interval: str
    discount: int
    discounts: str

    prompt_usage: int
    prompt_capacity: int
    credits_usage: int
    credits_capacity: int

    plan_name: str
    mrr: int
    mrr_change: int
    arr_change: int
    extra_credits_purchased: int
    surplus_credits: int
