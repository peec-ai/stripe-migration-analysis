import pandas as pd
import pytest
from .calculations import calculate_credits, calculate_scenarios_for_company


def test_calculate_credits():
    # Sample data for a row in the DataFrame
    data = {
        "model_ids": ["gpt-4o", "chatgpt", "llama-3-3-70b-instruct"],
        "prompts_count": 100,
    }
    row = pd.Series(data)

    # Expected result: (1 + 1 + 0.5) * 100 * 30 = 7500
    expected_credits = 7500.0
    calculated_credits = calculate_credits(row)

    assert calculated_credits == expected_credits, "Credit calculation is incorrect"


@pytest.fixture
def sample_company_in_house():
    """Fixture for a sample IN_HOUSE company data."""
    return pd.Series(
        {
            "type": "IN_HOUSE",
            "current_monthly_revenue": 1000,  # $1,000/mo
            "required_credits": 50000,
        }
    )


@pytest.fixture
def sample_company_agency():
    """Fixture for a sample AGENCY company data."""
    return pd.Series(
        {
            "type": "AGENCY",
            "current_monthly_revenue": 4200, # $4,200/mo
            "required_credits": 70000,
        }
    )


def test_calculate_scenarios_in_house(sample_company_in_house):
    """Test scenario calculation for an IN_HOUSE company."""
    result = calculate_scenarios_for_company(sample_company_in_house)

    # Rationale for Least Cost Scenario:
    # Required credits: 50,000/mo.
    # - 'starter': 4,450 credits for $89. Needs 45,550 extra. Cost = $89 + 45,550 * ($89/4450) = $999/mo.
    # - 'pro': 18,675 credits for $249. Needs 31,325 extra. Cost = $249 + 31,325 * ($249/18675) = $665.67/mo.
    # - 'enterprise': 49,900 credits for $499. Needs 100 extra. Cost = $499 + 100 * ($499/49900) = $500/mo.
    # The 'enterprise' plan is the most cost-effective.
    assert result["least_cost_plan_name"] == "enterprise"
    assert result["least_cost_monthly_revenue"] == pytest.approx(500.0)

    # Rationale for Matched MRR Scenario:
    # Current MRR is $1,000.
    # All IN_HOUSE plans are cheaper. The most expensive of these is 'enterprise' at $499/mo.
    # Remaining MRR to fill: $1,000 - $499 = $501.
    # Extra credits to buy: $501 / ($499/49900) = 50100 credits.
    # Total revenue will be approximately $1,000.
    assert result["match_mrr_plan_name"] == "enterprise"
    assert result["match_mrr_monthly_revenue"] == pytest.approx(1000.0, abs=1)


def test_calculate_scenarios_agency(sample_company_agency):
    """Test scenario calculation for an AGENCY company."""
    result = calculate_scenarios_for_company(sample_company_agency)

    # Rationale for Least Cost Scenario:
    # Required credits: 70,000/mo.
    # - 'intro': 14,950 credits for $299. Needs 55,050 extra. Cost = $299 + 55050 * ($299/14950) = $1397/mo.
    # - 'growth': 37,425 credits for $499. Needs 32,575 extra. Cost = $499 + 32575 * ($499/37425) = $932.33/mo.
    # - 'scale': 60,000 credits for $600. Needs 10,000 extra. Cost = $600 + 10000 * ($600/60000) = $700/mo.
    # The 'scale' plan is the most cost-effective.
    assert result["least_cost_plan_name"] == "scale"
    assert result["least_cost_monthly_revenue"] == pytest.approx(700.0)

    # Rationale for Matched MRR Scenario:
    # Current MRR is $4,200.
    # All agency plans are cheaper. The most expensive is 'scale' at $600/mo.
    # Remaining MRR to fill: $4,200 - $600 = $3,600.
    # Extra credits to buy: $3600 / ($600/60000) = 360,000 credits.
    # Total revenue will be approx $4,200.
    assert result["match_mrr_plan_name"] == "scale"
    assert result["match_mrr_monthly_revenue"] == pytest.approx(4200.0, abs=1)


def test_no_suitable_plan_for_match_arr(sample_company_in_house):
    """
    Test edge case where current MRR is lower than any available plan.
    The logic should default to picking the cheapest available plan.
    """
    company_data = sample_company_in_house.copy()
    company_data["current_monthly_revenue"] = 50  # $50/mo is cheaper than all plans.

    result = calculate_scenarios_for_company(company_data)

    # Rationale for Matched MRR Scenario:
    # No plan is cheaper than the $50 MRR.
    # The logic should select the absolute cheapest IN_HOUSE plan, which is 'starter' at $89/mo.
    # No extra credits are purchased because the plan cost already exceeds the original MRR.
    assert result["match_mrr_plan_name"] == "starter"
    assert result["match_mrr_monthly_revenue"] == pytest.approx(89.0)
    assert result["match_mrr_extra_credits_purchased"] == 0


def test_zero_required_credits(sample_company_in_house):
    """
    Test edge case where a company requires zero credits.
    """
    company_data = sample_company_in_house.copy()
    company_data["required_credits"] = 0

    result = calculate_scenarios_for_company(company_data)

    # Rationale for Least Cost Scenario:
    # With zero required credits, the least cost is the price of the cheapest plan.
    # For IN_HOUSE, this is 'starter' at $89/mo.
    # Surplus credits will be the monthly credits from that plan.
    assert result["least_cost_plan_name"] == "starter"
    assert result["least_cost_monthly_revenue"] == pytest.approx(89.0)
    assert result["least_cost_extra_credits_purchased"] == 0
    assert result["least_cost_surplus_credits"] == 4450
