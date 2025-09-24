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

    # Expected result: (1 + 1 + 0.5) * 100 = 250
    expected_credits = 250.0 * 30
    calculated_credits = calculate_credits(row)

    assert calculated_credits == expected_credits, "Credit calculation is incorrect"
