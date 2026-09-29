"""Unit tests for transforms.py, using in-memory data only (no database)."""

import pandas as pd

from transforms import standardize_phone


class TestStandardizePhone:
    def test_removes_formatting_characters(self):
        phones = pd.Series(["+1 (555) 123-4567", "555-987-6543", "(555) 333 4444"])

        result = standardize_phone(phones)

        assert result.tolist() == ["15551234567", "5559876543", "5553334444"]

    def test_leaves_digit_only_values_unchanged(self):
        result = standardize_phone(pd.Series(["15551234567"]))

        assert result.tolist() == ["15551234567"]

    def test_strips_letters(self):
        result = standardize_phone(pd.Series(["Ext 444", "1-800-555-DINO"]))

        assert result.tolist() == ["444", "1800555"]

    def test_missing_value_stays_missing(self):
        result = standardize_phone(pd.Series(["555-111-2222", None]))

        assert result.iloc[0] == "5551112222"
        assert pd.isna(result.iloc[1])

    def test_value_without_digits_becomes_missing(self):
        result = standardize_phone(pd.Series(["N/A"]))

        assert pd.isna(result.iloc[0])
