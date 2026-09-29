"""Unit tests for transforms.py, using in-memory data only (no database)."""

import pandas as pd

from transforms import (
    DEFAULT_EMAIL,
    clean_customers,
    deduplicate_customers,
    fill_missing_emails,
    filter_valid_orders,
    standardize_phone,
)


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


class TestDeduplicateCustomers:
    def test_keeps_most_recent_signup(self):
        customers = pd.DataFrame(
            {
                "customer_id": [1, 1, 2],
                "email": ["old@example.com", "new@example.com", "bob@example.com"],
                "signup_date": ["2023-01-15", "2023-06-01", "2023-02-20"],
            }
        )

        result = deduplicate_customers(customers)

        assert result["customer_id"].tolist() == [1, 2]
        assert result.loc[result["customer_id"] == 1, "email"].item() == "new@example.com"

    def test_order_of_rows_in_source_does_not_matter(self):
        customers = pd.DataFrame(
            {
                "customer_id": [1, 1],
                "email": ["new@example.com", "old@example.com"],
                "signup_date": ["2023-06-01", "2023-01-15"],
            }
        )

        result = deduplicate_customers(customers)

        assert result["email"].tolist() == ["new@example.com"]

    def test_valid_date_wins_over_unparseable_date(self):
        customers = pd.DataFrame(
            {
                "customer_id": [1, 1],
                "email": ["valid@example.com", "broken@example.com"],
                "signup_date": ["2023-01-15", "not a date"],
            }
        )

        result = deduplicate_customers(customers)

        assert result["email"].tolist() == ["valid@example.com"]

    def test_does_not_modify_input(self):
        customers = pd.DataFrame(
            {"customer_id": [1, 1], "signup_date": ["2023-01-15", "2023-06-01"]}
        )

        deduplicate_customers(customers)

        assert len(customers) == 2


class TestFillMissingEmails:
    def test_replaces_null_and_blank_emails(self):
        customers = pd.DataFrame({"email": [None, "", "   "]})

        result = fill_missing_emails(customers)

        assert result["email"].tolist() == [DEFAULT_EMAIL] * 3

    def test_keeps_existing_emails(self):
        customers = pd.DataFrame({"email": ["alice@example.com"]})

        result = fill_missing_emails(customers)

        assert result["email"].tolist() == ["alice@example.com"]


class TestCleanCustomers:
    def test_newer_email_is_kept_instead_of_placeholder(self):
        # Bob's older row has no email; his newer row does.
        customers = pd.DataFrame(
            {
                "customer_id": [2, 2],
                "full_name": ["Bob Jones", "Bob Jones"],
                "email": [None, "bob.jones@example.com"],
                "phone": ["555-987-6543", "555-987-6543"],
                "signup_date": ["2023-02-20", "2023-09-15"],
            }
        )

        result = clean_customers(customers)

        assert len(result) == 1
        assert result["email"].item() == "bob.jones@example.com"
        assert result["phone"].item() == "5559876543"


class TestFilterValidOrders:
    def test_drops_zero_and_negative_amounts(self):
        orders = pd.DataFrame({"order_id": [1, 2, 3], "total_amount": [-50.0, 0.0, 150.0]})

        result = filter_valid_orders(orders)

        assert result["order_id"].tolist() == [3]

    def test_drops_missing_amounts(self):
        orders = pd.DataFrame({"order_id": [1, 2], "total_amount": [None, 10.0]})

        result = filter_valid_orders(orders)

        assert result["order_id"].tolist() == [2]

    def test_filters_on_amount_regardless_of_status(self):
        # A zero-amount order marked COMPLETED is still a system error, while a
        # positive CANCELLED order is not removed by this rule.
        orders = pd.DataFrame(
            {
                "order_id": [1, 2],
                "total_amount": [0.0, 15.99],
                "status": ["COMPLETED", "CANCELLED"],
            }
        )

        result = filter_valid_orders(orders)

        assert result["order_id"].tolist() == [2]

    def test_does_not_modify_input(self):
        orders = pd.DataFrame({"order_id": [1, 2], "total_amount": [-1.0, 1.0]})

        filter_valid_orders(orders)

        assert len(orders) == 2
