"""Pure data-cleaning functions for the ShopData ETL pipeline.

Every function takes a pandas DataFrame and returns a new DataFrame (or
Series) without touching the database, so the business rules can be unit
tested in isolation from I/O and from Prefect.
"""

import pandas as pd

DEFAULT_EMAIL = "unknown@domain.com"
BASE_CURRENCY = "USD"


def standardize_phone(phones: pd.Series) -> pd.Series:
    """Remove every non-numeric character from phone numbers.

    Example: "+1 (555) 123-4567" -> "15551234567".
    Missing values stay missing, and values with no digits at all
    (e.g. "N/A") become missing rather than an empty string.
    """
    digits = phones.astype("string").str.replace(r"\D", "", regex=True)
    return digits.replace("", pd.NA)


def deduplicate_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per customer_id: the one with the most recent signup_date.

    Rows whose signup_date cannot be parsed are treated as the oldest, so a
    row with a valid date always wins. If two rows share the same date, the
    one that appears later in the source is kept.
    """
    signup_ts = pd.to_datetime(customers["signup_date"], errors="coerce")
    return (
        customers.assign(_signup_ts=signup_ts)
        .sort_values("_signup_ts", kind="stable", na_position="first")
        .drop_duplicates(subset="customer_id", keep="last")
        .drop(columns="_signup_ts")
        .sort_values("customer_id")
        .reset_index(drop=True)
    )


def fill_missing_emails(customers: pd.DataFrame) -> pd.DataFrame:
    """Replace missing or blank emails with DEFAULT_EMAIL."""
    emails = (
        customers["email"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .fillna(DEFAULT_EMAIL)
    )
    return customers.assign(email=emails)


def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Apply all customer cleaning rules.

    Deduplication runs first so that a newer record's email is kept
    instead of being overwritten by the placeholder from an older record.
    """
    deduped = deduplicate_customers(customers)
    with_emails = fill_missing_emails(deduped)
    return with_emails.assign(phone=standardize_phone(with_emails["phone"]))


def filter_valid_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Drop orders whose total_amount is zero, negative, or missing.

    These rows are system errors. Filtering is based on the amount alone,
    not on status, because some zero-amount orders are marked COMPLETED.
    """
    amounts = pd.to_numeric(orders["total_amount"], errors="coerce")
    return orders[amounts > 0].reset_index(drop=True)


def convert_to_usd(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Add a usd_amount column using the exchange rate for each order_date.

    Currencies are normalised to upper case. An order whose currency is
    missing, or has no rate for its order_date, is assumed to already be in
    USD (rate 1.0). Raises pandas.errors.MergeError if the rates table has
    more than one rate for the same currency and date, since that would
    duplicate orders in the join.
    """
    currency = (
        orders["currency"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .fillna(BASE_CURRENCY)
    )
    lookup = rates.assign(
        currency=rates["currency"].astype("string").str.strip().str.upper()
    ).rename(columns={"date": "order_date"})[["currency", "order_date", "rate_to_usd"]]

    merged = orders.assign(currency=currency).merge(
        lookup, on=["currency", "order_date"], how="left", validate="many_to_one"
    )
    rate = merged["rate_to_usd"].fillna(1.0)
    return merged.assign(
        usd_amount=(merged["total_amount"] * rate).round(2)
    ).drop(columns="rate_to_usd")
