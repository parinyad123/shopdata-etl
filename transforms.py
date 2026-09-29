"""Pure data-cleaning functions for the ShopData ETL pipeline.

Every function takes a pandas DataFrame and returns a new DataFrame (or
Series) without touching the database, so the business rules can be unit
tested in isolation from I/O and from Prefect.
"""

import pandas as pd

DEFAULT_EMAIL = "unknown@domain.com"


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
