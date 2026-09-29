"""Pure data-cleaning functions for the ShopData ETL pipeline.

Every function takes a pandas DataFrame and returns a new DataFrame (or
Series) without touching the database, so the business rules can be unit
tested in isolation from I/O and from Prefect.
"""

import pandas as pd


def standardize_phone(phones: pd.Series) -> pd.Series:
    """Remove every non-numeric character from phone numbers.

    Example: "+1 (555) 123-4567" -> "15551234567".
    Missing values stay missing, and values with no digits at all
    (e.g. "N/A") become missing rather than an empty string.
    """
    digits = phones.astype("string").str.replace(r"\D", "", regex=True)
    return digits.replace("", pd.NA)
