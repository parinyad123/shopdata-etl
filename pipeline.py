"""Prefect ETL flow that cleans ShopData's raw views for CLV reporting.

Extracts the raw views from shopdata.db (read-only), applies the cleaning
rules in transforms.py, and loads dim_customers and fct_orders into
analytics.db.

Run with:  python pipeline.py
"""

import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task

from transforms import DEFAULT_EMAIL, clean_customers, convert_to_usd, filter_valid_orders

PROJECT_DIR = Path(__file__).resolve().parent
SOURCE_DB = PROJECT_DIR / "shopdata.db"
TARGET_DB = PROJECT_DIR / "analytics.db"

SOURCE_VIEWS = ("vw_raw_customers", "vw_raw_orders", "vw_exchange_rates")


@task(retries=2, retry_delay_seconds=2)
def extract_view(db_path: Path, view_name: str) -> pd.DataFrame:
    """Read every row of one source view, opening the database read-only."""
    logger = get_run_logger()
    if view_name not in SOURCE_VIEWS:
        raise ValueError(f"Unknown source view: {view_name!r}")
    if not db_path.exists():
        raise FileNotFoundError(f"Source database not found: {db_path}")

    uri = f"{db_path.as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)

    logger.info("Extracted %d rows from %s", len(df), view_name)
    return df


@task
def transform_customers(raw_customers: pd.DataFrame) -> pd.DataFrame:
    """Build dim_customers: deduplicate, fill emails, standardise phones."""
    logger = get_run_logger()
    customers = clean_customers(raw_customers)

    logger.info(
        "Customers: %d raw rows -> %d unique customers (%d duplicates removed)",
        len(raw_customers),
        len(customers),
        len(raw_customers) - len(customers),
    )
    logger.info(
        "Customers: %d emails set to %s, %d without a phone number",
        (customers["email"] == DEFAULT_EMAIL).sum(),
        DEFAULT_EMAIL,
        customers["phone"].isna().sum(),
    )
    return customers


@task
def transform_orders(raw_orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Build fct_orders: drop non-positive amounts and convert to USD."""
    logger = get_run_logger()
    valid_orders = filter_valid_orders(raw_orders)
    orders = convert_to_usd(valid_orders, rates)

    logger.info(
        "Orders: %d raw rows -> %d valid orders (%d with amount <= 0 removed)",
        len(raw_orders),
        len(orders),
        len(raw_orders) - len(orders),
    )
    logger.info("Orders: total value %.2f USD", orders["usd_amount"].sum())
    return orders


@task
def load_table(df: pd.DataFrame, table_name: str, db_path: Path) -> None:
    """Write a DataFrame to db_path, replacing the table if it exists."""
    logger = get_run_logger()
    with closing(sqlite3.connect(db_path)) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    logger.info("Loaded %d rows into %s.%s", len(df), db_path.name, table_name)


@flow(name="shopdata-etl")
def shopdata_etl(source_db: Path = SOURCE_DB, target_db: Path = TARGET_DB) -> None:
    """Extract the raw ShopData views, clean them, and load analytics.db."""
    raw_customers = extract_view(source_db, "vw_raw_customers")
    raw_orders = extract_view(source_db, "vw_raw_orders")
    rates = extract_view(source_db, "vw_exchange_rates")

    dim_customers = transform_customers(raw_customers)
    fct_orders = transform_orders(raw_orders, rates)

    load_table(dim_customers, "dim_customers", target_db)
    load_table(fct_orders, "fct_orders", target_db)


if __name__ == "__main__":
    shopdata_etl()
