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

# Columns each source view must provide; extraction fails fast if any are missing.
REQUIRED_COLUMNS = {
    "vw_raw_customers": {"customer_id", "full_name", "email", "phone", "signup_date"},
    "vw_raw_orders": {"order_id", "customer_id", "order_date", "total_amount", "currency", "status"},
    "vw_exchange_rates": {"currency", "rate_to_usd", "date"},
}


def is_transient_db_error(task, task_run, state) -> bool:
    """Retry only when the source database is temporarily locked.

    pandas wraps driver errors in its own DatabaseError, so the original
    sqlite3 error is read from __cause__. Missing files, missing views, and
    schema problems will not fix themselves, so they fail immediately.
    """
    try:
        state.result()
    except Exception as exc:
        cause = exc.__cause__ or exc
        return isinstance(cause, sqlite3.OperationalError) and "locked" in str(cause)
    return False


@task(retries=2, retry_delay_seconds=2, retry_condition_fn=is_transient_db_error)
def extract_view(db_path: Path, view_name: str) -> pd.DataFrame:
    """Read every row of one source view, opening the database read-only."""
    logger = get_run_logger()
    if view_name not in REQUIRED_COLUMNS:
        raise ValueError(f"Unknown source view: {view_name!r}")
    if not db_path.exists():
        raise FileNotFoundError(f"Source database not found: {db_path}")

    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)

    missing = REQUIRED_COLUMNS[view_name] - set(df.columns)
    if missing:
        raise ValueError(f"{view_name} is missing required columns: {sorted(missing)}")

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
    try:
        with closing(sqlite3.connect(db_path)) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
    except sqlite3.Error as exc:
        raise RuntimeError(f"Could not write {table_name} to {db_path}") from exc

    logger.info("Loaded %d rows into %s.%s", len(df), db_path.name, table_name)


@flow(name="shopdata-etl")
def shopdata_etl(source_db: Path = SOURCE_DB, target_db: Path = TARGET_DB) -> dict[str, int]:
    """Extract the raw ShopData views, clean them, and load analytics.db.

    Returns the number of rows loaded into each target table.
    """
    logger = get_run_logger()
    logger.info("Starting ETL: %s -> %s", source_db.name, target_db.name)

    raw_customers = extract_view(source_db, "vw_raw_customers")
    raw_orders = extract_view(source_db, "vw_raw_orders")
    rates = extract_view(source_db, "vw_exchange_rates")

    dim_customers = transform_customers(raw_customers)
    fct_orders = transform_orders(raw_orders, rates)

    load_table(dim_customers, "dim_customers", target_db)
    load_table(fct_orders, "fct_orders", target_db)

    summary = {"dim_customers": len(dim_customers), "fct_orders": len(fct_orders)}
    logger.info("ETL complete: %s", summary)
    return summary


if __name__ == "__main__":
    shopdata_etl()
