# ShopData ETL Pipeline

ETL pipeline that extracts raw order-management data from `shopdata.db`, cleans it,
and loads it into `analytics.db` for Customer Lifetime Value (CLV) reporting.

## Setup

Requires Python 3.12+ and the `sqlite3` command-line shell (used to run the `.sql` files).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Project Structure

| File | Purpose |
|------|---------|
| `exploration.sql` | Data quality queries against the raw views (Part 1) |
| `transforms.py` | Pure cleaning functions: no database or Prefect dependencies |
| `pipeline.py` | Prefect flow: extract → transform → load (Part 2) |
| `tests/test_transforms.py` | Unit tests for every cleaning rule, using in-memory DataFrames (Part 3) |
| `clv_report.sql` | Customer Lifetime Value query on the cleaned tables (Part 4) |
| `shopdata.db` | Source database (read-only input) |

## Data Exploration Findings

Queries are in [`exploration.sql`](exploration.sql). Run them with:

```bash
sqlite3 -readonly -header -column shopdata.db ".read exploration.sql"
```

| # | Issue | Evidence | Handling in pipeline |
|---|-------|----------|----------------------|
| 1 | **Duplicate customers** | 12 rows but only 10 distinct `customer_id`s. Customers 1 (Alice) and 2 (Bob) each appear twice with different email/phone/signup_date. The newer record is more complete (Bob's older row has a NULL email). | Keep the row with the latest `signup_date` per `customer_id`. |
| 2 | **Missing emails** | 2 NULL emails (Bob's older record, Hannah). After deduplication only Hannah remains. | Replace with `unknown@domain.com`. |
| 3 | **Inconsistent phone formats** | Four formats: digits only (`15551234567`), punctuated (`+1 (555) 123-4567`, `(555) 333 4444`), containing letters (`Ext 444`, `1-800-555-DINO`), and NULL (2 rows). | Strip all non-numeric characters. Note: this turns `Ext 444` into `444` and drops the letters from `1-800-555-DINO`, so those values remain unreliable. |
| 4 | **Zero / negative amounts** | Orders 103 (-50.00) and 113 (-100.00) have status `SYSTEM_ERROR`; order 114 (0.00) is marked `COMPLETED`. Filtering by status alone would miss it. | Drop orders with `total_amount <= 0`. |
| 5 | **Missing currency** | Orders 107 and 116 have a NULL currency. | Treat as USD. |
| 6 | **Incomplete exchange-rate coverage** | Rates exist only for 2023-05-01 to 2023-05-05, but orders run to 2023-05-14. Six non-USD orders have no rate for their date (110, 111, 113, 115, 118, 120). | Treat as USD per the business rule.<br><br>**Impact:** order 115 (25,000 JPY, about $180 at the latest available rate) is counted as $25,000, which inflates its customer's CLV. Backfilling the rate table would fix this. |
| 7 | **Orphan orders** | Orders 106 and 118 reference `customer_id` 99, which does not exist in the customer view. | Kept in `fct_orders`. They fall out of the CLV report because it joins to `dim_customers`. |
| 8 | **Missing order date** | Order 117 has a NULL `order_date`. | Kept. It is a USD order, so no rate lookup is needed. |
| 9 | **Orders dated before signup** | 11 orders predate their customer's latest `signup_date` (e.g. Alice's orders in 2023-05 vs. a latest signup of 2023-06-01). This suggests `signup_date` behaves more like a "last updated" date. | Deduplicate as instructed, but note that `customer_cohort` in the CLV report may be later than the customer's true first activity. |

### Checks with no issues found

- No duplicate `order_id`s
- No duplicate exchange rates per currency and date, so rate joins do not fan out
- All non-null dates are valid `YYYY-MM-DD` strings

## Running the Pipeline

```bash
python pipeline.py
```

Prefect 3 starts a temporary local server automatically, so no separate server is needed. The flow:

1. **Extracts** the three source views, opening `shopdata.db` read-only and checking that each view has the expected columns.
2. **Transforms** customers (deduplicate, fill emails, standardize phones) and orders (drop amounts <= 0, convert to USD) using the functions in `transforms.py`.
3. **Loads** the results into `analytics.db` as `dim_customers` (10 rows) and `fct_orders` (17 rows).

Tables are replaced on every run, so the pipeline can be rerun safely. Each task logs row counts before and after cleaning. Extract retries only when the source database is locked. Missing files, missing views, or missing columns fail immediately with a descriptive error.

> **Note:** Prefect 3.8 may log a `database is locked` error from its own telemetry service. It does not affect the flow, and can be turned off with:
> ```bash
> prefect config set PREFECT_SERVER_ANALYTICS_ENABLED=false
> ```

### Output Tables (`analytics.db`)

**`dim_customers`**: one row per customer (grain: `customer_id`)

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | INTEGER | Unique customer identifier |
| `full_name` | TEXT | Customer name |
| `email` | TEXT | Email address; `unknown@domain.com` when missing in the source |
| `phone` | TEXT | Digits only; NULL when missing or when the source had no digits |
| `signup_date` | TEXT | `YYYY-MM-DD`, taken from the most recent source record |

**`fct_orders`**: one row per valid order (grain: `order_id`)

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | INTEGER | Unique order identifier |
| `customer_id` | INTEGER | May not exist in `dim_customers` (orphan orders are kept) |
| `order_date` | TEXT | `YYYY-MM-DD`; can be NULL |
| `total_amount` | REAL | Amount in the original currency; always > 0 |
| `currency` | TEXT | Upper-case ISO code; `USD` when missing in the source |
| `status` | TEXT | `COMPLETED`, `PENDING`, or `CANCELLED` (all kept) |
| `usd_amount` | REAL | `total_amount` converted at the rate for `order_date`, rounded to cents; equals `total_amount` when no rate exists |


## Running the Tests

```bash
pytest -v
```

23 unit tests cover phone standardization, customer deduplication, email filling, order filtering, and USD conversion. Tests build small DataFrames in memory and never touch a database, so they run in under a second.

## CLV Report

```bash
sqlite3 -readonly -header -column analytics.db ".read clv_report.sql"
```

### Assumptions

- **CANCELLED orders are excluded** from both `total_orders_placed` and `lifetime_value_usd`, since they generate no revenue. PENDING orders are included because they are still expected to complete. Both columns use the same rule so they stay consistent.
- **All customers are listed**, including those with no qualifying orders (value 0.00).
- **Orders from unknown customers** (`customer_id` 99) are excluded, because the report starts from `dim_customers`.
- **Filtering happens in the report, not the pipeline.** `fct_orders` keeps every status so other analyses (e.g. cancellation rate) remain possible.

### Result

| customer_id | full_name | total_orders_placed | lifetime_value_usd | customer_cohort |
|---:|---|---:|---:|---|
| 3 | Charlie Brown | 1 | 25,000.00 | 2023-03 |
| 1 | Alice Smith | 3 | 1,686.00 | 2023-06 |
| 6 | Fiona Gallagher | 2 | 525.00 | 2023-05 |
| 4 | Diana Prince | 2 | 389.50 | 2023-04 |
| 2 | Bob Jones | 2 | 275.00 | 2023-09 |
| 5 | Evan Wright | 2 | 219.99 | 2023-04 |
| 8 | Hannah Abbott | 1 | 89.00 | 2023-07 |
| 7 | George Costanza | 1 | 40.00 | 2023-06 |
| 9 | Ian Malcolm | 0 | 0.00 | 2023-08 |
| 10 | Jane Doe | 0 | 0.00 | 2023-09 |

**Charlie Brown ranks first only because of the missing exchange rate** (issue 6):

- His single order is 25,000 JPY on 2023-05-10, a date with no rate in `vw_exchange_rates`.
- The fallback rule treats it as USD (rate 1.0), so it is counted as $25,000.
- At the most recent available JPY rate (0.0072) it would be about $180, which would place him sixth.
- **Action:** backfill the source exchange rates from 2023-05-06 onward before this report is used for decisions.
