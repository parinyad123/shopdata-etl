# ShopData ETL Pipeline

ETL pipeline that extracts raw order-management data from `shopdata.db`, cleans it,
and loads it into `analytics.db` for Customer Lifetime Value (CLV) reporting.

## Setup

Requires Python 3.12+.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

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
| 6 | **Incomplete exchange-rate coverage** | Rates exist only for 2023-05-01 to 2023-05-05, but orders run to 2023-05-14. Six non-USD orders have no rate for their date (110, 111, 113, 115, 118, 120). | Treat as USD per the business rule.<br><br>**Impact:** order 115 (25,000 JPY, about $175 at recent rates) is counted as $25,000, which inflates its customer's CLV. Backfilling the rate table would fix this. |
| 7 | **Orphan orders** | Orders 106 and 118 reference `customer_id` 99, which does not exist in the customer view. | Kept in `fct_orders`. They fall out of the CLV report because it joins to `dim_customers`. |
| 8 | **Missing order date** | Order 117 has a NULL `order_date`. | Kept. It is a USD order, so no rate lookup is needed. |
| 9 | **Orders dated before signup** | 11 orders predate their customer's latest `signup_date` (e.g. Alice's orders in 2023-05 vs. a latest signup of 2023-06-01). This suggests `signup_date` behaves more like a "last updated" date. | Deduplicate as instructed, but note that `customer_cohort` in the CLV report may be later than the customer's true first activity. |

### Checks with no issues found

- No duplicate `order_id`s
- No duplicate exchange rates per currency and date, so rate joins do not fan out
- All non-null dates are valid `YYYY-MM-DD` strings

## Running the Pipeline

_TODO_

## Running the Tests

_TODO_

## CLV Report

_TODO_
