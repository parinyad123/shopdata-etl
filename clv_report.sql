-- =====================================================================
-- clv_report.sql
-- Customer Lifetime Value (CLV) report built on the cleaned tables in
-- analytics.db.
-- Run with: sqlite3 -readonly -header -column analytics.db ".read clv_report.sql"
--
-- Assumptions:
--   * CANCELLED orders are excluded from both order count and value,
--     since they generate no revenue. PENDING orders are included.
--   * Orders with total_amount <= 0 were already removed by the pipeline.
--   * Customers with no qualifying orders are listed with 0 orders and
--     0.00 value; orders from unknown customer_ids are not reported.
-- =====================================================================

SELECT
    c.customer_id,
    c.full_name,
    COUNT(o.order_id)                         AS total_orders_placed,
    ROUND(COALESCE(SUM(o.usd_amount), 0), 2)  AS lifetime_value_usd,
    STRFTIME('%Y-%m', c.signup_date)          AS customer_cohort
FROM dim_customers AS c
LEFT JOIN fct_orders AS o
       ON  o.customer_id = c.customer_id
       AND o.status IS NOT 'CANCELLED'   -- in ON, not WHERE, to keep customers without orders
GROUP BY
    c.customer_id,
    c.full_name,
    c.signup_date
ORDER BY
    lifetime_value_usd DESC,
    c.customer_id;
