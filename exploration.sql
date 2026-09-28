-- =====================================================================
-- exploration.sql
-- Data quality exploration of the raw ShopData views in shopdata.db.
-- Run with: sqlite3 shopdata.db < exploration.sql
-- =====================================================================


-- =====================================================================
-- 1. CUSTOMERS (vw_raw_customers)
-- =====================================================================

-- 1.1 Overview: total rows vs. distinct customer_ids, and null counts per column.
--     If total_rows > distinct_customer_ids, the view contains duplicates.
SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(DISTINCT customer_id)                     AS distinct_customer_ids,
    SUM(customer_id IS NULL)                        AS null_customer_id,
    SUM(full_name IS NULL OR TRIM(full_name) = '')  AS missing_full_name,
    SUM(email IS NULL OR TRIM(email) = '')          AS missing_email,
    SUM(phone IS NULL OR TRIM(phone) = '')          AS missing_phone,
    SUM(signup_date IS NULL)                        AS null_signup_date
FROM vw_raw_customers;

-- 1.2 Duplicate customer_ids: list every version of each duplicated customer
--     so we can see which attributes differ between versions.
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.signup_date,
    ROW_NUMBER() OVER (
        PARTITION BY c.customer_id
        ORDER BY c.signup_date DESC
    ) AS version_rank   -- 1 = most recent signup_date (the record to keep)
FROM vw_raw_customers AS c
WHERE c.customer_id IN (
    SELECT customer_id
    FROM vw_raw_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
ORDER BY c.customer_id, version_rank;

-- 1.3 Missing or malformed emails (NULL, blank, or not shaped like x@y.z).
SELECT customer_id, full_name, email
FROM vw_raw_customers
WHERE email IS NULL
   OR TRIM(email) = ''
   OR email NOT LIKE '%_@_%._%';

-- 1.4 Phone format inventory: shows how inconsistent the formats are and
--     flags values that contain letters or have an unusual digit count
--     once formatting characters are stripped.
SELECT
    customer_id,
    phone,
    LENGTH(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            phone, ' ', ''), '-', ''), '(', ''), ')', ''), '+', ''), '.', '')
    ) AS stripped_length,
    CASE
        WHEN phone IS NULL                THEN 'missing'
        WHEN phone GLOB '*[A-Za-z]*'      THEN 'contains letters'
        WHEN phone NOT GLOB '*[^0-9]*'    THEN 'digits only'
        ELSE 'formatted (symbols/spaces)'
    END AS phone_format
FROM vw_raw_customers
ORDER BY phone_format, customer_id;

-- 1.5 signup_date values that are not valid ISO dates (YYYY-MM-DD).
--     DATE() returns NULL for values SQLite cannot parse.
SELECT customer_id, signup_date
FROM vw_raw_customers
WHERE signup_date IS NULL
   OR DATE(signup_date) IS NULL
   OR DATE(signup_date) <> signup_date;
