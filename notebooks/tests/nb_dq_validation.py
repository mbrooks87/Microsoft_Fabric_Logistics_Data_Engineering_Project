# COMMAND ----------
# Notebook Header

# WMS SQL-First Medallion — Data Quality Validation
# Notebook: nb_dq_validation
# Runs as the final task in the CI/CD pipeline
# Fails the job if any critical data quality checks fail
#
# Called by GitHub Actions after Gold build completes.
# Parameters:
#   gold_schema   : schema name (default: gold)
#   fail_on_error : whether to raise exception on failure (default: true)


# COMMAND ----------
# CELL 1 — Parameters

gold_schema   = getArgument("gold_schema",   "gold")
fail_on_error = getArgument("fail_on_error", "true").lower() == "true"

print(f"Running DQ validation on schema: {gold_schema}")
print(f"Fail on error: {fail_on_error}")


# COMMAND ----------
# CELL 2 — Row Count Checks

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW dq_row_counts AS
SELECT tbl, rows, CASE WHEN rows > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'gold_fact_orders'      AS tbl, COUNT(*) AS rows FROM {gold_schema}.gold_fact_orders
    UNION ALL SELECT 'gold_fact_inventory',  COUNT(*) FROM {gold_schema}.gold_fact_inventory
    UNION ALL SELECT 'gold_fact_shipments',  COUNT(*) FROM {gold_schema}.gold_fact_shipments
    UNION ALL SELECT 'gold_fact_labor',      COUNT(*) FROM {gold_schema}.gold_fact_labor
    UNION ALL SELECT 'gold_fact_dock',       COUNT(*) FROM {gold_schema}.gold_fact_dock
    UNION ALL SELECT 'gold_dim_customer',    COUNT(*) FROM {gold_schema}.gold_dim_customer
    UNION ALL SELECT 'gold_dim_product',     COUNT(*) FROM {gold_schema}.gold_dim_product
    UNION ALL SELECT 'gold_dim_warehouse',   COUNT(*) FROM {gold_schema}.gold_dim_warehouse
    UNION ALL SELECT 'gold_dim_date',        COUNT(*) FROM {gold_schema}.gold_dim_date
    UNION ALL SELECT 'gold_kpi_supplier',    COUNT(*) FROM {gold_schema}.gold_kpi_supplier
    UNION ALL SELECT 'gold_kpi_returns',     COUNT(*) FROM {gold_schema}.gold_kpi_returns
    UNION ALL SELECT 'gold_kpi_iot_summary', COUNT(*) FROM {gold_schema}.gold_kpi_iot_summary
) t
""")

row_count_results = spark.sql("SELECT * FROM dq_row_counts ORDER BY tbl")
row_count_results.show(15, truncate=False)


# COMMAND ----------
# CELL 3 — Business Rule Checks

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW dq_business_rules AS

-- Rule 1: No negative order values
SELECT
    'no_negative_order_value'   AS rule,
    COUNT(*)                    AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM {gold_schema}.gold_fact_orders
WHERE order_value < 0

UNION ALL

-- Rule 2: Fulfillment rate must be between 0 and 1
SELECT
    'fulfillment_rate_range',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_orders
WHERE avg_fulfillment_rate < 0 OR avg_fulfillment_rate > 1

UNION ALL

-- Rule 3: No null customer_id in fact_orders
SELECT
    'no_null_customer_id',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_orders
WHERE customer_id IS NULL

UNION ALL

-- Rule 4: No null date_key in fact_orders
SELECT
    'no_null_date_key',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_orders
WHERE date_key IS NULL

UNION ALL

-- Rule 5: Inventory value must be non-negative
SELECT
    'no_negative_inventory_value',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_inventory
WHERE inventory_value < 0

UNION ALL

-- Rule 6: Composite score must be between 0 and 1
SELECT
    'supplier_score_range',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_kpi_supplier
WHERE composite_score < 0 OR composite_score > 1

UNION ALL

-- Rule 7: Labor cost must be positive
SELECT
    'positive_labor_cost',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_labor
WHERE labor_cost <= 0

UNION ALL

-- Rule 8: date_key format check (must be 8 digits YYYYMMDD)
SELECT
    'date_key_format',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_orders
WHERE LENGTH(CAST(date_key AS STRING)) != 8

UNION ALL

-- Rule 9: No orphaned fact_orders (customer must exist in dim)
SELECT
    'referential_integrity_customer',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_orders o
LEFT JOIN {gold_schema}.gold_dim_customer c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

-- Rule 10: No orphaned fact_orders (date must exist in dim)
SELECT
    'referential_integrity_date',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM {gold_schema}.gold_fact_orders o
LEFT JOIN {gold_schema}.gold_dim_date d ON o.date_key = d.date_key
WHERE d.date_key IS NULL
""")

business_rule_results = spark.sql(
    "SELECT * FROM dq_business_rules ORDER BY status DESC, rule"
)
business_rule_results.show(15, truncate=False)


# COMMAND ----------
# CELL 4 — Freshness Check

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW dq_freshness AS
SELECT
    tbl,
    last_loaded,
    hours_since_load,
    CASE WHEN hours_since_load <= 25 THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT 'gold_fact_orders' AS tbl,
           MAX(_loaded_at)    AS last_loaded,
           ROUND((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(MAX(_loaded_at))) / 3600, 1)
               AS hours_since_load
    FROM {gold_schema}.gold_fact_orders
    UNION ALL
    SELECT 'gold_fact_inventory',
           MAX(_loaded_at),
           ROUND((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(MAX(_loaded_at))) / 3600, 1)
    FROM {gold_schema}.gold_fact_inventory
    UNION ALL
    SELECT 'gold_kpi_iot_summary',
           MAX(_loaded_at),
           ROUND((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(MAX(_loaded_at))) / 3600, 1)
    FROM {gold_schema}.gold_kpi_iot_summary
) t
""")

freshness_results = spark.sql(
    "SELECT * FROM dq_freshness ORDER BY hours_since_load DESC"
)
freshness_results.show(truncate=False)


# COMMAND ----------
# CELL 5 — Summary Report + Pass/Fail Gate

summary = spark.sql("""
SELECT
    'Row Count Checks'    AS check_type,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    COUNT(*)                                           AS total
FROM dq_row_counts

UNION ALL

SELECT
    'Business Rule Checks',
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
    COUNT(*)
FROM dq_business_rules

UNION ALL

SELECT
    'Freshness Checks',
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END),
    COUNT(*)
FROM dq_freshness
""")

summary.show(truncate=False)

# Collect total failures
total_failures = spark.sql("""
SELECT
    SUM(failed) AS total_failures
FROM (
    SELECT SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed FROM dq_row_counts
    UNION ALL
    SELECT SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) FROM dq_business_rules
    UNION ALL
    SELECT SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) FROM dq_freshness
) t
""").collect()[0]["total_failures"]

print(f"\n{'='*50}")
print(f"TOTAL FAILURES: {total_failures}")
print(f"{'='*50}\n")

if total_failures > 0 and fail_on_error:
    raise Exception(
        f"❌ Data quality validation FAILED with {total_failures} check(s). "
        f"Review the DQ report above before investigating Gold tables."
    )
elif total_failures > 0:
    print(f"⚠️  {total_failures} DQ check(s) failed — fail_on_error=false, continuing.")
else:
    print("✅ All data quality checks PASSED. Pipeline is healthy.")
