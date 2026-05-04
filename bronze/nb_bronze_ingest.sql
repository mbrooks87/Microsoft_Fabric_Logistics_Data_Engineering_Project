-- ============================================================
-- nb_bronze_ingest.sql
-- WMS SQL-First Medallion Architecture
-- Part 1 of 4: Bronze Ingestion
-- 19 tables · read_files() CTAS · delta.enableChangeDataFeed
-- ============================================================


-- ------------------------------------------------------------
-- CELL 1: Schema Bootstrap (run once)
-- ------------------------------------------------------------
-- for schema in ['bronze', 'silver', 'gold']:
--     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


-- ------------------------------------------------------------
-- CELL 2: raw_orders (50,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_orders
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_orders.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE order_id IS NOT NULL;

SELECT 'bronze.raw_orders' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_orders;


-- ------------------------------------------------------------
-- CELL 3: raw_order_lines (128,899 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_order_lines
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_order_lines.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE line_id IS NOT NULL;

SELECT 'bronze.raw_order_lines' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_order_lines;


-- ------------------------------------------------------------
-- CELL 4: raw_customers (500 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_customers
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_customers.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE customer_id IS NOT NULL;

SELECT 'bronze.raw_customers' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_customers;


-- ------------------------------------------------------------
-- CELL 5: raw_products (1,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_products
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_products.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE sku IS NOT NULL;

SELECT 'bronze.raw_products' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_products;


-- ------------------------------------------------------------
-- CELL 6: raw_inventory (10,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_inventory
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_inventory.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE inventory_id IS NOT NULL;

SELECT 'bronze.raw_inventory' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_inventory;


-- ------------------------------------------------------------
-- CELL 7: raw_inventory_transactions (50,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_inventory_transactions
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_inventory_transactions.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE txn_id IS NOT NULL;

SELECT 'bronze.raw_inventory_transactions' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_inventory_transactions;


-- ------------------------------------------------------------
-- CELL 8: raw_shipments (30,053 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_shipments
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_shipments.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE shipment_id IS NOT NULL;

SELECT 'bronze.raw_shipments' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_shipments;


-- ------------------------------------------------------------
-- CELL 9: raw_carriers (10 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_carriers
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_carriers.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE carrier_id IS NOT NULL;

SELECT 'bronze.raw_carriers' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_carriers;


-- ------------------------------------------------------------
-- CELL 10: raw_warehouses (10 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_warehouses
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_warehouses.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE warehouse_id IS NOT NULL;

SELECT 'bronze.raw_warehouses' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_warehouses;


-- ------------------------------------------------------------
-- CELL 11: raw_locations (2,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_locations
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_locations.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE location_id IS NOT NULL;

SELECT 'bronze.raw_locations' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_locations;


-- ------------------------------------------------------------
-- CELL 12: raw_purchase_orders (8,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_purchase_orders
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_purchase_orders.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE po_id IS NOT NULL;

SELECT 'bronze.raw_purchase_orders' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_purchase_orders;


-- ------------------------------------------------------------
-- CELL 13: raw_suppliers (80 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_suppliers
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_suppliers.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE supplier_id IS NOT NULL;

SELECT 'bronze.raw_suppliers' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_suppliers;


-- ------------------------------------------------------------
-- CELL 14: raw_supplier_performance (2,400 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_supplier_performance
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_supplier_performance.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE perf_id IS NOT NULL;

SELECT 'bronze.raw_supplier_performance' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_supplier_performance;


-- ------------------------------------------------------------
-- CELL 15: raw_returns (5,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_returns
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_returns.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE return_id IS NOT NULL;

SELECT 'bronze.raw_returns' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_returns;


-- ------------------------------------------------------------
-- CELL 16: raw_employees (200 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_employees
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_employees.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE employee_id IS NOT NULL;

SELECT 'bronze.raw_employees' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_employees;


-- ------------------------------------------------------------
-- CELL 17: raw_labor_shifts (25,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_labor_shifts
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_labor_shifts.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE shift_id IS NOT NULL;

SELECT 'bronze.raw_labor_shifts' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_labor_shifts;


-- ------------------------------------------------------------
-- CELL 18: raw_dock_activity (15,000 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_dock_activity
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_dock_activity.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE dock_id IS NOT NULL;

SELECT 'bronze.raw_dock_activity' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_dock_activity;


-- ------------------------------------------------------------
-- CELL 19: raw_iot_events (100,000 rows)
-- NOTE: enableChangeDataFeed = true required for Part 4 streaming
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_iot_events
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
)
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_iot_events.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE event_id IS NOT NULL;

SELECT 'bronze.raw_iot_events' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_iot_events;


-- ------------------------------------------------------------
-- CELL 20: raw_date_dim (1,096 rows)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.raw_date_dim
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
AS
SELECT
    *,
    current_timestamp() AS _loaded_at
FROM read_files(
    'Files/raw/raw_date_dim.csv',
    format      => 'csv',
    header      => 'true',
    inferSchema => 'true',
    nullValue   => ''
)
WHERE date_key IS NOT NULL;

SELECT 'bronze.raw_date_dim' AS table_name, COUNT(*) AS row_count
FROM bronze.raw_date_dim;


-- ------------------------------------------------------------
-- CELL 21: Full Bronze Validation
-- All 19 rows must show status = 'OK' before running Silver
-- ------------------------------------------------------------
WITH audit AS (
    SELECT 'raw_orders'                 AS tbl, 'order_id'     AS pk, COUNT(*) AS rows, SUM(CASE WHEN order_id     IS NULL THEN 1 ELSE 0 END) AS pk_nulls FROM bronze.raw_orders
    UNION ALL
    SELECT 'raw_order_lines',                    'line_id',              COUNT(*),        SUM(CASE WHEN line_id     IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_order_lines
    UNION ALL
    SELECT 'raw_customers',                      'customer_id',          COUNT(*),        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_customers
    UNION ALL
    SELECT 'raw_products',                       'sku',                  COUNT(*),        SUM(CASE WHEN sku         IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_products
    UNION ALL
    SELECT 'raw_inventory',                      'inventory_id',         COUNT(*),        SUM(CASE WHEN inventory_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_inventory
    UNION ALL
    SELECT 'raw_inventory_transactions',         'txn_id',               COUNT(*),        SUM(CASE WHEN txn_id      IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_inventory_transactions
    UNION ALL
    SELECT 'raw_shipments',                      'shipment_id',          COUNT(*),        SUM(CASE WHEN shipment_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_shipments
    UNION ALL
    SELECT 'raw_carriers',                       'carrier_id',           COUNT(*),        SUM(CASE WHEN carrier_id  IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_carriers
    UNION ALL
    SELECT 'raw_warehouses',                     'warehouse_id',         COUNT(*),        SUM(CASE WHEN warehouse_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_warehouses
    UNION ALL
    SELECT 'raw_locations',                      'location_id',          COUNT(*),        SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_locations
    UNION ALL
    SELECT 'raw_purchase_orders',                'po_id',                COUNT(*),        SUM(CASE WHEN po_id       IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_purchase_orders
    UNION ALL
    SELECT 'raw_suppliers',                      'supplier_id',          COUNT(*),        SUM(CASE WHEN supplier_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_suppliers
    UNION ALL
    SELECT 'raw_supplier_performance',           'perf_id',              COUNT(*),        SUM(CASE WHEN perf_id     IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_supplier_performance
    UNION ALL
    SELECT 'raw_returns',                        'return_id',            COUNT(*),        SUM(CASE WHEN return_id   IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_returns
    UNION ALL
    SELECT 'raw_employees',                      'employee_id',          COUNT(*),        SUM(CASE WHEN employee_id IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_employees
    UNION ALL
    SELECT 'raw_labor_shifts',                   'shift_id',             COUNT(*),        SUM(CASE WHEN shift_id    IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_labor_shifts
    UNION ALL
    SELECT 'raw_dock_activity',                  'dock_id',              COUNT(*),        SUM(CASE WHEN dock_id     IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_dock_activity
    UNION ALL
    SELECT 'raw_iot_events',                     'event_id',             COUNT(*),        SUM(CASE WHEN event_id    IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_iot_events
    UNION ALL
    SELECT 'raw_date_dim',                       'date_key',             COUNT(*),        SUM(CASE WHEN date_key    IS NULL THEN 1 ELSE 0 END) FROM bronze.raw_date_dim
)
SELECT
    tbl,
    pk           AS primary_key,
    rows,
    pk_nulls,
    CASE WHEN pk_nulls = 0 THEN 'OK' ELSE 'FIX' END AS status
FROM audit
ORDER BY tbl;
