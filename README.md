# Microsoft Fabric — WMS SQL-First Medallion Architecture

A warehouse management system (WMS) data pipeline built entirely in SQL on Microsoft Fabric, following the Bronze → Silver → Gold medallion pattern.

**47 Delta tables · ~300K source rows · 64 SQL cells · ~18 lines of PySpark**

---

## Architecture

```
Raw CSVs (Files/raw/)
      │
      ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│           GOLD              │
│  19 tables  │     │  15 tables  │     │  4 dims · 5 facts · 3 KPIs  │
│ read_files()│     │ ROW_NUMBER()│     │  OPTIMIZE · ZORDER · VACUUM │
│    CTAS     │     │   dedup +   │     │                             │
│             │     │ typed casts │     │  + IoT streaming (Part 4)   │
└─────────────┘     └─────────────┘     └─────────────────────────────┘
```

---

## Files

```
bronze/
└── nb_bronze_ingest.sql       # Part 1 — 19 tables via read_files() CTAS

silver/
└── nb_silver_transform.sql    # Part 2 — 15 tables, CTE dedup + type casts

gold/
├── nb_gold_build.sql          # Part 3 — star schema + KPI marts + OPTIMIZE
└── nb_iot_stream.sql          # Part 4 — Delta readStream, 1-hour window agg
```

---

## Source Tables (Bronze)

| Table | PK | Rows |
|---|---|---|
| raw_orders | order_id | 50,000 |
| raw_order_lines | line_id | 128,899 |
| raw_customers | customer_id | 500 |
| raw_products | sku | 1,000 |
| raw_inventory | inventory_id | 10,000 |
| raw_inventory_transactions | txn_id | 50,000 |
| raw_shipments | shipment_id | 30,053 |
| raw_carriers | carrier_id | 10 |
| raw_warehouses | warehouse_id | 10 |
| raw_locations | location_id | 2,000 |
| raw_purchase_orders | po_id | 8,000 |
| raw_suppliers | supplier_id | 80 |
| raw_supplier_performance | perf_id | 2,400 |
| raw_returns | return_id | 5,000 |
| raw_employees | employee_id | 200 |
| raw_labor_shifts | shift_id | 25,000 |
| raw_dock_activity | dock_id | 15,000 |
| raw_iot_events | event_id | 100,000 |
| raw_date_dim | date_key | 1,096 |

---

## Gold Star Schema

**Facts**
- `gold_fact_orders` — 1 row per order, line + shipment aggregates
- `gold_fact_inventory` — 1 row per inventory_id (sku × warehouse × location)
- `gold_fact_shipments` — 1 row per shipment
- `gold_fact_labor` — 1 row per shift
- `gold_fact_dock` — 1 row per dock activity

**Dimensions**
- `gold_dim_date` — time-intelligence anchor for Power BI
- `gold_dim_customer`
- `gold_dim_product` — includes price_tier bucket
- `gold_dim_warehouse`

**KPI Marts**
- `gold_kpi_supplier` — composite score, A/B/C tier per supplier per month
- `gold_kpi_returns` — loss_amount on non-resellable returns
- `gold_kpi_iot_summary` — daily batch aggregate from IoT events
- `gold_kpi_iot_stream` — hourly streaming aggregate (Part 4)

---

## Key Design Decisions

**SQL-first:** All typing, deduplication, joining, aggregation, and optimization is expressed in SQL. PySpark is used only where unavoidable — 3 lines for schema bootstrap and ~15 lines for `readStream`/`writeStream` wrappers in the IoT streaming notebook.

**Deduplication:** `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _loaded_at DESC)` keeps the most recently loaded record. No `.dropDuplicates()`.

**Reference joins from Bronze:** Silver tables join Bronze reference data (`raw_products`, `raw_carriers`, `raw_suppliers`, `raw_warehouses`, `raw_employees`) directly to avoid inter-Silver ordering dependencies and keep each cell independently re-runnable.

**CTE pre-aggregation in gold_fact_orders:** `line_agg` and `ship_agg` CTEs roll 128k line rows and 30k shipment rows up to order grain before joining back to `silver_orders`, enabling a broadcast join instead of a full shuffle.

---

## Pipeline Order

```
[nb_bronze_ingest]  --on success-->
[nb_silver_transform]  --on success-->
[nb_gold_build]  --on success-->
[Wait 5 min]  --on success-->
[Power BI semantic model refresh]

[nb_iot_stream]  <-- runs continuously as a separate always-on job
```

---

## Deployment Notes

- Upload all 19 CSVs to `Files/raw/` in your Fabric Lakehouse before running Bronze.
- `delta.enableChangeDataFeed = true` on `raw_iot_events` is required for the streaming notebook. Do not disable it once the stream is running.
- `CREATE OR REPLACE TABLE` is a full overwrite. For incremental production loads replace with `INSERT INTO ... WHERE <date_col> > (SELECT MAX(...))`.
- Run the validation query at the end of each notebook before proceeding to the next layer.
