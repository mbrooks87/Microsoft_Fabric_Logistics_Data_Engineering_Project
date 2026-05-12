# Microsoft Fabric — WMS SQL-First Medallion Architecture

A warehouse management system (WMS) data pipeline built entirely in SQL on Microsoft Fabric, following the Bronze → Silver → Gold medallion pattern with a live IoT streaming layer.

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
                                                      │
                                                      ▼
                                          ┌──────────────────────┐
                                          │  Power BI / DirectLake│
                                          │  Star schema reports  │
                                          │  + Real-time IoT tiles│
                                          └──────────────────────┘
```

---

## Repository Structure

```
notebooks/
├── bronze/
│   └── nb_bronze_ingest_sql.py      # Part 1 — 19 tables via read_files() CTAS
├── silver/
│   └── nb_silver_transform_sql.py   # Part 2 — 15 tables, CTE dedup + type casts
├── gold/
│   └── nb_gold_build_sql.py         # Part 3 — star schema + KPI marts + OPTIMIZE
├── streaming/
│   └── nb_iot_stream_sql.py         # Part 4 — Delta readStream, 1-hour window agg
└── tests/
    └── nb_dq_validation.py          # Data quality validation across all layers

data/                                # 19 raw CSV source files
docs/
└── WMS_Fabric_SQL_Medallion_Full.pdf

.github/
└── workflows/
    └── deploy.yml                   # CI/CD pipeline

bundle.yml                           # Databricks Asset Bundle config
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

**Streaming architecture:** `raw_iot_events` is ingested in Bronze with `delta.enableChangeDataFeed = true`, enabling the streaming notebook to consume only new rows incrementally via `readStream`. This produces hourly window aggregates in `gold_kpi_iot_stream` alongside the daily batch `gold_kpi_iot_summary` — two views of the same data for different reporting needs.

---

## Pipeline Order

```
[nb_bronze_ingest_sql]    --on success-->
[nb_silver_transform_sql] --on success-->
[nb_gold_build_sql]       --on success-->
[Wait 5 min]              --on success-->
[Power BI semantic model refresh]

[nb_iot_stream_sql]  <-- runs continuously as a separate always-on job
```

---

## Deployment Notes — Microsoft Fabric

### Step 1 — Upload CSV files to your Lakehouse
Before running any notebook, upload all 19 CSVs from the `data/` folder into your Fabric Lakehouse:

1. Open your Fabric Workspace → open your **Lakehouse**
2. In the left panel, click **Files** → right-click → **New subfolder** → name it `raw`
3. Click into the `raw` folder → **Upload files**
4. Select all 19 CSV files from the `data/` folder in this repo
5. Confirm they appear under `Files/raw/` before running Bronze

### Step 2 — Import notebooks into Fabric
1. In your Fabric Workspace → **New** → **Import notebook**
2. Import in this order:
   - `notebooks/bronze/nb_bronze_ingest_sql.py`
   - `notebooks/silver/nb_silver_transform_sql.py`
   - `notebooks/gold/nb_gold_build_sql.py`
   - `notebooks/streaming/nb_iot_stream_sql.py`
   - `notebooks/tests/nb_dq_validation.py`
3. Attach each notebook to your Lakehouse

### Step 3 — Run in order
```
nb_bronze_ingest_sql    → wait for all 19 ✓ OK in validation cell
nb_silver_transform_sql → wait for all 15 ✓ OK in validation cell
nb_gold_build_sql       → wait for all 12 ✓ OK in validation cell
nb_dq_validation        → confirm all checks PASS
nb_iot_stream_sql       → run Cells 1–4 only to start the stream
                          (Cells 5–10 are manual spot-check cells — run separately)
```

### Step 4 — Wire the Data Factory pipeline
1. Fabric Workspace → **New** → **Data pipeline** → name it `pl_wms_medallion`
2. Add four **Notebook activities** in sequence (on-success arrows):
   - Bronze Ingest → Silver Transform → Gold Build → DQ Validation
3. Add a **Schedule trigger** → Daily at 06:00 UTC
4. Run `nb_iot_stream_sql` as a **separate always-on Notebook job** (not part of the daily batch)

### Important notes
- `delta.enableChangeDataFeed = true` on `raw_iot_events` is set automatically in Bronze Cell 19 — do not disable it once the stream is running.
- `CREATE OR REPLACE TABLE` is a full overwrite on each pipeline run. For incremental production loads replace with `INSERT INTO ... WHERE <date_col> > (SELECT MAX(...))`.
- Streaming notebook Cells 5–10 are **manual** spot-check cells — do not run them sequentially with the rest of the notebook.

### CI/CD (`bundle.yml` / `deploy.yml`)
The CI/CD in this repo is built natively for **Microsoft Fabric** using:
- **[fabric-cicd](https://github.com/microsoft/fabric-cicd)** — Microsoft's official Python library for deploying Fabric items via REST API
- **GitHub Actions** — triggers on push to `main` (dev deploy) or manual dispatch (prod deploy)
- **Azure Service Principal** — authenticates securely without personal tokens
- **GitHub Environments** — prod deployment requires manual approval gate

To activate CI/CD, add these secrets to your GitHub repo (Settings → Secrets → Actions):
- `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` — from your Azure service principal
- `FABRIC_WORKSPACE_ID_DEV` — GUID from your dev Fabric workspace URL
- `FABRIC_WORKSPACE_ID_PROD` — GUID from your prod Fabric workspace URL

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Microsoft Fabric | Unified data platform (Lakehouse, Notebooks, Pipelines) |
| Apache Spark SQL | All transformation logic |
| Delta Lake | Storage format with ACID, CDF, streaming support |
| Power BI / DirectLake | Reporting and real-time IoT dashboard tiles |
| GitHub Actions | CI/CD deployment |

---

## About Me

Hi, I'm **Melvin Brooks** — a Data Engineer based in Atlanta, GA with a focus on cloud-native data platforms, SQL-first pipeline design, and Microsoft Fabric.

I built this project to demonstrate end-to-end data engineering on Microsoft Fabric — from raw CSV ingestion through a full medallion architecture to a star schema optimized for Power BI, with a live IoT streaming layer on top.

### What I Work With

- **Microsoft Fabric** — Lakehouse, Notebooks, Data Factory pipelines, DirectLake
- **SQL / Spark SQL** — transformation, aggregation, window functions, CTEs
- **Delta Lake** — ACID transactions, Change Data Feed, streaming, OPTIMIZE/ZORDER
- **PySpark** — used surgically where SQL can't reach (readStream/writeStream)
- **Power BI** — semantic modeling, DAX, DirectLake connections
- **Azure** — Data Factory, ADLS Gen2, Synapse
- **CI/CD** — GitHub Actions, Databricks Asset Bundles

### Connect

- 🔗 [LinkedIn](https://www.linkedin.com/in/marcusbrooks87)
- 💻 [GitHub](https://github.com/mbrooks87)

---

*Feel free to fork, clone, or reach out with questions.*
