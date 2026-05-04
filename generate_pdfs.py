from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                 PageBreak, Table, TableStyle, HRFlowable)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

# ── Color palette ──────────────────────────────────────────────
BRONZE_COLOR  = colors.HexColor('#cd7f32')
SILVER_COLOR  = colors.HexColor('#708090')
GOLD_COLOR    = colors.HexColor('#b8860b')
BG_CODE       = colors.HexColor('#1e1e2e')
FG_CODE       = colors.HexColor('#cdd6f4')
BG_HEADER     = colors.HexColor('#2a2a3e')
WHITE         = colors.white
LIGHT_GRAY    = colors.HexColor('#f5f5f5')
DARK_TEXT     = colors.HexColor('#1a1a2e')

def make_styles(accent):
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle',
        fontSize=26, leading=32, textColor=WHITE,
        backColor=accent, alignment=TA_CENTER,
        spaceAfter=6, spaceBefore=6,
        leftIndent=-72, rightIndent=-72,
        fontName='Helvetica-Bold')
    h1 = ParagraphStyle('H1', fontSize=16, leading=20,
        textColor=accent, fontName='Helvetica-Bold',
        spaceBefore=18, spaceAfter=6)
    h2 = ParagraphStyle('H2', fontSize=12, leading=16,
        textColor=DARK_TEXT, fontName='Helvetica-Bold',
        spaceBefore=14, spaceAfter=4)
    h3 = ParagraphStyle('H3', fontSize=10, leading=14,
        textColor=accent, fontName='Helvetica-Bold',
        spaceBefore=10, spaceAfter=3)
    body = ParagraphStyle('Body', fontSize=9, leading=13,
        textColor=DARK_TEXT, fontName='Helvetica',
        spaceBefore=3, spaceAfter=3)
    code = ParagraphStyle('Code', fontSize=7.5, leading=11,
        textColor=FG_CODE, backColor=BG_CODE,
        fontName='Courier', leftIndent=8, rightIndent=8,
        spaceBefore=2, spaceAfter=2,
        borderPad=6)
    note = ParagraphStyle('Note', fontSize=8, leading=12,
        textColor=colors.HexColor('#555555'),
        fontName='Helvetica-Oblique',
        spaceBefore=2, spaceAfter=2, leftIndent=12)
    subtitle = ParagraphStyle('Subtitle', fontSize=11, leading=15,
        textColor=WHITE, backColor=accent,
        alignment=TA_CENTER, fontName='Helvetica',
        spaceAfter=4)
    cell_label = ParagraphStyle('CellLabel', fontSize=9, leading=12,
        textColor=WHITE, backColor=BG_HEADER,
        fontName='Helvetica-Bold', leftIndent=6,
        spaceBefore=8, spaceAfter=2)
    return dict(title=title_style, h1=h1, h2=h2, h3=h3, body=body,
                code=code, note=note, subtitle=subtitle, cell_label=cell_label)

def code_block(lines, s):
    """Wrap a list of code lines into a code Paragraph."""
    escaped = '<br/>'.join(
        l.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
        for l in lines
    )
    return Paragraph(escaped, s['code'])

def cell_header(label, s):
    return Paragraph(label, s['cell_label'])

def hr(accent):
    return HRFlowable(width='100%', thickness=1.5, color=accent,
                      spaceAfter=4, spaceBefore=4)

# ═══════════════════════════════════════════════════════════════
# BRONZE
# ═══════════════════════════════════════════════════════════════
def build_bronze(path):
    doc = SimpleDocTemplate(path, pagesize=letter,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)
    s = make_styles(BRONZE_COLOR)
    story = []

    story.append(Paragraph('Microsoft Fabric · WMS SQL-First Medallion', s['title']))
    story.append(Paragraph('PART 1 — BRONZE INGESTION', s['subtitle']))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        'Notebook: nb_bronze_ingest_sql  ·  19 Delta Tables  ·  21 Cells  ·  3 Lines of PySpark',
        s['h2']))
    story.append(hr(BRONZE_COLOR))

    # Philosophy
    story.append(Paragraph('Philosophy', s['h1']))
    for bullet in [
        '• All 19 tables ingested via SQL read_files() TVF — zero PySpark loops.',
        '• Python used ONCE: 3-line schema bootstrap only.',
        '• Each table is its own SQL cell for granular error isolation.',
        '• delta.enableChangeDataFeed = true on raw_iot_events so Part 4 can read it incrementally.',
        '• Run Cell 1, then Cells 2–20 in any order, then Cell 21 for validation.',
    ]:
        story.append(Paragraph(bullet, s['body']))
    story.append(Spacer(1, 8))

    # Cell 1 - Schema Bootstrap
    story.append(cell_header('CELL 1 — Schema Bootstrap (Python — only Python in this notebook)', s))
    story.append(code_block([
        "for schema in ['bronze', 'silver', 'gold']:",
        "    spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {schema}\")",
        "print(\"Schemas ready: bronze | silver | gold\")",
    ], s))

    # Tables mapping: (cell_num, table_name, pk, rows, filepath)
    tables = [
        (2,  'raw_orders',                'order_id',     '50,000',   'raw_orders.csv'),
        (3,  'raw_order_lines',           'line_id',      '128,899',  'raw_order_lines.csv'),
        (4,  'raw_customers',             'customer_id',  '500',      'raw_customers.csv'),
        (5,  'raw_products',              'sku',          '1,000',    'raw_products.csv'),
        (6,  'raw_inventory',             'inventory_id', '10,000',   'raw_inventory.csv'),
        (7,  'raw_inventory_transactions','txn_id',       '50,000',   'raw_inventory_transactions.csv'),
        (8,  'raw_shipments',             'shipment_id',  '30,053',   'raw_shipments.csv'),
        (9,  'raw_carriers',              'carrier_id',   '10',       'raw_carriers.csv'),
        (10, 'raw_warehouses',            'warehouse_id', '10',       'raw_warehouses.csv'),
        (11, 'raw_locations',             'location_id',  '2,000',    'raw_locations.csv'),
        (12, 'raw_purchase_orders',       'po_id',        '8,000',    'raw_purchase_orders.csv'),
        (13, 'raw_suppliers',             'supplier_id',  '80',       'raw_suppliers.csv'),
        (14, 'raw_supplier_performance',  'perf_id',      '2,400',    'raw_supplier_performance.csv'),
        (15, 'raw_returns',               'return_id',    '5,000',    'raw_returns.csv'),
        (16, 'raw_employees',             'employee_id',  '200',      'raw_employees.csv'),
        (17, 'raw_labor_shifts',          'shift_id',     '25,000',   'raw_labor_shifts.csv'),
        (18, 'raw_dock_activity',         'dock_id',      '15,000',   'raw_dock_activity.csv'),
        (19, 'raw_iot_events',            'event_id',     '100,000',  'raw_iot_events.csv'),
        (20, 'raw_date_dim',              'date_key',     '1,096',    'raw_date_dim.csv'),
    ]

    story.append(Paragraph('Cells 2–20 — read_files() CTAS for all 19 Tables', s['h1']))
    story.append(Paragraph(
        'Each cell follows the same pattern. Cells 2–18 use standard TBLPROPERTIES. '
        'Cell 19 (raw_iot_events) adds delta.enableChangeDataFeed = true and '
        'delta.autoOptimize.autoCompact = true for the streaming notebook in Part 4.',
        s['body']))

    for (num, tbl, pk, rows, csv) in tables:
        extra_props = ''
        if tbl == 'raw_iot_events':
            extra_props = "\n 'delta.autoOptimize.autoCompact' = 'true',\n 'delta.enableChangeDataFeed' = 'true'"

        story.append(cell_header(f'CELL {num} — bronze.{tbl}  ({rows} rows)', s))
        code_lines = [
            f"CREATE OR REPLACE TABLE bronze.{tbl}",
            "USING DELTA",
        ]
        if extra_props:
            code_lines += [
                "TBLPROPERTIES (",
                " 'delta.autoOptimize.optimizeWrite' = 'true',",
                " 'delta.autoOptimize.autoCompact'   = 'true',",
                " 'delta.enableChangeDataFeed'       = 'true'",
                ")",
            ]
        else:
            code_lines += ["TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"]
        code_lines += [
            "AS",
            "SELECT",
            "  *,",
            "  current_timestamp() AS _loaded_at",
            "FROM read_files(",
            f"  'Files/raw/{csv}',",
            "  format      => 'csv',",
            "  header      => 'true',",
            "  inferSchema => 'true',",
            "  nullValue   => ''",
            ")",
            f"WHERE {pk} IS NOT NULL;",
            "",
            f"SELECT 'bronze.{tbl}' AS table_name, COUNT(*) AS row_count",
            f"FROM bronze.{tbl};",
        ]
        story.append(code_block(code_lines, s))

    # Cell 21 - Validation
    story.append(PageBreak())
    story.append(Paragraph('Cell 21 — Full Bronze Validation (UNION ALL Audit)', s['h1']))
    story.append(Paragraph(
        'Row counts + PK null audit across all 19 tables in one query. '
        'All rows in the status column must read ✓ OK before proceeding to Silver (Part 2).',
        s['body']))

    audit_rows = ['raw_orders/order_id','raw_order_lines/line_id','raw_customers/customer_id',
                  'raw_products/sku','raw_inventory/inventory_id',
                  'raw_inventory_transactions/txn_id','raw_shipments/shipment_id',
                  'raw_carriers/carrier_id','raw_warehouses/warehouse_id',
                  'raw_locations/location_id','raw_purchase_orders/po_id',
                  'raw_suppliers/supplier_id','raw_supplier_performance/perf_id',
                  'raw_returns/return_id','raw_employees/employee_id',
                  'raw_labor_shifts/shift_id','raw_dock_activity/dock_id',
                  'raw_iot_events/event_id','raw_date_dim/date_key']

    union_lines = ['WITH audit AS (']
    for i, entry in enumerate(audit_rows):
        tbl, pk = entry.split('/')
        prefix = ' UNION ALL ' if i > 0 else ' '
        union_lines.append(f"{prefix}SELECT '{tbl}' AS tbl, '{pk}' AS pk,")
        union_lines.append(f"       COUNT(*) AS rows,")
        union_lines.append(f"       SUM(CASE WHEN {pk} IS NULL THEN 1 ELSE 0 END) AS pk_nulls")
        union_lines.append(f"  FROM bronze.{tbl}")
    union_lines += [
        ')',
        'SELECT',
        '  tbl, pk AS primary_key, rows, pk_nulls,',
        "  CASE WHEN pk_nulls = 0 THEN '✓ OK' ELSE '✗ FIX' END AS status",
        'FROM audit',
        'ORDER BY tbl;',
        '-- All 19 rows must show status = \'✓ OK\' before running Part 2.',
    ]
    story.append(code_block(union_lines, s))

    # Deployment notes
    story.append(Paragraph('Deployment Notes', s['h1']))
    notes = [
        ('read_files() path',
         "Use 'Files/raw/...' relative to your Lakehouse. For ADLS Gen2 replace with: "
         "abfss://<container>@<account>.dfs.core.windows.net/raw/<file>.csv"),
        ('inferSchema vs. explicit schema',
         'inferSchema works for initial builds. For production replace with an explicit '
         'SCHEMA clause to prevent silent type drift between runs.'),
        ('Overwrite safety',
         'CREATE OR REPLACE TABLE is a full overwrite each pipeline run. '
         'For incremental Bronze appends replace with INSERT INTO ... WHERE order_date > (SELECT MAX(...))'),
        ('raw_iot_events — Change Data Feed',
         'delta.enableChangeDataFeed = true is required so Part 4\'s streaming notebook can '
         'read this table with spark.readStream. Do NOT disable this after the stream is running.'),
        ('Pipeline order',
         'Bronze must complete fully before Silver starts. Wire in Data Factory as: '
         '[nb_bronze_ingest_sql] → success → [nb_silver_transform_sql]'),
    ]
    for title, detail in notes:
        story.append(Paragraph(f'<b>{title}:</b> {detail}', s['body']))
        story.append(Spacer(1, 4))

    doc.build(story)
    print(f'Bronze PDF written → {path}')


# ═══════════════════════════════════════════════════════════════
# SILVER
# ═══════════════════════════════════════════════════════════════
def build_silver(path):
    doc = SimpleDocTemplate(path, pagesize=letter,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)
    s = make_styles(SILVER_COLOR)
    story = []

    story.append(Paragraph('Microsoft Fabric · WMS SQL-First Medallion', s['title']))
    story.append(Paragraph('PART 2 — SILVER TRANSFORM', s['subtitle']))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        'Notebook: nb_silver_transform_sql  ·  15 Delta Tables  ·  16 Cells  ·  0 Lines of PySpark',
        s['h2']))
    story.append(hr(SILVER_COLOR))

    story.append(Paragraph('Philosophy', s['h1']))
    for bullet in [
        '• Zero PySpark. Every transform is a SQL CTAS with CTE chains.',
        '• Deduplication uses ROW_NUMBER() window function — no .dropDuplicates().',
        '• Reference lookups joined inline from Bronze to avoid inter-Silver ordering dependencies.',
        '• All derived columns (margins, rates, flags, buckets) are CASE WHEN or arithmetic inside SELECT.',
        '• Run Bronze validation (Part 1, Cell 21) before this notebook.',
    ]:
        story.append(Paragraph(bullet, s['body']))

    # Silver table definitions
    silver_tables = [
        {
            'cell': 1, 'name': 'silver_orders', 'source': 'bronze.raw_orders',
            'pk': 'order_id',
            'derived': ['days_to_required', 'order_month', 'order_year'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_orders",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_orders WHERE order_id IS NOT NULL",
                ")",
                "SELECT",
                "  order_id, customer_id, warehouse_id,",
                "  TO_DATE(order_date)     AS order_date,",
                "  TO_DATE(required_date)  AS required_date,",
                "  CAST(order_value AS DOUBLE)   AS order_value,",
                "  CAST(total_lines AS INT)       AS total_lines,",
                "  UPPER(TRIM(status))   AS status,",
                "  UPPER(TRIM(priority)) AS priority,",
                "  channel,",
                "  DATEDIFF(TO_DATE(required_date), TO_DATE(order_date)) AS days_to_required,",
                "  DATE_FORMAT(TO_DATE(order_date), 'yyyy-MM') AS order_month,",
                "  YEAR(TO_DATE(order_date)) AS order_year,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 2, 'name': 'silver_order_lines',
            'source': 'bronze.raw_order_lines + bronze.raw_products (lookup)',
            'pk': 'line_id',
            'derived': ['fulfillment_rate', 'line_margin', 'category/subcategory'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_order_lines",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY line_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_order_lines WHERE line_id IS NOT NULL),",
                "typed AS (SELECT line_id, order_id, sku,",
                "  CAST(qty_ordered AS INT) AS qty_ordered,",
                "  CAST(qty_fulfilled AS INT) AS qty_fulfilled,",
                "  CAST(unit_price AS DOUBLE) AS unit_price,",
                "  CAST(line_value AS DOUBLE) AS line_value,",
                "  CASE WHEN CAST(qty_ordered AS INT) > 0",
                "       THEN CAST(qty_fulfilled AS DOUBLE) / CAST(qty_ordered AS DOUBLE)",
                "       ELSE 0.0 END AS fulfillment_rate",
                "  FROM deduped WHERE rn = 1),",
                "with_product AS (SELECT t.*, p.unit_cost, p.category, p.subcategory,",
                "  t.line_value - (t.qty_fulfilled * p.unit_cost) AS line_margin",
                "  FROM typed t LEFT JOIN bronze.raw_products p ON t.sku = p.sku)",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_product;",
            ]
        },
        {
            'cell': 3, 'name': 'silver_customers', 'source': 'bronze.raw_customers',
            'pk': 'customer_id',
            'derived': ['segment/account_status normalized', 'customer_since_year'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_customers",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_customers WHERE customer_id IS NOT NULL)",
                "SELECT",
                "  customer_id, first_name, last_name, email, phone,",
                "  UPPER(TRIM(segment))        AS segment,",
                "  UPPER(TRIM(account_status)) AS account_status,",
                "  CAST(credit_limit AS DOUBLE) AS credit_limit,",
                "  city, state, region, country,",
                "  TO_DATE(created_date) AS created_date,",
                "  YEAR(TO_DATE(created_date)) AS customer_since_year,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 4, 'name': 'silver_products', 'source': 'bronze.raw_products',
            'pk': 'sku',
            'derived': ['margin_pct', 'volume_cuft'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_products",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY sku ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_products WHERE sku IS NOT NULL)",
                "SELECT sku, product_name, category, subcategory, supplier_id,",
                "  CAST(unit_cost AS DOUBLE)  AS unit_cost,",
                "  CAST(unit_price AS DOUBLE) AS unit_price,",
                "  CAST(weight_kg AS DOUBLE)  AS weight_kg,",
                "  CAST(reorder_point AS INT) AS reorder_point,",
                "  CAST(lead_time_days AS INT) AS lead_time_days,",
                "  CAST(is_hazmat AS BOOLEAN)    AS is_hazmat,",
                "  CAST(is_perishable AS BOOLEAN) AS is_perishable,",
                "  CASE WHEN CAST(unit_price AS DOUBLE) > 0",
                "       THEN (CAST(unit_price AS DOUBLE) - CAST(unit_cost AS DOUBLE))",
                "            / CAST(unit_price AS DOUBLE)",
                "       ELSE 0.0 END AS margin_pct,",
                "  (CAST(length_cm AS DOUBLE) * CAST(width_cm AS DOUBLE)",
                "   * CAST(height_cm AS DOUBLE)) / 28316.8 AS volume_cuft,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 5, 'name': 'silver_inventory',
            'source': 'bronze.raw_inventory + bronze.raw_products (lookup)',
            'pk': 'inventory_id',
            'derived': ['inventory_value', 'below_reorder flag', 'utilization_rate'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_inventory",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY inventory_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_inventory WHERE inventory_id IS NOT NULL AND sku IS NOT NULL),",
                "typed AS (SELECT inventory_id, sku, warehouse_id, location_id,",
                "  CAST(qty_on_hand AS INT) AS qty_on_hand,",
                "  CAST(qty_available AS INT) AS qty_available,",
                "  CAST(unit_cost AS DOUBLE) AS unit_cost,",
                "  CAST(reorder_point AS INT) AS reorder_point,",
                "  TO_TIMESTAMP(last_counted_at) AS last_counted_at",
                "  FROM deduped WHERE rn = 1),",
                "enriched AS (SELECT t.*,",
                "  t.qty_on_hand * t.unit_cost AS inventory_value,",
                "  t.qty_available < t.reorder_point AS below_reorder,",
                "  CASE WHEN t.reorder_point > 0",
                "       THEN CAST(t.qty_on_hand AS DOUBLE) / t.reorder_point",
                "       ELSE NULL END AS utilization_rate,",
                "  p.product_name, p.category, p.subcategory",
                "  FROM typed t LEFT JOIN bronze.raw_products p ON t.sku = p.sku)",
                "SELECT *, current_timestamp() AS _loaded_at FROM enriched;",
            ]
        },
        {
            'cell': 6, 'name': 'silver_inventory_transactions',
            'source': 'bronze.raw_inventory_transactions',
            'pk': 'txn_id',
            'derived': ['txn_date', 'txn_month', 'qty_mismatch_flag'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_inventory_transactions",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY txn_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_inventory_transactions WHERE txn_id IS NOT NULL)",
                "SELECT txn_id, inventory_id, sku, warehouse_id, location_id,",
                "  txn_type, reference_id, reference_type,",
                "  CAST(qty_change AS INT) AS qty_change,",
                "  CAST(qty_before AS INT) AS qty_before,",
                "  CAST(qty_after  AS INT) AS qty_after,",
                "  CASE WHEN CAST(qty_before AS INT) + CAST(qty_change AS INT)",
                "            <> CAST(qty_after AS INT)",
                "       THEN TRUE ELSE FALSE END AS qty_mismatch_flag,",
                "  TO_TIMESTAMP(txn_timestamp) AS txn_timestamp,",
                "  TO_DATE(txn_timestamp)      AS txn_date,",
                "  DATE_FORMAT(TO_DATE(txn_timestamp), 'yyyy-MM') AS txn_month,",
                "  performed_by,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 7, 'name': 'silver_shipments',
            'source': 'bronze.raw_shipments + bronze.raw_carriers (lookup)',
            'pk': 'shipment_id',
            'derived': ['delay_days', 'cost_per_lb', 'carrier attributes'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_shipments",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY shipment_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_shipments WHERE shipment_id IS NOT NULL),",
                "typed AS (SELECT shipment_id, order_id, carrier_id, origin_warehouse_id,",
                "  TO_DATE(ship_date)           AS ship_date,",
                "  TO_DATE(estimated_delivery)  AS estimated_delivery,",
                "  TO_DATE(actual_delivery)     AS actual_delivery,",
                "  UPPER(TRIM(status))          AS status,",
                "  CAST(shipping_cost AS DOUBLE) AS shipping_cost,",
                "  CAST(weight_lbs AS DOUBLE)    AS weight_lbs,",
                "  CAST(is_on_time AS BOOLEAN)   AS is_on_time,",
                "  CASE WHEN actual_delivery IS NOT NULL AND estimated_delivery IS NOT NULL",
                "       THEN DATEDIFF(TO_DATE(actual_delivery), TO_DATE(estimated_delivery))",
                "       ELSE 0 END AS delay_days,",
                "  CASE WHEN CAST(weight_lbs AS DOUBLE) > 0",
                "       THEN CAST(shipping_cost AS DOUBLE) / CAST(weight_lbs AS DOUBLE)",
                "       ELSE NULL END AS cost_per_lb",
                "  FROM deduped WHERE rn = 1),",
                "with_carrier AS (SELECT t.*, c.carrier_name, c.service_level,",
                "  c.avg_transit_days, c.cost_per_lb AS carrier_rate_per_lb",
                "  FROM typed t LEFT JOIN bronze.raw_carriers c ON t.carrier_id = c.carrier_id)",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_carrier;",
            ]
        },
        {
            'cell': 8, 'name': 'silver_purchase_orders',
            'source': 'bronze.raw_purchase_orders + bronze.raw_suppliers (lookup)',
            'pk': 'po_id',
            'derived': ['actual_lead_days', 'days_late'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_purchase_orders",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY po_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_purchase_orders WHERE po_id IS NOT NULL),",
                "typed AS (SELECT po_id, supplier_id, sku, warehouse_id,",
                "  TO_DATE(po_date)           AS po_date,",
                "  TO_DATE(expected_delivery) AS expected_delivery,",
                "  TO_DATE(actual_delivery)   AS actual_delivery,",
                "  UPPER(TRIM(status))        AS status,",
                "  CAST(qty_ordered AS INT)   AS qty_ordered,",
                "  CAST(unit_cost AS DOUBLE)  AS unit_cost,",
                "  CAST(total_cost AS DOUBLE) AS total_cost,",
                "  CAST(is_on_time AS BOOLEAN) AS is_on_time,",
                "  CASE WHEN actual_delivery IS NOT NULL",
                "       THEN DATEDIFF(TO_DATE(actual_delivery), TO_DATE(po_date))",
                "       ELSE NULL END AS actual_lead_days,",
                "  CASE WHEN actual_delivery IS NOT NULL AND expected_delivery IS NOT NULL",
                "       THEN DATEDIFF(TO_DATE(actual_delivery), TO_DATE(expected_delivery))",
                "       ELSE 0 END AS days_late",
                "  FROM deduped WHERE rn = 1),",
                "with_supplier AS (SELECT t.*, s.company_name AS supplier_name,",
                "  s.reliability_score, s.is_preferred, s.lead_time_days AS supplier_lead_time_days",
                "  FROM typed t LEFT JOIN bronze.raw_suppliers s ON t.supplier_id = s.supplier_id)",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_supplier;",
            ]
        },
        {
            'cell': 9, 'name': 'silver_returns', 'source': 'bronze.raw_returns',
            'pk': 'return_id',
            'derived': ['days_to_restock', 'is_resellable', 'reason_group'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_returns",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY return_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_returns WHERE return_id IS NOT NULL)",
                "SELECT return_id, order_id, sku,",
                "  TO_DATE(return_date)  AS return_date,",
                "  TO_DATE(restock_date) AS restock_date,",
                "  CAST(qty_returned AS INT)    AS qty_returned,",
                "  CAST(refund_amount AS DOUBLE) AS refund_amount,",
                "  reason_code, UPPER(TRIM(condition)) AS condition, disposition,",
                "  CASE WHEN restock_date IS NOT NULL",
                "       THEN DATEDIFF(TO_DATE(restock_date), TO_DATE(return_date))",
                "       ELSE NULL END AS days_to_restock,",
                "  UPPER(TRIM(condition)) IN ('NEW', 'LIKE_NEW') AS is_resellable,",
                "  CASE WHEN UPPER(reason_code) IN ('DAMAGED','DEFECTIVE') THEN 'Quality'",
                "       WHEN UPPER(reason_code) IN ('NOT_AS_DESCRIBED','WRONG_ITEM') THEN 'Accuracy'",
                "       WHEN UPPER(reason_code) = 'CHANGED_MIND' THEN 'Customer'",
                "       ELSE 'Other' END AS reason_group,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 10, 'name': 'silver_supplier_performance',
            'source': 'bronze.raw_supplier_performance + bronze.raw_suppliers (lookup)',
            'pk': 'perf_id',
            'derived': ['composite_score (weighted)', 'score_tier'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_supplier_performance",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY perf_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_supplier_performance WHERE perf_id IS NOT NULL),",
                "scored AS (SELECT perf_id, supplier_id,",
                "  CAST(on_time_rate AS DOUBLE) AS on_time_rate,",
                "  CAST(fill_rate    AS DOUBLE) AS fill_rate,",
                "  CAST(defect_rate  AS DOUBLE) AS defect_rate,",
                "  CAST(avg_lead_days AS DOUBLE) AS avg_lead_days,",
                "  -- composite = 40% on-time + 40% fill + 20% quality",
                "  (CAST(on_time_rate AS DOUBLE) * 0.4)",
                "  + (CAST(fill_rate AS DOUBLE) * 0.4)",
                "  + ((1.0 - CAST(defect_rate AS DOUBLE)) * 0.2) AS composite_score",
                "  FROM deduped WHERE rn = 1),",
                "tiered AS (SELECT *,",
                "  CASE WHEN composite_score >= 0.95 THEN 'A'",
                "       WHEN composite_score >= 0.85 THEN 'B'",
                "       ELSE 'C' END AS score_tier",
                "  FROM scored),",
                "with_supplier AS (SELECT t.*, s.company_name AS supplier_name,",
                "  s.is_preferred, s.country",
                "  FROM tiered t LEFT JOIN bronze.raw_suppliers s ON t.supplier_id = s.supplier_id)",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_supplier;",
            ]
        },
        {
            'cell': 11, 'name': 'silver_employees',
            'source': 'bronze.raw_employees + bronze.raw_warehouses (lookup)',
            'pk': 'employee_id',
            'derived': ['tenure_days', 'annual_cost_est'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_employees",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY employee_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_employees WHERE employee_id IS NOT NULL),",
                "typed AS (SELECT employee_id, first_name, last_name, role, warehouse_id,",
                "  TO_DATE(hire_date) AS hire_date,",
                "  CAST(hourly_rate AS DOUBLE) AS hourly_rate,",
                "  CAST(is_active AS BOOLEAN)  AS is_active,",
                "  DATEDIFF(CURRENT_DATE(), TO_DATE(hire_date)) AS tenure_days,",
                "  CAST(hourly_rate AS DOUBLE) * 2080.0 AS annual_cost_est",
                "  FROM deduped WHERE rn = 1),",
                "with_warehouse AS (SELECT t.*, w.name AS warehouse_name, w.region AS warehouse_region",
                "  FROM typed t LEFT JOIN bronze.raw_warehouses w ON t.warehouse_id = w.warehouse_id)",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_warehouse;",
            ]
        },
        {
            'cell': 12, 'name': 'silver_labor_shifts',
            'source': 'bronze.raw_labor_shifts + bronze.raw_employees (lookup)',
            'pk': 'shift_id',
            'derived': ['picks_per_hour', 'units_per_hour', 'labor_cost'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_labor_shifts",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY shift_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_labor_shifts WHERE shift_id IS NOT NULL),",
                "typed AS (SELECT shift_id, employee_id, warehouse_id, zone_assigned,",
                "  shift_type, TO_DATE(shift_date) AS shift_date,",
                "  CAST(hours_worked     AS DOUBLE) AS hours_worked,",
                "  CAST(picks_completed  AS INT)    AS picks_completed,",
                "  CAST(units_processed  AS INT)    AS units_processed,",
                "  DATE_FORMAT(TO_DATE(shift_date), 'yyyy-MM') AS shift_month,",
                "  CASE WHEN CAST(hours_worked AS DOUBLE) > 0",
                "       THEN CAST(picks_completed AS DOUBLE) / CAST(hours_worked AS DOUBLE)",
                "       ELSE 0.0 END AS picks_per_hour,",
                "  CASE WHEN CAST(hours_worked AS DOUBLE) > 0",
                "       THEN CAST(units_processed AS DOUBLE) / CAST(hours_worked AS DOUBLE)",
                "       ELSE 0.0 END AS units_per_hour",
                "  FROM deduped WHERE rn = 1),",
                "with_employee AS (SELECT t.*, e.first_name, e.last_name, e.role, e.hourly_rate,",
                "  t.hours_worked * e.hourly_rate AS labor_cost",
                "  FROM typed t LEFT JOIN bronze.raw_employees e ON t.employee_id = e.employee_id)",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_employee;",
            ]
        },
        {
            'cell': 13, 'name': 'silver_dock_activity', 'source': 'bronze.raw_dock_activity',
            'pk': 'dock_id',
            'derived': ['activity_date', 'activity_hour', 'pallets_per_hour', 'is_inbound'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_dock_activity",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY dock_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_dock_activity WHERE dock_id IS NOT NULL)",
                "SELECT dock_id, warehouse_id, carrier_id, shipment_id,",
                "  UPPER(TRIM(activity_type)) AS activity_type,",
                "  TO_TIMESTAMP(arrived_at)   AS arrived_at,",
                "  TO_TIMESTAMP(departed_at)  AS departed_at,",
                "  CAST(duration_minutes AS INT) AS duration_minutes,",
                "  CAST(num_pallets AS INT)       AS num_pallets,",
                "  TO_DATE(arrived_at)  AS activity_date,",
                "  HOUR(TO_TIMESTAMP(arrived_at)) AS activity_hour,",
                "  CASE WHEN CAST(duration_minutes AS INT) > 0",
                "       THEN CAST(num_pallets AS DOUBLE) / (CAST(duration_minutes AS DOUBLE) / 60.0)",
                "       ELSE NULL END AS pallets_per_hour,",
                "  UPPER(TRIM(activity_type)) = 'INBOUND_RECEIVE' AS is_inbound,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 14, 'name': 'silver_locations', 'source': 'bronze.raw_locations',
            'pk': 'location_id',
            'derived': ['clean typing and dedup only'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_locations",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY location_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_locations WHERE location_id IS NOT NULL)",
                "SELECT location_id, warehouse_id, zone, aisle, bay, level, position,",
                "  UPPER(TRIM(location_type)) AS location_type,",
                "  CAST(max_weight_kg    AS DOUBLE) AS max_weight_kg,",
                "  CAST(max_volume_cuft  AS DOUBLE) AS max_volume_cuft,",
                "  CAST(is_active AS BOOLEAN)        AS is_active,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 15, 'name': 'silver_date_dim', 'source': 'bronze.raw_date_dim',
            'pk': 'date_key',
            'derived': ['month_label for consistent yyyy-MM labeling in reports'],
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_date_dim",
                "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
                "    PARTITION BY date_key ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_date_dim WHERE date_key IS NOT NULL)",
                "SELECT CAST(date_key AS INT) AS date_key,",
                "  TO_DATE(full_date) AS full_date,",
                "  CAST(day_of_week AS INT) AS day_of_week, day_name,",
                "  CAST(week_num AS INT) AS week_num, CAST(month_num AS INT) AS month_num,",
                "  month_name, CAST(quarter AS INT) AS quarter, CAST(year AS INT) AS year,",
                "  fiscal_period,",
                "  CAST(is_weekend AS BOOLEAN) AS is_weekend,",
                "  CAST(is_holiday AS BOOLEAN) AS is_holiday,",
                "  CONCAT(CAST(CAST(year AS INT) AS STRING), '-',",
                "         LPAD(CAST(CAST(month_num AS INT) AS STRING), 2, '0')) AS month_label,",
                "  current_timestamp() AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
    ]

    story.append(Paragraph('Silver Table Transformations', s['h1']))
    for t in silver_tables:
        story.append(cell_header(
            f"CELL {t['cell']} — silver.{t['name']}  |  PK: {t['pk']}", s))
        story.append(Paragraph(
            f"<b>Source:</b> {t['source']}  ·  "
            f"<b>Key derived cols:</b> {', '.join(t['derived'])}",
            s['note']))
        story.append(code_block(t['code'], s))

    # Validation
    story.append(PageBreak())
    story.append(Paragraph('Cell 16 — Full Silver Validation', s['h1']))
    story.append(Paragraph(
        'Row count + PK null + duplicate check across all 15 Silver tables. '
        'All status values must read ✓ OK before running Part 3 (Gold).',
        s['body']))

    silver_names = [
        ('silver_orders','order_id'), ('silver_order_lines','line_id'),
        ('silver_customers','customer_id'), ('silver_products','sku'),
        ('silver_inventory','inventory_id'),
        ('silver_inventory_transactions','txn_id'),
        ('silver_shipments','shipment_id'),
        ('silver_purchase_orders','po_id'), ('silver_returns','return_id'),
        ('silver_supplier_performance','perf_id'),
        ('silver_employees','employee_id'), ('silver_labor_shifts','shift_id'),
        ('silver_dock_activity','dock_id'), ('silver_locations','location_id'),
        ('silver_date_dim','date_key'),
    ]
    val_lines = ['WITH audit AS (']
    for i, (tbl, pk) in enumerate(silver_names):
        prefix = ' UNION ALL ' if i > 0 else ' '
        val_lines += [
            f"{prefix}SELECT '{tbl}' AS tbl, '{pk}' AS pk,",
            f"       COUNT(*) AS rows,",
            f"       SUM(CASE WHEN {pk} IS NULL THEN 1 ELSE 0 END) AS pk_nulls,",
            f"       COUNT(*) - COUNT(DISTINCT {pk}) AS dupes",
            f"  FROM silver.{tbl}",
        ]
    val_lines += [
        ')',
        'SELECT tbl, pk AS primary_key, rows, pk_nulls, dupes,',
        "  CASE WHEN pk_nulls = 0 AND dupes = 0 THEN '✓ OK' ELSE '✗ FIX' END AS status",
        'FROM audit ORDER BY tbl;',
        "-- All 15 rows must show status = '✓ OK' before running Part 3.",
    ]
    story.append(code_block(val_lines, s))

    # Notes
    story.append(Paragraph('Notes', s['h1']))
    for heading, detail in [
        ('Running order',
         'Cells 1–15 can run in the order shown. Cells with no inter-Silver dependencies '
         '(3, 4, 9, 13, 14, 15) are safe to run concurrently. Cells that join Bronze reference '
         'tables (2, 5, 7, 8, 10, 11, 12) depend only on Bronze being complete.'),
        ('Deduplication strategy',
         'ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _loaded_at DESC) keeps the most '
         'recently loaded record. WHERE rn = 1 filters to that row. Equivalent to '
         'dropDuplicates() in PySpark but expressed entirely in SQL.'),
        ('Reference joins from Bronze (not Silver)',
         'Silver tables join Bronze reference data (raw_products, raw_carriers, raw_suppliers, '
         'raw_warehouses, raw_employees) rather than their Silver equivalents to avoid '
         'Silver-to-Silver ordering dependencies.'),
        ('PySpark usage in Part 2', 'ZERO LINES.'),
    ]:
        story.append(Paragraph(f'<b>{heading}:</b> {detail}', s['body']))
        story.append(Spacer(1, 4))

    doc.build(story)
    print(f'Silver PDF written → {path}')


# ═══════════════════════════════════════════════════════════════
# GOLD
# ═══════════════════════════════════════════════════════════════
def build_gold(path):
    doc = SimpleDocTemplate(path, pagesize=letter,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)
    s = make_styles(GOLD_COLOR)
    story = []

    story.append(Paragraph('Microsoft Fabric · WMS SQL-First Medallion', s['title']))
    story.append(Paragraph('PART 3 — GOLD BATCH', s['subtitle']))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        'Notebook: nb_gold_build_sql  ·  12 Delta Tables  ·  17 Cells  ·  0 Lines of PySpark',
        s['h2']))
    story.append(hr(GOLD_COLOR))

    story.append(Paragraph('Star Schema Overview', s['h1']))
    data = [
        ['Category', 'Tables'],
        ['Facts (5)',    'gold_fact_orders, gold_fact_inventory, gold_fact_shipments, gold_fact_labor, gold_fact_dock'],
        ['Dims (4)',     'gold_dim_customer, gold_dim_product, gold_dim_warehouse, gold_dim_date'],
        ['KPI Marts (3)','gold_kpi_supplier, gold_kpi_returns, gold_kpi_iot_summary'],
    ]
    tbl = Table(data, colWidths=[1.4*inch, 5.5*inch])
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), BG_HEADER),
        ('TEXTCOLOR',  (0,0), (-1,0), WHITE),
        ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0,0), (-1,-1), 8),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [LIGHT_GRAY, WHITE]),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#cccccc')),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 8))

    story.append(Paragraph('Philosophy', s['h1']))
    for b in [
        '• All 12 Gold tables built via SQL CTAS from Silver (gold_dim_warehouse reads Bronze directly).',
        '• Aggregations use GROUP BY + window functions — no PySpark aggs.',
        '• OPTIMIZE and ZORDER issued as SQL commands.',
        '• Build order: Dimensions first → Facts → KPI Marts → OPTIMIZE → VACUUM.',
        '• PySpark usage in Part 3: ZERO LINES.',
    ]:
        story.append(Paragraph(b, s['body']))

    # ── DIMENSIONS ──
    story.append(Paragraph('── DIMENSIONS ──', s['h1']))

    story.append(cell_header('CELL 1 — gold.gold_dim_date  |  Source: silver.silver_date_dim', s))
    story.append(Paragraph('Anchor for all time-intelligence in Power BI.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_dim_date",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT date_key, full_date, day_of_week, day_name, week_num,",
        "  month_num, month_name, month_label, quarter, year,",
        "  fiscal_period, is_weekend, is_holiday,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_date_dim;",
    ], s))

    story.append(cell_header('CELL 2 — gold.gold_dim_customer  |  Source: silver.silver_customers', s))
    story.append(Paragraph('Derived: full_name concatenation.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_dim_customer",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT customer_id, first_name, last_name,",
        "  CONCAT(first_name, ' ', last_name) AS full_name,",
        "  segment, account_status, credit_limit,",
        "  city, state, region, country, customer_since_year, created_date,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_customers;",
    ], s))

    story.append(cell_header('CELL 3 — gold.gold_dim_product  |  Source: silver.silver_products', s))
    story.append(Paragraph('Derived: price_tier bucket (Economy / Mid / Premium).', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_dim_product",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT sku, product_name, category, subcategory, supplier_id,",
        "  unit_cost, unit_price, margin_pct, weight_kg, volume_cuft,",
        "  reorder_point, reorder_qty, lead_time_days, is_hazmat, is_perishable,",
        "  CASE WHEN unit_price >= 500 THEN 'Premium'",
        "       WHEN unit_price >= 100 THEN 'Mid'",
        "       ELSE 'Economy' END AS price_tier,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_products;",
    ], s))

    story.append(cell_header('CELL 4 — gold.gold_dim_warehouse  |  Source: bronze.raw_warehouses', s))
    story.append(Paragraph('Small reference table — reads directly from Bronze.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_dim_warehouse",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "WITH deduped AS (SELECT *, ROW_NUMBER() OVER (",
        "    PARTITION BY warehouse_id ORDER BY _loaded_at DESC) AS rn",
        "  FROM bronze.raw_warehouses WHERE warehouse_id IS NOT NULL)",
        "SELECT warehouse_id, name AS warehouse_name, region, city, state, country,",
        "  manager_id, CAST(capacity_sqft AS DOUBLE) AS capacity_sqft,",
        "  CAST(num_docks AS INT) AS num_docks, CAST(num_zones AS INT) AS num_zones,",
        "  current_timestamp() AS _loaded_at",
        "FROM deduped WHERE rn = 1;",
    ], s))

    # ── FACTS ──
    story.append(PageBreak())
    story.append(Paragraph('── FACT TABLES ──', s['h1']))

    story.append(cell_header('CELL 5 — gold.gold_fact_orders  |  Grain: 1 row per order_id', s))
    story.append(Paragraph(
        'Sources: silver_orders + silver_order_lines (aggregated) + silver_shipments (aggregated). '
        'Two CTEs (line_agg, ship_agg) pre-aggregate 128k lines and 30k shipments to order grain '
        'before joining back to silver_orders.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_fact_orders",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "WITH line_agg AS (",
        "  SELECT order_id,",
        "    SUM(line_value)          AS total_line_value,",
        "    SUM(line_margin)         AS total_margin,",
        "    SUM(qty_ordered)         AS total_qty_ordered,",
        "    SUM(qty_fulfilled)       AS total_qty_fulfilled,",
        "    SUM(qty_backordered)     AS total_qty_backordered,",
        "    AVG(fulfillment_rate)    AS avg_fulfillment_rate,",
        "    COUNT(DISTINCT sku)      AS distinct_skus",
        "  FROM silver.silver_order_lines GROUP BY order_id),",
        "ship_agg AS (",
        "  SELECT order_id,",
        "    MIN(ship_date)           AS first_ship_date,",
        "    SUM(shipping_cost)       AS total_shipping_cost,",
        "    MAX(CAST(is_on_time AS INT)) = 1 AS was_on_time,",
        "    SUM(delay_days)          AS total_delay_days,",
        "    COUNT(shipment_id)       AS num_shipments",
        "  FROM silver.silver_shipments GROUP BY order_id)",
        "SELECT o.order_id, o.customer_id, o.warehouse_id,",
        "  CAST(DATE_FORMAT(o.order_date, 'yyyyMMdd') AS INT) AS date_key,",
        "  o.order_date, o.order_month, o.order_year, o.status, o.priority, o.order_value,",
        "  l.total_line_value, l.total_margin,",
        "  CASE WHEN l.total_line_value > 0",
        "       THEN l.total_margin / l.total_line_value ELSE 0.0 END AS margin_pct,",
        "  l.total_qty_ordered, l.total_qty_fulfilled, l.total_qty_backordered,",
        "  l.avg_fulfillment_rate, l.distinct_skus,",
        "  s.first_ship_date, s.total_shipping_cost, s.was_on_time,",
        "  s.total_delay_days, s.num_shipments,",
        "  DATEDIFF(s.first_ship_date, o.order_date) AS days_to_ship,",
        "  o.days_to_required,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_orders o",
        "LEFT JOIN line_agg l ON o.order_id = l.order_id",
        "LEFT JOIN ship_agg s ON o.order_id = s.order_id;",
    ], s))

    story.append(cell_header('CELL 6 — gold.gold_fact_inventory  |  Grain: 1 row per inventory_id', s))
    story.append(Paragraph('Derived: stock_status label, days_since_counted.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_fact_inventory",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT inventory_id, sku, warehouse_id, location_id,",
        "  qty_on_hand, qty_reserved, qty_available, qty_in_transit,",
        "  unit_cost, inventory_value, below_reorder, reorder_point, utilization_rate,",
        "  last_counted_at, last_received_at, category, subcategory, product_name,",
        "  DATEDIFF(CURRENT_DATE(), TO_DATE(last_counted_at)) AS days_since_counted,",
        "  CASE WHEN qty_available = 0    THEN 'Out of Stock'",
        "       WHEN below_reorder = TRUE THEN 'Low Stock'",
        "       ELSE 'In Stock' END AS stock_status,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_inventory;",
    ], s))

    story.append(cell_header('CELL 7 — gold.gold_fact_shipments  |  Grain: 1 row per shipment_id', s))
    story.append(Paragraph('Derived: date_key, ship_month.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_fact_shipments",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT shipment_id, order_id, carrier_id, carrier_name, service_level,",
        "  origin_warehouse_id, ship_date, estimated_delivery, actual_delivery, status,",
        "  weight_lbs, shipping_cost, is_on_time,",
        "  COALESCE(delay_days, 0) AS delay_days,",
        "  num_packages, cost_per_lb, avg_transit_days,",
        "  CAST(DATE_FORMAT(ship_date, 'yyyyMMdd') AS INT) AS date_key,",
        "  DATE_FORMAT(ship_date, 'yyyy-MM') AS ship_month,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_shipments;",
    ], s))

    story.append(cell_header('CELL 8 — gold.gold_fact_labor  |  Grain: 1 row per shift_id', s))
    story.append(Paragraph('Derived: date_key, cost_per_pick.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_fact_labor",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT shift_id, employee_id, warehouse_id, zone_assigned, shift_type,",
        "  shift_date, shift_month, hours_worked, picks_completed, units_processed,",
        "  picks_per_hour, units_per_hour, labor_cost, role, first_name, last_name, hourly_rate,",
        "  CAST(DATE_FORMAT(shift_date, 'yyyyMMdd') AS INT) AS date_key,",
        "  CASE WHEN picks_completed > 0",
        "       THEN labor_cost / picks_completed",
        "       ELSE NULL END AS cost_per_pick,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_labor_shifts;",
    ], s))

    story.append(cell_header('CELL 9 — gold.gold_fact_dock  |  Grain: 1 row per dock_id', s))
    story.append(Paragraph('Derived: date_key.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_fact_dock",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT dock_id, warehouse_id, carrier_id, shipment_id,",
        "  activity_type, activity_date, activity_hour, duration_minutes,",
        "  num_pallets, pallets_per_hour, is_inbound,",
        "  CAST(DATE_FORMAT(activity_date, 'yyyyMMdd') AS INT) AS date_key,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_dock_activity;",
    ], s))

    # ── KPI MARTS ──
    story.append(PageBreak())
    story.append(Paragraph('── KPI MARTS ──', s['h1']))

    story.append(cell_header('CELL 10 — gold.gold_kpi_supplier  |  Grain: supplier × month', s))
    story.append(Paragraph('Sources: silver_supplier_performance + bronze.raw_suppliers.', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_kpi_supplier",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT sp.perf_id, sp.supplier_id, sp.supplier_name,",
        "  CAST(s.is_preferred AS BOOLEAN) AS is_preferred, sp.country,",
        "  sp.period_month, sp.period_year, sp.month_label,",
        "  sp.total_pos, sp.on_time_deliveries, sp.on_time_rate,",
        "  sp.fill_rate, sp.defect_rate, sp.avg_lead_days,",
        "  sp.composite_score, sp.score_tier, s.reliability_score,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_supplier_performance sp",
        "LEFT JOIN bronze.raw_suppliers s ON sp.supplier_id = s.supplier_id;",
    ], s))

    story.append(cell_header('CELL 11 — gold.gold_kpi_returns  |  Grain: 1 row per return_id', s))
    story.append(Paragraph('Sources: silver_returns + silver_products. Derived: loss_amount (non-resellable only).', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_kpi_returns",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT r.return_id, r.order_id, r.sku, p.product_name, p.category,",
        "  r.qty_returned, r.refund_amount, r.return_date,",
        "  DATE_FORMAT(r.return_date, 'yyyy-MM') AS return_month,",
        "  YEAR(r.return_date) AS return_year,",
        "  r.reason_code, r.reason_group, r.condition, r.disposition,",
        "  r.is_resellable, r.days_to_restock, p.unit_cost,",
        "  CASE WHEN r.is_resellable = FALSE",
        "       THEN r.qty_returned * p.unit_cost",
        "       ELSE 0.0 END AS loss_amount,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_returns r",
        "LEFT JOIN silver.silver_products p ON r.sku = p.sku;",
    ], s))

    story.append(cell_header('CELL 12 — gold.gold_kpi_iot_summary  |  BATCH — daily grain', s))
    story.append(Paragraph(
        'Source: bronze.raw_iot_events (100,000 rows). '
        'Grain: device_id × warehouse_id × zone × event_date. '
        'The HOURLY streaming version lives in Part 4 (nb_iot_stream_sql).', s['note']))
    story.append(code_block([
        "CREATE OR REPLACE TABLE gold.gold_kpi_iot_summary",
        "USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "WITH cast_events AS (",
        "  SELECT event_id, device_id, device_type, warehouse_id, zone, event_type,",
        "    TO_DATE(event_timestamp)         AS event_date,",
        "    CAST(speed_mph      AS DOUBLE)   AS speed_mph,",
        "    CAST(battery_pct    AS DOUBLE)   AS battery_pct,",
        "    CAST(temperature_f  AS DOUBLE)   AS temperature_f,",
        "    CAST(carrying_qty   AS INT)      AS carrying_qty",
        "  FROM bronze.raw_iot_events WHERE event_id IS NOT NULL)",
        "SELECT device_id, device_type, warehouse_id, zone, event_date,",
        "  CAST(DATE_FORMAT(event_date, 'yyyyMMdd') AS INT) AS date_key,",
        "  COUNT(event_id)                         AS total_events,",
        "  ROUND(AVG(speed_mph),     2)             AS avg_speed_mph,",
        "  ROUND(MAX(speed_mph),     2)             AS max_speed_mph,",
        "  ROUND(AVG(battery_pct),   2)             AS avg_battery_pct,",
        "  ROUND(MIN(battery_pct),   2)             AS min_battery_pct,",
        "  ROUND(AVG(temperature_f), 2)             AS avg_temp_f,",
        "  SUM(carrying_qty)                        AS total_carrying_qty,",
        "  SUM(CASE WHEN event_type = 'ALERT' THEN 1 ELSE 0 END) AS alert_count,",
        "  SUM(CASE WHEN event_type = 'IDLE'  THEN 1 ELSE 0 END) AS idle_count,",
        "  SUM(CASE WHEN event_type = 'MOVE'  THEN 1 ELSE 0 END) AS move_count,",
        "  ROUND(SUM(CASE WHEN event_type = 'ALERT' THEN 1 ELSE 0 END)",
        "        / CAST(COUNT(event_id) AS DOUBLE), 4) AS alert_rate,",
        "  current_timestamp() AS _loaded_at",
        "FROM cast_events",
        "GROUP BY device_id, device_type, warehouse_id, zone, event_date;",
    ], s))

    # OPTIMIZE / ZORDER / VACUUM
    story.append(Paragraph('Post-Build: OPTIMIZE + ZORDER + VACUUM', s['h1']))
    story.append(Paragraph(
        'Run after all 12 Gold tables are written. OPTIMIZE compacts small Parquet files; '
        'ZORDER co-locates rows by the columns Power BI filters on most. '
        'VACUUM removes old snapshots (7-day / 168-hour history retained).',
        s['body']))

    story.append(cell_header('CELL 13 — OPTIMIZE + ZORDER: Fact Tables', s))
    story.append(code_block([
        "OPTIMIZE gold.gold_fact_orders    ZORDER BY (order_date, customer_id);",
        "OPTIMIZE gold.gold_fact_inventory ZORDER BY (sku, warehouse_id);",
        "OPTIMIZE gold.gold_fact_shipments ZORDER BY (ship_date, carrier_id);",
        "OPTIMIZE gold.gold_fact_labor     ZORDER BY (shift_date, warehouse_id);",
        "OPTIMIZE gold.gold_fact_dock      ZORDER BY (activity_date, warehouse_id);",
    ], s))

    story.append(cell_header('CELL 14 — OPTIMIZE + ZORDER: KPI Marts', s))
    story.append(code_block([
        "OPTIMIZE gold.gold_kpi_supplier    ZORDER BY (period_year, period_month);",
        "OPTIMIZE gold.gold_kpi_returns     ZORDER BY (return_date, reason_code);",
        "OPTIMIZE gold.gold_kpi_iot_summary ZORDER BY (event_date, warehouse_id);",
    ], s))

    story.append(cell_header('CELL 15 — VACUUM: All Gold Tables (retain 7 days)', s))
    story.append(code_block([
        "VACUUM gold.gold_fact_orders     RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_inventory  RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_shipments  RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_labor      RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_dock       RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_customer    RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_product     RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_warehouse   RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_date        RETAIN 168 HOURS;",
        "VACUUM gold.gold_kpi_supplier    RETAIN 168 HOURS;",
        "VACUUM gold.gold_kpi_returns     RETAIN 168 HOURS;",
        "VACUUM gold.gold_kpi_iot_summary RETAIN 168 HOURS;",
    ], s))

    # Validation
    story.append(cell_header('CELL 16 — Full Gold Validation (row counts, all 12 tables)', s))
    story.append(code_block([
        "SELECT tbl, rows,",
        "  CASE WHEN rows > 0 THEN '✓ OK' ELSE '✗ EMPTY' END AS status",
        "FROM (",
        "  SELECT 'gold_fact_orders'      AS tbl, COUNT(*) AS rows FROM gold.gold_fact_orders    UNION ALL",
        "  SELECT 'gold_fact_inventory',         COUNT(*)          FROM gold.gold_fact_inventory  UNION ALL",
        "  SELECT 'gold_fact_shipments',         COUNT(*)          FROM gold.gold_fact_shipments  UNION ALL",
        "  SELECT 'gold_fact_labor',             COUNT(*)          FROM gold.gold_fact_labor       UNION ALL",
        "  SELECT 'gold_fact_dock',              COUNT(*)          FROM gold.gold_fact_dock        UNION ALL",
        "  SELECT 'gold_dim_customer',           COUNT(*)          FROM gold.gold_dim_customer     UNION ALL",
        "  SELECT 'gold_dim_product',            COUNT(*)          FROM gold.gold_dim_product      UNION ALL",
        "  SELECT 'gold_dim_warehouse',          COUNT(*)          FROM gold.gold_dim_warehouse    UNION ALL",
        "  SELECT 'gold_dim_date',               COUNT(*)          FROM gold.gold_dim_date         UNION ALL",
        "  SELECT 'gold_kpi_supplier',           COUNT(*)          FROM gold.gold_kpi_supplier     UNION ALL",
        "  SELECT 'gold_kpi_returns',            COUNT(*)          FROM gold.gold_kpi_returns      UNION ALL",
        "  SELECT 'gold_kpi_iot_summary',        COUNT(*)          FROM gold.gold_kpi_iot_summary",
        ") t ORDER BY tbl;",
        "-- All 12 rows must show status = '✓ OK'.",
    ], s))

    story.append(cell_header('CELL 17 — Quick Sanity: Revenue + OTD Rate', s))
    story.append(code_block([
        "SELECT",
        "  ROUND(SUM(order_value), 2)             AS total_revenue,",
        "  ROUND(AVG(margin_pct) * 100, 2)        AS avg_margin_pct,",
        "  COUNT(*)                                AS total_orders,",
        "  SUM(CASE WHEN was_on_time THEN 1 ELSE 0 END) AS orders_on_time,",
        "  ROUND(SUM(CASE WHEN was_on_time THEN 1 ELSE 0 END)",
        "        / CAST(COUNT(*) AS DOUBLE) * 100, 2) AS otd_rate_pct",
        "FROM gold.gold_fact_orders;",
    ], s))

    # Notes
    story.append(Paragraph('Notes', s['h1']))
    for heading, detail in [
        ('Build order',
         'Dimensions (Cells 1–4) → Facts (Cells 5–9) → KPI Marts (Cells 10–12) → '
         'OPTIMIZE (Cells 13–14) → VACUUM (Cell 15) → Validate (Cell 16)'),
        ('gold_fact_orders CTE design',
         'The two sub-aggregations (line_agg, ship_agg) are CTEs rather than subqueries '
         'so the planner can broadcast-join the small aggregated results back to silver_orders, '
         'avoiding a full shuffle join of 128k raw line rows against 50k order rows.'),
        ('Incremental refresh (production)',
         'Replace CREATE OR REPLACE with INSERT INTO + WHERE clause filtering on order_date / '
         'ship_date >= (SELECT MAX(...) FROM gold.gold_fact_orders) to avoid full rewrites on '
         'daily pipeline runs.'),
        ('PySpark usage in Part 3', 'ZERO LINES. Total PySpark across Parts 1–3: 3 lines (schema bootstrap only).'),
    ]:
        story.append(Paragraph(f'<b>{heading}:</b> {detail}', s['body']))
        story.append(Spacer(1, 4))

    doc.build(story)
    print(f'Gold PDF written → {path}')


# ── Run all three ───────────────────────────────────────────────
build_bronze('/mnt/user-data/outputs/WMS_Bronze_Layer.pdf')
build_silver('/mnt/user-data/outputs/WMS_Silver_Layer.pdf')
build_gold(  '/mnt/user-data/outputs/WMS_Gold_Layer.pdf')
print('All three PDFs complete.')
