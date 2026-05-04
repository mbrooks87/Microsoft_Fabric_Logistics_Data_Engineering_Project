from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                 PageBreak, Table, TableStyle, HRFlowable)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import Flowable
from reportlab.pdfgen import canvas as pdfcanvas
import re

# ── Catppuccin Mocha palette ────────────────────────────────────
BG_BASE      = colors.HexColor('#1e1e2e')   # editor background
BG_SURFACE   = colors.HexColor('#181825')   # sidebar / header
BG_MANTLE    = colors.HexColor('#11111b')   # page background
BG_CRUST     = colors.HexColor('#24273a')   # cell header bar
LINE_NUM_BG  = colors.HexColor('#181825')
LINE_NUM_FG  = colors.HexColor('#45475a')

FG_TEXT      = colors.HexColor('#cdd6f4')   # base text
FG_SUBTEXT   = colors.HexColor('#6c7086')   # comments
FG_OVERLAY   = colors.HexColor('#7f849c')

# Syntax colours
C_KEYWORD    = colors.HexColor('#cba6f7')   # purple  — SQL keywords
C_FUNCTION   = colors.HexColor('#89b4fa')   # blue    — functions / TVF
C_STRING     = colors.HexColor('#a6e3a1')   # green   — string literals
C_NUMBER     = colors.HexColor('#fab387')   # peach   — numbers
C_COLUMN     = colors.HexColor('#89dceb')   # sky     — column names
C_TABLE      = colors.HexColor('#f38ba8')   # red     — table refs
C_OPERATOR   = colors.HexColor('#94e2d5')   # teal    — operators / AS
C_COMMENT    = colors.HexColor('#585b70')   # overlay — comments
C_PYTHON     = colors.HexColor('#f9e2af')   # yellow  — python tokens

# Accent per layer
BRONZE_ACC   = colors.HexColor('#e8a065')
SILVER_ACC   = colors.HexColor('#8aadd4')
GOLD_ACC     = colors.HexColor('#f9e2af')

WHITE        = colors.white

MONO         = 'Courier'
MONO_BOLD    = 'Courier-Bold'
SANS         = 'Helvetica'
SANS_BOLD    = 'Helvetica-Bold'

# ── SQL token highlighter ───────────────────────────────────────
SQL_KW = re.compile(
    r'\b(SELECT|FROM|WHERE|AS|WITH|CREATE|OR\s+REPLACE|TABLE|USING|DELTA|'
    r'TBLPROPERTIES|INSERT|INTO|GROUP\s+BY|ORDER\s+BY|PARTITION\s+BY|'
    r'OVER|UNION\s+ALL|LEFT\s+JOIN|JOIN|ON|AND|OR|NOT|IN|IS|NULL|CASE|WHEN|'
    r'THEN|ELSE|END|DISTINCT|COUNT|SUM|AVG|MIN|MAX|ROUND|CAST|COALESCE|'
    r'DATEDIFF|DATE_FORMAT|TO_DATE|TO_TIMESTAMP|YEAR|HOUR|LPAD|CONCAT|'
    r'UPPER|TRIM|CURRENT_DATE|CURRENT_TIMESTAMP|current_timestamp|'
    r'ROW_NUMBER|OPTIMIZE|ZORDER\s+BY|VACUUM|RETAIN|HOURS|IF|EXISTS|'
    r'DESCRIBE|LIMIT|INTERVAL|TRUE|FALSE|BIGINT|INT|DOUBLE|STRING|TIMESTAMP|'
    r'BOOLEAN)\b',
    re.IGNORECASE)

def esc(t):
    return t.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

def highlight_sql(line):
    """Return a rich-text string with colour spans for one line of SQL/Python."""
    raw = line.rstrip()
    e   = esc(raw)

    # Full-line comment
    stripped = raw.lstrip()
    if stripped.startswith('--') or stripped.startswith('#'):
        return f'<font color="{C_COMMENT.hexval()}">{e}</font>'

    # Python detection (starts with from/import/spark/query/print/iot)
    if re.match(r'^\s*(from |import |spark\.|iot_|query|print\(|CHECKPOINT)', raw):
        # Highlight string literals in python yellow
        def py_str(m):
            return f'<font color="{C_STRING.hexval()}">{esc(m.group(0))}</font>'
        highlighted = re.sub(r'(".*?"|\'.*?\')', py_str, e)
        return f'<font color="{C_PYTHON.hexval()}">{highlighted}</font>'

    # String literals
    def repl_str(m):
        return f'<font color="{C_STRING.hexval()}">{esc(m.group(0))}</font>'
    # Temporarily replace strings so we don't keyword-match inside them
    strings = {}
    def extract_str(m):
        k = f'\x00S{len(strings)}\x00'
        strings[k] = repl_str(m)
        return k
    cleaned = re.sub(r"'[^']*'", extract_str, raw)
    e2 = esc(cleaned)

    # Numbers
    def repl_num(m):
        return f'<font color="{C_NUMBER.hexval()}">{esc(m.group(0))}</font>'
    e2 = re.sub(r'\b(\d+\.?\d*)\b', repl_num, e2)

    # SQL Keywords
    def repl_kw(m):
        return f'<font color="{C_KEYWORD.hexval()}">{esc(m.group(0))}</font>'
    e2 = SQL_KW.sub(repl_kw, e2)

    # bronze./silver./gold. table references
    def repl_tbl(m):
        return f'<font color="{C_TABLE.hexval()}">{esc(m.group(0))}</font>'
    e2 = re.sub(r'\b(bronze|silver|gold)\.\w+', repl_tbl, e2)

    # Functions: word followed by (
    def repl_fn(m):
        return f'<font color="{C_FUNCTION.hexval()}">{esc(m.group(1))}</font>{esc(m.group(2))}'
    e2 = re.sub(r'\b([A-Za-z_]\w*?)(\s*\()', repl_fn, e2)

    # Restore string placeholders
    for k, v in strings.items():
        e2 = e2.replace(esc(k), v)

    return f'<font color="{FG_TEXT.hexval()}">{e2}</font>'

def make_code_para(lines, font_size=6.8, with_line_nums=True):
    """Build a list of Paragraph flowables for a code block with line numbers."""
    style_code = ParagraphStyle('code',
        fontName=MONO, fontSize=font_size, leading=font_size*1.45,
        textColor=FG_TEXT, backColor=BG_BASE,
        leftIndent=0, rightIndent=0,
        spaceBefore=0, spaceAfter=0,
        borderPad=0)

    rows = []
    for i, line in enumerate(lines, 1):
        hl = highlight_sql(line)
        num_str = f'<font color="{LINE_NUM_FG.hexval()}" name="Courier">{i:>3} </font>'
        text = f'{num_str}{hl}' if with_line_nums else hl
        rows.append([Paragraph(text, style_code)])

    t = Table(rows, colWidths=[6.8*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BG_BASE),
        ('LEFTPADDING',  (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING',   (0,0), (-1,-1), 1),
        ('BOTTOMPADDING',(0,0), (-1,-1), 1),
        ('LINEBELOW', (0,-1), (-1,-1), 0, BG_BASE),
    ]))
    return t

def section_header(title, subtitle, accent, layer_tag):
    """Full-width dark header bar for a cell or section."""
    s_tag = ParagraphStyle('tag',
        fontName=MONO_BOLD, fontSize=7, textColor=accent,
        backColor=BG_SURFACE, alignment=TA_LEFT,
        leftIndent=8, spaceBefore=0, spaceAfter=0, leading=11)
    s_title = ParagraphStyle('htitle',
        fontName=MONO_BOLD, fontSize=9, textColor=FG_TEXT,
        backColor=BG_SURFACE, alignment=TA_LEFT,
        leftIndent=8, spaceBefore=0, spaceAfter=0, leading=13)
    s_sub = ParagraphStyle('hsub',
        fontName=MONO, fontSize=7, textColor=FG_SUBTEXT,
        backColor=BG_SURFACE, alignment=TA_LEFT,
        leftIndent=8, spaceBefore=0, spaceAfter=2, leading=10)

    rows = [
        [Paragraph(f'// {layer_tag}', s_tag)],
        [Paragraph(title, s_title)],
    ]
    if subtitle:
        rows.append([Paragraph(subtitle, s_sub)])

    t = Table(rows, colWidths=[6.8*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BG_SURFACE),
        ('LEFTPADDING',  (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING',   (0,0), (-1,-1), 3),
        ('BOTTOMPADDING',(0,0), (-1,-1), 3),
        ('LINEABOVE', (0,0), (-1,0), 2, accent),
    ]))
    return t

def page_title_block(part, title, subtitle, accent):
    """Top-of-page title."""
    rows = [
        [Paragraph(f'<font color="{accent.hexval()}">// {part}</font>', ParagraphStyle(
            'pt', fontName=MONO_BOLD, fontSize=11, textColor=accent,
            backColor=BG_MANTLE, leading=15, leftIndent=0))],
        [Paragraph(title, ParagraphStyle(
            'pt2', fontName=MONO_BOLD, fontSize=20, textColor=WHITE,
            backColor=BG_MANTLE, leading=24, leftIndent=0))],
        [Paragraph(subtitle, ParagraphStyle(
            'pt3', fontName=MONO, fontSize=8, textColor=FG_SUBTEXT,
            backColor=BG_MANTLE, leading=12, leftIndent=0))],
    ]
    t = Table(rows, colWidths=[6.8*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BG_MANTLE),
        ('LEFTPADDING',  (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING',   (0,0), (-1,-1), 4),
        ('BOTTOMPADDING',(0,0), (-1,-1), 4),
        ('LINEBELOW', (0,-1), (-1,-1), 2, accent),
    ]))
    return t

def comment_block(lines, accent):
    """A /* comment */ style note block."""
    style = ParagraphStyle('cmt',
        fontName=MONO, fontSize=7.5, textColor=C_COMMENT,
        backColor=BG_BASE, leading=11,
        leftIndent=8, rightIndent=8,
        spaceBefore=0, spaceAfter=0)
    rows = [[Paragraph(
        f'<font color="{accent.hexval()}">/*</font> '
        f'<font color="{C_COMMENT.hexval()}">{esc(l)}</font>',
        style)] for l in lines]
    rows.append([Paragraph(f'<font color="{accent.hexval()}"> */</font>', style)])
    t = Table(rows, colWidths=[6.8*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), BG_BASE),
        ('LEFTPADDING',  (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ('TOPPADDING',   (0,0), (-1,-1), 1),
        ('BOTTOMPADDING',(0,0), (-1,-1), 1),
    ]))
    return t

def spacer_line():
    return Spacer(1, 6)

def make_doc(path, accent):
    return SimpleDocTemplate(
        path, pagesize=letter,
        leftMargin=0.6*inch, rightMargin=0.6*inch,
        topMargin=0.55*inch, bottomMargin=0.55*inch)

def add_bg(canvas, doc, accent):
    """Draw full-page background on every page."""
    canvas.saveState()
    canvas.setFillColor(BG_MANTLE)
    canvas.rect(0, 0, letter[0], letter[1], fill=1, stroke=0)
    # thin accent strip top
    canvas.setFillColor(accent)
    canvas.rect(0, letter[1]-3, letter[0], 3, fill=1, stroke=0)
    # footer
    canvas.setFont(MONO, 6)
    canvas.setFillColor(FG_SUBTEXT)
    canvas.drawString(0.6*inch, 0.3*inch,
        f'Microsoft Fabric · WMS SQL-First Medallion')
    canvas.drawRightString(letter[0]-0.6*inch, 0.3*inch,
        f'Page {doc.page}')
    canvas.restoreState()

# ═══════════════════════════════════════════════════════════════
# BRONZE
# ═══════════════════════════════════════════════════════════════
def build_bronze(path):
    acc = BRONZE_ACC
    doc = make_doc(path, acc)
    story = []

    story.append(page_title_block(
        'PART 1 OF 4',
        'BRONZE  INGESTION',
        'nb_bronze_ingest_sql  ·  19 Delta Tables  ·  21 Cells  ·  3 Lines PySpark  ·  read_files() CTAS',
        acc))
    story.append(spacer_line())
    story.append(comment_block([
        'Philosophy:',
        '  · All 19 tables ingested via SQL read_files() TVF — zero PySpark loops',
        '  · Python used ONCE: 3-line schema bootstrap (Cell 1) only',
        '  · delta.enableChangeDataFeed = true on raw_iot_events for Part 4 streaming',
        '  · Run Cell 1, then Cells 2-20 in any order, then Cell 21 for validation',
    ], acc))
    story.append(spacer_line())

    # Cell 1
    story.append(section_header('CELL 1 — Schema Bootstrap', '# Python — only Python in this notebook', acc, 'nb_bronze_ingest_sql'))
    story.append(make_code_para([
        "for schema in ['bronze', 'silver', 'gold']:",
        "    spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {schema}\")",
        "print(\"Schemas ready: bronze | silver | gold\")",
    ]))
    story.append(spacer_line())

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

    for num, tbl, pk, rows, csv in tables:
        iot = tbl == 'raw_iot_events'
        story.append(section_header(
            f'CELL {num} — bronze.{tbl}',
            f'pk: {pk}  ·  expected rows: {rows}',
            acc, 'nb_bronze_ingest_sql'))

        props = [
            "TBLPROPERTIES (",
            "  'delta.autoOptimize.optimizeWrite' = 'true',",
        ]
        if iot:
            props += [
                "  'delta.autoOptimize.autoCompact'   = 'true',",
                "  'delta.enableChangeDataFeed'       = 'true'",
            ]
        else:
            props[-1] = props[-1].rstrip(',') + ''
            props = ["TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"]

        code = [
            f"CREATE OR REPLACE TABLE bronze.{tbl}",
            "USING DELTA",
        ] + props + [
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
            f"-- row count check",
            f"SELECT 'bronze.{tbl}' AS table_name, COUNT(*) AS row_count",
            f"FROM   bronze.{tbl};",
        ]
        if iot:
            code.insert(2, "TBLPROPERTIES (")
            code.insert(3, "  'delta.autoOptimize.optimizeWrite' = 'true',")
            code.insert(4, "  'delta.autoOptimize.autoCompact'   = 'true',")
            code.insert(5, "  'delta.enableChangeDataFeed'       = 'true'")
            code.insert(6, ")")
            # remove the simple props line we added
            code = [l for l in code if "TBLPROPERTIES ('delta" not in l]

        story.append(make_code_para(code))
        story.append(spacer_line())

    # Cell 21
    story.append(PageBreak())
    story.append(section_header(
        'CELL 21 — Full Bronze Validation',
        'UNION ALL audit · all 19 PKs · status must be ✓ OK before Part 2',
        acc, 'nb_bronze_ingest_sql'))

    tbl_entries = [
        ('raw_orders','order_id'), ('raw_order_lines','line_id'),
        ('raw_customers','customer_id'), ('raw_products','sku'),
        ('raw_inventory','inventory_id'), ('raw_inventory_transactions','txn_id'),
        ('raw_shipments','shipment_id'), ('raw_carriers','carrier_id'),
        ('raw_warehouses','warehouse_id'), ('raw_locations','location_id'),
        ('raw_purchase_orders','po_id'), ('raw_suppliers','supplier_id'),
        ('raw_supplier_performance','perf_id'), ('raw_returns','return_id'),
        ('raw_employees','employee_id'), ('raw_labor_shifts','shift_id'),
        ('raw_dock_activity','dock_id'), ('raw_iot_events','event_id'),
        ('raw_date_dim','date_key'),
    ]
    val = ['WITH audit AS (']
    for i, (t, p) in enumerate(tbl_entries):
        pfx = '  UNION ALL ' if i else '  '
        val += [
            f'{pfx}SELECT  \'{t}\' AS tbl,  \'{p}\' AS pk,',
            f'          COUNT(*) AS rows,',
            f'          SUM(CASE WHEN {p} IS NULL THEN 1 ELSE 0 END) AS pk_nulls',
            f'  FROM    bronze.{t}',
        ]
    val += [
        ')',
        'SELECT',
        '  tbl,',
        '  pk         AS primary_key,',
        '  rows,',
        '  pk_nulls,',
        "  CASE WHEN pk_nulls = 0 THEN '✓ OK' ELSE '✗ FIX' END AS status",
        'FROM  audit',
        'ORDER BY tbl;',
        '',
        "-- All 19 rows must show status = '✓ OK' before running Part 2.",
    ]
    story.append(make_code_para(val))
    story.append(spacer_line())

    # Deployment notes as code comments
    story.append(section_header('DEPLOYMENT NOTES', '', acc, 'nb_bronze_ingest_sql'))
    story.append(make_code_para([
        "-- 1. read_files() path:",
        "--    'Files/raw/...' is relative to your Lakehouse Files section.",
        "--    For ADLS Gen2: abfss://<container>@<acct>.dfs.core.windows.net/raw/<file>.csv",
        "",
        "-- 2. inferSchema vs explicit schema:",
        "--    inferSchema works for initial builds. Production → use explicit SCHEMA clause",
        "--    to prevent silent type drift between pipeline runs.",
        "",
        "-- 3. Overwrite safety:",
        "--    CREATE OR REPLACE = full overwrite each run.",
        "--    For incremental appends: INSERT INTO ... WHERE order_date > (SELECT MAX(...))",
        "",
        "-- 4. raw_iot_events — Change Data Feed:",
        "--    delta.enableChangeDataFeed = true is REQUIRED for Part 4 readStream.",
        "--    Do NOT disable this property once the stream is running.",
        "",
        "-- 5. Pipeline order:",
        "--    [nb_bronze_ingest_sql] --success--> [nb_silver_transform_sql]",
    ]))

    doc.build(story, onFirstPage=lambda c,d: add_bg(c,d,acc),
                      onLaterPages=lambda c,d: add_bg(c,d,acc))
    print(f'Bronze PDF written → {path}')


# ═══════════════════════════════════════════════════════════════
# SILVER
# ═══════════════════════════════════════════════════════════════
def build_silver(path):
    acc = SILVER_ACC
    doc = make_doc(path, acc)
    story = []

    story.append(page_title_block(
        'PART 2 OF 4',
        'SILVER  TRANSFORM',
        'nb_silver_transform_sql  ·  15 Delta Tables  ·  16 Cells  ·  0 Lines PySpark  ·  ROW_NUMBER() dedup',
        acc))
    story.append(spacer_line())
    story.append(comment_block([
        'Philosophy:',
        '  · Zero PySpark — every transform is a SQL CTAS with CTE chains',
        '  · Deduplication: ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _loaded_at DESC)',
        '  · Reference lookups join Bronze directly to avoid inter-Silver dependencies',
        '  · All derived columns (margins, rates, flags) are CASE WHEN / arithmetic in SELECT',
    ], acc))
    story.append(spacer_line())

    silver_cells = [
        {
            'cell': 1, 'name': 'silver_orders', 'pk': 'order_id',
            'source': 'bronze.raw_orders',
            'derived': 'days_to_required, order_month, order_year',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_orders",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *,",
                "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_orders",
                "  WHERE order_id IS NOT NULL",
                ")",
                "SELECT",
                "  order_id,",
                "  customer_id,",
                "  warehouse_id,",
                "  TO_DATE(order_date)                                       AS order_date,",
                "  TO_DATE(required_date)                                    AS required_date,",
                "  CAST(order_value  AS DOUBLE)                              AS order_value,",
                "  CAST(total_lines  AS INT)                                 AS total_lines,",
                "  UPPER(TRIM(status))                                       AS status,",
                "  UPPER(TRIM(priority))                                     AS priority,",
                "  channel,",
                "  DATEDIFF(TO_DATE(required_date), TO_DATE(order_date))     AS days_to_required,",
                "  DATE_FORMAT(TO_DATE(order_date), 'yyyy-MM')               AS order_month,",
                "  YEAR(TO_DATE(order_date))                                 AS order_year,",
                "  current_timestamp()                                       AS _loaded_at",
                "FROM deduped",
                "WHERE rn = 1;",
            ]
        },
        {
            'cell': 2, 'name': 'silver_order_lines', 'pk': 'line_id',
            'source': 'bronze.raw_order_lines + bronze.raw_products (lookup)',
            'derived': 'fulfillment_rate, line_margin, category, subcategory',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_order_lines",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY line_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_order_lines",
                "  WHERE line_id IS NOT NULL",
                "),",
                "typed AS (",
                "  SELECT",
                "    line_id, order_id, sku,",
                "    CAST(qty_ordered   AS INT)    AS qty_ordered,",
                "    CAST(qty_fulfilled AS INT)    AS qty_fulfilled,",
                "    CAST(unit_price    AS DOUBLE) AS unit_price,",
                "    CAST(line_value    AS DOUBLE) AS line_value,",
                "    TO_DATE(fulfilled_date)       AS fulfilled_date,",
                "    CAST(is_backordered AS BOOLEAN) AS is_backordered,",
                "    CASE WHEN CAST(qty_ordered AS INT) > 0",
                "         THEN CAST(qty_fulfilled AS DOUBLE) / CAST(qty_ordered AS DOUBLE)",
                "         ELSE 0.0 END                       AS fulfillment_rate",
                "  FROM deduped WHERE rn = 1",
                "),",
                "with_product AS (",
                "  SELECT",
                "    t.*,",
                "    p.unit_cost,",
                "    p.category,",
                "    p.subcategory,",
                "    t.line_value - (t.qty_fulfilled * p.unit_cost) AS line_margin",
                "  FROM typed t",
                "  LEFT JOIN bronze.raw_products p ON t.sku = p.sku",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_product;",
            ]
        },
        {
            'cell': 3, 'name': 'silver_customers', 'pk': 'customer_id',
            'source': 'bronze.raw_customers',
            'derived': 'segment / account_status normalised, customer_since_year',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_customers",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_customers WHERE customer_id IS NOT NULL",
                ")",
                "SELECT",
                "  customer_id, first_name, last_name, email, phone,",
                "  UPPER(TRIM(segment))        AS segment,",
                "  UPPER(TRIM(account_status)) AS account_status,",
                "  CAST(credit_limit AS DOUBLE) AS credit_limit,",
                "  city, state, region, country,",
                "  TO_DATE(created_date)        AS created_date,",
                "  YEAR(TO_DATE(created_date))  AS customer_since_year,",
                "  current_timestamp()          AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 4, 'name': 'silver_products', 'pk': 'sku',
            'source': 'bronze.raw_products',
            'derived': 'margin_pct, volume_cuft',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_products",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_products WHERE sku IS NOT NULL",
                ")",
                "SELECT",
                "  sku, product_name, category, subcategory, supplier_id,",
                "  CAST(unit_cost  AS DOUBLE) AS unit_cost,",
                "  CAST(unit_price AS DOUBLE) AS unit_price,",
                "  CAST(weight_kg  AS DOUBLE) AS weight_kg,",
                "  CAST(reorder_point  AS INT) AS reorder_point,",
                "  CAST(reorder_qty    AS INT) AS reorder_qty,",
                "  CAST(lead_time_days AS INT) AS lead_time_days,",
                "  CAST(is_hazmat     AS BOOLEAN) AS is_hazmat,",
                "  CAST(is_perishable AS BOOLEAN) AS is_perishable,",
                "  -- margin_pct = (price - cost) / price",
                "  CASE WHEN CAST(unit_price AS DOUBLE) > 0",
                "       THEN (CAST(unit_price AS DOUBLE) - CAST(unit_cost AS DOUBLE))",
                "            / CAST(unit_price AS DOUBLE)",
                "       ELSE 0.0 END             AS margin_pct,",
                "  -- volume in cubic feet (1 ft³ = 28316.8 cm³)",
                "  (CAST(length_cm AS DOUBLE) * CAST(width_cm AS DOUBLE)",
                "   * CAST(height_cm AS DOUBLE)) / 28316.8 AS volume_cuft,",
                "  current_timestamp()          AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 5, 'name': 'silver_inventory', 'pk': 'inventory_id',
            'source': 'bronze.raw_inventory + bronze.raw_products (lookup)',
            'derived': 'inventory_value, below_reorder flag, utilization_rate',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_inventory",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY inventory_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_inventory",
                "  WHERE inventory_id IS NOT NULL AND sku IS NOT NULL",
                "),",
                "typed AS (",
                "  SELECT",
                "    inventory_id, sku, warehouse_id, location_id,",
                "    CAST(qty_on_hand   AS INT)    AS qty_on_hand,",
                "    CAST(qty_reserved  AS INT)    AS qty_reserved,",
                "    CAST(qty_available AS INT)    AS qty_available,",
                "    CAST(qty_in_transit AS INT)   AS qty_in_transit,",
                "    CAST(unit_cost     AS DOUBLE) AS unit_cost,",
                "    CAST(reorder_point AS INT)    AS reorder_point,",
                "    TO_TIMESTAMP(last_counted_at) AS last_counted_at,",
                "    TO_TIMESTAMP(last_received_at) AS last_received_at",
                "  FROM deduped WHERE rn = 1",
                "),",
                "enriched AS (",
                "  SELECT",
                "    t.*,",
                "    t.qty_on_hand * t.unit_cost                       AS inventory_value,",
                "    t.qty_available < t.reorder_point                 AS below_reorder,",
                "    CASE WHEN t.reorder_point > 0",
                "         THEN CAST(t.qty_on_hand AS DOUBLE) / t.reorder_point",
                "         ELSE NULL END                                AS utilization_rate,",
                "    p.product_name, p.category, p.subcategory",
                "  FROM typed t",
                "  LEFT JOIN bronze.raw_products p ON t.sku = p.sku",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM enriched;",
            ]
        },
        {
            'cell': 6, 'name': 'silver_inventory_transactions', 'pk': 'txn_id',
            'source': 'bronze.raw_inventory_transactions',
            'derived': 'txn_date, txn_month, qty_mismatch_flag',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_inventory_transactions",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_inventory_transactions WHERE txn_id IS NOT NULL",
                ")",
                "SELECT",
                "  txn_id, inventory_id, sku, warehouse_id, location_id,",
                "  txn_type, reference_id, reference_type,",
                "  CAST(qty_change AS INT)  AS qty_change,",
                "  CAST(qty_before AS INT)  AS qty_before,",
                "  CAST(qty_after  AS INT)  AS qty_after,",
                "  -- sign-check: flag transactions where qty math doesn't reconcile",
                "  CASE WHEN CAST(qty_before AS INT) + CAST(qty_change AS INT)",
                "            <> CAST(qty_after AS INT)",
                "       THEN TRUE ELSE FALSE END               AS qty_mismatch_flag,",
                "  TO_TIMESTAMP(txn_timestamp)                 AS txn_timestamp,",
                "  TO_DATE(txn_timestamp)                      AS txn_date,",
                "  DATE_FORMAT(TO_DATE(txn_timestamp),'yyyy-MM') AS txn_month,",
                "  performed_by,",
                "  current_timestamp()                         AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 7, 'name': 'silver_shipments', 'pk': 'shipment_id',
            'source': 'bronze.raw_shipments + bronze.raw_carriers (lookup)',
            'derived': 'delay_days, cost_per_lb, carrier attributes',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_shipments",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_shipments WHERE shipment_id IS NOT NULL",
                "),",
                "typed AS (",
                "  SELECT",
                "    shipment_id, order_id, carrier_id, origin_warehouse_id,",
                "    TO_DATE(ship_date)          AS ship_date,",
                "    TO_DATE(estimated_delivery) AS estimated_delivery,",
                "    TO_DATE(actual_delivery)    AS actual_delivery,",
                "    UPPER(TRIM(status))         AS status,",
                "    CAST(shipping_cost AS DOUBLE) AS shipping_cost,",
                "    CAST(weight_lbs    AS DOUBLE) AS weight_lbs,",
                "    CAST(num_packages  AS INT)    AS num_packages,",
                "    CAST(is_on_time    AS BOOLEAN) AS is_on_time,",
                "    -- positive = late, negative = early",
                "    CASE WHEN actual_delivery IS NOT NULL AND estimated_delivery IS NOT NULL",
                "         THEN DATEDIFF(TO_DATE(actual_delivery), TO_DATE(estimated_delivery))",
                "         ELSE 0 END                          AS delay_days,",
                "    CASE WHEN CAST(weight_lbs AS DOUBLE) > 0",
                "         THEN CAST(shipping_cost AS DOUBLE) / CAST(weight_lbs AS DOUBLE)",
                "         ELSE NULL END                       AS cost_per_lb",
                "  FROM deduped WHERE rn = 1",
                "),",
                "with_carrier AS (",
                "  SELECT t.*, c.carrier_name, c.service_level,",
                "    c.avg_transit_days, c.cost_per_lb AS carrier_rate_per_lb",
                "  FROM typed t",
                "  LEFT JOIN bronze.raw_carriers c ON t.carrier_id = c.carrier_id",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_carrier;",
            ]
        },
        {
            'cell': 8, 'name': 'silver_purchase_orders', 'pk': 'po_id',
            'source': 'bronze.raw_purchase_orders + bronze.raw_suppliers (lookup)',
            'derived': 'actual_lead_days, days_late',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_purchase_orders",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY po_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_purchase_orders WHERE po_id IS NOT NULL",
                "),",
                "typed AS (",
                "  SELECT",
                "    po_id, supplier_id, sku, warehouse_id,",
                "    TO_DATE(po_date)           AS po_date,",
                "    TO_DATE(expected_delivery) AS expected_delivery,",
                "    TO_DATE(actual_delivery)   AS actual_delivery,",
                "    UPPER(TRIM(status))        AS status,",
                "    CAST(qty_ordered  AS INT)    AS qty_ordered,",
                "    CAST(unit_cost    AS DOUBLE) AS unit_cost,",
                "    CAST(total_cost   AS DOUBLE) AS total_cost,",
                "    CAST(is_on_time   AS BOOLEAN) AS is_on_time,",
                "    CASE WHEN actual_delivery IS NOT NULL",
                "         THEN DATEDIFF(TO_DATE(actual_delivery), TO_DATE(po_date))",
                "         ELSE NULL END           AS actual_lead_days,",
                "    CASE WHEN actual_delivery IS NOT NULL AND expected_delivery IS NOT NULL",
                "         THEN DATEDIFF(TO_DATE(actual_delivery), TO_DATE(expected_delivery))",
                "         ELSE 0 END              AS days_late",
                "  FROM deduped WHERE rn = 1",
                "),",
                "with_supplier AS (",
                "  SELECT t.*, s.company_name AS supplier_name,",
                "    s.reliability_score, s.is_preferred,",
                "    s.lead_time_days AS supplier_lead_time_days",
                "  FROM typed t",
                "  LEFT JOIN bronze.raw_suppliers s ON t.supplier_id = s.supplier_id",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_supplier;",
            ]
        },
        {
            'cell': 9, 'name': 'silver_returns', 'pk': 'return_id',
            'source': 'bronze.raw_returns',
            'derived': 'days_to_restock, is_resellable, reason_group',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_returns",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_returns WHERE return_id IS NOT NULL",
                ")",
                "SELECT",
                "  return_id, order_id, sku,",
                "  TO_DATE(return_date)         AS return_date,",
                "  TO_DATE(restock_date)        AS restock_date,",
                "  CAST(qty_returned  AS INT)   AS qty_returned,",
                "  CAST(refund_amount AS DOUBLE) AS refund_amount,",
                "  reason_code,",
                "  UPPER(TRIM(condition))       AS condition,",
                "  disposition,",
                "  CASE WHEN restock_date IS NOT NULL",
                "       THEN DATEDIFF(TO_DATE(restock_date), TO_DATE(return_date))",
                "       ELSE NULL END                         AS days_to_restock,",
                "  UPPER(TRIM(condition)) IN ('NEW','LIKE_NEW') AS is_resellable,",
                "  CASE WHEN UPPER(reason_code) IN ('DAMAGED','DEFECTIVE')      THEN 'Quality'",
                "       WHEN UPPER(reason_code) IN ('NOT_AS_DESCRIBED','WRONG_ITEM') THEN 'Accuracy'",
                "       WHEN UPPER(reason_code) = 'CHANGED_MIND'                THEN 'Customer'",
                "       ELSE 'Other' END                      AS reason_group,",
                "  current_timestamp()                        AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 10, 'name': 'silver_supplier_performance', 'pk': 'perf_id',
            'source': 'bronze.raw_supplier_performance + bronze.raw_suppliers (lookup)',
            'derived': 'composite_score (weighted), score_tier (A/B/C)',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_supplier_performance",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY perf_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_supplier_performance WHERE perf_id IS NOT NULL",
                "),",
                "scored AS (",
                "  SELECT perf_id, supplier_id,",
                "    CAST(on_time_rate AS DOUBLE) AS on_time_rate,",
                "    CAST(fill_rate    AS DOUBLE) AS fill_rate,",
                "    CAST(defect_rate  AS DOUBLE) AS defect_rate,",
                "    CAST(avg_lead_days AS DOUBLE) AS avg_lead_days,",
                "    CAST(total_pos    AS INT)    AS total_pos,",
                "    CAST(on_time_deliveries AS INT) AS on_time_deliveries,",
                "    month_label,",
                "    -- composite = 40% on-time + 40% fill + 20% quality (1 - defect_rate)",
                "    (CAST(on_time_rate AS DOUBLE) * 0.4)",
                "    + (CAST(fill_rate  AS DOUBLE) * 0.4)",
                "    + ((1.0 - CAST(defect_rate AS DOUBLE)) * 0.2) AS composite_score",
                "  FROM deduped WHERE rn = 1",
                "),",
                "tiered AS (",
                "  SELECT *,",
                "    CASE WHEN composite_score >= 0.95 THEN 'A'",
                "         WHEN composite_score >= 0.85 THEN 'B'",
                "         ELSE 'C' END                        AS score_tier",
                "  FROM scored",
                "),",
                "with_supplier AS (",
                "  SELECT t.*, s.company_name AS supplier_name, s.is_preferred, s.country",
                "  FROM tiered t",
                "  LEFT JOIN bronze.raw_suppliers s ON t.supplier_id = s.supplier_id",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_supplier;",
            ]
        },
        {
            'cell': 11, 'name': 'silver_employees', 'pk': 'employee_id',
            'source': 'bronze.raw_employees + bronze.raw_warehouses (lookup)',
            'derived': 'tenure_days, annual_cost_est',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_employees",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_employees WHERE employee_id IS NOT NULL",
                "),",
                "typed AS (",
                "  SELECT",
                "    employee_id, first_name, last_name, role, warehouse_id,",
                "    TO_DATE(hire_date)          AS hire_date,",
                "    CAST(hourly_rate AS DOUBLE) AS hourly_rate,",
                "    CAST(is_active   AS BOOLEAN) AS is_active,",
                "    -- days employed as of today",
                "    DATEDIFF(CURRENT_DATE(), TO_DATE(hire_date)) AS tenure_days,",
                "    -- estimated annual cost: 52 wk × 40 hr = 2,080 hours",
                "    CAST(hourly_rate AS DOUBLE) * 2080.0         AS annual_cost_est",
                "  FROM deduped WHERE rn = 1",
                "),",
                "with_warehouse AS (",
                "  SELECT t.*, w.name AS warehouse_name, w.region AS warehouse_region",
                "  FROM typed t",
                "  LEFT JOIN bronze.raw_warehouses w ON t.warehouse_id = w.warehouse_id",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_warehouse;",
            ]
        },
        {
            'cell': 12, 'name': 'silver_labor_shifts', 'pk': 'shift_id',
            'source': 'bronze.raw_labor_shifts + bronze.raw_employees (lookup)',
            'derived': 'picks_per_hour, units_per_hour, labor_cost',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_labor_shifts",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY shift_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_labor_shifts WHERE shift_id IS NOT NULL",
                "),",
                "typed AS (",
                "  SELECT",
                "    shift_id, employee_id, warehouse_id, zone_assigned, shift_type,",
                "    TO_DATE(shift_date)          AS shift_date,",
                "    CAST(hours_worked    AS DOUBLE) AS hours_worked,",
                "    CAST(picks_completed AS INT)    AS picks_completed,",
                "    CAST(units_processed AS INT)    AS units_processed,",
                "    DATE_FORMAT(TO_DATE(shift_date), 'yyyy-MM') AS shift_month,",
                "    CASE WHEN CAST(hours_worked AS DOUBLE) > 0",
                "         THEN CAST(picks_completed AS DOUBLE) / CAST(hours_worked AS DOUBLE)",
                "         ELSE 0.0 END                          AS picks_per_hour,",
                "    CASE WHEN CAST(hours_worked AS DOUBLE) > 0",
                "         THEN CAST(units_processed AS DOUBLE) / CAST(hours_worked AS DOUBLE)",
                "         ELSE 0.0 END                          AS units_per_hour",
                "  FROM deduped WHERE rn = 1",
                "),",
                "with_employee AS (",
                "  SELECT t.*, e.first_name, e.last_name, e.role, e.hourly_rate,",
                "    t.hours_worked * e.hourly_rate AS labor_cost",
                "  FROM typed t",
                "  LEFT JOIN bronze.raw_employees e ON t.employee_id = e.employee_id",
                ")",
                "SELECT *, current_timestamp() AS _loaded_at FROM with_employee;",
            ]
        },
        {
            'cell': 13, 'name': 'silver_dock_activity', 'pk': 'dock_id',
            'source': 'bronze.raw_dock_activity',
            'derived': 'activity_date, activity_hour, pallets_per_hour, is_inbound',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_dock_activity",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY dock_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_dock_activity WHERE dock_id IS NOT NULL",
                ")",
                "SELECT",
                "  dock_id, warehouse_id, carrier_id, shipment_id,",
                "  UPPER(TRIM(activity_type))     AS activity_type,",
                "  TO_TIMESTAMP(arrived_at)        AS arrived_at,",
                "  TO_TIMESTAMP(departed_at)       AS departed_at,",
                "  CAST(duration_minutes AS INT)   AS duration_minutes,",
                "  CAST(num_pallets      AS INT)   AS num_pallets,",
                "  TO_DATE(arrived_at)             AS activity_date,",
                "  HOUR(TO_TIMESTAMP(arrived_at))  AS activity_hour,",
                "  CASE WHEN CAST(duration_minutes AS INT) > 0",
                "       THEN CAST(num_pallets AS DOUBLE) / (CAST(duration_minutes AS DOUBLE) / 60.0)",
                "       ELSE NULL END               AS pallets_per_hour,",
                "  UPPER(TRIM(activity_type)) = 'INBOUND_RECEIVE' AS is_inbound,",
                "  current_timestamp()              AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 14, 'name': 'silver_locations', 'pk': 'location_id',
            'source': 'bronze.raw_locations',
            'derived': 'clean typing and dedup only (not in original spec)',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_locations",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_locations WHERE location_id IS NOT NULL",
                ")",
                "SELECT",
                "  location_id, warehouse_id, zone, aisle, bay, level, position,",
                "  UPPER(TRIM(location_type))    AS location_type,",
                "  CAST(max_weight_kg  AS DOUBLE) AS max_weight_kg,",
                "  CAST(max_volume_cuft AS DOUBLE) AS max_volume_cuft,",
                "  CAST(is_active      AS BOOLEAN) AS is_active,",
                "  current_timestamp()            AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
        {
            'cell': 15, 'name': 'silver_date_dim', 'pk': 'date_key',
            'source': 'bronze.raw_date_dim',
            'derived': 'month_label — consistent yyyy-MM label for all fact joins',
            'code': [
                "CREATE OR REPLACE TABLE silver.silver_date_dim",
                "USING DELTA",
                "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                "AS",
                "WITH deduped AS (",
                "  SELECT *, ROW_NUMBER() OVER (PARTITION BY date_key ORDER BY _loaded_at DESC) AS rn",
                "  FROM bronze.raw_date_dim WHERE date_key IS NOT NULL",
                ")",
                "SELECT",
                "  CAST(date_key   AS INT)     AS date_key,",
                "  TO_DATE(full_date)           AS full_date,",
                "  CAST(day_of_week AS INT)     AS day_of_week,",
                "  day_name,",
                "  CAST(week_num   AS INT)      AS week_num,",
                "  CAST(month_num  AS INT)      AS month_num,",
                "  month_name,",
                "  CAST(quarter    AS INT)      AS quarter,",
                "  CAST(year       AS INT)      AS year,",
                "  fiscal_period,",
                "  CAST(is_weekend AS BOOLEAN)  AS is_weekend,",
                "  CAST(is_holiday AS BOOLEAN)  AS is_holiday,",
                "  -- canonical yyyy-MM label for Power BI time-series joins",
                "  CONCAT(",
                "    CAST(CAST(year AS INT) AS STRING), '-',",
                "    LPAD(CAST(CAST(month_num AS INT) AS STRING), 2, '0')",
                "  )                            AS month_label,",
                "  current_timestamp()          AS _loaded_at",
                "FROM deduped WHERE rn = 1;",
            ]
        },
    ]

    for t in silver_cells:
        story.append(section_header(
            f"CELL {t['cell']} — silver.{t['name']}",
            f"pk: {t['pk']}  ·  source: {t['source']}  ·  derived: {t['derived']}",
            acc, 'nb_silver_transform_sql'))
        story.append(make_code_para(t['code']))
        story.append(spacer_line())

    # Validation
    story.append(PageBreak())
    story.append(section_header(
        'CELL 16 — Full Silver Validation',
        'pk_nulls + dupe check · all 15 tables · status must be ✓ OK before Part 3',
        acc, 'nb_silver_transform_sql'))

    sv = [
        ('silver_orders','order_id'), ('silver_order_lines','line_id'),
        ('silver_customers','customer_id'), ('silver_products','sku'),
        ('silver_inventory','inventory_id'), ('silver_inventory_transactions','txn_id'),
        ('silver_shipments','shipment_id'), ('silver_purchase_orders','po_id'),
        ('silver_returns','return_id'), ('silver_supplier_performance','perf_id'),
        ('silver_employees','employee_id'), ('silver_labor_shifts','shift_id'),
        ('silver_dock_activity','dock_id'), ('silver_locations','location_id'),
        ('silver_date_dim','date_key'),
    ]
    val = ['WITH audit AS (']
    for i, (t, p) in enumerate(sv):
        pfx = '  UNION ALL ' if i else '  '
        val += [
            f'{pfx}SELECT  \'{t}\' AS tbl, \'{p}\' AS pk,',
            f'          COUNT(*) AS rows,',
            f'          SUM(CASE WHEN {p} IS NULL THEN 1 ELSE 0 END) AS pk_nulls,',
            f'          COUNT(*) - COUNT(DISTINCT {p}) AS dupes',
            f'  FROM silver.{t}',
        ]
    val += [
        ')',
        'SELECT',
        '  tbl,  pk AS primary_key,  rows,  pk_nulls,  dupes,',
        "  CASE WHEN pk_nulls = 0 AND dupes = 0 THEN '✓ OK' ELSE '✗ FIX' END AS status",
        'FROM  audit',
        'ORDER BY tbl;',
        '',
        "-- All 15 rows must show status = '✓ OK' before running Part 3 (Gold).",
    ]
    story.append(make_code_para(val))

    doc.build(story, onFirstPage=lambda c,d: add_bg(c,d,acc),
                      onLaterPages=lambda c,d: add_bg(c,d,acc))
    print(f'Silver PDF written → {path}')


# ═══════════════════════════════════════════════════════════════
# GOLD
# ═══════════════════════════════════════════════════════════════
def build_gold(path):
    acc = GOLD_ACC
    doc = make_doc(path, acc)
    story = []

    story.append(page_title_block(
        'PART 3 OF 4',
        'GOLD  BATCH',
        'nb_gold_build_sql  ·  12 Delta Tables  ·  17 Cells  ·  0 Lines PySpark  ·  Star Schema + KPI Marts',
        acc))
    story.append(spacer_line())
    story.append(comment_block([
        'Star schema:',
        '  Facts (5) : gold_fact_orders, gold_fact_inventory, gold_fact_shipments, gold_fact_labor, gold_fact_dock',
        '  Dims  (4) : gold_dim_customer, gold_dim_product, gold_dim_warehouse, gold_dim_date',
        '  KPI   (3) : gold_kpi_supplier, gold_kpi_returns, gold_kpi_iot_summary',
        '',
        'Build order: Dimensions (1-4) → Facts (5-9) → KPI Marts (10-12) → OPTIMIZE (13-14) → VACUUM (15)',
    ], acc))
    story.append(spacer_line())

    # ── DIMENSIONS ──
    story.append(section_header('── DIMENSIONS ──', 'Build dims first — facts join to these', acc, 'nb_gold_build_sql'))
    story.append(spacer_line())

    story.append(section_header('CELL 1 — gold.gold_dim_date', 'source: silver.silver_date_dim  ·  anchor for all Power BI time-intelligence', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_dim_date",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  date_key, full_date, day_of_week, day_name, week_num,",
        "  month_num, month_name, month_label, quarter, year,",
        "  fiscal_period, is_weekend, is_holiday,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_date_dim;",
        "",
        "SELECT 'gold.gold_dim_date' AS tbl, COUNT(*) AS rows FROM gold.gold_dim_date;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 2 — gold.gold_dim_customer', 'source: silver.silver_customers  ·  derived: full_name', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_dim_customer",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  customer_id, first_name, last_name,",
        "  CONCAT(first_name, ' ', last_name) AS full_name,",
        "  segment, account_status, credit_limit,",
        "  city, state, region, country, customer_since_year, created_date,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_customers;",
        "",
        "SELECT 'gold.gold_dim_customer' AS tbl, COUNT(*) AS rows FROM gold.gold_dim_customer;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 3 — gold.gold_dim_product', 'source: silver.silver_products  ·  derived: price_tier (Economy / Mid / Premium)', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_dim_product",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  sku, product_name, category, subcategory, supplier_id,",
        "  unit_cost, unit_price, margin_pct, weight_kg, volume_cuft,",
        "  reorder_point, reorder_qty, lead_time_days, is_hazmat, is_perishable,",
        "  CASE",
        "    WHEN unit_price >= 500 THEN 'Premium'",
        "    WHEN unit_price >= 100 THEN 'Mid'",
        "    ELSE                        'Economy'",
        "  END AS price_tier,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_products;",
        "",
        "SELECT 'gold.gold_dim_product' AS tbl, COUNT(*) AS rows FROM gold.gold_dim_product;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 4 — gold.gold_dim_warehouse', 'source: bronze.raw_warehouses  ·  small reference table — reads Bronze directly', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_dim_warehouse",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "WITH deduped AS (",
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY _loaded_at DESC) AS rn",
        "  FROM bronze.raw_warehouses WHERE warehouse_id IS NOT NULL",
        ")",
        "SELECT",
        "  warehouse_id,",
        "  name        AS warehouse_name,",
        "  region, city, state, country, manager_id,",
        "  CAST(capacity_sqft AS DOUBLE) AS capacity_sqft,",
        "  CAST(num_docks     AS INT)    AS num_docks,",
        "  CAST(num_zones     AS INT)    AS num_zones,",
        "  current_timestamp()           AS _loaded_at",
        "FROM deduped WHERE rn = 1;",
        "",
        "SELECT 'gold.gold_dim_warehouse' AS tbl, COUNT(*) AS rows FROM gold.gold_dim_warehouse;",
    ]))
    story.append(spacer_line())

    # ── FACTS ──
    story.append(PageBreak())
    story.append(section_header('── FACT TABLES ──', 'Dimensions must exist before running these cells', acc, 'nb_gold_build_sql'))
    story.append(spacer_line())

    story.append(section_header(
        'CELL 5 — gold.gold_fact_orders',
        'grain: 1 row per order_id  ·  sources: silver_orders + silver_order_lines (agg) + silver_shipments (agg)',
        acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_fact_orders",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "WITH line_agg AS (",
        "  -- Roll 128,899 line rows up to order grain",
        "  SELECT",
        "    order_id,",
        "    SUM(line_value)       AS total_line_value,",
        "    SUM(line_margin)      AS total_margin,",
        "    SUM(qty_ordered)      AS total_qty_ordered,",
        "    SUM(qty_fulfilled)    AS total_qty_fulfilled,",
        "    SUM(qty_backordered)  AS total_qty_backordered,",
        "    AVG(fulfillment_rate) AS avg_fulfillment_rate,",
        "    COUNT(DISTINCT sku)   AS distinct_skus",
        "  FROM silver.silver_order_lines",
        "  GROUP BY order_id",
        "),",
        "ship_agg AS (",
        "  -- Roll multiple shipments per order to order grain",
        "  SELECT",
        "    order_id,",
        "    MIN(ship_date)                        AS first_ship_date,",
        "    SUM(shipping_cost)                    AS total_shipping_cost,",
        "    MAX(CAST(is_on_time AS INT)) = 1      AS was_on_time,",
        "    SUM(delay_days)                       AS total_delay_days,",
        "    COUNT(shipment_id)                    AS num_shipments",
        "  FROM silver.silver_shipments",
        "  GROUP BY order_id",
        ")",
        "SELECT",
        "  o.order_id,",
        "  o.customer_id,",
        "  o.warehouse_id,",
        "  CAST(DATE_FORMAT(o.order_date,'yyyyMMdd') AS INT)   AS date_key,",
        "  o.order_date, o.order_month, o.order_year,",
        "  o.status, o.priority, o.order_value,",
        "  l.total_line_value,",
        "  l.total_margin,",
        "  CASE WHEN l.total_line_value > 0",
        "       THEN l.total_margin / l.total_line_value",
        "       ELSE 0.0 END                                    AS margin_pct,",
        "  l.total_qty_ordered, l.total_qty_fulfilled, l.total_qty_backordered,",
        "  l.avg_fulfillment_rate, l.distinct_skus,",
        "  s.first_ship_date, s.total_shipping_cost, s.was_on_time,",
        "  s.total_delay_days, s.num_shipments,",
        "  DATEDIFF(s.first_ship_date, o.order_date)           AS days_to_ship,",
        "  o.days_to_required,",
        "  current_timestamp()                                  AS _loaded_at",
        "FROM silver.silver_orders o",
        "LEFT JOIN line_agg l ON o.order_id = l.order_id",
        "LEFT JOIN ship_agg s ON o.order_id = s.order_id;",
        "",
        "SELECT 'gold.gold_fact_orders' AS tbl, COUNT(*) AS rows FROM gold.gold_fact_orders;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 6 — gold.gold_fact_inventory', 'grain: 1 row per inventory_id  ·  derived: stock_status, days_since_counted', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_fact_inventory",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  inventory_id, sku, warehouse_id, location_id,",
        "  qty_on_hand, qty_reserved, qty_available, qty_in_transit,",
        "  unit_cost, inventory_value, below_reorder, reorder_point, utilization_rate,",
        "  last_counted_at, last_received_at, category, subcategory, product_name,",
        "  DATEDIFF(CURRENT_DATE(), TO_DATE(last_counted_at)) AS days_since_counted,",
        "  CASE",
        "    WHEN qty_available = 0    THEN 'Out of Stock'",
        "    WHEN below_reorder = TRUE THEN 'Low Stock'",
        "    ELSE                           'In Stock'",
        "  END AS stock_status,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_inventory;",
        "",
        "SELECT 'gold.gold_fact_inventory' AS tbl, COUNT(*) AS rows FROM gold.gold_fact_inventory;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 7 — gold.gold_fact_shipments', 'grain: 1 row per shipment_id  ·  derived: date_key, ship_month', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_fact_shipments",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  shipment_id, order_id, carrier_id, carrier_name, service_level,",
        "  origin_warehouse_id, ship_date, estimated_delivery, actual_delivery, status,",
        "  weight_lbs, shipping_cost, is_on_time,",
        "  COALESCE(delay_days, 0)                            AS delay_days,",
        "  num_packages, cost_per_lb, avg_transit_days,",
        "  CAST(DATE_FORMAT(ship_date,'yyyyMMdd') AS INT)     AS date_key,",
        "  DATE_FORMAT(ship_date,'yyyy-MM')                   AS ship_month,",
        "  current_timestamp()                                AS _loaded_at",
        "FROM silver.silver_shipments;",
        "",
        "SELECT 'gold.gold_fact_shipments' AS tbl, COUNT(*) AS rows FROM gold.gold_fact_shipments;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 8 — gold.gold_fact_labor', 'grain: 1 row per shift_id  ·  derived: date_key, cost_per_pick', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_fact_labor",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  shift_id, employee_id, warehouse_id, zone_assigned, shift_type,",
        "  shift_date, shift_month, hours_worked, picks_completed, units_processed,",
        "  picks_per_hour, units_per_hour, labor_cost,",
        "  role, first_name, last_name, hourly_rate,",
        "  CAST(DATE_FORMAT(shift_date,'yyyyMMdd') AS INT)    AS date_key,",
        "  CASE WHEN picks_completed > 0",
        "       THEN labor_cost / picks_completed",
        "       ELSE NULL END                                 AS cost_per_pick,",
        "  current_timestamp()                                AS _loaded_at",
        "FROM silver.silver_labor_shifts;",
        "",
        "SELECT 'gold.gold_fact_labor' AS tbl, COUNT(*) AS rows FROM gold.gold_fact_labor;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 9 — gold.gold_fact_dock', 'grain: 1 row per dock_id  ·  derived: date_key', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_fact_dock",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  dock_id, warehouse_id, carrier_id, shipment_id,",
        "  activity_type, activity_date, activity_hour,",
        "  duration_minutes, num_pallets, pallets_per_hour, is_inbound,",
        "  CAST(DATE_FORMAT(activity_date,'yyyyMMdd') AS INT) AS date_key,",
        "  current_timestamp()                                AS _loaded_at",
        "FROM silver.silver_dock_activity;",
        "",
        "SELECT 'gold.gold_fact_dock' AS tbl, COUNT(*) AS rows FROM gold.gold_fact_dock;",
    ]))
    story.append(spacer_line())

    # ── KPI MARTS ──
    story.append(PageBreak())
    story.append(section_header('── KPI MARTS ──', '', acc, 'nb_gold_build_sql'))
    story.append(spacer_line())

    story.append(section_header('CELL 10 — gold.gold_kpi_supplier', 'grain: supplier × month  ·  purpose: Supplier Scorecard report', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_kpi_supplier",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  sp.perf_id, sp.supplier_id, sp.supplier_name,",
        "  CAST(s.is_preferred AS BOOLEAN) AS is_preferred,",
        "  sp.country, sp.period_month, sp.period_year, sp.month_label,",
        "  sp.total_pos, sp.on_time_deliveries, sp.on_time_rate,",
        "  sp.fill_rate, sp.defect_rate, sp.avg_lead_days,",
        "  sp.composite_score, sp.score_tier,",
        "  s.reliability_score,",
        "  current_timestamp() AS _loaded_at",
        "FROM silver.silver_supplier_performance sp",
        "LEFT JOIN bronze.raw_suppliers s ON sp.supplier_id = s.supplier_id;",
        "",
        "SELECT 'gold.gold_kpi_supplier' AS tbl, COUNT(*) AS rows FROM gold.gold_kpi_supplier;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 11 — gold.gold_kpi_returns', 'grain: 1 row per return_id  ·  derived: loss_amount (non-resellable only)', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_kpi_returns",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "SELECT",
        "  r.return_id, r.order_id, r.sku,",
        "  p.product_name, p.category,",
        "  r.qty_returned, r.refund_amount, r.return_date,",
        "  DATE_FORMAT(r.return_date,'yyyy-MM')               AS return_month,",
        "  YEAR(r.return_date)                                AS return_year,",
        "  r.reason_code, r.reason_group,",
        "  r.condition, r.disposition, r.is_resellable, r.days_to_restock,",
        "  p.unit_cost,",
        "  -- non-resellable items written off at cost",
        "  CASE WHEN r.is_resellable = FALSE",
        "       THEN r.qty_returned * p.unit_cost",
        "       ELSE 0.0 END                                  AS loss_amount,",
        "  current_timestamp()                                AS _loaded_at",
        "FROM silver.silver_returns r",
        "LEFT JOIN silver.silver_products p ON r.sku = p.sku;",
        "",
        "SELECT 'gold.gold_kpi_returns' AS tbl, COUNT(*) AS rows FROM gold.gold_kpi_returns;",
    ]))
    story.append(spacer_line())

    story.append(section_header(
        'CELL 12 — gold.gold_kpi_iot_summary  (BATCH)',
        'grain: device_id × warehouse_id × zone × event_date  ·  hourly streaming version → Part 4',
        acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "CREATE OR REPLACE TABLE gold.gold_kpi_iot_summary",
        "USING DELTA",
        "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
        "AS",
        "WITH cast_events AS (",
        "  SELECT",
        "    event_id, device_id, device_type, warehouse_id, zone, event_type,",
        "    TO_DATE(event_timestamp)       AS event_date,",
        "    CAST(speed_mph     AS DOUBLE)  AS speed_mph,",
        "    CAST(battery_pct   AS DOUBLE)  AS battery_pct,",
        "    CAST(temperature_f AS DOUBLE)  AS temperature_f,",
        "    CAST(carrying_qty  AS INT)     AS carrying_qty",
        "  FROM bronze.raw_iot_events",
        "  WHERE event_id IS NOT NULL",
        ")",
        "SELECT",
        "  device_id, device_type, warehouse_id, zone, event_date,",
        "  CAST(DATE_FORMAT(event_date,'yyyyMMdd') AS INT)    AS date_key,",
        "  COUNT(event_id)                                    AS total_events,",
        "  ROUND(AVG(speed_mph),     2)                       AS avg_speed_mph,",
        "  ROUND(MAX(speed_mph),     2)                       AS max_speed_mph,",
        "  ROUND(AVG(battery_pct),   2)                       AS avg_battery_pct,",
        "  ROUND(MIN(battery_pct),   2)                       AS min_battery_pct,",
        "  ROUND(AVG(temperature_f), 2)                       AS avg_temp_f,",
        "  SUM(carrying_qty)                                  AS total_carrying_qty,",
        "  SUM(CASE WHEN event_type = 'ALERT' THEN 1 ELSE 0 END) AS alert_count,",
        "  SUM(CASE WHEN event_type = 'IDLE'  THEN 1 ELSE 0 END) AS idle_count,",
        "  SUM(CASE WHEN event_type = 'MOVE'  THEN 1 ELSE 0 END) AS move_count,",
        "  ROUND(",
        "    SUM(CASE WHEN event_type = 'ALERT' THEN 1 ELSE 0 END)",
        "    / CAST(COUNT(event_id) AS DOUBLE), 4",
        "  )                                                  AS alert_rate,",
        "  current_timestamp()                                AS _loaded_at",
        "FROM cast_events",
        "GROUP BY device_id, device_type, warehouse_id, zone, event_date;",
        "",
        "SELECT 'gold.gold_kpi_iot_summary' AS tbl, COUNT(*) AS rows FROM gold.gold_kpi_iot_summary;",
    ]))
    story.append(spacer_line())

    # OPTIMIZE / ZORDER / VACUUM
    story.append(PageBreak())
    story.append(section_header('── POST-BUILD: OPTIMIZE + ZORDER + VACUUM ──',
        'Run after all 12 Gold tables are written · stop stream first if running', acc, 'nb_gold_build_sql'))
    story.append(spacer_line())

    story.append(section_header('CELL 13 — OPTIMIZE + ZORDER: Fact Tables', '', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "OPTIMIZE gold.gold_fact_orders    ZORDER BY (order_date,    customer_id);",
        "OPTIMIZE gold.gold_fact_inventory ZORDER BY (sku,           warehouse_id);",
        "OPTIMIZE gold.gold_fact_shipments ZORDER BY (ship_date,     carrier_id);",
        "OPTIMIZE gold.gold_fact_labor     ZORDER BY (shift_date,    warehouse_id);",
        "OPTIMIZE gold.gold_fact_dock      ZORDER BY (activity_date, warehouse_id);",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 14 — OPTIMIZE + ZORDER: KPI Marts', '', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "OPTIMIZE gold.gold_kpi_supplier    ZORDER BY (period_year,  period_month);",
        "OPTIMIZE gold.gold_kpi_returns     ZORDER BY (return_date,  reason_code);",
        "OPTIMIZE gold.gold_kpi_iot_summary ZORDER BY (event_date,   warehouse_id);",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 15 — VACUUM: All Gold Tables (retain 7 days / 168 hours)', '', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "VACUUM gold.gold_fact_orders      RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_inventory   RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_shipments   RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_labor       RETAIN 168 HOURS;",
        "VACUUM gold.gold_fact_dock        RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_customer     RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_product      RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_warehouse    RETAIN 168 HOURS;",
        "VACUUM gold.gold_dim_date         RETAIN 168 HOURS;",
        "VACUUM gold.gold_kpi_supplier     RETAIN 168 HOURS;",
        "VACUUM gold.gold_kpi_returns      RETAIN 168 HOURS;",
        "VACUUM gold.gold_kpi_iot_summary  RETAIN 168 HOURS;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 16 — Full Gold Validation', 'all 12 tables must show ✓ OK', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "SELECT tbl, rows,",
        "  CASE WHEN rows > 0 THEN '✓ OK' ELSE '✗ EMPTY' END AS status",
        "FROM (",
        "  SELECT 'gold_fact_orders'      AS tbl, COUNT(*) AS rows FROM gold.gold_fact_orders      UNION ALL",
        "  SELECT 'gold_fact_inventory',         COUNT(*)          FROM gold.gold_fact_inventory   UNION ALL",
        "  SELECT 'gold_fact_shipments',         COUNT(*)          FROM gold.gold_fact_shipments   UNION ALL",
        "  SELECT 'gold_fact_labor',             COUNT(*)          FROM gold.gold_fact_labor        UNION ALL",
        "  SELECT 'gold_fact_dock',              COUNT(*)          FROM gold.gold_fact_dock         UNION ALL",
        "  SELECT 'gold_dim_customer',           COUNT(*)          FROM gold.gold_dim_customer      UNION ALL",
        "  SELECT 'gold_dim_product',            COUNT(*)          FROM gold.gold_dim_product       UNION ALL",
        "  SELECT 'gold_dim_warehouse',          COUNT(*)          FROM gold.gold_dim_warehouse     UNION ALL",
        "  SELECT 'gold_dim_date',               COUNT(*)          FROM gold.gold_dim_date          UNION ALL",
        "  SELECT 'gold_kpi_supplier',           COUNT(*)          FROM gold.gold_kpi_supplier      UNION ALL",
        "  SELECT 'gold_kpi_returns',            COUNT(*)          FROM gold.gold_kpi_returns       UNION ALL",
        "  SELECT 'gold_kpi_iot_summary',        COUNT(*)          FROM gold.gold_kpi_iot_summary",
        ") t",
        "ORDER BY tbl;",
    ]))
    story.append(spacer_line())

    story.append(section_header('CELL 17 — Quick Sanity: Revenue + OTD Rate', '', acc, 'nb_gold_build_sql'))
    story.append(make_code_para([
        "SELECT",
        "  ROUND(SUM(order_value), 2)                                       AS total_revenue,",
        "  ROUND(AVG(margin_pct) * 100, 2)                                  AS avg_margin_pct,",
        "  COUNT(*)                                                          AS total_orders,",
        "  SUM(CASE WHEN was_on_time THEN 1 ELSE 0 END)                     AS orders_on_time,",
        "  ROUND(",
        "    SUM(CASE WHEN was_on_time THEN 1 ELSE 0 END)",
        "    / CAST(COUNT(*) AS DOUBLE) * 100, 2",
        "  )                                                                 AS otd_rate_pct",
        "FROM gold.gold_fact_orders;",
    ]))

    doc.build(story, onFirstPage=lambda c,d: add_bg(c,d,acc),
                      onLaterPages=lambda c,d: add_bg(c,d,acc))
    print(f'Gold PDF written → {path}')


# ── Run ─────────────────────────────────────────────────────────
build_bronze('/mnt/user-data/outputs/WMS_Bronze_CodeStyle.pdf')
build_silver('/mnt/user-data/outputs/WMS_Silver_CodeStyle.pdf')
build_gold(  '/mnt/user-data/outputs/WMS_Gold_CodeStyle.pdf')
print('Done.')
