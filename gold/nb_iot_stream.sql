-- ============================================================
-- nb_iot_stream.sql
-- WMS SQL-First Medallion Architecture
-- Part 4 of 4: IoT Streaming + Pipeline
-- Delta readStream → SQL 1-hour tumbling window → writeStream
-- ~15 lines PySpark (readStream/writeStream wrappers only)
-- prereq: bronze.raw_iot_events must have enableChangeDataFeed = true
-- ============================================================


-- ------------------------------------------------------------
-- CELL 1: Create the streaming target table (SQL — run once)
-- skip on subsequent restarts, the table already exists
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.gold_kpi_iot_stream
(
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    device_id        STRING,
    device_type      STRING,
    warehouse_id     STRING,
    zone             STRING,
    total_events     BIGINT,
    avg_speed_mph    DOUBLE,
    max_speed_mph    DOUBLE,
    avg_battery_pct  DOUBLE,
    min_battery_pct  DOUBLE,
    avg_temp_f       DOUBLE,
    total_carrying_qty BIGINT,
    alert_count      BIGINT,
    idle_count       BIGINT,
    move_count       BIGINT,
    alert_rate       DOUBLE,
    date_key         INT,
    event_hour       INT,
    _loaded_at       TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

DESCRIBE TABLE gold.gold_kpi_iot_stream;


-- ------------------------------------------------------------
-- CELL 2: Read the Delta stream + register as temp view
-- (Python — unavoidable for readStream)
-- ------------------------------------------------------------
-- from pyspark.sql import functions as F
--
-- iot_stream = (
--     spark.readStream
--         .format("delta")
--         .option("maxFilesPerTrigger", 1)
--         .table("bronze.raw_iot_events")
-- )
--
-- iot_typed = iot_stream.withColumn(
--     "event_ts", F.to_timestamp("event_timestamp")
-- )
--
-- iot_typed.createOrReplaceTempView("iot_stream_view")


-- ------------------------------------------------------------
-- CELL 3: SQL tumbling window aggregation (1-hour buckets)
-- this view is lazy — execution happens when writeStream starts
-- ------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW iot_windowed AS
SELECT
    window(event_ts, '1 hour').start                                AS window_start,
    window(event_ts, '1 hour').end                                  AS window_end,
    device_id,
    device_type,
    warehouse_id,
    zone,
    COUNT(event_id)                                                 AS total_events,
    ROUND(AVG(CAST(speed_mph     AS DOUBLE)), 2)                    AS avg_speed_mph,
    ROUND(MAX(CAST(speed_mph     AS DOUBLE)), 2)                    AS max_speed_mph,
    ROUND(AVG(CAST(battery_pct   AS DOUBLE)), 2)                    AS avg_battery_pct,
    ROUND(MIN(CAST(battery_pct   AS DOUBLE)), 2)                    AS min_battery_pct,
    ROUND(AVG(CAST(temperature_f AS DOUBLE)), 2)                    AS avg_temp_f,
    SUM(CAST(carrying_qty AS INT))                                  AS total_carrying_qty,
    SUM(CASE WHEN event_type = 'ALERT' THEN 1 ELSE 0 END)          AS alert_count,
    SUM(CASE WHEN event_type = 'IDLE'  THEN 1 ELSE 0 END)          AS idle_count,
    SUM(CASE WHEN event_type = 'MOVE'  THEN 1 ELSE 0 END)          AS move_count,
    ROUND(
        SUM(CASE WHEN event_type = 'ALERT' THEN 1 ELSE 0 END)
        / CAST(COUNT(event_id) AS DOUBLE),
        4
    )                                                               AS alert_rate,
    CAST(DATE_FORMAT(window(event_ts, '1 hour').start, 'yyyyMMdd') AS INT) AS date_key,
    HOUR(window(event_ts, '1 hour').start)                          AS event_hour,
    current_timestamp()                                             AS _loaded_at
FROM iot_stream_view
GROUP BY
    window(event_ts, '1 hour'),
    device_id,
    device_type,
    warehouse_id,
    zone;


-- ------------------------------------------------------------
-- CELL 4: writeStream → gold.gold_kpi_iot_stream
-- (Python — unavoidable for writeStream)
-- outputMode append: each completed 1-hour window writes one row
-- trigger 5 minutes: tune down to 1 min for lower PBI latency
-- checkpoint: stream resumes exactly where it left off on restart
-- ------------------------------------------------------------
-- CHECKPOINT_PATH = "Files/checkpoints/iot_stream"
--
-- query = (
--     spark.sql("SELECT * FROM iot_windowed")
--         .writeStream
--         .format("delta")
--         .outputMode("append")
--         .trigger(processingTime="5 minutes")
--         .option("checkpointLocation", CHECKPOINT_PATH)
--         .toTable("gold.gold_kpi_iot_stream")
-- )


-- ------------------------------------------------------------
-- CELL 5: Monitor stream progress
-- (Python — run anytime while stream is active)
-- ------------------------------------------------------------
-- import json
-- if query.isActive:
--     progress = query.lastProgress
--     if progress:
--         print(f"Status          : {query.status['message']}")
--         print(f"Input rows/sec  : {progress.get('inputRowsPerSecond', 0):.2f}")
--         print(f"Processed rows  : {progress.get('numInputRows', 0)}")
--         print(f"Batch duration  : {progress.get('durationMs', {}).get('triggerExecution', 0)} ms")
--         print(f"Watermark       : {progress.get('eventTime', {}).get('watermark', 'none')}")


-- ------------------------------------------------------------
-- CELL 6: Graceful stop
-- (Python — run to stop the stream cleanly before OPTIMIZE)
-- ------------------------------------------------------------
-- query.stop()


-- ------------------------------------------------------------
-- CELL 7: Spot-check streamed results (SQL — run anytime)
-- ------------------------------------------------------------
SELECT
    window_start,
    window_end,
    device_id,
    device_type,
    warehouse_id,
    zone,
    total_events,
    avg_speed_mph,
    avg_battery_pct,
    alert_count,
    alert_rate,
    event_hour
FROM gold.gold_kpi_iot_stream
ORDER BY window_start DESC
LIMIT 20;


-- ------------------------------------------------------------
-- CELL 8: Alert summary by warehouse — last 24 hours
-- Power BI-ready query
-- ------------------------------------------------------------
SELECT
    warehouse_id,
    zone,
    window_start,
    event_hour,
    SUM(alert_count)                                        AS total_alerts,
    SUM(total_events)                                       AS total_events,
    ROUND(SUM(alert_count) / SUM(total_events) * 100, 2)   AS alert_rate_pct,
    ROUND(AVG(avg_battery_pct), 2)                          AS avg_battery,
    ROUND(MIN(min_battery_pct), 2)                          AS lowest_battery
FROM gold.gold_kpi_iot_stream
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY warehouse_id, zone, window_start, event_hour
ORDER BY window_start DESC, alert_rate_pct DESC;


-- ------------------------------------------------------------
-- CELL 9: Batch vs stream row count comparison (sanity check)
-- ------------------------------------------------------------
SELECT
    'Batch (daily grain)'  AS source,
    COUNT(*)               AS rows,
    MIN(event_date)        AS earliest,
    MAX(event_date)        AS latest
FROM gold.gold_kpi_iot_summary
UNION ALL
SELECT
    'Stream (hourly grain)' AS source,
    COUNT(*)                AS rows,
    MIN(DATE(window_start)) AS earliest,
    MAX(DATE(window_start)) AS latest
FROM gold.gold_kpi_iot_stream;


-- ------------------------------------------------------------
-- CELL 10: OPTIMIZE the stream table
-- stop the stream first (Cell 6), then restart after (Cell 4)
-- ------------------------------------------------------------
OPTIMIZE gold.gold_kpi_iot_stream ZORDER BY (window_start, warehouse_id);
VACUUM   gold.gold_kpi_iot_stream RETAIN 168 HOURS;
