-- ==========================================================
-- 1. Economía a nivel de cuenta (Account Level Unit Economics)
-- ==========================================================

WITH wh_metrics AS (
    SELECT
        DATE_TRUNC('month', start_time) AS date,
        COUNT(*) AS num_exec_queries,
        SUM(bytes_scanned) / 1024 / 1024 / 1024 / 1024 AS tb_scanned,
        SUM(execution_time) / 1000 / 3600 AS execution_hours,
        COUNT(DISTINCT warehouse_name) AS live_warehouses,
        COUNT(DISTINCT database_name || '.' || schema_name) AS live_schemas,
        COUNT(DISTINCT user_name) AS active_users
    FROM snowflake.account_usage.query_history
    WHERE end_time::DATE >= DATEADD(DAY, -30, CURRENT_DATE)
    AND execution_time > 0
    AND cluster_number IS NOT NULL
    GROUP BY 1
),
credits AS (
    SELECT
        DATE_TRUNC('month', start_time) AS date,
        SUM(credits_used) AS credits,
        SUM(credits_attributed_compute_queries) AS attributed_credits
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time::DATE >= DATEADD(DAY, -30, CURRENT_DATE)
    GROUP BY 1
)
SELECT
    t1.date,
    num_exec_queries,
    tb_scanned,
    execution_hours,
    t2.credits,
    t2.attributed_credits,
    live_warehouses,
    live_schemas,
    active_users,
    DIV0(credits, tb_scanned) AS credits_per_tb_scanned,
    DIV0(credits, (num_exec_queries / 1000)) AS credits_per_thousand_queries,
    100 - (DIV0(attributed_credits, credits) * 100) AS idle_time_percentage
FROM wh_metrics t1
JOIN credits t2 ON t1.date = t2.date
ORDER BY t1.date;

-- ==========================================================
-- 2. Economía a nivel de warehouse (Warehouse Level Unit Economics)
-- ==========================================================

WITH wh_metrics AS (
    SELECT
        DATE_TRUNC('month', start_time) AS date,
        warehouse_name,
        COUNT(*) AS num_exec_queries,
        SUM(bytes_scanned) / 1024 / 1024 / 1024 / 1024 AS tb_scanned,
        SUM(execution_time) / 1000 / 3600 AS execution_hours,
        COUNT(DISTINCT database_name || '.' || schema_name) AS live_schemas,
        COUNT(DISTINCT user_name) AS active_users
    FROM snowflake.account_usage.query_history
    WHERE end_time::DATE >= DATEADD(DAY, -30, CURRENT_DATE)
    AND execution_time > 0
    AND cluster_number IS NOT NULL
    GROUP BY 1, 2
),
credits AS (
    SELECT
        DATE_TRUNC('month', start_time) AS date,
        warehouse_name,
        SUM(credits_used) AS credits,
        SUM(credits_attributed_compute_queries) AS attributed_credits
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time::DATE >= DATEADD(DAY, -30, CURRENT_DATE)
    GROUP BY 1, 2
)
SELECT
    t1.date,
    t1.warehouse_name,
    num_exec_queries,
    tb_scanned,
    execution_hours,
    t2.credits,
    t2.attributed_credits,
    live_schemas,
    active_users,
    DIV0(credits, tb_scanned) AS credits_per_tb_scanned,
    DIV0(credits, (num_exec_queries / 1000)) AS credits_per_thousand_queries,
    100 - (DIV0(attributed_credits, credits) * 100) AS idle_time_percentage
FROM wh_metrics t1
JOIN credits t2 ON t1.date = t2.date AND t1.warehouse_name = t2.warehouse_name
ORDER BY t1.date, t1.warehouse_name;

-- ==========================================================
-- 3. Consolidación de warehouses
-- ==========================================================

WITH consolidation AS (
    SELECT
        wlh.warehouse_name,
        AVG(avg_running) AS avg_running,
        AVG(avg_queued_load) AS avg_queued_load,
        AVG(avg_queued_provisioning) AS avg_queued_provisioning,
        AVG(avg_blocked) AS avg_blocked,
        APPROX_PERCENTILE(avg_running, 0.8) AS p80_avg_running,
        APPROX_PERCENTILE(avg_running, 0.95) AS p95_avg_running,
        (SUM(DATEDIFF('minute', start_time, end_time)) / 60) AS hours_reported,
        CASE
            WHEN p95_avg_running < 1 THEN 'HIGH CONFIDENCE CONSOLIDATION OPPORTUNITY'
            WHEN p80_avg_running < 1 THEN 'LOW CONFIDENCE CONSOLIDATION OPPORTUNITY'
            ELSE 'NO CHANGE'
        END AS recommendation
    FROM snowflake.account_usage.warehouse_load_history wlh
    WHERE start_time >= CURRENT_DATE - 30
    GROUP BY 1
),
credits AS (
    SELECT
        warehouse_name,
        SUM(credits_used) AS credits
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= CURRENT_DATE - 30
    GROUP BY 1
)
SELECT
    c.warehouse_name,
    avg_running,
    avg_queued_load,
    avg_queued_provisioning,
    avg_blocked,
    p80_avg_running,
    p95_avg_running,
    hours_reported,
    recommendation,
    credits
FROM consolidation c
JOIN credits cr ON cr.warehouse_name = c.warehouse_name
ORDER BY credits DESC;

-- ==========================================================
-- 4. Identificación de warehouses subutilizados
-- ==========================================================

WITH low_util_wh AS (
    SELECT 
        warehouse_name,
        APPROX_PERCENTILE(utilization, .5) AS p50_utilization
    FROM snowflake.account_usage.warehouse_utilization
    WHERE start_time::DATE >= DATEADD(day, -7, CURRENT_DATE)
    AND utilization > 0
    GROUP BY warehouse_name
    HAVING p50_utilization < 30
)
SELECT 
    l.warehouse_name,
    l.p50_utilization,
    SUM(a.credits_used) AS total_credits
FROM snowflake.account_usage.warehouse_metering_history a
JOIN low_util_wh l ON a.warehouse_name = l.warehouse_name
WHERE a.start_time::DATE >= DATEADD(day, -7, CURRENT_DATE)
GROUP BY l.warehouse_name, l.p50_utilization
ORDER BY total_credits DESC;

-- ==========================================================
-- 5. Almacenamiento inactivo y oportunidades de optimización
-- ==========================================================

SELECT  
    tsm.id AS table_id,
    tsm.table_catalog AS database_name,
    tsm.table_schema AS schema_name,
    tsm.table_name,
    tsm.is_transient,
    tsm.active_bytes,
    tsm.time_travel_bytes,
    tsm.failsafe_bytes,
    tsm.retained_for_clone_bytes,
    t.clustering_key,
    t.AUTO_CLUSTERING_ON,
    t.is_dynamic,
    t.is_iceberg,
    t.retention_time,
    (TIME_TRAVEL_BYTES + FAILSAFE_BYTES + RETAINED_FOR_CLONE_BYTES) / POW(1024, 4) AS inactive_storage_tb,
    (inactive_storage_tb + active_bytes / POW(1024, 4)) AS total_storage_tb,
    DIV0(((TIME_TRAVEL_BYTES + FAILSAFE_BYTES) / POW(1024, 4)), (active_bytes / POW(1024, 4))) * 100 AS churn_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS tsm 
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLES t ON t.table_id = tsm.id 
WHERE ACTIVE_BYTES / POW(1024, 3) > .1 
AND tsm.deleted = FALSE 
ORDER BY inactive_storage_tb DESC;
