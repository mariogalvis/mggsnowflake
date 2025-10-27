-- ==========================================================
-- Escenario 1: Asignación de costos usando etiquetas de objetos (Object Tagging)
-- ==========================================================
SELECT 
    COALESCE(tag_references.tag_value, 'untagged') AS tag_value, 
    SUM(warehouse_metering_history.credits_used_compute) AS total_credits
FROM snowflake.account_usage.warehouse_metering_history
LEFT JOIN snowflake.account_usage.tag_references
ON warehouse_metering_history.warehouse_id = tag_references.object_id
WHERE warehouse_metering_history.start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE)) 
  AND warehouse_metering_history.start_time < DATE_TRUNC('MONTH',  CURRENT_DATE)
GROUP BY COALESCE(tag_references.tag_value, 'untagged')
ORDER BY total_credits DESC;

-- ==========================================================
-- Escenario 2: Asignación de costos usando convenciones de nombres en warehouses
-- ==========================================================
SELECT 
    CASE 
        WHEN POSITION('_' IN warehouse_metering_history.warehouse_name) > 0 
             THEN SPLIT_PART(warehouse_metering_history.warehouse_name, '_', 1)
        ELSE 'others'
    END AS team_name,
    SUM(warehouse_metering_history.credits_used_compute) AS total_credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE warehouse_metering_history.start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE)) 
  AND warehouse_metering_history.start_time < DATE_TRUNC('MONTH',  CURRENT_DATE)
GROUP BY team_name
ORDER BY total_credits DESC;

-- ==========================================================
-- Método Común 1: Atribución de costos por usuario
-- ==========================================================
WITH wh_bill AS (
   SELECT SUM(credits_used_compute) AS compute_credits
   FROM snowflake.account_usage.warehouse_metering_history
   WHERE start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
   AND start_time < DATE_TRUNC('MONTH', CURRENT_DATE)
),
user_credits AS (
   SELECT user_name, SUM(credits_attributed_compute) AS credits
   FROM snowflake.account_usage.query_attribution_history
   WHERE start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
   AND start_time < DATE_TRUNC('MONTH', CURRENT_DATE)
   GROUP BY user_name
),
total_credit AS (
   SELECT SUM(credits) AS sum_all_credits
   FROM user_credits
)
SELECT 
    u.user_name, 
    u.credits / t.sum_all_credits * w.compute_credits AS attributed_credits
FROM user_credits u, total_credit t, wh_bill w
ORDER BY attributed_credits DESC;

-- ==========================================================
-- Método Común 2: Atribución de costos usando etiquetas de consultas (Query Tags)
-- ==========================================================
WITH wh_bill AS (
   SELECT SUM(credits_used_compute) AS compute_credits
   FROM snowflake.account_usage.warehouse_metering_history
   WHERE start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
   AND start_time < DATE_TRUNC('MONTH', CURRENT_DATE)
),
tag_credits AS (
   SELECT COALESCE(NULLIF(query_tag, ''), 'untagged') AS tag, SUM(credits_attributed_compute) AS credits
   FROM snowflake.account_usage.query_attribution_history
   WHERE start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
   AND start_time < DATE_TRUNC('MONTH', CURRENT_DATE)
   GROUP BY tag
),
total_credit AS (
   SELECT SUM(credits) AS sum_all_credits
   FROM tag_credits
)
SELECT 
    tc.tag, 
    tc.credits / t.sum_all_credits * w.compute_credits AS attributed_credits
FROM tag_credits tc, total_credit t, wh_bill w
ORDER BY attributed_credits DESC;

-- ==========================================================
-- Método Común 3: Atribución de costos para consultas recurrentes (Query Hash)
-- ==========================================================
SELECT 
    query_parameterized_hash, 
    COUNT(*) AS query_count, 
    SUM(credits_attributed_compute) AS total_credits
FROM snowflake.account_usage.query_attribution_history
WHERE start_time >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE))
  AND start_time < DATE_TRUNC('MONTH', CURRENT_DATE)
GROUP BY query_parameterized_hash
ORDER BY total_credits DESC
LIMIT 20;

-- ==========================================================
-- Verificación de créditos atribuidos a una consulta específica
-- ==========================================================
SET query_id = '<query_id>';  -- Reemplaza <query_id> con el ID real de la consulta

SELECT 
    SUM(credits_attributed_compute) AS total_attributed_credits
FROM snowflake.account_usage.query_attribution_history
WHERE (root_query_id = $query_id OR query_id = $query_id);

-- ==========================================================
-- Verificación de créditos Tokens
-- ==========================================================

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY WHERE SERVICE_TYPE='AI_SERVICES' LIMIT 10;;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY LIMIT 10;

SELECT *  FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY LIMIT 10;;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY WHERE query_id='01bc55af-0105-3a1a-0008-d4d300028566';
