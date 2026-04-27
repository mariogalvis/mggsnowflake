-- =============================================================================
-- GESTION INTEGRAL DE COSTOS DE CORTEX AI EN SNOWFLAKE
-- Fuente: https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-func-cost-management
-- Ref: https://medium.com/towards-data-engineering/snowflake-cortex-code-heres-how-to-track-your-costs-before-they-surprise-you-eada5247eee6
-- =============================================================================


-- =============================================================================
-- SECCION 0: LIMITES DE CREDITOS PARA CORTEX CODE (Snowsight y CLI)
-- Controla el consumo diario estimado por usuario en ventana rodante de 24h.
-- Valores: -1 = sin limite (default), 0 = bloqueado, N = limite en creditos.
-- El nivel de usuario sobreescribe el nivel de cuenta.
-- =============================================================================

-- Nivel cuenta: aplica a todos los usuarios
ALTER ACCOUNT SET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = 20;
ALTER ACCOUNT SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = 20;

-- Nivel usuario: sobreescribe el nivel de cuenta para ese usuario
ALTER USER jsmith SET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = 10;
ALTER USER jsmith SET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER = 10;

-- Quitar limites (restaurar default ilimitado)
-- ALTER ACCOUNT UNSET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER;
-- ALTER ACCOUNT UNSET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER;
-- ALTER USER jsmith UNSET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER;
-- ALTER USER jsmith UNSET CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER;

-- Bloquear acceso a un usuario especifico
-- ALTER USER restricted_user SET CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER = 0;

-- Listar usuarios con override de limite CLI
EXECUTE IMMEDIATE $$
DECLARE
  current_user STRING;
  rs_users RESULTSET;
  res      RESULTSET;
BEGIN
  CREATE OR REPLACE TEMPORARY TABLE _param_overrides (user_name STRING, param_value STRING);
  SHOW USERS;
  rs_users := (SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  FOR record IN rs_users DO
    current_user := record."name";
    EXECUTE IMMEDIATE
      'SHOW PARAMETERS LIKE ''CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER'' IN USER "' || :current_user || '"';
    INSERT INTO _param_overrides (user_name, param_value)
      SELECT :current_user, "value"
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
      WHERE "level" = 'USER';
  END FOR;
  res := (SELECT * FROM _param_overrides);
  RETURN TABLE(res);
END;
$$;


-- =============================================================================
-- SECCION 1: MONITOREO BASICO DE USO DE FUNCIONES CORTEX AI
-- Vista principal: SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
-- Latencia maxima: 60 min. Datos desde enero 5, 2026.
-- =============================================================================

-- 1a. Consumo diario de creditos por funcion y modelo (ultimos 30 dias)
SELECT
    DATE_TRUNC('day', START_TIME) AS usage_date,
    FUNCTION_NAME,
    MODEL_NAME,
    SUM(CREDITS) AS total_credits,
    COUNT(DISTINCT QUERY_ID) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY usage_date DESC, total_credits DESC;

-- 1b. Consumo mensual de creditos por usuario (ultimos 3 meses)
SELECT
    DATE_TRUNC('month', h.START_TIME) AS usage_month,
    u.NAME AS user_name,
    u.EMAIL,
    u.DEFAULT_ROLE,
    SUM(h.CREDITS) AS total_credits,
    COUNT(DISTINCT h.QUERY_ID) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY h
JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
    ON h.USER_ID = u.USER_ID
WHERE h.START_TIME >= DATEADD('month', -3, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3, 4
ORDER BY usage_month DESC, total_credits DESC;

-- 1c. Desglose de tokens (input vs output) usando columna METRICS
SELECT
    DATE_TRUNC('day', START_TIME) AS usage_date,
    FUNCTION_NAME,
    MODEL_NAME,
    SUM(CREDITS) AS total_credits,
    SUM(m.value:"value"::NUMBER) AS total_tokens,
    m.value:"key":"metric"::VARCHAR AS token_type,
    m.value:"key":"unit"::VARCHAR AS token_unit
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY,
    LATERAL FLATTEN(input => METRICS) m
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3, 6, 7
ORDER BY usage_date DESC, total_credits DESC;

-- 1d. Top 20 queries mas costosas (ultimos 7 dias)
SELECT
    h.QUERY_ID,
    u.NAME AS user_name,
    h.FUNCTION_NAME,
    h.MODEL_NAME,
    h.CREDITS,
    h.START_TIME,
    h.QUERY_TAG
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY h
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON h.USER_ID = u.USER_ID
WHERE h.START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY h.CREDITS DESC
LIMIT 20;

-- 1e. Tendencia por hora - detectar picos de consumo
SELECT
    DATE_TRUNC('hour', START_TIME) AS usage_hour,
    SUM(CREDITS) AS total_credits,
    COUNT(DISTINCT QUERY_ID) AS query_count,
    COUNT(DISTINCT USER_ID) AS active_users
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- =============================================================================
-- SECCION 2: MONITOREO DE CORTEX CODE (Snowsight y CLI)
-- Vistas dedicadas para rastrear consumo de Cortex Code separado de AI Functions.
-- =============================================================================

-- 2a. Creditos totales de Cortex Code Snowsight por usuario (ultimos 30 dias)
SELECT
    h.USER_ID,
    u.NAME AS user_name,
    SUM(h.TOKEN_CREDITS) AS total_credits,
    SUM(h.TOKENS) AS total_tokens,
    COUNT(*) AS request_count
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY h
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON h.USER_ID = u.USER_ID
WHERE h.USAGE_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY total_credits DESC;

-- 2b. Creditos totales de Cortex Code CLI por usuario (ultimos 30 dias)
SELECT
    h.USER_ID,
    u.NAME AS user_name,
    SUM(h.TOKEN_CREDITS) AS total_credits,
    SUM(h.TOKENS) AS total_tokens,
    COUNT(*) AS request_count
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY h
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON h.USER_ID = u.USER_ID
WHERE h.USAGE_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY total_credits DESC;

-- 2c. Desglose granular por modelo de Cortex Code Snowsight
SELECT
    h.USER_ID,
    u.NAME AS user_name,
    h.USAGE_TIME,
    h.TOKEN_CREDITS,
    h.TOKENS,
    h.TOKENS_GRANULAR,
    h.CREDITS_GRANULAR
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY h
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON h.USER_ID = u.USER_ID
WHERE h.USAGE_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY h.TOKEN_CREDITS DESC
LIMIT 50;

-- 2d. Tendencia diaria combinada Cortex Code (Snowsight + CLI)
SELECT
    DATE_TRUNC('day', usage_time) AS usage_date,
    source,
    SUM(token_credits) AS total_credits,
    SUM(tokens) AS total_tokens,
    COUNT(*) AS request_count
FROM (
    SELECT USAGE_TIME, TOKEN_CREDITS, TOKENS, 'SNOWSIGHT' AS source
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
    WHERE USAGE_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    UNION ALL
    SELECT USAGE_TIME, TOKEN_CREDITS, TOKENS, 'CLI' AS source
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE USAGE_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- =============================================================================
-- SECCION 3: VISION GENERAL DE CREDITOS AI_SERVICES (METERING_DAILY_HISTORY)
-- Consumo agregado a nivel de cuenta de todos los servicios AI.
-- Incluye AI Functions, Cortex Analyst, etc.
-- =============================================================================

-- 3a. Creditos diarios de AI_SERVICES (ultimos 30 dias)
SELECT
    USAGE_DATE,
    CREDITS_USED_COMPUTE,
    CREDITS_USED_CLOUD_SERVICES,
    CREDITS_USED,
    CREDITS_BILLED
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE SERVICE_TYPE = 'AI_SERVICES'
    AND USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE DESC;

-- 3b. Comparativa de costos por tipo de servicio (ultimos 30 dias)
SELECT
    SERVICE_TYPE,
    SUM(CREDITS_BILLED) AS total_credits_billed,
    AVG(CREDITS_BILLED) AS avg_daily_credits,
    MAX(CREDITS_BILLED) AS max_daily_credits,
    COUNT(*) AS days_with_usage
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY SERVICE_TYPE
ORDER BY total_credits_billed DESC;


-- =============================================================================
-- SECCION 4: ALERTA DE GASTO MENSUAL A NIVEL DE CUENTA
-- Monitoreo automatizado que envia email cuando se excede un umbral.
-- =============================================================================

-- 4a. Crear integracion de notificaciones por email
CREATE OR REPLACE NOTIFICATION INTEGRATION ai_cost_alerts
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('admin@company.com', 'finops@company.com');

-- 4b. Tabla de estado de alertas (evita duplicados por mes)
CREATE TABLE IF NOT EXISTS AI_FUNCTIONS_ALERT_STATE (
    ALERT_NAME VARCHAR NOT NULL,
    ALERT_MONTH DATE NOT NULL,
    SENT_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREDITS_AT_ALERT NUMBER(38,6),
    PRIMARY KEY (ALERT_NAME, ALERT_MONTH)
);

-- 4c. Stored procedure para enviar la alerta
CREATE OR REPLACE PROCEDURE SEND_MONTHLY_SPEND_ALERT(P_THRESHOLD FLOAT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var check_sent = snowflake.execute({
        sqlText: `SELECT COUNT(*) AS cnt FROM AI_FUNCTIONS_ALERT_STATE
                WHERE ALERT_NAME = 'monthly_spend'
                AND ALERT_MONTH = DATE_TRUNC('month', CURRENT_DATE())`
    });
    check_sent.next();
    var already_sent = check_sent.getColumnValue(1);

    if (already_sent > 0) {
        return 'Alert already sent for this month';
    }

    var spend_result = snowflake.execute({
        sqlText: `SELECT COALESCE(SUM(CREDITS), 0) AS total
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
                WHERE START_TIME >= DATE_TRUNC('month', CURRENT_TIMESTAMP())`
    });
    spend_result.next();
    var v_credits = spend_result.getColumnValue(1);

    if (v_credits <= P_THRESHOLD) {
        return 'Threshold not exceeded. Current: ' + v_credits + ' / ' + P_THRESHOLD;
    }

    snowflake.execute({
        sqlText: `INSERT INTO AI_FUNCTIONS_ALERT_STATE (ALERT_NAME, ALERT_MONTH, CREDITS_AT_ALERT)
                VALUES ('monthly_spend', DATE_TRUNC('month', CURRENT_DATE()), ?)`,
        binds: [v_credits]
    });

    snowflake.execute({
        sqlText: `CALL SYSTEM$SEND_EMAIL(
            'ai_cost_alerts',
            'admin@company.com',
            'AI Functions Monthly Spend Alert',
            'Monthly AI Function credit consumption has exceeded the threshold.\\n\\n' ||
            'Current spend: ' || ${v_credits}::VARCHAR || ' credits\\n' ||
            'Threshold: ' || ${P_THRESHOLD}::VARCHAR || ' credits\\n\\n' ||
            'Please review usage accordingly.'
        )`
    });

    return 'Alert sent. Credits: ' + v_credits;
$$;

-- 4d. Crear y activar la alerta (cada hora, umbral 1000 creditos)
CREATE OR REPLACE ALERT ai_functions_monthly_spend_alert
    WAREHOUSE = VW_GENAI
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    IF (EXISTS (
        SELECT 1
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
        WHERE START_TIME >= DATE_TRUNC('month', CURRENT_TIMESTAMP())
        HAVING SUM(CREDITS) > 1000
    ))
    THEN
        CALL SEND_MONTHLY_SPEND_ALERT(1000);

ALTER ALERT ai_functions_monthly_spend_alert RESUME;

-- 4e. Resetear alerta para re-enviar en el mes actual
DELETE FROM AI_FUNCTIONS_ALERT_STATE
WHERE ALERT_NAME = 'monthly_spend'
AND ALERT_MONTH = DATE_TRUNC('month', CURRENT_DATE());

-- 4f. Verificacion de alertas
SHOW ALERTS LIKE 'ai_functions_monthly_spend_alert';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    ALERT_NAME => 'ai_functions_monthly_spend_alert'
))
ORDER BY SCHEDULED_TIME DESC;

SELECT * FROM AI_FUNCTIONS_ALERT_STATE ORDER BY ALERT_MONTH DESC;


-- =============================================================================
-- SECCION 5: LIMITES DE GASTO MENSUAL POR USUARIO (RBAC)
-- Revoca acceso publico a Cortex AI, crea rol dedicado con limites por usuario.
-- =============================================================================

-- 5a. Revocar acceso publico a funciones Cortex AI
REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE PUBLIC;

-- 5b. Crear rol dedicado para funciones AI
CREATE ROLE IF NOT EXISTS AI_FUNCTIONS_USER_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_FUNCTIONS_USER_ROLE;
GRANT USAGE ON WAREHOUSE VW_GENAI TO ROLE AI_FUNCTIONS_USER_ROLE;

-- 5c. Tabla de control de acceso
CREATE TABLE IF NOT EXISTS AI_FUNCTIONS_ACCESS_CONTROL (
    USER_NAME VARCHAR NOT NULL,
    USER_ID NUMBER,
    GRANTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    MONTHLY_CREDIT_LIMIT NUMBER(38,6) DEFAULT 100,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    REVOKED_AT TIMESTAMP_LTZ,
    REVOCATION_REASON VARCHAR,
    PRIMARY KEY (USER_NAME)
);

-- 5d. Procedure para otorgar acceso con limite mensual
CREATE OR REPLACE PROCEDURE GRANT_AI_FUNCTIONS_ACCESS(
    P_USER_NAME VARCHAR,
    P_MONTHLY_LIMIT NUMBER(38,6) DEFAULT 100
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_user_id NUMBER;
BEGIN
    SELECT USER_ID INTO :v_user_id
    FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
    WHERE NAME = :P_USER_NAME
    LIMIT 1;

    EXECUTE IMMEDIATE 'GRANT ROLE AI_FUNCTIONS_USER_ROLE TO USER ' || P_USER_NAME;

    MERGE INTO AI_FUNCTIONS_ACCESS_CONTROL tgt
    USING (SELECT :P_USER_NAME AS USER_NAME) src
    ON tgt.USER_NAME = src.USER_NAME
    WHEN MATCHED THEN
        UPDATE SET
            USER_ID = :v_user_id,
            IS_ACTIVE = TRUE,
            MONTHLY_CREDIT_LIMIT = :P_MONTHLY_LIMIT,
            GRANTED_AT = CURRENT_TIMESTAMP(),
            REVOKED_AT = NULL,
            REVOCATION_REASON = NULL
    WHEN NOT MATCHED THEN
        INSERT (USER_NAME, USER_ID, MONTHLY_CREDIT_LIMIT, IS_ACTIVE)
        VALUES (:P_USER_NAME, :v_user_id, :P_MONTHLY_LIMIT, TRUE);

    RETURN 'Access granted to ' || P_USER_NAME || ' with monthly limit of ' || P_MONTHLY_LIMIT || ' credits';
END;
$$;

-- 5e. Otorgar acceso a usuarios
CALL GRANT_AI_FUNCTIONS_ACCESS('ALICE', 1000);
CALL GRANT_AI_FUNCTIONS_ACCESS('BOB', 2000);

-- 5f. Procedure y task para renovar acceso el 1ro de cada mes
CREATE OR REPLACE PROCEDURE GRANT_ALL_ENTITLED_USERS()
RETURNS TABLE (USER_NAME VARCHAR, CREDIT_LIMIT NUMBER, ACTION VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        SELECT
            USER_NAME,
            MONTHLY_CREDIT_LIMIT AS CREDIT_LIMIT,
            'GRANTED' AS ACTION
        FROM AI_FUNCTIONS_ACCESS_CONTROL
    );

    FOR rec IN result DO
        CALL GRANT_AI_FUNCTIONS_ACCESS(rec.USER_NAME, rec.CREDIT_LIMIT);
    END FOR;

    RETURN TABLE(result);
END;
$$;

CREATE OR REPLACE TASK MONTHLY_AI_FUNCTIONS_ACCESS_REFRESH
    WAREHOUSE = VW_GENAI
    SCHEDULE = 'USING CRON 0 0 1 * * UTC'
AS
    CALL GRANT_ALL_ENTITLED_USERS();

ALTER TASK MONTHLY_AI_FUNCTIONS_ACCESS_REFRESH RESUME;

CALL GRANT_ALL_ENTITLED_USERS();

SHOW TASKS LIKE 'MONTHLY_AI_FUNCTIONS_ACCESS_REFRESH';


-- =============================================================================
-- SECCION 6: DETECCION Y CANCELACION DE QUERIES DESBOCADOS (RUNAWAY)
-- Detecta queries AI con consumo excesivo aun en ejecucion y los cancela.
-- =============================================================================

-- 6a. Procedure para monitorear y cancelar queries desbocados
CREATE OR REPLACE PROCEDURE MONITOR_AND_CANCEL_RUNAWAY_QUERIES(
    P_CREDIT_THRESHOLD NUMBER DEFAULT 50
)
RETURNS TABLE (
    QUERY_ID VARCHAR,
    USER_NAME VARCHAR,
    FUNCTION_NAME VARCHAR,
    MODEL_NAME VARCHAR,
    CREDITS NUMBER,
    START_TIME TIMESTAMP_LTZ,
    ACTION VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    result := (
        SELECT
            h.QUERY_ID,
            u.NAME AS USER_NAME,
            h.FUNCTION_NAME,
            h.MODEL_NAME,
            h.CREDITS,
            h.START_TIME,
            h.ROLE_NAMES,
            h.QUERY_TAG,
            h.WAREHOUSE_ID,
            'CANCELLED' AS ACTION
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY h
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u
            ON h.USER_ID = u.USER_ID
        WHERE h.START_TIME >= DATEADD('hour', -48, CURRENT_TIMESTAMP())
        AND h.CREDITS > :P_CREDIT_THRESHOLD
        AND h.IS_COMPLETED = FALSE
    );

    FOR rec IN result DO
        BEGIN
            EXECUTE IMMEDIATE 'SELECT SYSTEM$CANCEL_QUERY(''' || rec.QUERY_ID || ''')';
        EXCEPTION
            WHEN OTHER THEN
                NULL;
        END;

        CALL SYSTEM$SEND_EMAIL(
            'ai_cost_alerts',
            'admin@company.com',
            'Runaway AI Query Cancelled - ' || rec.QUERY_ID,
            'A runaway AI Function query has been cancelled due to excessive cost.\n\n' ||
            'Query Details:\n' ||
            '- Query ID: ' || rec.QUERY_ID || '\n' ||
            '- User: ' || COALESCE(rec.USER_NAME, 'Unknown') || '\n' ||
            '- Function: ' || rec.FUNCTION_NAME || '\n' ||
            '- Model: ' || rec.MODEL_NAME || '\n' ||
            '- Credits Used: ' || rec.CREDITS::VARCHAR || '\n' ||
            '- Threshold: ' || :P_CREDIT_THRESHOLD::VARCHAR || '\n' ||
            '- Start Time: ' || rec.START_TIME::VARCHAR || '\n' ||
            '- Roles: ' || COALESCE(rec.ROLE_NAMES::VARCHAR, 'N/A') || '\n' ||
            '- Query Tag: ' || COALESCE(rec.QUERY_TAG, 'N/A') || '\n' ||
            '- Warehouse ID: ' || COALESCE(rec.WAREHOUSE_ID::VARCHAR, 'N/A') || '\n\n' ||
            'Please investigate this query and take appropriate action.'
        );
    END FOR;

    RETURN TABLE(result);
END;
$$;

CREATE OR REPLACE TASK MONITOR_RUNAWAY_AI_QUERIES
    WAREHOUSE = VW_GENAI
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    CALL MONITOR_AND_CANCEL_RUNAWAY_QUERIES(50);

ALTER TASK MONITOR_RUNAWAY_AI_QUERIES RESUME;

SHOW TASKS LIKE 'MONITOR_RUNAWAY_AI_QUERIES';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    TASK_NAME => 'MONITOR_RUNAWAY_AI_QUERIES'
))
ORDER BY SCHEDULED_TIME DESC;

-- 6b. Exencion para queries de larga duracion
CREATE ROLE AI_FUNCTIONS_USER_LONG_RUNNING_ROLE;
GRANT ROLE AI_FUNCTIONS_USER_ROLE TO ROLE AI_FUNCTIONS_USER_LONG_RUNNING_ROLE;
GRANT ROLE AI_FUNCTIONS_USER_LONG_RUNNING_ROLE TO USER LONG_RUNNING_USER;

-- Agregar al WHERE del procedure de cancelacion para excluir este rol:
-- AND NOT ARRAY_CONTAINS(h.ROLE_NAMES, 'AI_FUNCTIONS_USER_LONG_RUNNING_ROLE')

-- Uso:
-- USE ROLE AI_FUNCTIONS_USER_LONG_RUNNING_ROLE;
-- (ejecutar el query de larga duracion aqui)


-- =============================================================================
-- SECCION 7: MEJORES PRACTICAS
-- =============================================================================
-- 1. Empezar con monitoreo antes de implementar controles automaticos.
-- 2. Establecer limites conservadores e ir ajustando segun uso real.
-- 3. Usar QUERY_TAG para atribuir costos por proyecto o equipo:
--      ALTER SESSION SET QUERY_TAG = 'proyecto_x';
-- 4. Revisar periodicamente la tabla AI_FUNCTIONS_ACCESS_CONTROL.
-- 5. Probar alertas con umbral 0 antes de usar valores reales.
-- 6. Considerar latencia: CORTEX_AI_FUNCTIONS_USAGE_HISTORY tiene hasta 60 min.
-- 7. Las vistas CORTEX_CODE_*_USAGE_HISTORY son separadas de AI Functions.
-- 8. METERING_DAILY_HISTORY con SERVICE_TYPE='AI_SERVICES' da la vision agregada.
