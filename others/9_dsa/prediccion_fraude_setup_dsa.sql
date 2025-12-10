-- =====================================================
-- SISTEMA DE DETECCIÓN DE FRAUDE AVANZADO - PASO 1
-- CREAR ESTRUCTURA CON 100 CARACTERÍSTICAS PROFESIONALES
-- =====================================================
-- Ejecutar statement por statement en Snowflake
-- Rol requerido: ACCOUNTADMIN
-- =====================================================

-- Step 1: Configurar contexto básico
USE ROLE ACCOUNTADMIN;

-- Step 2: Crear base de datos
CREATE OR REPLACE DATABASE DETECCION_FRAUDE
COMMENT = 'Base de datos profesional para detección de fraude con 100 características';

-- Step 3: Usar la base de datos
USE DATABASE DETECCION_FRAUDE;

-- Step 4: Crear esquema
CREATE OR REPLACE SCHEMA FRAUD_DETECTION_SCHEMA
COMMENT = 'Esquema principal para sistema avanzado de detección de fraude';

-- Step 5: Usar el esquema
USE SCHEMA FRAUD_DETECTION_SCHEMA;

-- Step 6: Crear o usar warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse para procesamiento avanzado de detección de fraude';

-- Step 7: Usar el warehouse
USE WAREHOUSE COMPUTE_WH;

-- Step 8: Crear tabla principal de transacciones con 100 características
CREATE OR REPLACE TABLE TRANSACCIONES_AVANZADAS (
    -- Identificador único
    transaction_id STRING NOT NULL,
    
    -- === CARACTERÍSTICAS TRANSACCIONALES (1-20) ===
    monto_transaccion NUMBER(15,2) NOT NULL,                    -- 1. Monto de la transacción
    frecuencia_diaria NUMBER(6,0),                             -- 2. Frecuencia de transacciones diarias
    transacciones_por_hora NUMBER(6,0),                        -- 3. Número de transacciones por hora
    pais_origen VARCHAR(3),                                     -- 4. País de origen
    pais_destino VARCHAR(3),                                    -- 5. País de destino
    diferencia_horaria_horas NUMBER(3,0),                      -- 6. Diferencia horaria origen–destino
    tipo_transaccion VARCHAR(20),                              -- 7. Tipo de transacción
    canal_transaccion VARCHAR(20),                             -- 8. Canal de transacción
    categoria_comercio_mcc VARCHAR(10),                        -- 9. Categoría del comercio (MCC)
    tipo_comercio VARCHAR(50),                                 -- 10. Tipo de comercio
    hora_dia NUMBER(2,0),                                      -- 11. Hora del día (0-23)
    dia_semana NUMBER(1,0),                                    -- 12. Día de la semana (0-6)
    dia_mes NUMBER(2,0),                                       -- 13. Día del mes (1-31)
    es_dia_festivo BOOLEAN,                                    -- 14. Día festivo o no
    patron_horario_habitual BOOLEAN,                          -- 15. Patrón de horarios habituales
    velocidad_transacciones_min NUMBER(10,2),                 -- 16. Velocidad entre transacciones (min)
    transferencias_cuentas_nuevas NUMBER(6,0),                -- 17. Transferencias a cuentas nuevas
    beneficiarios_frecuentes NUMBER(6,0),                     -- 18. Número de beneficiarios frecuentes
    transacciones_internacionales NUMBER(6,0),                -- 19. Número de transacciones internacionales
    variacion_gasto_mes_anterior NUMBER(10,2),                -- 20. Variación del gasto (%)
    
    -- === DISPOSITIVO Y CANAL (21-40) ===
    direccion_ip VARCHAR(45),                                 -- 21. Dirección IP
    isp_proveedor VARCHAR(100),                               -- 22. ISP asociado a la IP
    usa_vpn_proxy BOOLEAN,                                    -- 23. Uso de VPN o proxy
    tipo_dispositivo VARCHAR(30),                             -- 24. Tipo de dispositivo
    sistema_operativo VARCHAR(50),                            -- 25. Sistema operativo
    version_sistema_operativo VARCHAR(20),                    -- 26. Versión del SO
    navegador VARCHAR(50),                                    -- 27. Navegador utilizado
    version_navegador VARCHAR(20),                            -- 28. Versión del navegador
    huella_digital_dispositivo VARCHAR(128),                  -- 29. Device fingerprint
    dispositivos_asociados_cuenta NUMBER(3,0),               -- 30. Dispositivos por cuenta
    cuentas_asociadas_dispositivo NUMBER(3,0),               -- 31. Cuentas por dispositivo
    modelo_telefono VARCHAR(50),                              -- 32. Modelo de teléfono
    sim_duplicada_cambiada BOOLEAN,                           -- 33. SIM duplicada/cambiada
    ubicacion_gps_lat FLOAT,                                  -- 34. Ubicación GPS (latitud)
    ubicacion_gps_lon FLOAT,                                  -- 34. Ubicación GPS (longitud)
    distancia_domicilio_km FLOAT,                             -- 35. Distancia domicilio (km)
    velocidad_cambio_ubicacion_kmh FLOAT,                     -- 36. Velocidad cambio ubicación
    usa_emulador_virtual BOOLEAN,                             -- 37. Uso de emuladores
    idioma_dispositivo VARCHAR(5),                            -- 38. Lenguaje del dispositivo
    zona_horaria_dispositivo VARCHAR(50),                     -- 39. Zona horaria dispositivo
    dispositivo_confiable BOOLEAN,                            -- 40. Dispositivo en lista confiable
    
    -- === CLIENTE Y CUENTA (41-50) ===
    antiguedad_cuenta_dias NUMBER(6,0),                       -- 41. Antigüedad de la cuenta
    antiguedad_tarjeta_dias NUMBER(6,0),                      -- 42. Antigüedad de la tarjeta
    antiguedad_relacion_banco_dias NUMBER(6,0),               -- 43. Antigüedad relación banco
    productos_financieros_activos NUMBER(3,0),                -- 44. Productos financieros activos
    cuentas_vinculadas NUMBER(3,0),                           -- 45. Cuentas vinculadas
    tarjetas_activas NUMBER(3,0),                             -- 46. Tarjetas activas
    tarjetas_bloqueadas_previas NUMBER(3,0),                  -- 47. Tarjetas bloqueadas antes
    cambios_datos_personales_30d BOOLEAN,                     -- 48. Cambios datos personales
    cambios_direccion_30d BOOLEAN,                            -- 49. Cambios dirección
    cambios_email_30d BOOLEAN,                                -- 50. Cambios email
    
    -- === CRÉDITO Y LÍMITES (51-60) ===
    limite_credito_disponible NUMBER(15,2),                   -- 51. Límite crédito disponible
    porcentaje_uso_limite FLOAT,                              -- 52. % uso límite crédito
    promedio_gasto_mensual NUMBER(15,2),                      -- 53. Promedio gasto mensual
    desviacion_estandar_gasto NUMBER(15,2),                   -- 54. Desviación estándar gasto
    maximo_historico_gasto NUMBER(15,2),                      -- 55. Máximo histórico gasto
    relacion_ingresos_gasto FLOAT,                            -- 56. Relación ingresos/gasto
    variacion_gasto_semana FLOAT,                             -- 57. Variación gasto semana (%)
    frecuencia_limite_credito NUMBER(3,0),                    -- 58. Frecuencia cerca límite
    pagos_minimos_frecuentes BOOLEAN,                         -- 59. Pagos mínimos frecuentes
    incumplimientos_pago_previos NUMBER(3,0),                 -- 60. Incumplimientos previos
    
    -- === COMPORTAMIENTO (61-70) ===
    intentos_fallidos_previos NUMBER(6,0),                    -- 61. Intentos fallidos previos
    intentos_login_24h NUMBER(6,0),                           -- 62. Intentos login 24h
    tiempo_medio_sesion_min NUMBER(10,2),                     -- 63. Tiempo medio sesión
    velocidad_digitacion_cpm NUMBER(6,0),                     -- 64. Velocidad digitación
    uso_autocompletado_copypaste BOOLEAN,                     -- 65. Uso autocompletado/copy-paste
    cambio_frecuente_passwords BOOLEAN,                       -- 66. Cambio frecuente passwords
    usa_passwords_debiles BOOLEAN,                            -- 67. Uso passwords débiles
    usa_emails_temporales BOOLEAN,                            -- 68. Uso emails temporales
    historial_contracargos NUMBER(6,0),                       -- 69. Historial contracargos
    historial_reclamos_fraude NUMBER(6,0),                    -- 70. Historial reclamos fraude
    
    -- === CONTEXTO Y ENTORNO (71-80) ===
    tipo_moneda VARCHAR(3),                                   -- 71. Tipo de moneda
    requiere_conversion_divisas BOOLEAN,                      -- 72. Conversión de divisas
    pais_emision_tarjeta VARCHAR(3),                          -- 73. País emisión tarjeta
    pais_residencia_declarado VARCHAR(3),                     -- 74. País residencia declarado
    diferencia_residencia_compra_km FLOAT,                    -- 75. Diferencia residencia-compra
    comercio_alto_riesgo BOOLEAN,                             -- 76. Comercio alto riesgo
    comercio_recien_registrado BOOLEAN,                       -- 77. Comercio recién registrado
    comercio_historial_fraude BOOLEAN,                        -- 78. Comercio con historial fraude
    cajero_no_habitual BOOLEAN,                               -- 79. Cajero no habitual
    retiro_efectivo_inusual BOOLEAN,                          -- 80. Retiro efectivo inusual
    
    -- === REDES Y PATRONES (81-90) ===
    relacion_cuentas_fraudulentas BOOLEAN,                    -- 81. Relación cuentas fraudulentas
    transferencias_cuentas_nuevas_30d NUMBER(6,0),            -- 82. Transferencias cuentas nuevas
    transferencias_cuentas_cerradas NUMBER(6,0),              -- 83. Transferencias cuentas cerradas
    conexion_emails_sospechosos BOOLEAN,                      -- 84. Conexión emails sospechosos
    conexion_telefonos_sospechosos BOOLEAN,                   -- 85. Conexión teléfonos sospechosos
    cuentas_mismo_dispositivo NUMBER(6,0),                    -- 86. Cuentas mismo dispositivo
    cuentas_misma_ip NUMBER(6,0),                             -- 87. Cuentas misma IP
    conexion_beneficiarios_bloqueados BOOLEAN,                -- 88. Conexión beneficiarios bloqueados
    transferencias_multiples_paises_1h NUMBER(3,0),           -- 89. Transferencias múltiples países 1h
    patron_smurfing_score FLOAT,                              -- 90. Score patrón smurfing
    
    -- === SEÑALES AVANZADAS (91-100) ===
    variacion_monto_autorizado_liquidado NUMBER(15,2),        -- 91. Variación autorizado-liquidado
    intentos_multiples_comercios_min NUMBER(6,0),             -- 92. Intentos múltiples comercios
    cambio_geolocalizacion_imposible BOOLEAN,                 -- 93. Cambio geolocalización imposible
    diferencia_timezone_dispositivo_ip BOOLEAN,               -- 94. Diferencia timezone dispositivo-IP
    usa_bin_alto_riesgo BOOLEAN,                              -- 95. BIN alto riesgo
    identificacion_dark_web BOOLEAN,                          -- 96. Identificación dark web
    discrepancia_idioma_ubicacion BOOLEAN,                    -- 97. Discrepancia idioma-ubicación
    frecuencia_cambio_sim_30d NUMBER(3,0),                    -- 98. Frecuencia cambio SIM
    dispositivos_activos_paralelo NUMBER(3,0),                -- 99. Dispositivos activos paralelo
    transacciones_redirecciones_sospechosas BOOLEAN,          -- 100. Redireccionamientos sospechosos
    
    -- === LABEL OBJETIVO ===
    is_fraud BOOLEAN NOT NULL,
    
    -- === METADATOS ===
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (transaction_id, created_at)
COMMENT = 'Tabla principal con 100 características avanzadas para detección de fraude';

-- Step 9: Crear tabla de registro de modelos
CREATE OR REPLACE TABLE MODELO_REGISTRO (
    model_name VARCHAR(100) NOT NULL,
    version VARCHAR(50) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    auc_roc FLOAT,
    auc_pr FLOAT,
    precision_score FLOAT,
    recall FLOAT,
    f1_score FLOAT,
    accuracy FLOAT,
    threshold FLOAT DEFAULT 0.5,
    is_active BOOLEAN DEFAULT FALSE,
    feature_list VARIANT,
    feature_importance VARIANT,
    model_params VARIANT,
    performance_metrics VARIANT
)
CLUSTER BY (model_name, version)
COMMENT = 'Registro avanzado de modelos con métricas detalladas';

-- Step 10: Crear tabla de logs de predicciones
CREATE OR REPLACE TABLE PREDICCION_LOGS (
    prediction_id STRING DEFAULT UUID_STRING(),
    transaction_data VARIANT,
    prediction_result VARIANT,
    model_version VARCHAR(50),
    processing_time_ms NUMBER,
    risk_score FLOAT,
    risk_factors VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    user_session VARCHAR(100),
    client_info VARIANT
)
CLUSTER BY (created_at, model_version)
COMMENT = 'Logs detallados de predicciones con información de riesgo';

-- Step 11: Crear tabla de análisis de características
CREATE OR REPLACE TABLE FEATURE_ANALYSIS (
    analysis_id STRING DEFAULT UUID_STRING(),
    feature_name VARCHAR(100),
    feature_category VARCHAR(50),
    importance_score FLOAT,
    correlation_with_fraud FLOAT,
    data_quality_score FLOAT,
    missing_values_pct FLOAT,
    unique_values_count NUMBER,
    analysis_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    model_version VARCHAR(50)
)
CLUSTER BY (analysis_date, model_version)
COMMENT = 'Análisis de importancia y calidad de características';

-- Step 12: Crear tabla de logs de aplicación
CREATE OR REPLACE TABLE APP_LOGS (
    log_id NUMBER AUTOINCREMENT,
    ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    log_level VARCHAR(10), -- DEBUG, INFO, WARN, ERROR
    action STRING,
    status STRING,
    detail STRING,
    user_session VARCHAR(100),
    execution_time_ms NUMBER,
    error_code VARCHAR(20),
    error_message STRING
)
CLUSTER BY (ts, log_level)
COMMENT = 'Logs avanzados de aplicación con niveles de severidad';

-- Step 13: Crear vista de características por categoría
CREATE OR REPLACE VIEW V_FEATURES_POR_CATEGORIA AS
SELECT 
    'TRANSACCIONALES' AS categoria,
    'Características relacionadas con la transacción' AS descripcion,
    ARRAY_CONSTRUCT(
        'monto_transaccion', 'frecuencia_diaria', 'transacciones_por_hora',
        'pais_origen', 'pais_destino', 'diferencia_horaria_horas',
        'tipo_transaccion', 'canal_transaccion', 'categoria_comercio_mcc',
        'tipo_comercio', 'hora_dia', 'dia_semana', 'dia_mes',
        'es_dia_festivo', 'patron_horario_habitual', 'velocidad_transacciones_min',
        'transferencias_cuentas_nuevas', 'beneficiarios_frecuentes',
        'transacciones_internacionales', 'variacion_gasto_mes_anterior'
    ) AS features

UNION ALL

SELECT 
    'DISPOSITIVO_Y_CANAL' AS categoria,
    'Características del dispositivo y canal de acceso' AS descripcion,
    ARRAY_CONSTRUCT(
        'direccion_ip', 'isp_proveedor', 'usa_vpn_proxy', 'tipo_dispositivo',
        'sistema_operativo', 'version_sistema_operativo', 'navegador',
        'version_navegador', 'huella_digital_dispositivo', 'dispositivos_asociados_cuenta',
        'cuentas_asociadas_dispositivo', 'modelo_telefono', 'sim_duplicada_cambiada',
        'ubicacion_gps_lat', 'ubicacion_gps_lon', 'distancia_domicilio_km',
        'velocidad_cambio_ubicacion_kmh', 'usa_emulador_virtual', 'idioma_dispositivo',
        'zona_horaria_dispositivo', 'dispositivo_confiable'
    ) AS features

UNION ALL

SELECT 
    'CLIENTE_Y_CUENTA' AS categoria,
    'Características del cliente y su cuenta' AS descripcion,
    ARRAY_CONSTRUCT(
        'antiguedad_cuenta_dias', 'antiguedad_tarjeta_dias', 'antiguedad_relacion_banco_dias',
        'productos_financieros_activos', 'cuentas_vinculadas', 'tarjetas_activas',
        'tarjetas_bloqueadas_previas', 'cambios_datos_personales_30d',
        'cambios_direccion_30d', 'cambios_email_30d'
    ) AS features

UNION ALL

SELECT 
    'CREDITO_Y_LIMITES' AS categoria,
    'Características de crédito y límites financieros' AS descripcion,
    ARRAY_CONSTRUCT(
        'limite_credito_disponible', 'porcentaje_uso_limite', 'promedio_gasto_mensual',
        'desviacion_estandar_gasto', 'maximo_historico_gasto', 'relacion_ingresos_gasto',
        'variacion_gasto_semana', 'frecuencia_limite_credito', 'pagos_minimos_frecuentes',
        'incumplimientos_pago_previos'
    ) AS features

UNION ALL

SELECT 
    'COMPORTAMIENTO' AS categoria,
    'Patrones de comportamiento del usuario' AS descripcion,
    ARRAY_CONSTRUCT(
        'intentos_fallidos_previos', 'intentos_login_24h', 'tiempo_medio_sesion_min',
        'velocidad_digitacion_cpm', 'uso_autocompletado_copypaste', 'cambio_frecuente_passwords',
        'usa_passwords_debiles', 'usa_emails_temporales', 'historial_contracargos',
        'historial_reclamos_fraude'
    ) AS features

UNION ALL

SELECT 
    'CONTEXTO_Y_ENTORNO' AS categoria,
    'Contexto y entorno de la transacción' AS descripcion,
    ARRAY_CONSTRUCT(
        'tipo_moneda', 'requiere_conversion_divisas', 'pais_emision_tarjeta',
        'pais_residencia_declarado', 'diferencia_residencia_compra_km',
        'comercio_alto_riesgo', 'comercio_recien_registrado', 'comercio_historial_fraude',
        'cajero_no_habitual', 'retiro_efectivo_inusual'
    ) AS features

UNION ALL

SELECT 
    'REDES_Y_PATRONES' AS categoria,
    'Patrones de redes y conexiones sospechosas' AS descripcion,
    ARRAY_CONSTRUCT(
        'relacion_cuentas_fraudulentas', 'transferencias_cuentas_nuevas_30d',
        'transferencias_cuentas_cerradas', 'conexion_emails_sospechosos',
        'conexion_telefonos_sospechosos', 'cuentas_mismo_dispositivo',
        'cuentas_misma_ip', 'conexion_beneficiarios_bloqueados',
        'transferencias_multiples_paises_1h', 'patron_smurfing_score'
    ) AS features

UNION ALL

SELECT 
    'SEÑALES_AVANZADAS' AS categoria,
    'Señales avanzadas y anomalías complejas' AS descripcion,
    ARRAY_CONSTRUCT(
        'variacion_monto_autorizado_liquidado', 'intentos_multiples_comercios_min',
        'cambio_geolocalizacion_imposible', 'diferencia_timezone_dispositivo_ip',
        'usa_bin_alto_riesgo', 'identificacion_dark_web', 'discrepancia_idioma_ubicacion',
        'frecuencia_cambio_sim_30d', 'dispositivos_activos_paralelo',
        'transacciones_redirecciones_sospechosas'
    ) AS features;

-- Step 14: Verificar que las tablas se crearon correctamente
SELECT 'ESTRUCTURA AVANZADA CREADA EXITOSAMENTE' as status;

-- Step 15: Mostrar información del sistema
SELECT 
    'Sistema Avanzado de Detección de Fraude' AS sistema,
    '100 características profesionales' AS caracteristicas,
    '8 categorías de features' AS categorias,
    'Listo para datos sintéticos' AS estado;

-- Step 16: Mostrar tablas creadas
SHOW TABLES;

-- Step 17: Mostrar vista de categorías
SELECT * FROM V_FEATURES_POR_CATEGORIA ORDER BY categoria;



-- POBLAR CON DATOS SINTÉTICOS


-- =====================================================
-- SISTEMA DE DETECCIÓN DE FRAUDE AVANZADO - PASO 2  
-- POBLAR CON DATOS SINTÉTICOS PROFESIONALES (100 CARACTERÍSTICAS)
-- =====================================================
-- Ejecutar DESPUÉS del script 01_crear_estructura_avanzada.sql
-- =====================================================

-- Step 1: Verificar contexto
USE ROLE ACCOUNTADMIN;
USE DATABASE DETECCION_FRAUDE;
USE SCHEMA FRAUD_DETECTION_SCHEMA;
USE WAREHOUSE COMPUTE_WH;

-- Step 2: Limpiar tabla si existe data previa (opcional)
TRUNCATE TABLE TRANSACCIONES_AVANZADAS;

-- Step 3: Insertar 100,000 registros sintéticos con las 100 características
INSERT INTO TRANSACCIONES_AVANZADAS (
    transaction_id,
    -- TRANSACCIONALES (1-20)
    monto_transaccion, frecuencia_diaria, transacciones_por_hora, pais_origen, pais_destino,
    diferencia_horaria_horas, tipo_transaccion, canal_transaccion, categoria_comercio_mcc,
    tipo_comercio, hora_dia, dia_semana, dia_mes, es_dia_festivo, patron_horario_habitual,
    velocidad_transacciones_min, transferencias_cuentas_nuevas, beneficiarios_frecuentes,
    transacciones_internacionales, variacion_gasto_mes_anterior,
    -- DISPOSITIVO Y CANAL (21-40)
    direccion_ip, isp_proveedor, usa_vpn_proxy, tipo_dispositivo, sistema_operativo,
    version_sistema_operativo, navegador, version_navegador, huella_digital_dispositivo,
    dispositivos_asociados_cuenta, cuentas_asociadas_dispositivo, modelo_telefono,
    sim_duplicada_cambiada, ubicacion_gps_lat, ubicacion_gps_lon, distancia_domicilio_km,
    velocidad_cambio_ubicacion_kmh, usa_emulador_virtual, idioma_dispositivo,
    zona_horaria_dispositivo, dispositivo_confiable,
    -- CLIENTE Y CUENTA (41-50)
    antiguedad_cuenta_dias, antiguedad_tarjeta_dias, antiguedad_relacion_banco_dias,
    productos_financieros_activos, cuentas_vinculadas, tarjetas_activas,
    tarjetas_bloqueadas_previas, cambios_datos_personales_30d, cambios_direccion_30d,
    cambios_email_30d,
    -- CREDITO Y LIMITES (51-60)
    limite_credito_disponible, porcentaje_uso_limite, promedio_gasto_mensual,
    desviacion_estandar_gasto, maximo_historico_gasto, relacion_ingresos_gasto,
    variacion_gasto_semana, frecuencia_limite_credito, pagos_minimos_frecuentes,
    incumplimientos_pago_previos,
    -- COMPORTAMIENTO (61-70)
    intentos_fallidos_previos, intentos_login_24h, tiempo_medio_sesion_min,
    velocidad_digitacion_cpm, uso_autocompletado_copypaste, cambio_frecuente_passwords,
    usa_passwords_debiles, usa_emails_temporales, historial_contracargos,
    historial_reclamos_fraude,
    -- CONTEXTO Y ENTORNO (71-80)
    tipo_moneda, requiere_conversion_divisas, pais_emision_tarjeta,
    pais_residencia_declarado, diferencia_residencia_compra_km, comercio_alto_riesgo,
    comercio_recien_registrado, comercio_historial_fraude, cajero_no_habitual,
    retiro_efectivo_inusual,
    -- REDES Y PATRONES (81-90)
    relacion_cuentas_fraudulentas, transferencias_cuentas_nuevas_30d,
    transferencias_cuentas_cerradas, conexion_emails_sospechosos,
    conexion_telefonos_sospechosos, cuentas_mismo_dispositivo, cuentas_misma_ip,
    conexion_beneficiarios_bloqueados, transferencias_multiples_paises_1h,
    patron_smurfing_score,
    -- SEÑALES AVANZADAS (91-100)
    variacion_monto_autorizado_liquidado, intentos_multiples_comercios_min,
    cambio_geolocalizacion_imposible, diferencia_timezone_dispositivo_ip,
    usa_bin_alto_riesgo, identificacion_dark_web, discrepancia_idioma_ubicacion,
    frecuencia_cambio_sim_30d, dispositivos_activos_paralelo,
    transacciones_redirecciones_sospechosas,
    -- LABEL
    is_fraud
)
WITH 
-- Generar datos base con múltiples números aleatorios
base_data AS (
    SELECT 
        'TXN_' || LPAD(SEQ4(), 12, '0') AS transaction_id,
        ABS(HASH(SEQ4())) % 100000 AS rnd1,
        ABS(HASH(SEQ4() * 2)) % 100000 AS rnd2,
        ABS(HASH(SEQ4() * 3)) % 100000 AS rnd3,
        ABS(HASH(SEQ4() * 4)) % 100000 AS rnd4,
        ABS(HASH(SEQ4() * 5)) % 100000 AS rnd5,
        ABS(HASH(SEQ4() * 6)) % 100000 AS rnd6,
        ABS(HASH(SEQ4() * 7)) % 100000 AS rnd7,
        ABS(HASH(SEQ4() * 8)) % 100000 AS rnd8
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
),

-- Generar características transaccionales (1-20)
transaccional_data AS (
    SELECT 
        transaction_id, rnd1, rnd2, rnd3, rnd4, rnd5, rnd6, rnd7, rnd8,
        
        -- 1. Monto transacción (distribución realista)
        CASE 
            WHEN rnd1 % 100 < 60 THEN (rnd1 % 200) + 5.0        -- $5-200 (60%)
            WHEN rnd1 % 100 < 80 THEN (rnd1 % 800) + 200.0      -- $200-1000 (20%)
            WHEN rnd1 % 100 < 95 THEN (rnd1 % 3000) + 1000.0    -- $1000-4000 (15%)
            ELSE (rnd1 % 15000) + 4000.0                         -- $4000-19000 (5%)
        END AS monto_transaccion,
        
        -- 2-3. Frecuencias
        CASE WHEN rnd2 % 100 < 80 THEN rnd2 % 5 ELSE rnd2 % 15 END AS frecuencia_diaria,
        CASE WHEN rnd3 % 100 < 85 THEN rnd3 % 3 ELSE rnd3 % 10 END AS transacciones_por_hora,
        
        -- 4-5. Países (simulando distribución real)
        CASE 
            WHEN rnd1 % 20 < 10 THEN 'USA'      -- 50%
            WHEN rnd1 % 20 < 13 THEN 'CAN'      -- 15%
            WHEN rnd1 % 20 < 15 THEN 'MEX'      -- 10%
            WHEN rnd1 % 20 = 15 THEN 'GBR'      -- 5%
            WHEN rnd1 % 20 = 16 THEN 'FRA'      -- 5%
            WHEN rnd1 % 20 = 17 THEN 'DEU'      -- 5%
            WHEN rnd1 % 20 = 18 THEN 'BRA'      -- 5%
            ELSE 'ESP'                           -- 5%
        END AS pais_origen,
        
        CASE 
            WHEN rnd2 % 20 < 10 THEN 'USA'
            WHEN rnd2 % 20 < 13 THEN 'CAN'
            WHEN rnd2 % 20 < 15 THEN 'MEX'
            ELSE 'COL'
        END AS pais_destino,
        
        -- 6. Diferencia horaria
        (rnd3 % 24) - 12 AS diferencia_horaria_horas,
        
        -- 7-8. Tipos y canales
        CASE 
            WHEN rnd4 % 6 < 2 THEN 'compra'
            WHEN rnd4 % 6 < 4 THEN 'retiro'
            WHEN rnd4 % 6 = 4 THEN 'transferencia'
            ELSE 'pago_servicios'
        END AS tipo_transaccion,
        
        CASE 
            WHEN rnd5 % 8 < 3 THEN 'web'
            WHEN rnd5 % 8 < 5 THEN 'app'
            WHEN rnd5 % 8 < 7 THEN 'pos'
            ELSE 'cajero'
        END AS canal_transaccion,
        
        -- 9-10. Comercio
        LPAD((5411 + (rnd6 % 50))::STRING, 4, '0') AS categoria_comercio_mcc,
        CASE 
            WHEN rnd7 % 12 = 0 THEN 'supermercado'
            WHEN rnd7 % 12 = 1 THEN 'gasolinera'
            WHEN rnd7 % 12 = 2 THEN 'restaurante'
            WHEN rnd7 % 12 = 3 THEN 'farmacia'
            WHEN rnd7 % 12 = 4 THEN 'tienda_ropa'
            WHEN rnd7 % 12 = 5 THEN 'electronica'
            WHEN rnd7 % 12 = 6 THEN 'banco'
            WHEN rnd7 % 12 = 7 THEN 'hospital'
            WHEN rnd7 % 12 = 8 THEN 'hotel'
            WHEN rnd7 % 12 = 9 THEN 'aeropuerto'
            WHEN rnd7 % 12 = 10 THEN 'ecommerce'
            ELSE 'otros'
        END AS tipo_comercio,
        
        -- 11-13. Tiempo
        CASE 
            WHEN rnd8 % 100 < 5 THEN rnd8 % 6       -- 0-5am (5%)
            WHEN rnd8 % 100 < 15 THEN 6 + (rnd8 % 6) -- 6-11am (10%)
            WHEN rnd8 % 100 < 45 THEN 12 + (rnd8 % 6) -- 12-5pm (30%)
            WHEN rnd8 % 100 < 80 THEN 18 + (rnd8 % 4) -- 6-9pm (35%)
            ELSE 22 + (rnd8 % 2)                    -- 10-11pm (20%)
        END AS hora_dia,
        
        rnd1 % 7 AS dia_semana,
        (rnd2 % 28) + 1 AS dia_mes,
        
        -- 14-15. Patrones
        CASE WHEN rnd3 % 100 < 15 THEN TRUE ELSE FALSE END AS es_dia_festivo,
        CASE WHEN rnd4 % 100 < 70 THEN TRUE ELSE FALSE END AS patron_horario_habitual,
        
        -- 16-20. Métricas avanzadas
        CASE WHEN rnd5 % 100 < 80 THEN (rnd5 % 30) + 1 ELSE (rnd5 % 300) + 30 END AS velocidad_transacciones_min,
        CASE WHEN rnd6 % 100 < 95 THEN rnd6 % 3 ELSE rnd6 % 8 END AS transferencias_cuentas_nuevas,
        (rnd7 % 15) + 1 AS beneficiarios_frecuentes,
        CASE WHEN rnd8 % 100 < 90 THEN rnd8 % 3 ELSE rnd8 % 12 END AS transacciones_internacionales,
        ((rnd1 % 200) - 100) / 10.0 AS variacion_gasto_mes_anterior
        
    FROM base_data
),

-- Generar características de dispositivo y canal (21-40)
dispositivo_data AS (
    SELECT 
        t.*,
        
        -- 21-23. IP y conexión
        CONCAT('192.168.', (rnd1 % 256)::STRING, '.', (rnd2 % 256)::STRING) AS direccion_ip,
        CASE 
            WHEN rnd3 % 8 < 2 THEN 'Claro'
            WHEN rnd3 % 8 < 4 THEN 'Movistar'
            WHEN rnd3 % 8 = 4 THEN 'Tigo'
            WHEN rnd3 % 8 = 5 THEN 'ETB'
            WHEN rnd3 % 8 = 6 THEN 'DirecTV'
            ELSE 'Une'
        END AS isp_proveedor,
        CASE WHEN rnd4 % 100 < 8 THEN TRUE ELSE FALSE END AS usa_vpn_proxy,
        
        -- 24-28. Dispositivo
        CASE 
            WHEN rnd5 % 6 < 3 THEN 'smartphone'
            WHEN rnd5 % 6 = 3 THEN 'tablet'
            WHEN rnd5 % 6 = 4 THEN 'laptop'
            ELSE 'desktop'
        END AS tipo_dispositivo,
        
        CASE 
            WHEN rnd6 % 6 < 3 THEN 'Android'
            WHEN rnd6 % 6 < 5 THEN 'iOS'
            ELSE 'Windows'
        END AS sistema_operativo,
        
        CASE 
            WHEN rnd7 % 100 < 30 THEN '12.0'
            WHEN rnd7 % 100 < 60 THEN '13.0'
            WHEN rnd7 % 100 < 85 THEN '14.0'
            ELSE '15.0'
        END AS version_sistema_operativo,
        
        CASE 
            WHEN rnd8 % 5 < 2 THEN 'Chrome'
            WHEN rnd8 % 5 = 2 THEN 'Safari'
            WHEN rnd8 % 5 = 3 THEN 'Firefox'
            ELSE 'Edge'
        END AS navegador,
        
        CONCAT('v', (100 + (rnd1 % 20))::STRING, '.0') AS version_navegador,
        
        -- 29-33. Identificación y asociaciones
        MD5(CONCAT(transaction_id, rnd2::STRING)) AS huella_digital_dispositivo,
        CASE WHEN rnd3 % 100 < 85 THEN 1 + (rnd3 % 2) ELSE 1 + (rnd3 % 5) END AS dispositivos_asociados_cuenta,
        CASE WHEN rnd4 % 100 < 90 THEN 1 ELSE 1 + (rnd4 % 3) END AS cuentas_asociadas_dispositivo,
        
        CASE 
            WHEN rnd5 % 8 < 2 THEN 'iPhone 14'
            WHEN rnd5 % 8 = 2 THEN 'iPhone 13'
            WHEN rnd5 % 8 = 3 THEN 'Samsung Galaxy S23'
            WHEN rnd5 % 8 = 4 THEN 'Samsung Galaxy A54'
            WHEN rnd5 % 8 = 5 THEN 'Xiaomi Redmi Note 12'
            WHEN rnd5 % 8 = 6 THEN 'Huawei P50'
            ELSE 'OnePlus 11'
        END AS modelo_telefono,
        
        CASE WHEN rnd6 % 100 < 3 THEN TRUE ELSE FALSE END AS sim_duplicada_cambiada,
        
        -- 34-36. Geolocalización
        4.6 + ((rnd7 % 2000) - 1000) / 1000.0 AS ubicacion_gps_lat,   -- Bogotá aprox
        -74.1 + ((rnd8 % 2000) - 1000) / 1000.0 AS ubicacion_gps_lon,
        
        CASE 
            WHEN rnd1 % 100 < 60 THEN (rnd1 % 20) / 2.0         -- 0-10 km (60%)
            WHEN rnd1 % 100 < 80 THEN (rnd1 % 100) / 2.0        -- 0-50 km (20%)
            WHEN rnd1 % 100 < 95 THEN (rnd1 % 500) / 2.0        -- 0-250 km (15%)
            ELSE (rnd1 % 2000) / 2.0                            -- 0-1000 km (5%)
        END AS distancia_domicilio_km,
        
        CASE 
            WHEN rnd2 % 100 < 80 THEN (rnd2 % 50) / 10.0        -- 0-5 km/h (80%)
            WHEN rnd2 % 100 < 95 THEN (rnd2 % 200) / 10.0       -- 0-20 km/h (15%)
            ELSE (rnd2 % 1000) / 10.0                           -- 0-100 km/h (5%)
        END AS velocidad_cambio_ubicacion_kmh,
        
        -- 37-40. Configuración avanzada
        CASE WHEN rnd3 % 100 < 2 THEN TRUE ELSE FALSE END AS usa_emulador_virtual,
        
        CASE 
            WHEN rnd4 % 6 < 3 THEN 'es'
            WHEN rnd4 % 6 < 5 THEN 'en'
            ELSE 'pt'
        END AS idioma_dispositivo,
        
        CASE 
            WHEN rnd5 % 6 < 3 THEN 'America/Bogota'
            WHEN rnd5 % 6 = 3 THEN 'America/New_York'
            WHEN rnd5 % 6 = 4 THEN 'America/Mexico_City'
            ELSE 'Europe/Madrid'
        END AS zona_horaria_dispositivo,
        
        CASE WHEN rnd6 % 100 < 75 THEN TRUE ELSE FALSE END AS dispositivo_confiable
        
    FROM transaccional_data t
),

-- Generar características de cliente y cuenta (41-50)
cliente_data AS (
    SELECT 
        d.*,
        
        -- 41-43. Antigüedades
        (rnd1 % 2000) + 30 AS antiguedad_cuenta_dias,
        (rnd2 % 1500) + 15 AS antiguedad_tarjeta_dias,
        (rnd3 % 3000) + 60 AS antiguedad_relacion_banco_dias,
        
        -- 44-47. Productos y tarjetas
        CASE WHEN rnd4 % 100 < 60 THEN 1 + (rnd4 % 3) ELSE 1 + (rnd4 % 8) END AS productos_financieros_activos,
        CASE WHEN rnd5 % 100 < 80 THEN 1 + (rnd5 % 2) ELSE 1 + (rnd5 % 5) END AS cuentas_vinculadas,
        CASE WHEN rnd6 % 100 < 70 THEN 1 + (rnd6 % 2) ELSE 1 + (rnd6 % 4) END AS tarjetas_activas,
        CASE WHEN rnd7 % 100 < 90 THEN 0 ELSE rnd7 % 3 END AS tarjetas_bloqueadas_previas,
        
        -- 48-50. Cambios recientes
        CASE WHEN rnd8 % 100 < 8 THEN TRUE ELSE FALSE END AS cambios_datos_personales_30d,
        CASE WHEN rnd1 % 100 < 5 THEN TRUE ELSE FALSE END AS cambios_direccion_30d,
        CASE WHEN rnd2 % 100 < 12 THEN TRUE ELSE FALSE END AS cambios_email_30d
        
    FROM dispositivo_data d
),

-- Generar características de crédito y límites (51-60)
credito_data AS (
    SELECT 
        c.*,
        
        -- 51-55. Límites y gastos
        CASE 
            WHEN rnd3 % 100 < 40 THEN (rnd3 % 5000) + 1000      -- $1K-6K (40%)
            WHEN rnd3 % 100 < 70 THEN (rnd3 % 10000) + 5000     -- $5K-15K (30%)
            WHEN rnd3 % 100 < 90 THEN (rnd3 % 20000) + 10000    -- $10K-30K (20%)
            ELSE (rnd3 % 50000) + 30000                         -- $30K-80K (10%)
        END AS limite_credito_disponible,
        
        ROUND((rnd4 % 85) / 100.0, 2) AS porcentaje_uso_limite,
        (rnd5 % 3000) + 200.0 AS promedio_gasto_mensual,
        (rnd6 % 800) + 100.0 AS desviacion_estandar_gasto,
        (rnd7 % 8000) + 500.0 AS maximo_historico_gasto,
        
        -- 56-60. Ratios y comportamiento
        ROUND(0.5 + (rnd8 % 200) / 100.0, 2) AS relacion_ingresos_gasto,
        ((rnd1 % 200) - 100) / 10.0 AS variacion_gasto_semana,
        CASE WHEN rnd2 % 100 < 85 THEN rnd2 % 3 ELSE rnd2 % 8 END AS frecuencia_limite_credito,
        CASE WHEN rnd3 % 100 < 20 THEN TRUE ELSE FALSE END AS pagos_minimos_frecuentes,
        CASE WHEN rnd4 % 100 < 95 THEN 0 ELSE rnd4 % 3 END AS incumplimientos_pago_previos
        
    FROM cliente_data c
),

-- Generar características de comportamiento (61-70)
comportamiento_data AS (
    SELECT 
        cr.*,
        
        -- 61-64. Intentos y sesión
        CASE WHEN rnd5 % 100 < 90 THEN rnd5 % 3 ELSE rnd5 % 10 END AS intentos_fallidos_previos,
        CASE WHEN rnd6 % 100 < 80 THEN rnd6 % 5 ELSE rnd6 % 15 END AS intentos_login_24h,
        (rnd7 % 180) + 30.0 AS tiempo_medio_sesion_min,
        (rnd8 % 300) + 200 AS velocidad_digitacion_cpm,
        
        -- 65-68. Patrones de entrada
        CASE WHEN rnd1 % 100 < 40 THEN TRUE ELSE FALSE END AS uso_autocompletado_copypaste,
        CASE WHEN rnd2 % 100 < 15 THEN TRUE ELSE FALSE END AS cambio_frecuente_passwords,
        CASE WHEN rnd3 % 100 < 25 THEN TRUE ELSE FALSE END AS usa_passwords_debiles,
        CASE WHEN rnd4 % 100 < 8 THEN TRUE ELSE FALSE END AS usa_emails_temporales,
        
        -- 69-70. Historial de problemas
        CASE WHEN rnd5 % 100 < 92 THEN 0 WHEN rnd5 % 100 < 98 THEN 1 ELSE rnd5 % 4 END AS historial_contracargos,
        CASE WHEN rnd6 % 100 < 95 THEN 0 WHEN rnd6 % 100 < 99 THEN 1 ELSE rnd6 % 3 END AS historial_reclamos_fraude
        
    FROM credito_data cr
),

-- Generar características de contexto y entorno (71-80)
contexto_data AS (
    SELECT 
        co.*,
        
        -- 71-75. Monedas y ubicaciones
        CASE 
            WHEN rnd7 % 8 < 4 THEN 'USD'
            WHEN rnd7 % 8 < 6 THEN 'COP'
            WHEN rnd7 % 8 = 6 THEN 'EUR'
            ELSE 'MXN'
        END AS tipo_moneda,
        
        CASE WHEN rnd8 % 100 < 25 THEN TRUE ELSE FALSE END AS requiere_conversion_divisas,
        pais_origen AS pais_emision_tarjeta,  -- Reutilizar para simplicidad
        pais_destino AS pais_residencia_declarado,
        
        CASE 
            WHEN rnd1 % 100 < 70 THEN (rnd1 % 50) / 2.0
            WHEN rnd1 % 100 < 90 THEN (rnd1 % 200) / 2.0
            ELSE (rnd1 % 1000) / 2.0
        END AS diferencia_residencia_compra_km,
        
        -- 76-80. Características del comercio
        CASE WHEN rnd2 % 100 < 12 THEN TRUE ELSE FALSE END AS comercio_alto_riesgo,
        CASE WHEN rnd3 % 100 < 8 THEN TRUE ELSE FALSE END AS comercio_recien_registrado,
        CASE WHEN rnd4 % 100 < 5 THEN TRUE ELSE FALSE END AS comercio_historial_fraude,
        CASE WHEN rnd5 % 100 < 15 THEN TRUE ELSE FALSE END AS cajero_no_habitual,
        CASE WHEN rnd6 % 100 < 10 THEN TRUE ELSE FALSE END AS retiro_efectivo_inusual
        
    FROM comportamiento_data co
),

-- Generar características de redes y patrones (81-90)
redes_data AS (
    SELECT 
        ctx.*,
        
        -- 81-85. Conexiones sospechosas
        CASE WHEN rnd7 % 100 < 3 THEN TRUE ELSE FALSE END AS relacion_cuentas_fraudulentas,
        CASE WHEN rnd8 % 100 < 95 THEN rnd8 % 3 ELSE rnd8 % 8 END AS transferencias_cuentas_nuevas_30d,
        CASE WHEN rnd1 % 100 < 98 THEN 0 ELSE rnd1 % 3 END AS transferencias_cuentas_cerradas,
        CASE WHEN rnd2 % 100 < 7 THEN TRUE ELSE FALSE END AS conexion_emails_sospechosos,
        CASE WHEN rnd3 % 100 < 6 THEN TRUE ELSE FALSE END AS conexion_telefonos_sospechosos,
        
        -- 86-90. Patrones de dispositivos y transferencias
        CASE WHEN rnd4 % 100 < 85 THEN 1 ELSE 1 + (rnd4 % 4) END AS cuentas_mismo_dispositivo,
        CASE WHEN rnd5 % 100 < 80 THEN 1 ELSE 1 + (rnd5 % 5) END AS cuentas_misma_ip,
        CASE WHEN rnd6 % 100 < 4 THEN TRUE ELSE FALSE END AS conexion_beneficiarios_bloqueados,
        CASE WHEN rnd7 % 100 < 97 THEN 0 ELSE rnd7 % 3 END AS transferencias_multiples_paises_1h,
        ROUND((rnd8 % 100) / 100.0, 2) AS patron_smurfing_score
        
    FROM contexto_data ctx
),

-- Generar señales avanzadas (91-100)
senales_data AS (
    SELECT 
        r.*,
        
        -- 91-95. Variaciones y anomalías
        ((rnd1 % 2000) - 1000) / 100.0 AS variacion_monto_autorizado_liquidado,
        CASE WHEN rnd2 % 100 < 95 THEN 0 WHEN rnd2 % 100 < 99 THEN 1 ELSE rnd2 % 5 END AS intentos_multiples_comercios_min,
        CASE WHEN rnd3 % 100 < 2 THEN TRUE ELSE FALSE END AS cambio_geolocalizacion_imposible,
        CASE WHEN rnd4 % 100 < 8 THEN TRUE ELSE FALSE END AS diferencia_timezone_dispositivo_ip,
        CASE WHEN rnd5 % 100 < 12 THEN TRUE ELSE FALSE END AS usa_bin_alto_riesgo,
        
        -- 96-100. Señales de seguridad avanzadas
        CASE WHEN rnd6 % 100 < 1 THEN TRUE ELSE FALSE END AS identificacion_dark_web,
        CASE WHEN rnd7 % 100 < 6 THEN TRUE ELSE FALSE END AS discrepancia_idioma_ubicacion,
        CASE WHEN rnd8 % 100 < 97 THEN 0 WHEN rnd8 % 100 < 99 THEN 1 ELSE 2 END AS frecuencia_cambio_sim_30d,
        CASE WHEN rnd1 % 100 < 90 THEN 1 ELSE 1 + (rnd1 % 3) END AS dispositivos_activos_paralelo,
        CASE WHEN rnd2 % 100 < 5 THEN TRUE ELSE FALSE END AS transacciones_redirecciones_sospechosas
        
    FROM redes_data r
),

-- Calcular probabilidad de fraude basada en múltiples factores de riesgo
fraud_calculation AS (
    SELECT *,
        -- Score de riesgo avanzado basado en múltiples categorías
        LEAST(98, GREATEST(1, 
            2 +  -- Base rate 2%
            -- Factores transaccionales
            (CASE WHEN monto_transaccion > 5000 THEN 15 ELSE 0 END) +
            (CASE WHEN frecuencia_diaria > 10 THEN 8 ELSE 0 END) +
            (CASE WHEN transacciones_por_hora > 5 THEN 10 ELSE 0 END) +
            (CASE WHEN diferencia_horaria_horas > 8 THEN 6 ELSE 0 END) +
            (CASE WHEN velocidad_transacciones_min < 5 THEN 12 ELSE 0 END) +
            
            -- Factores de dispositivo
            (CASE WHEN usa_vpn_proxy THEN 15 ELSE 0 END) +
            (CASE WHEN dispositivos_asociados_cuenta > 3 THEN 8 ELSE 0 END) +
            (CASE WHEN cuentas_asociadas_dispositivo > 2 THEN 10 ELSE 0 END) +
            (CASE WHEN sim_duplicada_cambiada THEN 20 ELSE 0 END) +
            (CASE WHEN distancia_domicilio_km > 200 THEN 12 ELSE 0 END) +
            (CASE WHEN velocidad_cambio_ubicacion_kmh > 50 THEN 15 ELSE 0 END) +
            (CASE WHEN usa_emulador_virtual THEN 25 ELSE 0 END) +
            (CASE WHEN NOT dispositivo_confiable THEN 8 ELSE 0 END) +
            
            -- Factores de cliente
            (CASE WHEN antiguedad_cuenta_dias < 30 THEN 18 ELSE 0 END) +
            (CASE WHEN tarjetas_bloqueadas_previas > 0 THEN 15 ELSE 0 END) +
            (CASE WHEN cambios_datos_personales_30d THEN 12 ELSE 0 END) +
            (CASE WHEN cambios_direccion_30d THEN 10 ELSE 0 END) +
            (CASE WHEN cambios_email_30d THEN 8 ELSE 0 END) +
            
            -- Factores de crédito
            (CASE WHEN porcentaje_uso_limite > 0.9 THEN 12 ELSE 0 END) +
            (CASE WHEN pagos_minimos_frecuentes THEN 8 ELSE 0 END) +
            (CASE WHEN incumplimientos_pago_previos > 0 THEN 15 ELSE 0 END) +
            
            -- Factores de comportamiento
            (CASE WHEN intentos_fallidos_previos > 5 THEN 15 ELSE 0 END) +
            (CASE WHEN intentos_login_24h > 10 THEN 12 ELSE 0 END) +
            (CASE WHEN usa_passwords_debiles THEN 10 ELSE 0 END) +
            (CASE WHEN usa_emails_temporales THEN 15 ELSE 0 END) +
            (CASE WHEN historial_contracargos > 0 THEN 20 ELSE 0 END) +
            (CASE WHEN historial_reclamos_fraude > 0 THEN 25 ELSE 0 END) +
            
            -- Factores de contexto
            (CASE WHEN comercio_alto_riesgo THEN 15 ELSE 0 END) +
            (CASE WHEN comercio_recien_registrado THEN 10 ELSE 0 END) +
            (CASE WHEN comercio_historial_fraude THEN 20 ELSE 0 END) +
            (CASE WHEN retiro_efectivo_inusual THEN 12 ELSE 0 END) +
            
            -- Factores de red
            (CASE WHEN relacion_cuentas_fraudulentas THEN 30 ELSE 0 END) +
            (CASE WHEN transferencias_cuentas_cerradas > 0 THEN 18 ELSE 0 END) +
            (CASE WHEN conexion_emails_sospechosos THEN 15 ELSE 0 END) +
            (CASE WHEN conexion_telefonos_sospechosos THEN 15 ELSE 0 END) +
            (CASE WHEN cuentas_mismo_dispositivo > 3 THEN 12 ELSE 0 END) +
            (CASE WHEN cuentas_misma_ip > 3 THEN 10 ELSE 0 END) +
            (CASE WHEN conexion_beneficiarios_bloqueados THEN 25 ELSE 0 END) +
            (CASE WHEN transferencias_multiples_paises_1h > 1 THEN 18 ELSE 0 END) +
            (CASE WHEN patron_smurfing_score > 0.7 THEN 15 ELSE 0 END) +
            
            -- Señales avanzadas
            (CASE WHEN ABS(variacion_monto_autorizado_liquidado) > 500 THEN 12 ELSE 0 END) +
            (CASE WHEN intentos_multiples_comercios_min > 2 THEN 15 ELSE 0 END) +
            (CASE WHEN cambio_geolocalizacion_imposible THEN 25 ELSE 0 END) +
            (CASE WHEN diferencia_timezone_dispositivo_ip THEN 12 ELSE 0 END) +
            (CASE WHEN usa_bin_alto_riesgo THEN 18 ELSE 0 END) +
            (CASE WHEN identificacion_dark_web THEN 35 ELSE 0 END) +
            (CASE WHEN discrepancia_idioma_ubicacion THEN 10 ELSE 0 END) +
            (CASE WHEN frecuencia_cambio_sim_30d > 1 THEN 20 ELSE 0 END) +
            (CASE WHEN dispositivos_activos_paralelo > 2 THEN 15 ELSE 0 END) +
            (CASE WHEN transacciones_redirecciones_sospechosas THEN 20 ELSE 0 END) +
            
            -- Factor aleatorio controlado
            (rnd8 % 5)
        )) AS fraud_probability
    FROM senales_data
)

-- Seleccionar datos finales con etiqueta de fraude
SELECT 
    transaction_id,
    -- TRANSACCIONALES
    monto_transaccion, frecuencia_diaria, transacciones_por_hora, pais_origen, pais_destino,
    diferencia_horaria_horas, tipo_transaccion, canal_transaccion, categoria_comercio_mcc,
    tipo_comercio, hora_dia, dia_semana, dia_mes, es_dia_festivo, patron_horario_habitual,
    velocidad_transacciones_min, transferencias_cuentas_nuevas, beneficiarios_frecuentes,
    transacciones_internacionales, variacion_gasto_mes_anterior,
    -- DISPOSITIVO Y CANAL
    direccion_ip, isp_proveedor, usa_vpn_proxy, tipo_dispositivo, sistema_operativo,
    version_sistema_operativo, navegador, version_navegador, huella_digital_dispositivo,
    dispositivos_asociados_cuenta, cuentas_asociadas_dispositivo, modelo_telefono,
    sim_duplicada_cambiada, ubicacion_gps_lat, ubicacion_gps_lon, distancia_domicilio_km,
    velocidad_cambio_ubicacion_kmh, usa_emulador_virtual, idioma_dispositivo,
    zona_horaria_dispositivo, dispositivo_confiable,
    -- CLIENTE Y CUENTA
    antiguedad_cuenta_dias, antiguedad_tarjeta_dias, antiguedad_relacion_banco_dias,
    productos_financieros_activos, cuentas_vinculadas, tarjetas_activas,
    tarjetas_bloqueadas_previas, cambios_datos_personales_30d, cambios_direccion_30d,
    cambios_email_30d,
    -- CREDITO Y LIMITES
    limite_credito_disponible, porcentaje_uso_limite, promedio_gasto_mensual,
    desviacion_estandar_gasto, maximo_historico_gasto, relacion_ingresos_gasto,
    variacion_gasto_semana, frecuencia_limite_credito, pagos_minimos_frecuentes,
    incumplimientos_pago_previos,
    -- COMPORTAMIENTO
    intentos_fallidos_previos, intentos_login_24h, tiempo_medio_sesion_min,
    velocidad_digitacion_cpm, uso_autocompletado_copypaste, cambio_frecuente_passwords,
    usa_passwords_debiles, usa_emails_temporales, historial_contracargos,
    historial_reclamos_fraude,
    -- CONTEXTO Y ENTORNO
    tipo_moneda, requiere_conversion_divisas, pais_emision_tarjeta,
    pais_residencia_declarado, diferencia_residencia_compra_km, comercio_alto_riesgo,
    comercio_recien_registrado, comercio_historial_fraude, cajero_no_habitual,
    retiro_efectivo_inusual,
    -- REDES Y PATRONES
    relacion_cuentas_fraudulentas, transferencias_cuentas_nuevas_30d,
    transferencias_cuentas_cerradas, conexion_emails_sospechosos,
    conexion_telefonos_sospechosos, cuentas_mismo_dispositivo, cuentas_misma_ip,
    conexion_beneficiarios_bloqueados, transferencias_multiples_paises_1h,
    patron_smurfing_score,
    -- SEÑALES AVANZADAS
    variacion_monto_autorizado_liquidado, intentos_multiples_comercios_min,
    cambio_geolocalizacion_imposible, diferencia_timezone_dispositivo_ip,
    usa_bin_alto_riesgo, identificacion_dark_web, discrepancia_idioma_ubicacion,
    frecuencia_cambio_sim_30d, dispositivos_activos_paralelo,
    transacciones_redirecciones_sospechosas,
    -- LABEL
    CASE WHEN (rnd1 % 100) < fraud_probability THEN TRUE ELSE FALSE END AS is_fraud
FROM fraud_calculation;

-- Step 4: Insertar log de la operación
INSERT INTO APP_LOGS (log_level, action, status, detail)
VALUES ('INFO', 'DATA_GENERATION_ADVANCED', 'COMPLETED', 'Generated 100,000 transactions with 100 professional features');

-- Step 5: Verificar los datos generados
SELECT 
    '🎉 DATOS AVANZADOS GENERADOS EXITOSAMENTE' AS status,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_cases,
    ROUND((SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS fraud_percentage,
    ROUND(AVG(monto_transaccion), 2) AS avg_amount,
    MIN(monto_transaccion) AS min_amount,
    MAX(monto_transaccion) AS max_amount
FROM TRANSACCIONES_AVANZADAS;

-- Step 6: Estadísticas por categorías principales
SELECT '=== DISTRIBUCIÓN POR TIPO DE TRANSACCIÓN ===' AS analisis;

SELECT 
    tipo_transaccion,
    COUNT(*) AS total,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraudes,
    ROUND((SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS tasa_fraude,
    ROUND(AVG(monto_transaccion), 2) AS monto_promedio
FROM TRANSACCIONES_AVANZADAS
GROUP BY tipo_transaccion
ORDER BY tasa_fraude DESC;

-- Step 7: Análisis por canal
SELECT '=== DISTRIBUCIÓN POR CANAL ===' AS analisis;

SELECT 
    canal_transaccion,
    COUNT(*) AS total,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraudes,
    ROUND((SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS tasa_fraude
FROM TRANSACCIONES_AVANZADAS
GROUP BY canal_transaccion
ORDER BY tasa_fraude DESC;

-- Step 8: Muestra de datos con características clave
SELECT 'MUESTRA DE DATOS GENERADOS CON CARACTERÍSTICAS CLAVE' AS info;

SELECT *
FROM TRANSACCIONES_AVANZADAS 
WHERE is_fraud = TRUE
LIMIT 10;
