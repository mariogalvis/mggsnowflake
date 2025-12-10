-- ==========================================================
-- Configuración del entorno de trabajo
-- ==========================================================
USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA PUBLIC;

-- ==========================================================
-- Verificación del número total de registros en CLIENTES
-- ==========================================================
SELECT COUNT(*) AS total_registros FROM CLIENTES;

-- ==========================================================
-- Ajuste del tamaño del warehouse para operaciones de clonado
-- ==========================================================
ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'Medium';

-- ==========================================================
-- Clonado sin consumo de almacenamiento adicional (Zero-Copy Clone)
-- ==========================================================
CREATE TABLE CLIENTES_DEV CLONE CLIENTES;

-- ==========================================================
-- Prueba de borrado y restauración de tablas
-- ==========================================================
DROP TABLE CLIENTES_DEV;       -- Eliminando la tabla clonada
UNDROP TABLE CLIENTES_DEV;     -- Restaurando la tabla eliminada

-- ==========================================================
-- Clonado de toda la base de datos
-- ==========================================================
CREATE DATABASE EMPRESA CLONE BD_EMPRESA;
DROP DATABASE EMPRESA;         -- Eliminando la base de datos clonada
UNDROP DATABASE EMPRESA;       -- Restaurando la base de datos eliminada

-- ==========================================================
-- Actualización de datos con errores para probar Time Travel
-- ==========================================================
-- Vista rápida de los primeros 10 registros antes de actualizar
SELECT * FROM CLIENTES LIMIT 10;

-- Simulación de un error de actualización
UPDATE CLIENTES SET start_station_name = 'UUPS! Un error de actualizacion.';

-- Vista rápida para verificar los cambios
SELECT * FROM CLIENTES LIMIT 10;

-- ==========================================================
-- Resumen de viajes por estación después del error
-- ==========================================================
SELECT
    start_station_name AS "station",
    COUNT(*) AS "rides"
FROM CLIENTES
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- ==========================================================
-- Recuperación usando Time Travel por Statement ID
-- ==========================================================
-- ID de la transacción que introdujo el error
SELECT * 
FROM CLIENTES 
BEFORE (STATEMENT => '01baed43-0004-b882-0000-0005ea538091') 
LIMIT 10;

-- Recreación de la tabla antes del error
CREATE OR REPLACE TABLE CLIENTES AS
(SELECT * FROM CLIENTES BEFORE (STATEMENT => '01baed43-0004-b882-0000-0005ea538091'));

-- ==========================================================
-- Verificación después de la recuperación
-- ==========================================================
SELECT
    start_station_name AS "station",
    COUNT(*) AS "rides"
FROM CLIENTES
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- ==========================================================
-- Pruebas adicionales de Time Travel usando timestamps
-- ==========================================================
-- Simulación de otro error
UPDATE CLIENTES SET start_station_name = 'ESTO ES OTRO ERROR';

-- Verificación del impacto del error
SELECT * FROM CLIENTES LIMIT 10;

-- Resumen de viajes por estación después del segundo error
SELECT
    start_station_name AS "station",
    COUNT(*) AS "rides"
FROM CLIENTES
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- ==========================================================
-- Configuración de zona horaria para pruebas de Time Travel
-- ==========================================================
ALTER SESSION SET TIMEZONE = 'America/Bogota';

-- Verificación de la hora actual
SELECT CURRENT_TIMESTAMP();
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_LTZ;

-- ==========================================================
-- Recuperación usando Time Travel con timestamp específico
-- ==========================================================
SELECT * 
FROM CLIENTES 
AT (TIMESTAMP => '2025-03-10 21:22:00'::TIMESTAMP_LTZ) 
LIMIT 10;

-- Restauración de la tabla a un estado anterior usando timestamp
CREATE OR REPLACE TABLE CLIENTES AS
(SELECT * FROM CLIENTES AT (TIMESTAMP => '2025-03-10 21:15:00'::TIMESTAMP_LTZ));

-- ==========================================================
-- Reducción del tamaño del warehouse para optimización de costos
-- ==========================================================
ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-small';
