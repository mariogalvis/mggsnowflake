-- ==========================================================
-- Configuración del entorno de trabajo
-- ==========================================================
USE DATABASE BD_EMPRESA;
USE SCHEMA BRONZE;
USE WAREHOUSE VW_DATALOADER;

-- Ajuste del tamaño del warehouse para operaciones iniciales
ALTER WAREHOUSE VW_DATALOADER SET WAREHOUSE_SIZE = 'SMALL';

-- ==========================================================
-- Creación de la tabla para datos de clientes crudos
-- ==========================================================
CREATE OR REPLACE TABLE CLIENTES_RAW (
    tripduration INTEGER,
    starttime TIMESTAMP,
    stoptime TIMESTAMP,
    start_station_id INTEGER,
    start_station_name STRING,
    start_station_latitude FLOAT,
    start_station_longitude FLOAT,
    end_station_id INTEGER,
    end_station_name STRING,
    end_station_latitude FLOAT,
    end_station_longitude FLOAT,
    bikeid INTEGER,
    membership_type STRING,
    usertype STRING,
    birth_year INTEGER,
    gender INTEGER
);

-- ==========================================================
-- Creación del stage para carga de datos desde S3
-- ==========================================================
CREATE OR REPLACE STAGE S3_EXTERNAL_STAGE
 URL = 's3://snowflake-workshop-lab/japan/citibike-trips/' 
 COMMENT = 'Stage Externo para el cargado de datos desde Cloud';

-- Comando opcional para listar los archivos en el stage
-- LIST @S3_EXTERNAL_STAGE;

-- ==========================================================
-- Definición del formato de archivo CSV
-- ==========================================================
CREATE OR REPLACE FILE FORMAT CSV 
TYPE = 'CSV' 
COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 0 
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\\' 
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO' 
NULL_IF = ('') 
COMMENT = 'Formato de archivo para el cargado de los datos del stage';

-- ==========================================================
-- Carga inicial de datos a la tabla CLIENTES_RAW
-- ==========================================================
COPY INTO CLIENTES_RAW
FROM @S3_EXTERNAL_STAGE/trips
FILE_FORMAT = CSV;

-- Verificación del número de registros cargados
SELECT COUNT(*) AS total_registros FROM CLIENTES_RAW;

-- ==========================================================
-- Limpiar la tabla para pruebas de rendimiento
-- ==========================================================
TRUNCATE TABLE CLIENTES_RAW;

-- Escalar el warehouse para pruebas de rendimiento
ALTER WAREHOUSE VW_DATALOADER SET WAREHOUSE_SIZE = 'X-LARGE';

-- Cargar nuevamente los datos con mayor capacidad de cómputo
COPY INTO CLIENTES_RAW
FROM @S3_EXTERNAL_STAGE/trips
FILE_FORMAT = CSV;

-- Verificación del número de registros después de la recarga
SELECT COUNT(*) AS total_registros FROM CLIENTES_RAW;

-- ==========================================================
-- Limpieza final de recursos para optimizar costos
-- ==========================================================
DROP TABLE CLIENTES_RAW;
DROP STAGE S3_EXTERNAL_STAGE;
DROP FILE FORMAT CSV;
ALTER WAREHOUSE VW_DATALOADER SET WAREHOUSE_SIZE = 'X-SMALL';
