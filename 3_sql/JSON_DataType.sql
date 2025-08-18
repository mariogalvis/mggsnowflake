-- ==========================================================
-- Configuración del entorno de trabajo
-- ==========================================================
USE DATABASE BD_EMPRESA;
USE SCHEMA BRONZE;

CREATE OR REPLACE WAREHOUSE VW_DATALOADER WITH WAREHOUSE_SIZE = 'X-SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 30 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD';

USE WAREHOUSE VW_DATALOADER;

-- Ajuste del tamaño del warehouse para manejo de datos JSON
ALTER WAREHOUSE VW_DATALOADER SET WAREHOUSE_SIZE = 'MEDIUM';

-- ==========================================================
-- Creación de tabla para almacenar datos en formato JSON
-- ==========================================================
CREATE OR REPLACE TABLE JSON_WEATHER_DATA (V VARIANT);

-- ==========================================================
-- Configuración del stage para cargar datos JSON desde S3
-- ==========================================================
CREATE OR REPLACE STAGE COL_CLIMA
 URL = 's3://snowflake-workshop-lab/weather-nyc'
 COMMENT = 'Stage para cargar datos de clima desde S3';

-- Opcional: listar los archivos disponibles en el stage
LIST @COL_CLIMA;

-- ==========================================================
-- Carga de datos JSON a la tabla JSON_WEATHER_DATA
-- ==========================================================
COPY INTO JSON_WEATHER_DATA 
FROM @COL_CLIMA 
FILE_FORMAT = (TYPE = 'JSON');

-- Verificación rápida de los datos cargados
SELECT * FROM JSON_WEATHER_DATA LIMIT 10;

-- ==========================================================
-- Creación de vista para datos de clima
-- ==========================================================
//CREATE OR REPLACE VIEW JSON_WEATHER_DATA_VIEW AS
SELECT
  v:time::timestamp AS observation_time,
  v:city.id::int AS city_id,
  v:city.name::string AS city_name,
  v:city.country::string AS country,
  v:city.coord.lat::float AS city_lat,
  v:city.coord.lon::float AS city_lon,
  v:clouds.all::int AS clouds,
  (v:main.temp::float) - 273.15 AS temp_avg,   -- Conversión de Kelvin a Celsius
  (v:main.temp_min::float) - 273.15 AS temp_min,
  (v:main.temp_max::float) - 273.15 AS temp_max,
  v:weather[0].main::string AS weather,
  v:weather[0].description::string AS weather_desc,
  v:weather[0].icon::string AS weather_icon,
  v:wind.deg::float AS wind_dir,
  v:wind.speed::float AS wind_speed
FROM JSON_WEATHER_DATA
WHERE v:city.id::int = 5128638;  -- Filtrando por ciudad específica

-- ==========================================================
-- Consulta de ejemplo para filtrar datos por mes
-- ==========================================================
SELECT * FROM JSON_WEATHER_DATA_VIEW
WHERE DATE_TRUNC('month', observation_time) = '2018-01-01'
LIMIT 20;

-- ==========================================================
-- Limpieza de recursos para optimizar costos
-- ==========================================================
DROP TABLE JSON_WEATHER_DATA;
DROP STAGE COL_CLIMA;
DROP WAREHOUSE VW_DATALOADER;
