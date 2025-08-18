ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'X-SMALL'  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

CREATE OR REPLACE WAREHOUSE VW_REPORTING WITH WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 60;

CREATE OR REPLACE WAREHOUSE VW_PREDICCION_CHURN WITH WAREHOUSE_SIZE = 'LARGE' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE VW_ADVANCED_ANALYTICS WITH WAREHOUSE_SIZE = 'X-SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 30 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE VW_GENAI WITH WAREHOUSE_SIZE = 'X-LARGE' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 30 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE VW_CORTEX_ANALYST WITH WAREHOUSE_SIZE = 'SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE VW_NEXTBESTOFFER WITH WAREHOUSE_SIZE = 'MEDIUM' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE ROLE DATA_ENGINEER;
CREATE OR REPLACE ROLE DATA_SCIENTIST;
CREATE OR REPLACE ROLE BI_ANALYTICS;
CREATE OR REPLACE ROLE BUSINESS_USER;

ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE OR REPLACE DATABASE BD_EMPRESA;

USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA PUBLIC;

CREATE OR replace TABLE CLIENTES
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

CREATE STAGE S3_EXTERNAL_STAGE
 URL = 's3://snowflake-workshop-lab/japan/citibike-trips/' 
 COMMENT = 'Stage Externo para el cargado de datos desde Cloud';

CREATE STAGE SF_INTERNAL_STAGE
	DIRECTORY = ( ENABLE = true );

CREATE OR REPLACE FILE FORMAT CSV 
TYPE = 'CSV' COMPRESSION = 'AUTO' 
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER = 0 
FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134' 
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('') 
COMMENT = 'Formato de archivo para el cargado de los datos del stage';

copy into clientes
from @S3_EXTERNAL_STAGE/trips
file_format=CSV;

/*
create table CLIENTES_DEV clone CLIENTES;
DROP TABLE CLIENTES_DEV;
UNDROP TABLE CLIENTES_DEV;
*/

CREATE OR REPLACE VIEW VISTA_CLIENTES_VIP AS
SELECT CC_NAME
FROM (SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER);

CREATE OR REPLACE FUNCTION EVALUAR_RIESGO_CREDITICIO()
RETURNS string LANGUAGE JAVASCRIPT AS
$$ return Object.getOwnPropertyNames(this); $$;

CREATE OR REPLACE FUNCTION PREDECIR_FUGA_CLIENTES()
RETURNS string LANGUAGE JAVASCRIPT AS
$$ return Object.getOwnPropertyNames(this); $$;

CREATE OR REPLACE FUNCTION VALIDAR_CUMPLIMIENTO_REGULATORIO()
RETURNS string LANGUAGE JAVASCRIPT AS
$$ return Object.getOwnPropertyNames(this); $$;

CREATE OR REPLACE FUNCTION DETECTAR_FRAUDE_RECLAMACIONES()
RETURNS string LANGUAGE JAVASCRIPT AS
$$ return Object.getOwnPropertyNames(this); $$;

CREATE OR REPLACE FUNCTION SEGMENTAR_USUARIOS_POR_PREFERENCIAS()
RETURNS string LANGUAGE JAVASCRIPT AS
$$ return Object.getOwnPropertyNames(this); $$;

-- LINAJE --
-- Creación de tablas base
CREATE OR REPLACE TABLE Clientes_lin (
    cliente_id INT PRIMARY KEY,
    nombre VARCHAR(100),
    fecha_nacimiento DATE,
    genero CHAR(1),
    pais VARCHAR(50)
);

CREATE OR REPLACE TABLE Transacciones (
    transaccion_id INT PRIMARY KEY,
    cliente_id INT,
    fecha DATE,
    monto DECIMAL(10, 2),
    tipo_transaccion VARCHAR(20),
    FOREIGN KEY (cliente_id) REFERENCES Clientes_lin(cliente_id)
);

CREATE OR REPLACE TABLE Cuentas (
    cuenta_id INT PRIMARY KEY,
    cliente_id INT,
    tipo_cuenta VARCHAR(50),
    saldo DECIMAL(10, 2),
    FOREIGN KEY (cliente_id) REFERENCES Clientes_lin(cliente_id)
);

-- Población de datos de ejemplo en Clientes_lin
INSERT INTO Clientes_lin (cliente_id, nombre, fecha_nacimiento, genero, pais) VALUES
(1, 'Juan Pérez', '1980-01-01', 'M', 'Colombia'),
(2, 'Ana Gómez', '1990-05-10', 'F', 'Perú'),
(3, 'Luis Torres', '1975-08-20', 'M', 'Chile');

-- Población de datos de ejemplo en Transacciones
INSERT INTO Transacciones (transaccion_id, cliente_id, fecha, monto, tipo_transaccion) VALUES
(1, 1, '2024-01-15', 5000.00, 'Depósito'),
(2, 1, '2024-01-20', -200.00, 'Retiro'),
(3, 2, '2024-01-18', 1000.00, 'Depósito'),
(4, 2, '2024-01-22', -150.00, 'Pago Tarjeta'),
(5, 3, '2024-01-19', 7000.00, 'Depósito');

-- Población de datos de ejemplo en Cuentas
INSERT INTO Cuentas (cuenta_id, cliente_id, tipo_cuenta, saldo) VALUES
(1, 1, 'Ahorros', 5200.00),
(2, 1, 'Corriente', 300.00),
(3, 2, 'Ahorros', 1150.00),
(4, 3, 'Ahorros', 7000.00);

-- Creación de vista intermedia para resumen de transacciones por cliente
CREATE OR REPLACE VIEW ResumenTransacciones AS
SELECT 
    c.cliente_id,
    c.nombre,
    c.pais,
    SUM(CASE WHEN t.tipo_transaccion = 'Depósito' THEN t.monto ELSE 0 END) AS total_depositos,
    SUM(CASE WHEN t.tipo_transaccion = 'Retiro' THEN -t.monto ELSE 0 END) AS total_retiros,
    COUNT(t.transaccion_id) AS total_transacciones
FROM 
    Clientes_lin c
JOIN 
    Transacciones t ON c.cliente_id = t.cliente_id
GROUP BY 
    c.cliente_id, c.nombre, c.pais;

-- Creación de vista para reporte de ingresos mensuales por país
CREATE OR REPLACE VIEW ReporteIngresosPais AS
SELECT
    c.pais,
    DATE_TRUNC('month', t.fecha) AS mes,
    SUM(CASE WHEN t.monto > 0 THEN t.monto ELSE 0 END) AS ingresos_mensuales,
    COUNT(t.transaccion_id) AS total_transacciones
FROM
    Transacciones t
JOIN 
    Clientes_lin c ON t.cliente_id = c.cliente_id
GROUP BY 
    c.pais, DATE_TRUNC('month', t.fecha);

-- Creación de vista de monitoreo de alertas de riesgo para transacciones sospechosas
CREATE OR REPLACE VIEW AlertasRiesgo AS
SELECT
    t.transaccion_id,
    t.cliente_id,
    c.nombre,
    t.fecha,
    t.monto,
    CASE 
        WHEN t.monto > 5000 THEN 'Alta'
        WHEN t.monto > 1000 THEN 'Media'
        ELSE 'Baja'
    END AS nivel_riesgo
FROM 
    Transacciones t
JOIN 
    Clientes_lin c ON t.cliente_id = c.cliente_id
WHERE 
    t.monto > 1000;

-- Creación de vista de tercer nivel que combina AlertasRiesgo y Cuentas
CREATE OR REPLACE VIEW ReporteRiesgoDetallado AS
SELECT
    a.transaccion_id,
    a.cliente_id,
    a.nombre AS cliente_nombre,
    a.fecha AS fecha_transaccion,
    a.monto AS monto_transaccion,
    a.nivel_riesgo,
    c.tipo_cuenta,
    c.saldo AS saldo_cuenta,
    cl.pais
FROM 
    AlertasRiesgo a
JOIN 
    Cuentas c ON a.cliente_id = c.cliente_id
JOIN 
    Clientes_lin cl ON a.cliente_id = cl.cliente_id
WHERE 
    a.nivel_riesgo = 'Alta';

-- LINAJE --

CREATE OR REPLACE SECURE FUNCTION obtener_score_crediticio(id_cliente STRING)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    // Usar arguments[0] para acceder al primer argumento que es el 'id_cliente'
    var input_id = arguments[0];

    // Generar un score crediticio ficticio basado en el ID
    var hash_value = 0;
    for (var i = 0; i < input_id.length; i++) {
        hash_value += input_id.charCodeAt(i) * (i + 1);
    }

    // Crear una fórmula ficticia para generar un score entre 300 y 850
    var score_ficticio = 300 + (hash_value % 550);
    
    return parseFloat(score_ficticio);
$$;

SELECT obtener_score_crediticio('801783645') AS score;

CREATE OR REPLACE SECURE FUNCTION datos_complementarios(n NUMBER)
RETURNS TABLE(col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING)
LANGUAGE SQL
AS
$$
    SELECT 
        TO_CHAR(UNIFORM(1, 1000, RANDOM())) AS col1,
        TO_CHAR('A') AS col2,
        TO_CHAR(UNIFORM(1, 1000, RANDOM())) AS col3,
        TO_CHAR(110121) AS col4,
        TO_CHAR('CO') AS col5
$$;

SELECT * FROM TABLE(datos_complementarios(801783645));

CREATE OR REPLACE TABLE segmentos_clientes (
    cliente_id STRING,                 -- ID anónimo del cliente
    segmento STRING,                   -- Segmento del cliente (Ej: "Premium", "Regular", "Económico")
    antiguedad_cliente INT,            -- Antigüedad del cliente en meses
    promedio_compra FLOAT,             -- Promedio de compra en los últimos 12 meses
    frecuencia_compra INT,             -- Frecuencia de compra (número de compras en los últimos 12 meses)
    nivel_ingreso_est STRING,          -- Nivel de ingreso estimado del cliente (Ej: "Alto", "Medio", "Bajo")
    tipo_producto_preferido STRING,    -- Categoría de producto preferido (Ej: "Tecnología", "Ropa")
    canal_preferido STRING,            -- Canal de compra preferido (Ej: "Online", "Tienda física")
    puntaje_satisfaccion INT,          -- Puntaje de satisfacción del cliente (0 a 100)
    probabilidad_recompra FLOAT,       -- Probabilidad de recompra (0 a 1, donde 1 es 100%)
    riesgo_crediticio_est STRING       -- Estimación del riesgo crediticio (Ej: "Alto", "Medio", "Bajo")
);

CREATE OR REPLACE PROCEDURE CALCULAR_BALANCE_GENERAL(ARGUMENT1 VARCHAR)
RETURNS string not null
language javascript AS
$$
var INPUT_ARGUMENT1 = ARGUMENT1;
var result = `${INPUT_ARGUMENT1}`
return result;
$$;

CREATE OR REPLACE PROCEDURE PROCESAR_RECLAMACIONES_DE_SINIESTROS(ARGUMENT1 VARCHAR)
RETURNS string not null
language javascript AS
$$
var INPUT_ARGUMENT1 = ARGUMENT1;
var result = `${INPUT_ARGUMENT1}`
return result;
$$;

CREATE OR REPLACE PROCEDURE ACTUALIZAR_LIMITES_DE_CREDITOS(ARGUMENT1 VARCHAR)
RETURNS string not null
language javascript AS
$$
var INPUT_ARGUMENT1 = ARGUMENT1;
var result = `${INPUT_ARGUMENT1}`
return result;
$$;

CREATE OR REPLACE SEQUENCE SEQ_01 START = 1 INCREMENT = 1;
CREATE OR REPLACE STREAM STREAM_A ON TABLE CLIENTES;

CREATE OR REPLACE PROCEDURE drop_db() RETURNS STRING NOT NULL
 LANGUAGE javascript AS
 $$
 var cmd = `DROP DATABASE "DEMO3B_DB";`
 var sql = snowflake.createStatement({sqlText: cmd});
 var result = sql.execute();
 return 'Database has been successfully dropped';
 $$;
 
CREATE OR REPLACE TASK tsk_wait_15
WAREHOUSE = VW_GENAI SCHEDULE = '11520 MINUTE'
AS CALL drop_db();

ALTER TASK tsk_wait_15 SUSPEND;

CREATE SCHEMA BRONZE;
CREATE SCHEMA SILVER;
CREATE SCHEMA GOLD;

//VARIANT DATA TYPE PARA ARCHIVOS JSON
USE WAREHOUSE VW_PREDICCION_CHURN;
USE DATABASE BD_EMPRESA;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE JSON_DATOS_CLIMA (V VARIANT);

create OR REPLACE stage CLIMA
url = 's3://snowflake-workshop-lab/weather-nyc';

copy into JSON_DATOS_CLIMA
from @CLIMA 
file_format = (type=json);

create or replace view JSON_DATOS_CLIMA_VISTA as
select
  v:time::timestamp as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from JSON_DATOS_CLIMA
where city_id = 5128638;

//CORTEX AI

CREATE OR REPLACE DATABASE BD_AI_CORTEX COMMENT = 'Base de datos de prueba Cortex AI';

CREATE or REPLACE file format csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE or REPLACE stage call_transcripts_data_stage
  file_format = csvformat
  url = 's3://sfquickstarts/misc/call_transcripts/';

CREATE or REPLACE table CALL_TRANSCRIPTS ( 
  date_created date,
  language varchar(60),
  country varchar(60),
  product varchar(60),
  category varchar(60),
  damage_type varchar(90),
  transcript varchar
);

COPY into CALL_TRANSCRIPTS
  from @call_transcripts_data_stage;

CREATE OR REPLACE STAGE MGG_DATA_TRANSCRIPTS
 file_format = csvformat
 URL = 's3://mggsnowflake/data/transcripts';

list @MGG_DATA_TRANSCRIPTS;
 
CREATE or REPLACE table CALL_TRANSCRIPTS_ES ( 
  date_created date,
  language varchar(60),
  country varchar(60),
  product varchar(60),
  category varchar(60),
  damage_type varchar(90),
  transcript varchar
);

COPY into CALL_TRANSCRIPTS_ES
  from @MGG_DATA_TRANSCRIPTS;


//RAG
--https://quickstarts.snowflake.com/guide/ask_questions_to_your_own_documents_with_snowflake_cortex_search/index.html#1

CREATE OR REPLACE DATABASE BD_CORTEX_RAG;
CREATE SCHEMA DATA;
USE WAREHOUSE VW_GENAI; 

create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512, #Adjust this as you see fit
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;

create or replace stage docs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);

// CORTEX ANALYST

-- create demo database
CREATE OR REPLACE DATABASE BD_cortex_analyst;

-- Create Stage
CREATE OR REPLACE STAGE CORTEX_ANALYSTSTAGE DIRECTORY = (ENABLE = TRUE);

-- create schema
CREATE OR REPLACE SCHEMA revenue_timeseries_banca;
CREATE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DAILY_REVENUE (
	DATE DATE,
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_PRODUCT (
	DATE DATE,
	PRODUCT_LINE VARCHAR(16777216),
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_REGION (
	DATE DATE,
	SALES_REGION VARCHAR(16777216),
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE STAGE MGG_BANCA
 URL = 's3://mggsnowflake/analyst/BANCA/';

list @MGG_BANCA;
 
COPY INTO DAILY_REVENUE
FROM @MGG_BANCA
FILES = ('daily_revenue_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO DAILY_REVENUE_BY_PRODUCT
FROM @MGG_BANCA
FILES = ('daily_revenue_by_product_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO DAILY_REVENUE_BY_REGION
FROM @MGG_BANCA
FILES = ('daily_revenue_by_region_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

-- create schema
CREATE OR REPLACE SCHEMA revenue_timeseries_seguros;
CREATE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DAILY_REVENUE (
	DATE DATE,
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_PRODUCT (
	DATE DATE,
	PRODUCT_LINE VARCHAR(16777216),
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_REGION (
	DATE DATE,
	SALES_REGION VARCHAR(16777216),
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE STAGE MGG_SEGUROS
 URL = 's3://mggsnowflake/analyst/SEGUROS/';

list @MGG_SEGUROS;
 
COPY INTO DAILY_REVENUE
FROM @MGG_SEGUROS
FILES = ('daily_revenue_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO DAILY_REVENUE_BY_PRODUCT
FROM @MGG_SEGUROS
FILES = ('daily_revenue_by_product_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO DAILY_REVENUE_BY_REGION
FROM @MGG_SEGUROS
FILES = ('daily_revenue_by_region_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;


CREATE OR REPLACE SCHEMA revenue_timeseries_telco;
CREATE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DAILY_REVENUE (
	DATE DATE,
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_PRODUCT (
	DATE DATE,
	PRODUCT_LINE VARCHAR(16777216),
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_REGION (
	DATE DATE,
	SALES_REGION VARCHAR(16777216),
	REVENUE FLOAT,
	COGS FLOAT,
	FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE STAGE MGG_TELCO
 URL = 's3://mggsnowflake/analyst/TELCO/';

list @MGG_TELCO;
 
COPY INTO DAILY_REVENUE
FROM @MGG_TELCO
FILES = ('daily_revenue_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO DAILY_REVENUE_BY_PRODUCT
FROM @MGG_TELCO
FILES = ('daily_revenue_by_product_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO DAILY_REVENUE_BY_REGION
FROM @MGG_TELCO
FILES = ('daily_revenue_by_region_combined.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

//DYNAMIC TABLES

USE WAREHOUSE VW_ADVANCED_ANALYTICS;
USE DATABASE BD_EMPRESA;
USE SCHEMA PUBLIC;

create or replace TABLE CUST_INFO (
	CUSTID NUMBER(10,0),
	CNAME VARCHAR(100),
	SPENDLIMIT NUMBER(10,2)
);

insert into cust_info values (1001,'John Villarreal',8187.42);
insert into cust_info values (1002,'Austin Carroll',4933.94);
insert into cust_info values (1003,'Connie Curtis',5217.71);
insert into cust_info values (1004,'Michael Harrison',3003.78);
insert into cust_info values (1005,'Amanda Mendoza',3382.70);

create or replace TABLE PROD_STOCK_INV (
	PID NUMBER(10,0),
	PNAME VARCHAR(100),
	STOCK NUMBER(10,2),
	STOCKDATE DATE
);

insert into prod_stock_inv values( 101,'Wooden pegs',500,'2023-12-15');
insert into prod_stock_inv values( 102,'Automated desk',600,'2023-12-15');
insert into prod_stock_inv values( 103,'Multi-layered widtgh',300,'2023-12-15');
insert into prod_stock_inv values( 104,'Quantum tester',100,'2023-12-15');
insert into prod_stock_inv values( 105,'Vision-oriented product',700,'2023-12-15');

-- Lastly, we'll create a table for sales data with products purchased online by various customers:
create or replace TABLE SALESDATA (
	CUSTID NUMBER(10,0),
	PURCHASE VARIANT
);

insert into salesdata select 1001,PARSE_JSON('{"prodid": 101,"purchase_amount": 919.8,"purchase_date": "2023-12-20","quantity": 4}');
insert into salesdata select 1001,PARSE_JSON('{"prodid": 102,"purchase_amount": 505.05,"purchase_date": "2023-12-21","quantity": 4}');
insert into salesdata select 1002,PARSE_JSON('{"prodid": 103,"purchase_amount": 898.92,"purchase_date": "2023-12-21","quantity": 3}');
insert into salesdata select 1004,PARSE_JSON('{"prodid": 104,"purchase_amount": 852.52,"purchase_date": "2023-12-20","quantity": 5}');
insert into salesdata select 1003,PARSE_JSON('{"prodid": 105,"purchase_amount": 546.43,"purchase_date": "2023-12-21","quantity": 2}');

CREATE OR REPLACE DYNAMIC TABLE customer_sales_data_history
    LAG='DOWNSTREAM'
    WAREHOUSE=VW_ADVANCED_ANALYTICS
AS
select 
    s.custid as customer_id,
    c.cname as customer_name,
    s.purchase:"prodid"::number(5) as product_id,
    s.purchase:"purchase_amount"::number(10) as saleprice,
    s.purchase:"quantity"::number(5) as quantity,
    s.purchase:"purchase_date"::date as salesdate
from
    cust_info c inner join salesdata s on c.custid = s.custid
;

CREATE OR REPLACE DYNAMIC TABLE salesreport
    LAG = '90 DAYS'
    WAREHOUSE=VW_ADVANCED_ANALYTICS
AS
    select
        t1.customer_id,
        t1.customer_name, 
        t1.product_id,
        p.pname as product_name,
        t1.saleprice,
        t1.quantity,
        (t1.saleprice/t1.quantity) as unitsalesprice,
        t1.salesdate as CreationTime,
        customer_id || '-' || t1.product_id  || '-' || t1.salesdate AS CUSTOMER_SK,
        LEAD(CreationTime) OVER (PARTITION BY t1.customer_id ORDER BY CreationTime ASC) AS END_TIME
    from 
        customer_sales_data_history t1 inner join prod_stock_inv p 
        on t1.product_id = p.pid
;

-- We’ll add a few new records into the salesdata table: 
insert into salesdata select 1002,PARSE_JSON('{"prodid": 105,"purchase_amount": 200.8,"purchase_date": "2023-12-25","quantity": 6}');
insert into salesdata select 1005,PARSE_JSON('{"prodid": 102,"purchase_amount": 500,"purchase_date": "2023-12-25","quantity": 3}');


CREATE OR REPLACE WAREHOUSE VW_DOCAI WITH WAREHOUSE_SIZE = 'X-LARGE' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 30 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE DATABASE tb_doc_ai;

CREATE OR REPLACE SCHEMA tb_doc_ai.doc_ai_schema;

CREATE OR REPLACE STAGE tb_doc_ai.doc_ai_schema.doc_ai_stage
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

CREATE OR REPLACE STAGE MGG_DOC_AI
 URL = 's3://mggsnowflake/docai/extraction';

COPY FILES INTO @doc_ai_stage
FROM @MGG_DOC_AI;

ALTER STAGE doc_ai_stage REFRESH;

-- SCHEMA FOR THE STREAMLIT APP
CREATE OR REPLACE SCHEMA tb_doc_ai.streamlit_schema;

-- TABLE FOR THE STREAMLIT APP
CREATE OR REPLACE TABLE tb_doc_ai.doc_ai_schema.CO_BRANDING_AGREEMENTS_VERIFIED
(
    file_name string
    , snowflake_file_url string
    , verification_date TIMESTAMP
    , verification_user string
);

--LS @doc_ai_stage;

USE DATABASE BD_CORTEX_RAG;
USE SCHEMA DATA;
USE WAREHOUSE VW_GENAI; 

CREATE OR REPLACE STAGE MGG_RAG
 URL = 's3://mggsnowflake/rag/';

COPY FILES INTO @docs
FROM @MGG_RAG
FILES = ('Cartilla practica para solicitud de credito.pdf');

ALTER STAGE docs REFRESH;

ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

insert into docs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk)
    select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@docs, relative_path) as scoped_file_url,
            func.chunk as chunk
    from 
        directory(@docs),
        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, 
                              relative_path, {'mode': 'LAYOUT'})))) as func;
  
create or replace CORTEX SEARCH SERVICE CC_SEARCH_SERVICE_CS
ON chunk
ATTRIBUTES category
warehouse = VW_GENAI
TARGET_LAG = '90 days'
as (
    select chunk,
        relative_path,
        file_url,
        category
    from docs_chunks_table
);

//App de Compras

USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;

CREATE or REPLACE file format csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE OR REPLACE STAGE MGG_DATA_PURCHASES
 file_format = csvformat
 URL = 's3://mggsnowflake/data/purchases';

create or replace TABLE CUSTOMER_PURCHASE_SUMMARY (
	TRANSACTION_ID VARCHAR(16777216),
	CUSTOMER_ID NUMBER(38,0),
	CUSTOMER_AGE NUMBER(38,0),
	PRODUCT_ID NUMBER(38,0),
	PRODUCT_NAME VARCHAR(16777216),
	PRODUCT_CATEGORY VARCHAR(16777216),
	MERCHANT_ID NUMBER(38,0),
	MERCHANT_NAME VARCHAR(16777216),
	MERCHANT_CATEGORY VARCHAR(16777216),
	TRANSACTION_DATE DATE,
	TRANSACTION_TIME TIME(9),
	QUANTITY NUMBER(38,0),
	TOTAL_PRICE NUMBER(38,2),
	TRANSACTION_CARD VARCHAR(16777216),
	TRANSACTION_CATEGORY VARCHAR(16777216)
);

COPY into CUSTOMER_PURCHASE_SUMMARY
  from @MGG_DATA_PURCHASES;

CREATE OR REPLACE USER juan_perez
PASSWORD = 'Contrasena123!'
DEFAULT_ROLE = 'ACCOUNTADMIN';

CREATE OR REPLACE USER maria_gonzalez
PASSWORD = 'Contrasena123!'
DEFAULT_ROLE = 'ACCOUNTADMIN';

CREATE OR REPLACE USER carlos_lopez
PASSWORD = 'Contrasena123!'
DEFAULT_ROLE = 'ACCOUNTADMIN';

CREATE OR REPLACE USER ana_martinez
PASSWORD = 'Contrasena123!'
DEFAULT_ROLE = 'ACCOUNTADMIN';

CREATE OR REPLACE USER sofia_fernandez
PASSWORD = 'Contrasena123!'
DEFAULT_ROLE = 'ACCOUNTADMIN';

//Conexión a GIT

/*create or replace api integration mggsnowflake_git
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/mariogalvis/mggsnowflake')
    enabled = true
    allowed_authentication_secrets = all;
*/

CREATE OR REPLACE GIT REPOSITORY mggsnowflake_git 
	ORIGIN = 'https://github.com/mariogalvis/mggsnowflake' 
	API_INTEGRATION = 'MGGSNOWFLAKE_GIT';

//App de Reviews

CREATE DATABASE IF NOT EXISTS advanced_analytics;
USE ADVANCED_ANALYTICS.PUBLIC;
CREATE OR REPLACE FILE FORMAT csv_format_nocompression TYPE = csv
FIELD_OPTIONALLY_ENCLOSED_BY = '"' FIELD_DELIMITER = ',' skip_header = 1;

CREATE OR REPLACE STAGE AA_STAGE URL = 's3://sfquickstarts/hol_geo_spatial_ml_using_snowflake_cortex/';

CREATE OR REPLACE TABLE ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS AS
SELECT  $1::NUMBER as order_id,
        $2::VARCHAR as customer_id,
        TO_GEOGRAPHY($3) as delivery_location,
        $4::NUMBER as delivery_postcode,
        $5::FLOAT as delivery_distance_miles,
        $6::VARCHAR as restaurant_food_type,
        TO_GEOGRAPHY($7) as restaurant_location,
        $8::NUMBER as restaurant_postcode,
        $9::VARCHAR as restaurant_id,
        $10::VARCHAR as review
FROM @ADVANCED_ANALYTICS.PUBLIC.AA_STAGE/food_delivery_reviews.csv (file_format => 'csv_format_nocompression');

CREATE OR REPLACE TABLE ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT_TEST as
SELECT TOP 10
    order_id
    , customer_id
    , delivery_location
    , delivery_postcode
    , delivery_distance_miles
    , restaurant_food_type
    , restaurant_location
    , restaurant_postcode
    , restaurant_id
    , review
    , snowflake.cortex.complete('mixtral-8x7b'
        , concat('You are a helpful data assistant and your job is to return a JSON formatted response that classifies a customer review (represented in the <review> section) as one of the following seven sentiment categories (represented in the <categories> section). Return your classification exclusively in the JSON format: {classification: <<value>>}, where <<value>> is one of the 7 classification categories in the section <categories>. 
        
        <categories>
        Very Positive
        Positive
        Neutral
        Mixed 
        Negative 
        Very Negative
        Other
        </categories>
        
        "Other" should be used for the classification if you are unsure of what to put. No other classifications apart from these seven in the <categories> section should be used.
        
        Here are some examples: 
            1. If review is: "This place is awesome! The food tastes great, delivery was super fast, and the cost was cheap. Amazing!", then the output should only be {"Classification": "Very Positive"}
            2. If review is: "Tried this new place and it was a good experience. Good food delivered fast.", then the output should only be {"Classification": "Positive"}
            3. If review is: "Got food from this new joint. It was OK. Nothing special but nothing to complain about either", then the output should only be {"Classification": "Neural"}
            4. If review is: "The pizza place we ordered from had the food delivered real quick and it tasted good. It just was pretty expensive for what we got.", then the output should only be {"Classification": "Mixed"}
            5. If review is: "The hamburgers we ordered took a very long time and when they arrived they were just OK.", then the output should only be {"Classification": "Negative"}
            6. If review is: "This food delivery experience was super bad. Overpriced, super slow, and the food was not that great. Disappointed.", then the output should only be {"Classification": "Very Negative"}
            7. If review is: "An experience like none other", then the output should be "{"Classification": Other"}
        
         It is very important that you do not return anything but the JSON formatted response. 
            
        <review>', review, '</review>
        JSON formatted Classification Response: '
                )
    ) as sentiment_assessment   
    , snowflake.cortex.complete(
        'mixtral-8x7b'
        , concat('You are a helpful data assistant. Your job is to classify customer input <review>. If you are unsure, return null. For a given category classify the sentiment for that category as: Very Positive, Positive, Mixed, Neutral, Negative, Very Negative. Respond exclusively in JSON format.

        {
        food_cost:
        food_quality:
        food_delivery_time:
    
        }
      '  
, review 
, 'Return results'
        )) as sentiment_categories
FROM 
    ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS;

CREATE OR REPLACE TABLE ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_ID VARCHAR(16777216),
	DELIVERY_LOCATION GEOGRAPHY,
	DELIVERY_POSTCODE NUMBER(38,0),
	DELIVERY_DISTANCE_MILES FLOAT,
	RESTAURANT_FOOD_TYPE VARCHAR(16777216),
	RESTAURANT_LOCATION GEOGRAPHY,
	RESTAURANT_POSTCODE NUMBER(38,0),
	RESTAURANT_ID VARCHAR(16777216),
	REVIEW VARCHAR(16777216),
	SENTIMENT_ASSESSMENT VARCHAR(16777216),
	SENTIMENT_CATEGORIES VARCHAR(16777216)
);

COPY INTO ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT
FROM @ADVANCED_ANALYTICS.PUBLIC.AA_STAGE/food_delivery_reviews.csv
FILE_FORMAT = (FORMAT_NAME = csv_format_nocompression);

CREATE OR REPLACE TABLE ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT_ANALYSIS AS
SELECT * exclude (food_cost, food_quality, food_delivery_time, sentiment) ,
         CASE
             WHEN sentiment = 'very positive' THEN 5
             WHEN sentiment = 'positive' THEN 4
             WHEN sentiment = 'neutral'
                  OR sentiment = 'mixed' THEN 3
             WHEN sentiment = 'negative' THEN 2
             WHEN sentiment = 'very negative' THEN 1
             ELSE NULL
         END sentiment_score ,
         CASE
             WHEN food_cost = 'very positive' THEN 5
             WHEN food_cost = 'positive' THEN 4
             WHEN food_cost = 'neutral'
                  OR food_cost = 'mixed' THEN 3
             WHEN food_cost = 'negative' THEN 2
             WHEN food_cost = 'very negative' THEN 1
             ELSE NULL
         END cost_score ,
         CASE
             WHEN food_quality = 'very positive' THEN 5
             WHEN food_quality = 'positive' THEN 4
             WHEN food_quality = 'neutral'
                  OR food_quality = 'mixed' THEN 3
             WHEN food_quality = 'negative' THEN 2
             WHEN food_quality = 'very negative' THEN 1
             ELSE NULL
         END food_quality_score ,
         CASE
             WHEN food_delivery_time = 'very positive' THEN 5
             WHEN food_delivery_time = 'positive' THEN 4
             WHEN food_delivery_time = 'neutral'
                  OR food_delivery_time = 'mixed' THEN 3
             WHEN food_delivery_time = 'negative' THEN 2
             WHEN food_delivery_time = 'very negative' THEN 1
             ELSE NULL
         END delivery_time_score
FROM
  (SELECT order_id ,
          customer_id ,
          delivery_location ,
          delivery_postcode ,
          delivery_distance_miles ,
          restaurant_food_type ,
          restaurant_location ,
          restaurant_postcode ,
          restaurant_id ,
          review ,
          try_parse_json(lower(sentiment_assessment)):classification::varchar AS sentiment ,
          try_parse_json(lower(sentiment_categories)):food_cost::varchar AS food_cost ,
          try_parse_json(lower(sentiment_categories)):food_quality::varchar AS food_quality ,
          try_parse_json(lower(sentiment_categories)):food_delivery_time::varchar AS food_delivery_time
   FROM ADVANCED_ANALYTICS.PUBLIC.ORDERS_REVIEWS_SENTIMENT);


-- Start AI Agent
-- Create database and schema
CREATE OR REPLACE DATABASE sales_intelligence;
CREATE OR REPLACE SCHEMA sales_intelligence.data;
CREATE OR REPLACE WAREHOUSE sales_intelligence_wh;

USE DATABASE sales_intelligence;
USE SCHEMA data;

-- Create tables for sales data
CREATE TABLE sales_conversations (
    conversation_id VARCHAR,
    transcript_text TEXT,
    customer_name VARCHAR,
    deal_stage VARCHAR,
    sales_rep VARCHAR,
    conversation_date TIMESTAMP,
    deal_value FLOAT,
    product_line VARCHAR
);

CREATE TABLE sales_metrics (
    deal_id VARCHAR,
    customer_name VARCHAR,
    deal_value FLOAT,
    close_date DATE,
    sales_stage VARCHAR,
    win_status BOOLEAN,
    sales_rep VARCHAR,
    product_line VARCHAR
);

-- First, let's insert data into sales_conversations
INSERT INTO sales_conversations 
(conversation_id, transcript_text, customer_name, deal_stage, sales_rep, conversation_date, deal_value, product_line)
VALUES
('CONV001', 'Llamada inicial de descubrimiento con el Director de TI y el Arquitecto de Soluciones de Café Dorado Ltda. El cliente mostró un gran interés en las características de nuestra solución empresarial, en particular en las capacidades de automatización del flujo de trabajo. La conversación principal se centró en el cronograma y la complejidad de la integración. Actualmente utilizan Legacy System X para sus operaciones principales y expresaron su preocupación por posibles interrupciones durante la migración. El equipo formuló preguntas detalladas sobre la compatibilidad de las API y las herramientas de migración de datos.

Las acciones incluyen proporcionar un cronograma de integración detallado, programar una revisión técnica exhaustiva con su equipo de infraestructura y compartir casos prácticos de migraciones similares de Legacy System X. El cliente mencionó una asignación presupuestaria del segundo trimestre para iniciativas de transformación digital. En general, fue una interacción positiva con próximos pasos claros.', 'Café Dorado Ltda', 'Descubrimiento', 'Mariana Torres', '2025-01-15 10:30:00', 75000, 'Servicio Empresarial'),

('CONV002', 'Llamada de seguimiento con el Gerente de Operaciones y el Director de Finanzas de FinanCol S.A.. El enfoque principal se centró en la estructura de precios y el cronograma de retorno de la inversión (ROI). Compararon los precios de nuestro Paquete Básico con la oferta para pequeñas empresas del competidor Y. Los puntos clave de conversación incluyeron las opciones de facturación mensual vs. anual, las limitaciones de las licencias de usuario y el potencial ahorro de costos gracias a la automatización de procesos.

El cliente solicitó un análisis detallado del ROI, centrado en el tiempo ahorrado en las operaciones diarias, las mejoras en la asignación de recursos y las ganancias de eficiencia proyectadas. Se comunicaron claramente las limitaciones presupuestarias, con un presupuesto máximo de $30,000 para este año. Mostraron interés en comenzar con el paquete básico, con posibilidad de una actualización en el cuarto trimestre. Los próximos pasos incluyen proporcionar un análisis competitivo y una calculadora de ROI personalizada para la próxima semana.', 'FinanCol S.A.', 'Negociación', 'Mateo Herrera', '2025-01-16 14:45:00', 25000, 'Paquete Básico'),

('CONV003', 'Sesión de estrategia con el CISO y el equipo de Operaciones de Seguridad de TransAndes Logística S.A.S.. Una sesión de 90 minutos sumamente positiva sobre nuestro paquete de Seguridad Premium. El cliente destacó la necesidad inmediata de implementación debido a las recientes actualizaciones de cumplimiento normativo del sector. Nuestras funciones de seguridad avanzadas, en especial la autenticación multifactor y los protocolos de cifrado, se identificaron como la solución perfecta para sus necesidades. El equipo técnico quedó especialmente impresionado con nuestro enfoque de arquitectura de confianza cero y nuestras capacidades de monitorización de amenazas en tiempo real. Ya han obtenido la aprobación presupuestaria y cuentan con la aprobación ejecutiva. La documentación de cumplimiento está lista para su revisión. Las acciones a seguir incluyen: finalizar el cronograma de implementación, programar una auditoría de seguridad y preparar la documentación necesaria para su equipo de evaluación de riesgos. El cliente está listo para avanzar con las negociaciones del contrato.', 'TransAndes Logística S.A.S.', 'Cierre', 'Santiago Valencia', '2025-01-17 11:20:00', 150000, 'Seguridad Premium'),

('CONV004', 'Llamada de descubrimiento integral con el director de tecnología y los jefes de departamento de Moda Caribe S.A.S.. Un equipo de más de 500 empleados en 3 continentes analizó los desafíos actuales de su solución. Se identificaron los principales problemas: fallos del sistema durante picos de uso, capacidades limitadas de generación de informes interdepartamentales y baja escalabilidad para equipos remotos. Un análisis profundo de su flujo de trabajo actual reveló cuellos de botella en el intercambio de datos y la colaboración. Se recopilaron los requisitos técnicos de cada departamento. La demostración de la plataforma se centró en las funciones de escalabilidad y la gestión global de equipos. El cliente se interesó especialmente en nuestro ecosistema de API y nuestro motor de informes personalizado. Próximos pasos: programar un análisis del flujo de trabajo específico para cada departamento y preparar un plan detallado de migración de la plataforma.', 'Moda Caribe S.A.S.', 'Descubrimiento', 'Mariana Torres', '2025-01-18 09:15:00', 100000, 'Servicio Empresarial'),

('CONV005', 'Sesión de demostración exhaustiva con el equipo de análisis de BioSalud Colombia Ltda.. y los responsables de Business Intelligence. La presentación se centró en las capacidades de análisis avanzado, la creación de paneles personalizados y el procesamiento de datos en tiempo real. El equipo quedó especialmente impresionado con nuestra integración de aprendizaje automático y nuestros modelos de análisis predictivo. Se solicitó una comparación con la competencia específicamente con el Líder del Mercado Z y la Start-up Innovadora X. El precio se ajusta a su presupuesto asignado, pero el equipo expresó interés en un compromiso plurianual con la correspondiente estructura de descuentos. Las preguntas técnicas se centraron en la integración del almacén de datos y las capacidades de visualización personalizada. Acciones: preparar una matriz detallada de comparación de características de la competencia y elaborar propuestas de precios plurianuales con diversos escenarios de descuentos.', 'BioSalud Colombia Ltda.', 'Demostración', 'Isabela Mejía', '2025-01-19 13:30:00', 85000, 'Analytics Pro'),

('CONV006', 'Análisis técnico exhaustivo con el equipo de seguridad informática de AgroCol Verde Ltda., el responsable de cumplimiento normativo y los arquitectos de sistemas. La sesión de cuatro horas se centró en la infraestructura de API, los protocolos de seguridad de datos y los requisitos de cumplimiento normativo. El equipo planteó inquietudes específicas sobre el cumplimiento de HIPAA, los estándares de cifrado de datos y la limitación de velocidad de las API. Se analizó en detalle nuestra arquitectura de seguridad, incluyendo cifrado de extremo a extremo, registros de auditoría y protocolos de recuperación ante desastres. El cliente requiere documentación exhaustiva sobre las certificaciones de cumplimiento normativo, en particular SOC 2 y HITRUST. El equipo de seguridad realizó una revisión inicial de la arquitectura y solicitó información adicional sobre la segregación de bases de datos, los procedimientos de copia de seguridad y los protocolos de respuesta a incidentes. Se programó una sesión de seguimiento con su equipo de cumplimiento normativo la próxima semana.', 'AgroCol Verde Ltda.', 'Revisión Técnica', 'Santiago Valencia', '2025-01-20 15:45:00', 120000, 'Seguridad Premium'),

('CONV007', 'Reunión de revisión de contrato con el Asesor Jurídico General, el Director de Adquisiciones y el Gerente de TI de Colosal Seguros S.A. . Se realizó un análisis detallado de los términos del SLA, con especial atención a las garantías de disponibilidad y los tiempos de respuesta del soporte. El equipo legal solicitó modificaciones específicas a las cláusulas de responsabilidad y a los acuerdos de gestión de datos. El equipo de Adquisiciones planteó preguntas sobre las condiciones de pago y la estructura de crédito por servicio. Los puntos clave de discusión incluyeron: compromisos de recuperación ante desastres, políticas de retención de datos y especificaciones de la cláusula de rescisión. El Gerente de TI confirmó que se cumplen los requisitos técnicos, a la espera de la evaluación final de seguridad. Se llegó a un acuerdo sobre la mayoría de los términos, quedando solo por discutir las modificaciones del SLA. El equipo legal proporcionará la redacción revisada del contrato a finales de semana. La sesión, en general, fue positiva, con un camino claro hacia el cierre.', 'Colosal Seguros S.A.', 'Negociación', 'Mateo Herrera', '2025-01-21 10:00:00', 95000, 'Servicio Empresarial'),

('CONV008', 'Análisis trimestral del negocio con el equipo de implementación actual de EcoAseo S.A.. y las posibles partes interesadas en la expansión. La implementación actual en el departamento de Finanzas muestra una sólida tasa de adopción y una mejora del 40 % en los tiempos de procesamiento. El debate se centró en la expansión de la solución a los departamentos de Operaciones y RR. HH. Los usuarios destacaron experiencias positivas con la atención al cliente y la estabilidad de la plataforma. Desafíos identificados en el uso actual: necesidad de informes personalizados adicionales y mayor automatización de los procesos de flujo de trabajo. Requisitos de expansión recopilados del Director de Operaciones: integración de la gestión de inventario, acceso al portal de proveedores y mejoras en las capacidades de seguimiento. El equipo de RR. HH. está interesado en la automatización del flujo de trabajo para la contratación y la incorporación. Próximos pasos: elaboración de planes de implementación específicos para cada departamento y análisis del ROI para la expansión.', 'EcoAseo S.A.', 'Expansión', 'Isabela Mejía', '2025-01-22 14:20:00', 45000, 'Paquete Básico'),

('CONV009', 'Sesión de planificación de emergencia con el equipo ejecutivo y los gerentes de proyecto de RápidoExpress S.A.S.. Necesidad crítica de una implementación rápida debido a una falla actual del sistema. El equipo está dispuesto a pagar una prima por una implementación acelerada y un equipo de soporte dedicado. Se debatió detalladamente el cronograma de implementación acelerada y los recursos necesarios. Requisitos clave: mínima interrupción de las operaciones, migración de datos por fases y protocolos de soporte de emergencia. El equipo técnico confía en cumplir con el ajustado cronograma con recursos adicionales. El patrocinador ejecutivo enfatizó la importancia de la puesta en marcha en 30 días. Próximos pasos inmediatos: finalizar el plan de implementación acelerada, asignar un equipo de soporte dedicado e iniciar los procedimientos de incorporación de emergencia. El equipo se reunirá diariamente para informar sobre el progreso.', 'RápidoExpress S.A.S.', 'Cierre', 'Mariana Torres', '2025-01-23 16:30:00', 180000, 'Seguridad Premium'),

('CONV010', 'Revisión estratégica trimestral con los jefes de departamento y el equipo de análisis de SaludAndes Ltda. . La implementación actual satisface las necesidades básicas, pero el equipo requiere capacidades de análisis más sofisticadas. Un análisis profundo de los patrones de uso actuales reveló oportunidades para la optimización del flujo de trabajo y la generación de informes avanzados. Los usuarios expresaron gran satisfacción con la estabilidad de la plataforma y las funciones básicas, pero requirieron mejoras en la visualización de datos y las capacidades de análisis predictivo. El equipo de análisis presentó requisitos específicos: creación de paneles personalizados, herramientas avanzadas de modelado de datos y funciones de inteligencia empresarial integradas. Se debatió sobre la actualización del paquete actual a Analytics Pro. El análisis del ROI presentado muestra una mejora potencial del 60 % en la eficiencia de los informes. El equipo presentará una propuesta de actualización al comité ejecutivo el próximo mes.', 'SaludAndes Ltda. ', 'Expansión', 'Santiago Valencia', '2025-01-24 11:45:00', 65000, 'Analytics Pro');

-- Now, let's insert corresponding data into sales_metrics
INSERT INTO sales_metrics 
(deal_id, customer_name, deal_value, close_date, sales_stage, win_status, sales_rep, product_line)
VALUES
('DEAL001', 'Café Dorado Ltda', 75000, '2025-02-15', 'Ganado', true, 'Mariana Torres', 'Servicio Empresarial'),

('DEAL002', 'FinanCol S.A.', 25000, '2025-02-01', 'Perdido', false, 'Mateo Herrera', 'Paquete Básico'),

('DEAL003', 'TransAndes Logística S.A.S.', 150000, '2025-01-30', 'Ganado', true, 'Santiago Valencia', 'Seguridad Premium'),

('DEAL004', 'Moda Caribe S.A.S.', 100000, '2025-02-10', 'Pendiente', false, 'Mariana Torres', 'Servicio Empresarial'),

('DEAL005', 'BioSalud Colombia Ltda.', 85000, '2025-02-05', 'Ganado', true, 'Isabela Mejía', 'Analytics Pro'),

('DEAL006', 'AgroCol Verde Ltda.', 120000, '2025-02-20', 'Pendiente', false, 'Santiago Valencia', 'Seguridad Premium'),

('DEAL007', 'Colosal Seguros S.A. ', 95000, '2025-01-25', 'Ganado', true, 'Mateo Herrera', 'Servicio Empresarial'),

('DEAL008', 'EcoAseo S.A.', 45000, '2025-02-08', 'Ganado', true, 'Isabela Mejía', 'Paquete Básico'),

('DEAL009', 'RápidoExpress S.A.S.', 180000, '2025-02-12', 'Ganado', true, 'Mariana Torres', 'Seguridad Premium'),

('DEAL010', 'SaludAndes Ltda. ', 65000, '2025-02-18', 'Pendiente', false, 'Santiago Valencia', 'Analytics Pro');

-- Enable change tracking
ALTER TABLE sales_conversations SET CHANGE_TRACKING = TRUE;

-- Create the search service
CREATE OR REPLACE CORTEX SEARCH SERVICE sales_conversation_search
  ON transcript_text
  ATTRIBUTES customer_name, deal_stage, sales_rep, product_line, conversation_date, deal_value
  WAREHOUSE = sales_intelligence_wh
  TARGET_LAG = '90 days'
  AS (
    SELECT
        conversation_id,
        transcript_text,
        customer_name,
        deal_stage,
        sales_rep,
        conversation_date,
        deal_value,
        product_line
    FROM sales_conversations
    WHERE conversation_date >= '2025-01-01'  -- Fixed date instead of CURRENT_TIMESTAMP
);

CREATE OR REPLACE STAGE models 
    DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE STAGE MGG_GENAI_AGENT
 URL = 's3://mggsnowflake/genai_agent/';

COPY FILES INTO @models
FROM @MGG_GENAI_AGENT
FILES = ('sales_metrics_model.yaml');

ALTER STAGE models REFRESH;
-- End AI Agent

-- Start Multimodal GenAI

USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;

CREATE OR REPLACE STAGE myimages
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

CREATE OR REPLACE STAGE MGG_IMAGES
 URL = 's3://mggsnowflake/images/';

COPY FILES INTO @myimages
FROM @MGG_IMAGES;

ALTER STAGE myimages REFRESH;
-- End Multimodal GenAI

--Start AISQL 24JUN25
-- Run the following statements to create a database, schema, and a table with data loaded from AWS S3.

CREATE DATABASE IF NOT EXISTS AISQL_DB;
CREATE SCHEMA IF NOT EXISTS AISQL_SCHEMA;
--CREATE WAREHOUSE IF NOT EXISTS AISQL_WH_S WAREHOUSE_SIZE=SMALL;

USE AISQL_DB.AISQL_SCHEMA;
--USE WAREHOUSE AISQL_WH_S;
  
create or replace file format csvformat  
  skip_header = 1  
  field_optionally_enclosed_by = '"'  
  type = 'CSV';  

-- Emails table
create or replace stage emails_data_stage  
  file_format = csvformat  
  url = 's3://sfquickstarts/sfguide_getting_started_with_cortex_aisql/emails/';  
  
create or replace TABLE EMAILS (
	USER_ID NUMBER(38,0),
	TICKET_ID NUMBER(18,0),
	CREATED_AT TIMESTAMP_NTZ(9),
	CONTENT VARCHAR(16777216)
);
  
copy into EMAILS  
  from @emails_data_stage;

-- Solutions Center Articles table

create or replace stage sc_articles_data_stage  
  file_format = csvformat  
  url = 's3://sfquickstarts/sfguide_getting_started_with_cortex_aisql/sc_articles/';  

 create or replace TABLE SOLUTION_CENTER_ARTICLES (
	ARTICLE_ID VARCHAR(16777216),
	TITLE VARCHAR(16777216),
	SOLUTION VARCHAR(16777216),
	TAGS VARCHAR(16777216)
);

copy into SOLUTION_CENTER_ARTICLES  
  from @sc_articles_data_stage;

-- Run the following statement to create a Snowflake managed internal stage to store the sample image files.
create or replace stage AISQL_IMAGE_FILES encryption = (TYPE = 'SNOWFLAKE_SSE') directory = ( ENABLE = true );

CREATE OR REPLACE STAGE AISQL_IMAGE_FILES_EXT directory = ( ENABLE = true )
 URL = 's3://mggsnowflake/aisql/';

--list @AISQL_IMAGE_FILES;

COPY FILES INTO @AISQL_IMAGE_FILES
FROM @AISQL_IMAGE_FILES_EXT;

ALTER STAGE AISQL_IMAGE_FILES REFRESH;

-- Image Files table
create or replace table IMAGES as
select to_file(file_url) img_file, 
    DATEADD(SECOND, UNIFORM(0, 13046400, RANDOM()),
    TO_TIMESTAMP('2025-01-01 00:00:00')) as created_at,
    UNIFORM(0, 200, RANDOM()) as user_id,
    * from directory(@AISQL_DB.AISQL_SCHEMA.AISQL_IMAGE_FILES);

-- End AISQL

ALTER WAREHOUSE VW_ADVANCED_ANALYTICS SET WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

DECLARE user_name STRING;
BEGIN
  user_name := CURRENT_USER();
  EXECUTE IMMEDIATE 
    'ALTER USER "' || user_name || '" SET DEFAULT_WAREHOUSE = ''VW_GENAI''';
END;


