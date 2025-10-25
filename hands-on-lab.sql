/* ********************************** HANDS ON LAB ********************************************* /
Este archivo SQL es un guía para el Hands on Lab (HoL) sobre la cuenta
de prueba en Snowflake.
Por favor copia todo el contenido de este archivo en un worksheet de Snowflake
************************************************************************************************/

/* ************************************ PARTE 1 ***************************************************
 Definiremos nuestro ambiente: Base de datos y warehouse (capacidad de cómputo)
 Nota: Estas tareas las puedes hacer vía línea de comandos o interfaz gráfica.
*************************************************************************************************/
use role accountadmin;

-- Crear base de datos
----------------------
CREATE OR REPLACE DATABASE DB_EMPRESA COMMENT = 'Base de datos de prueba para HOL EMPRESA';

-- Crear warehouse
CREATE OR REPLACE WAREHOUSE COMPUTE_HOL_WH
WITH WAREHOUSE_SIZE = 'SMALL'
   WAREHOUSE_TYPE = 'STANDARD'
   AUTO_SUSPEND = 15
   AUTO_RESUME = TRUE
   MIN_CLUSTER_COUNT = 1
   MAX_CLUSTER_COUNT = 2
   SCALING_POLICY = 'STANDARD';

-- definir el warehouse, database y schema que utilizaremos.
------------------------------------------------------------
use warehouse COMPUTE_HOL_WH;
use database DB_EMPRESA;
use schema public;

/* ************************************ PARTE 2 **********************************************
 Crearemos  una tabla y un stage externo que apunta a AWS S3.
 Nota: Existen dos tipos de Stage: Interno y externo. El Interno es dentro de Snowflake
       y muy útil cuando no contamos con un data lake o queremos obtener un mejor desempeño.
       Externo puede ser hacia AWS S3, GCP Cloud Storage o Azure Data Lake Storage y Blob Storage
*********************************************************************************************/

-- Creemos nuestra TABLE T_CLIENTES
------------------------------
create or replace table T_CLIENTES
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

-- Creemos nuestro STAGE que apunta a un S3
-------------------------------------------
CREATE STAGE "DB_EMPRESA"."PUBLIC".STG_CLIENTES
URL = 's3://snowflake-workshop-lab/japan/citibike-trips/' -- bucket en S3 a donde está apuntando nuestro Stage
COMMENT = 'Stage Externo para el cargado de datos de Clientes';

-- Listemos los archivos qué tenemos en nuestro Stage
-----------------------------------------------------
list @STG_CLIENTES;

-- Creemos un file format para describir datos que utilizaremos
---------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT DB_EMPRESA.PUBLIC.CSV
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 0
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
TRIM_SPACE = FALSE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
ESCAPE = 'NONE'
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO'
NULL_IF = ('')
COMMENT = 'Formato de archivo para el cargado de los datos de Clientes';

-- Validemos que datos en el Stage, estén en el formato que previamente creamos
select $1, $2, $3, $4, $5
from @stg_clientes/trips_2013_0_0_0.csv.gz
(file_format => CSV)
limit 10;

/* ************************************ PARTE 3 *****************************************
 Copiaremos los datos de S3 a nuestra tabla trips. Para esto se utilizará nuestro
 warehouse previamente creado.
****************************************************************************************/

-- Copiemos los datos y anotemos el tiempo de ejecución (lo utilizaremos en un momento)
---------------------------------------------------------------------------------------
copy into T_CLIENTES
from @stg_clientes/trips
file_format=CSV;

-- ¿Cuántos registros cargamos a la tabla?
-----------------------------------------
select count(*) from T_CLIENTES;

-- Hagamos una prueba rápida de performance.
--------------------------------------------
truncate table T_CLIENTES;


-- Cambiemos el tamaño del warehouse
------------------------------------
ALTER WAREHOUSE "COMPUTE_HOL_WH"
SET WAREHOUSE_SIZE = 'XLARGE'
    AUTO_SUSPEND = 15
    AUTO_RESUME = TRUE;

-- Copiamos los datos a la tabla y comparemos con el tiempo de ejecución anterior.
----------------------------------------------------------------------------------
copy into T_CLIENTES
from @stg_clientes/trips
file_format=CSV;

-- ¿Cuántos registros tenemos en la tabla?
-----------------------------------------
select count(*) from T_CLIENTES;

-- Ahora, reduzcamos el tamaño del warehouse
--------------------------------------------
alter warehouse COMPUTE_HOL_WH set warehouse_size = 'SMALL';


/* ************************************ PARTE 4 *****************************************
 Utilizaremos el cache y el zero copy cloning. Estas funcionalidades son óptimas para
 ahorrar tiempos de procesamiento y almacenamiento.
 Vamos adelante!
****************************************************************************************/

-- Demos un vistazo rápido a nuestros datos (solo 20 registros)
---------------------------------------------------------------
select * from T_CLIENTES limit 20;

-- Analicemos y ejecutemos la siguiente consulta:
-------------------------------------------------
select
date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)",
60*("avg distance (km)"/"avg duration (mins)") "Speed km/h"
from T_CLIENTES
group by 1 order by 1;

-- Volvamos a ejecutar el query anterior para ver el uso del caché
------------------------------------------------------------------
-- Ahora utilicemos el Zero Copy Cloning!
-- Esta funcionalidad puede aplicarse sobre una tabla, esquema o incluso una base de datos completa!
----------------------------------------------------------------------------------------------------

-- Clonemos nuestra tabla trips
-------------------------------
create table t_clientes_dev CLONE t_clientes;

-- Clonemos nuestra BD
----------------------
create or replace database DB_EMPRESA_DEV CLONE DB_EMPRESA;

-- Un error intencional. Borremos la base de datos de producción.
------------------------------------------
drop database DB_EMPRESA;

-- No hay que llamar un DBA para reestablecer un buckup.
undrop database DB_EMPRESA;

-- actualicemos una columna para simular un error
-------------------------------------------------
update t_clientes set start_station_name = 'UUPS! Un error de actualizacion.';

select
start_station_name as "station",
count(*) as "rides"
from t_clientes
group by 1
order by 2 desc
limit 20;

-- Todo es auditable en Snowflake.
-- Recuperemos el id de la última transacción
------------------------------------------------------------------------------
set query_id =
(select query_id from
table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time limit 1);

select $query_id;

-- Recreamos la tabla antes de la última ejecución
--------------------------------------------------------
create or replace table t_clientes as
(select * from t_clientes before (statement => $query_id));

-- Verifiquemos que nuestros datos esten bien.
select
start_station_name as "station",
count(*) as "rides"
from t_clientes
group by 1
order by 2 desc
limit 20;


/* ************************************ PARTE 5 *****************************************
 Archivos JSON no son un problema en Snowflake.
 Vamos adelante!
****************************************************************************************/
-- Creemos una nueva base de datos
----------------------------------
create or replace database db_clima;

-- Creemos una tabla
--------------------
create or replace table t_json_datos_clima (v variant);

-- Creemos un nuevo Stage
-------------------------
create stage stg_clima
url = 's3://snowflake-workshop-lab/weather-nyc';

-- ¿Qué archivos tenemos en el nuevo Stage?
------------------------------------------
list @stg_clima;

-- Copiemos datos del stage a la tabla
--------------------------------------
copy into t_json_datos_clima
from @stg_clima
file_format = (type=json);

-- ¿Qué datos tiene la tabla?
----------------------------
select * from t_json_datos_clima limit 10;

-- Creemos una vista con el json aplanado
-----------------------------------------
create or replace view vw_json_datos_clima as
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
  from t_json_datos_clima
  where city_id = 5128638;

-- Consultemos la información del clima para un mes
---------------------------------------------------
select *
from vw_json_datos_clima
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

-- Crucemos y analicemos la información de viajes y clima
---------------------------------------------------------
select weather as conditions
  ,count(*) as num_trips
from DB_EMPRESA.public.t_clientes
left outer join vw_json_datos_clima
  on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


/* ************************************ PARTE 6 *****************************************
 Enmascaramiento dinámico para gobernar los datos evaluando el rol que desea consumir los datos
 Fácil y rápido!
****************************************************************************************/
use role accountadmin;
use database DB_EMPRESA;

-- Creemos un nuevo rol
-----------------------
create or replace role OPERADOR_JUNIOR;

-- Agrega tu nombre de usuario
------------------------------
grant role OPERADOR_JUNIOR to user TU_USUARIO; ------ REEMPLAZA POR TU USUARIO

-- Permisos para nuestro Operador Junior
-----------------------------------
grant usage on database DB_EMPRESA to role OPERADOR_JUNIOR;
grant usage on schema public to role OPERADOR_JUNIOR;
grant select on all tables in schema DB_EMPRESA.public to role OPERADOR_JUNIOR;
grant usage on database db_clima to role OPERADOR_JUNIOR;
grant usage on warehouse COMPUTE_HOL_WH to role OPERADOR_JUNIOR;

-- Creemos una política de enmascaramiento dinámico
---------------------------------------------------
create or replace masking policy membership_mask as (val string)
returns string ->
case
  when current_role() in ('OPERADOR_JUNIOR') then '*******'
  else val
end;

-- Asociemos la política de enmascaramiento a una tabla y columna
-----------------------------------------------------------------
alter table t_clientes modify column membership_type set masking policy membership_mask;

-- Consultemos la tabla con el rol del administrador
----------------------------------------------------
select membership_type, count(1)
from t_clientes
group by membership_type;

-- Consultemos la tabla con el rol del Operador junior
-----------------------------------------------------------------
use role OPERADOR_JUNIOR;

-- Confirmemos que los datos no serán expuestos al Operador junior
-------------------------------------------------------------
select membership_type, count(1)
from t_clientes
group by membership_type;

select membership_type, * from t_clientes
limit 100;


/* ********************* PARTE 7 - usar Gen AI ahora es muy fácil! *******************************
******************************************************************************************/
use role accountadmin;

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

-- Exploremos la tabla CALL_TRANSCRIPTS
select * from CALL_TRANSCRIPTS limit 20;

-- Traducir sin necesidad de herramientas externas
update CALL_TRANSCRIPTS
set damage_type = snowflake.cortex.translate(damage_type,'en','es'),
    transcript = snowflake.cortex.translate(transcript,'en','es'),
    language = 'Español'
where language='English';

-- Análisis de sentimientos con Cortex
select transcript, snowflake.cortex.sentiment(transcript) 
from call_transcripts 
where language = 'Español';

-- Utilizar LLMs de vanguardia y de múltiples fabricantes sin sacar los datos de nuestro entorno de seguridad
select transcript,
       snowflake.cortex.complete('mistral-large',concat('resume en max. 30 palabras:',transcript)) as summary,
       snowflake.cortex.count_tokens('mistral-large',concat('resume en max. 30 palabras:',transcript)) as number_of_tokens 
from call_transcripts 
where language = 'Español' 
limit 10;

-- Resolver preguntas con LLMs sin necesidad de APIs!
select snowflake.cortex.complete('claude-3-5-sonnet','Que es Lulo Bank en Colombia?');

-- Resolver tareas complejas con GenAI LLMs muy fácil!
select 
snowflake.cortex.complete('claude-3-5-sonnet',
           concat('Arma un XML, con los siguientes atributos: 
           resumen del problema en no más de 100 palabras, 
           sentimiento del cliente (negativo, neutro, positivo),
           producto,
           tiempo de resolución,
           Solución otorgada (máximo 10 palabras).
           Todo esto utilizando la transcripción de siguiente llamada: ',transcript) 
)
from call_transcripts 
where language = 'Español'
limit 2;


/* ************** PARTE 8 - Marketplace & Cortex Playground  **********
************************************************************************/
/*
1. Vamos al MARKERPLACE y sincronicemos un dataset llamado: YOUTUBE_PROFILES__POSTS__COMMENTS

2. Vamos a “AI & ML” -> “Studio” -> “Cortex Playground” 
3. Selecciona la base de datos recien creada, el esquema PUBLIC y la tabla YOUTUBE_PROFILES_NEW
4. Columna DESCRIPTION y filtro SUBSCRIBERS. 
5. Selecciona 2 LLMs a comparar
6. Selecciona un registro con múltiples suscriptores y ordenale la siguiente tarea:

7. 
Genérame la traducción del texto y luego créame un json y un XML con los siguientes atributos: tipo del perfil, sentimiento (positivo, negativo, neutro), resumen (no más de 5 palabras), tipo de contenido, posible nombre para el canal, contacto y solicita suscribirse.

*/

/* ***************** PARTE 9 - Trabajar con NOTEBOOKS ahora es muy fácil!  *******************
****************************************************************************************/

1. Descarguemos el archivo 02 - Recorrido_por_Notebooks.ipynb
2. Carguemos el Notebook a Snowflake
3. Continuemos con el paso a paso del Notebook

/* ******************************* PARTE 10 - Cortex Analyst  **********************************
 Hablar con datos es realmente fácil!
****************************************************************************************/

-- Descarguemos el contenido de la carpeta-> Cortex Analyst
-- Creemos un nuevo Worksheet
-- Carguemos en nuestro Worksheet el contenido del archivo-> Script - SQL
-- Continuemos con el paso a paso del Script del paso anterior.

/* ******************************* PARTE 11 - Opcional **********************************
 Eliminemos los elementos creados en este laboratorio
****************************************************************************************/
use role accountadmin;
show shares;
drop share if exists clientes_share;
drop database if exists DB_EMPRESA;
drop database if exists DB_EMPRESA_DEV;
drop database if exists db_clima;
drop warehouse if exists COMPUTE_HOL_WH;
drop role if exists OPERADOR_JUNIOR;


-- Links recomendados:
-- Explore múltiples labs y arquitecturas para implementar en minutos
-- https://developers.snowflake.com/solutions/

-- Lab - RAG sobre documentos PDF
-- https://developers.snowflake.com/solution/rag-based-ai-app-for-equipment-maintenance-using-snowflake-cortex/

-- Lab - Getting Started with Cortex Analyst: Augment BI with AI
-- https://quickstarts.snowflake.com/guide/getting_started_with_cortex_analyst

-- Lab - Extracting Insights from Unstructured Data with Document AI
-- https://quickstarts.snowflake.com/guide/tasty_bytes_extracting_insights_with_docai

-- Lab - Run 3 useful LLM inference jobs in minutes with Snowflake Cortex
-- https://medium.com/snowflake/run-3-useful-llm-inference-jobs-in-minutes-with-snowflake-cortex-743a6096fff8

-- Lab - Build A Document Search Assistant using Vector Embeddings in Cortex AI
-- https://quickstarts.snowflake.com/guide/asking_questions_to_your_own_documents_with_snowflake_cortex
