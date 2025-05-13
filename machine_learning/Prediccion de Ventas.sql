/*https://www.youtube.com/watch?v=KDZpjQZDRyQ&ab_channel=SnowflakeDevelopers
https://quickstarts.snowflake.com/guide/tasty_bytes_snowpark_101_for_data_science/#0

Click -> SafeGraph: frostbyte
Click -> Get
Rename Database -> FROSTBYTE_SAFEGRAPH (all capital letters)
Grant to Additional Roles -> PUBLIC*/


USE ROLE accountadmin;

-- create a development database for data science work
CREATE OR REPLACE DATABASE frostbyte_tasty_bytes_dev;

-- create raw, harmonized, and analytics schemas
-- raw zone for data ingestion
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_dev.raw;
-- harmonized zone for data processing
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_dev.harmonized;
-- analytics zone for development
CREATE OR REPLACE SCHEMA frostbyte_tasty_bytes_dev.analytics;

-- create csv file format
CREATE OR REPLACE FILE FORMAT frostbyte_tasty_bytes_dev.raw.csv_ff 
type = 'csv';

-- create an external stage pointing to S3
CREATE OR REPLACE STAGE frostbyte_tasty_bytes_dev.raw.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = frostbyte_tasty_bytes_dev.raw.csv_ff;


-- create and use a compute warehouse
CREATE OR REPLACE WAREHOUSE tasty_dsci_wh AUTO_SUSPEND = 60;
USE WAREHOUSE tasty_dsci_wh;
show warehouses;
---------------------------------------------------------------
---------------------------------------------------------------
---- CREATING RBAC FOR SNOWPARK 101 DEMO  ---------------------
---------------------------------------------------------------
---------------------------------------------------------------

USE ROLE securityadmin;

CREATE ROLE IF NOT EXISTS tasty_bytes_admin;
CREATE ROLE IF NOT EXISTS tasty_bytes_ds_role;

/* role hierarchy */
USE ROLE ACCOUNTADMIN;
GRANT ROLE tasty_bytes_admin TO ROLE SYSADMIN;
GRANT ROLE tasty_bytes_ds_role TO ROLE tasty_bytes_admin;

/* grant privileges */
USE ROLE ACCOUNTADMIN;

-- databases
GRANT USAGE ON DATABASE frostbyte_tasty_bytes_dev TO ROLE tasty_bytes_ds_role;

--schemas 
GRANT USAGE ON SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_ds_role;
GRANT USAGE ON SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_ds_role;
GRANT USAGE ON SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes_dev TO ROLE tasty_bytes_ds_role;

GRANT ALL ON SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_ds_role;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_ds_role;
GRANT ALL ON SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

--tables
GRANT ALL ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_admin;
GRANT ALL ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_admin;
GRANT ALL ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT ALL ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_ds_role;
GRANT ALL ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_ds_role;
GRANT ALL ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

GRANT CREATE TABLE ON SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_ds_role;
GRANT CREATE TABLE ON SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_ds_role;
GRANT CREATE TABLE ON SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

GRANT CREATE STAGE ON SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_ds_role;
GRANT CREATE STAGE ON SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_ds_role;
GRANT CREATE STAGE ON SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

-- views
GRANT ALL ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_admin;
GRANT ALL ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_admin;
GRANT ALL ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT ALL ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.raw TO ROLE tasty_bytes_ds_role;
GRANT ALL ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.harmonized TO ROLE tasty_bytes_ds_role;
GRANT ALL ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

-- warehouse grants
GRANT ALL ON WAREHOUSE tasty_dsci_wh TO ROLE tasty_bytes_admin;
GRANT ALL ON WAREHOUSE tasty_dsci_wh TO ROLE tasty_bytes_ds_role;

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_admin;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA frostbyte_tasty_bytes_dev.analytics TO ROLE tasty_bytes_ds_role;

---------------------------------------------------------------
---------------------------------------------------------------
-------- CREATE TABLES/VIEWS FOR SNOWPARK 101  ----------------
---------------------------------------------------------------
---------------------------------------------------------------

-- define shift sales table
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_dev.raw.shift_sales(
	location_id NUMBER(19,0),
	city VARCHAR(16777216),
	date DATE,
	shift_sales FLOAT,
	shift VARCHAR(2),
	month NUMBER(2,0),
	day_of_week NUMBER(2,0),
	city_population NUMBER(38,0)
);

-- ingest from S3 into the shift sales table
COPY INTO frostbyte_tasty_bytes_dev.raw.shift_sales
FROM @frostbyte_tasty_bytes_dev.raw.s3load/analytics/shift_sales/;

-- join in SafeGraph data
CREATE OR REPLACE TABLE frostbyte_tasty_bytes_dev.harmonized.shift_sales
  AS
SELECT
    a.location_id,
    a.city,
    a.date,
    a.shift_sales,
    a.shift,
    a.month,
    a.day_of_week,
    a.city_population,
    b.latitude,
    b.longitude,
    b.location_name
FROM frostbyte_tasty_bytes_dev.raw.shift_sales a
JOIN frostbyte_safegraph.public.frostbyte_tb_safegraph_s b
ON a.location_id = b.location_id;

-- promote the harmonized table to the analytics layer for data science development
CREATE OR REPLACE VIEW frostbyte_tasty_bytes_dev.analytics.shift_sales_v
  AS
SELECT * FROM frostbyte_tasty_bytes_dev.harmonized.shift_sales;

-- view shift sales data
SELECT * FROM frostbyte_tasty_bytes_dev.analytics.shift_sales_v;