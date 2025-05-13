--https://quickstarts.snowflake.com/guide/getting_started_with_pandas_on_snowflake/index.html?index=..%2F..index#1

-- Instale Finance & Economics del Marketplace
-- https://app.snowflake.com/marketplace/listing/GZTSZAS2KF7/snowflake-data-finance-economics?_fsi=JPxvZrjh

-- Databases
CREATE OR REPLACE DATABASE PANDAS_DB;

-- Warehouses
CREATE OR REPLACE WAREHOUSE PANDAS_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;

-- Create a table from the secure shared view
CREATE OR REPLACE TABLE STOCK_PRICE_TIMESERIES AS SELECT * FROM FINANCE__ECONOMICS.CYBERSYN.STOCK_PRICE_TIMESERIES;

select * from STOCK_PRICE_TIMESERIES limit 10;