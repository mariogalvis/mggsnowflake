CREATE OR REPLACE DATABASE BD_cortex_analyst;

CREATE OR REPLACE STAGE CORTEX_ANALYSTSTAGE
  DIRECTORY = (ENABLE = TRUE);

-- ====================================================================
-- CORTEX ANALYST - SCHEMA BANCA
-- ====================================================================

CREATE OR REPLACE SCHEMA revenue_timeseries_banca;
CREATE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DAILY_REVENUE (
  DATE              DATE,
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_PRODUCT (
  DATE              DATE,
  PRODUCT_LINE      VARCHAR(16777216),
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_REGION (
  DATE              DATE,
  SALES_REGION      VARCHAR(16777216),
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

create or replace stage MGG_BANCA ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

COPY FILES INTO @MGG_BANCA FROM @BD_EMPRESA.GOLD.MGG_FILES/csv/BANCA/;

ALTER STAGE MGG_BANCA REFRESH;

COPY INTO DAILY_REVENUE
  FROM @MGG_BANCA
  FILES = ('daily_revenue_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

COPY INTO DAILY_REVENUE_BY_PRODUCT
  FROM @MGG_BANCA
  FILES = ('daily_revenue_by_product_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

COPY INTO DAILY_REVENUE_BY_REGION
  FROM @MGG_BANCA
  FILES = ('daily_revenue_by_region_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

-- ====================================================================
-- CORTEX ANALYST - SCHEMA SEGUROS
-- ====================================================================

CREATE OR REPLACE SCHEMA revenue_timeseries_seguros;
CREATE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DAILY_REVENUE (
  DATE              DATE,
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_PRODUCT (
  DATE              DATE,
  PRODUCT_LINE      VARCHAR(16777216),
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_REGION (
  DATE              DATE,
  SALES_REGION      VARCHAR(16777216),
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

create or replace stage MGG_SEGUROS ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

COPY FILES INTO @MGG_SEGUROS FROM @BD_EMPRESA.GOLD.MGG_FILES/csv/SEGUROS/;

ALTER STAGE MGG_SEGUROS REFRESH;

COPY INTO DAILY_REVENUE
  FROM @MGG_SEGUROS
  FILES = ('daily_revenue_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

COPY INTO DAILY_REVENUE_BY_PRODUCT
  FROM @MGG_SEGUROS
  FILES = ('daily_revenue_by_product_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

COPY INTO DAILY_REVENUE_BY_REGION
  FROM @MGG_SEGUROS
  FILES = ('daily_revenue_by_region_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

-- ====================================================================
-- CORTEX ANALYST - SCHEMA TELCO
-- ====================================================================

CREATE OR REPLACE SCHEMA revenue_timeseries_telco;
CREATE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE DAILY_REVENUE (
  DATE              DATE,
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_PRODUCT (
  DATE              DATE,
  PRODUCT_LINE      VARCHAR(16777216),
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

CREATE OR REPLACE TABLE DAILY_REVENUE_BY_REGION (
  DATE              DATE,
  SALES_REGION      VARCHAR(16777216),
  REVENUE           FLOAT,
  COGS              FLOAT,
  FORECASTED_REVENUE FLOAT
);

create or replace stage MGG_TELCO ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

COPY FILES INTO @MGG_TELCO FROM @BD_EMPRESA.GOLD.MGG_FILES/csv/TELCO/;

ALTER STAGE MGG_TELCO REFRESH;

COPY INTO DAILY_REVENUE
  FROM @MGG_TELCO
  FILES = ('daily_revenue_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

COPY INTO DAILY_REVENUE_BY_PRODUCT
  FROM @MGG_TELCO
  FILES = ('daily_revenue_by_product_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

COPY INTO DAILY_REVENUE_BY_REGION
  FROM @MGG_TELCO
  FILES = ('daily_revenue_by_region_combined.csv')
  FILE_FORMAT = (
    TYPE                          = CSV,
    SKIP_HEADER                   = 1,
    FIELD_DELIMITER               = ',',
    TRIM_SPACE                    = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY  = NONE,
    REPLACE_INVALID_CHARACTERS    = TRUE,
    DATE_FORMAT                   = AUTO,
    TIME_FORMAT                   = AUTO,
    TIMESTAMP_FORMAT              = AUTO,
    EMPTY_FIELD_AS_NULL           = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  )
  ON_ERROR = CONTINUE
  FORCE = TRUE;

  