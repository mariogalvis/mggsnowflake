-- Run the following statements to create a database, schema, and a table with data loaded from aws s3.

use role accountadmin;

create or replace database dash_mcp_db;
create or replace schema data;
create warehouse if not exists dash_wh_s warehouse_size=small;

use dash_mcp_db.data;
use warehouse dash_wh_s;
  
create or replace file format mcp_csvformat  
  skip_header = 1  
  field_optionally_enclosed_by = '"'  
  type = 'csv';  

create or replace table dim_customers (
    customer_id varchar(10) not null,
    first_name varchar(50) not null,
    last_name varchar(50) not null,
    email varchar(100) not null,
    phone varchar(30),
    date_of_birth date,
    ssn_hash varchar(32),
    address varchar(200),
    city varchar(50),
    state varchar(2),
    zip_code varchar(10),
    region varchar(20),
    customer_segment varchar(20),
    credit_score number(3,0),
    annual_income number(10,2),
    net_worth number(12,2),
    join_date date,
    status varchar(20),
    acquisition_channel varchar(30),
    lifetime_value number(10,2),
    risk_profile varchar(10),
    constraint uk_dim_customers_email unique (email),
    constraint pk_dim_customers primary key (customer_id)
);

create or replace stage customers_data_stage  
  file_format = mcp_csvformat  
  url = 's3://sfquickstarts/sfguide-getting-started-with-snowflake-mcp-server/customers/';  

copy into dim_customers
  from @customers_data_stage;

create or replace stage campaigns_data_stage  
  file_format = mcp_csvformat  
  url = 's3://sfquickstarts/sfguide-getting-started-with-snowflake-mcp-server/campaigns/';

create or replace table dim_campaigns (
    campaign_id varchar(10) not null,
    campaign_name varchar(200) not null,
    campaign_type varchar(20),
    objective varchar(20),
    product_promoted varchar(8),
    start_date date,
    end_date date,
    budget number(12,2),
    actual_spend number(12,2),
    target_audience varchar(20),
    target_region varchar(20),
    impressions number(10,0),
    clicks number(8,0),
    conversions number(8,0),
    revenue_generated number(12,2),
    roi number(8,2),
    status varchar(20),
    constraint pk_dim_campaigns primary key (campaign_id)
);

copy into dim_campaigns  
  from @campaigns_data_stage;

create or replace stage marketing_data_stage  
  file_format = mcp_csvformat  
  url = 's3://sfquickstarts/sfguide-getting-started-with-snowflake-mcp-server/marketing/';

create or replace table fact_marketing_responses (
    response_id varchar(12) not null,
    customer_id varchar(10) not null,
    campaign_id varchar(10) not null,
    contacted_date date not null,
    geo_id varchar(8),
    response_type varchar(20),
    conversion_value number(10,2) default 0,
    channel_used varchar(20),
    engagement_score number(3,0),
    constraint pk_fact_marketing_responses primary key (response_id),
    constraint fk_mkt_customer foreign key (customer_id) references dim_customers(customer_id),
    constraint fk_mkt_campaign foreign key (campaign_id) references dim_campaigns(campaign_id)
);

copy into fact_marketing_responses
  from @marketing_data_stage;

create or replace stage risk_assessments_data_stage  
  file_format = mcp_csvformat  
  url = 's3://sfquickstarts/sfguide-getting-started-with-snowflake-mcp-server/risk_assessment/';

create or replace table fact_risk_assessments (
	assessment_id varchar(10) not null,
	customer_id varchar(10) not null,
	assessment_date date not null,
	credit_risk_score number(5,2),
	fraud_risk_score number(5,2),
	aml_risk_score number(5,2),
	overall_risk_rating varchar(15),
	risk_factors varchar(500),
	review_required number(1,0) default 0,
	last_review_date date,
	next_review_date date,
	constraint pk_fact_risk_assessments primary key (assessment_id),
	constraint fk_risk_customer foreign key (customer_id) references dim_customers(customer_id)
);

copy into fact_risk_assessments
  from @risk_assessments_data_stage;

create or replace stage transactions_data_stage
  file_format = mcp_csvformat
  url = 's3://sfquickstarts/sfguide-getting-started-with-snowflake-mcp-server/transactions/';

create or replace table fact_transactions (
    transaction_id varchar(16777216),
    account_id varchar(16777216),
    customer_id varchar(16777216),
    transaction_date timestamp_ntz(9),
    transaction_type varchar(16777216),
    amount number(38,2),
    balance_after number(38,2),
    merchant_name varchar(16777216),
    merchant_category varchar(16777216),
    channel varchar(16777216),
    location varchar(16777216),
    description varchar(16777216),
    is_flagged number(38,0),
    fraud_score number(38,2)
);

copy into fact_transactions
  from @transactions_data_stage;

create or replace stage support_data_stage
  file_format = mcp_csvformat
  url = 's3://sfquickstarts/sfguide-getting-started-with-snowflake-mcp-server/support/';

create or replace table fact_support_tickets (
    ticket_id varchar(11) not null,
    customer_id varchar(10) not null,
    account_id varchar(10),
    created_date date not null,
    geo_id varchar(8),
    category varchar(30),
    subcategory varchar(50),
    priority varchar(10),
    channel varchar(20),
    subject varchar(500),
    description varchar(1000),
    status varchar(20),
    assigned_agent varchar(8),
    resolution_date date,
    resolution_time_hours number(8,2),
    satisfaction_score number(1,0),
    first_response_time_minutes number(8,2),
    constraint pk_fact_support_tickets primary key (ticket_id),
    constraint fk_support_customer foreign key (customer_id) references dim_customers(customer_id)
);

copy into fact_support_tickets
  from @support_data_stage;

-- Run the following statement to create a snowflake managed internal stage to store the semantic model files.
create or replace stage semantic_models encryption = (type = 'snowflake_sse') directory = ( enable = true );

-- Enable cross-region inference
alter account set cortex_enabled_cross_region = 'any_region';

select 'Congratulations! The setup is complete. You can now proceed to the next step in the guide.' as message;


create or replace mcp server dash_mcp_server from specification
$$
tools:
  - name: "Support Tickets Search Service"
    identifier: "dash_mcp_db.data.support_tickets"
    type: "CORTEX_SEARCH_SERVICE_QUERY"
    description: "A tool that performs keyword and vector search over support tickets and call transcripts."
    title: "Support Tickets"
$$;

-- parar escribir esto por si acaso se pierden los network policy

-- Crear un usuario de emergencia (ajusta parámetros reales y añade MFA después)
CREATE USER BREAK_GLASS_ADMIN PASSWORD = 'wElcome1234567'
  DEFAULT_ROLE = ACCOUNTADMIN MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE ACCOUNTADMIN TO USER BREAK_GLASS_ADMIN;
-- No le asignes network policy a este usuario.


CREATE OR REPLACE NETWORK POLICY mcp_policy
  ALLOWED_IP_LIST = (
    '186.84.21.107/32'--,   -- tu Mac (dinámica; se puede actualizar luego)
   -- '18.117.233.53/32',  -- tu EC2
   -- '34.203.10.77/32'    -- tu IP estática (VPN/VPS) -> tu salvavidas
  );
-- Si usas IPv6, agrega ALLOWED_IP_LIST_V6 = ('<tu-prefijo>/64');

ALTER USER mggsnowflake4 SET NETWORK_POLICY = mcp_policy;

-- Confirma que el DB y Schema existen
SHOW DATABASES LIKE 'DASH_MCP_DB';
SHOW SCHEMAS IN DATABASE DASH_MCP_DB LIKE 'DATA';

-- Lista MCP servers en la cuenta o en el schema
SHOW MCP SERVERS IN ACCOUNT;
SHOW MCP SERVERS IN SCHEMA DASH_MCP_DB.DATA;

-- (Opcional) Describe si aparece
DESCRIBE MCP SERVER DASH_MCP_SERVER;

-- Accesos de descubrimiento
GRANT USAGE ON DATABASE DASH_MCP_DB       TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA   DASH_MCP_DB.DATA  TO ROLE PUBLIC;

-- Acceso al MCP server
GRANT USAGE ON MCP SERVER DASH_MCP_SERVER TO ROLE PUBLIC;

-- Accesos a los objetos que usan los tools del MCP:
-- (ajusta nombres a tus objetos reales)
GRANT USAGE ON CORTEX SEARCH SERVICE SUPPORT_TICKETS TO ROLE PUBLIC;
--GRANT USAGE ON SEMANTIC VIEW DASH_MCP_DB.DATA.REVENUE_SEMANTIC_VIEW TO ROLE PUBLIC;

-- Verifica
SHOW GRANTS ON MCP SERVER DASH_MCP_SERVER;
SHOW GRANTS TO ROLE PUBLIC;

/*
en Cursor

{
    "mcpServers": {
      "Snowflake": {
        "url": "https://TPB2345X.us-west-2.snowflakecomputing.com/api/v2/databases/dash_mcp_db/schemas/data/mcp-servers/dash_mcp_server",
            "headers": {
              "Authorization": "Bearer einIjoiRVMyNTYifQ.eyJwIjoiMjEyMzg3NTg4OjIxMjM4NzU4OCIsImlzcyI6IlNGOjEwNDkiLCJleHAiOjE3NjIwOTY3MDV9.diQYTMc1QsbehqndvJY4vLbnGij550bW_xJzLycxy99XIvEh4lBAr-gMaHWnD3fnC1TIozGNaan1TOaExH_OSg"
            }
      }
    }
}

*/