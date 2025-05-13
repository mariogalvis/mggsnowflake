-- Create or use your metadata database
create database metadata_db;

-- Create or use your metadata database Schema
create schema metadata_schema;

-- Create your metadata table
create OR REPLACE table metadata_schema.column_descriptions 
(  run_id datetime, 
    model_name varchar, 
    database_name varchar, 
    schema_name varchar, 
    table_name varchar, 
    column_name varchar, 
    description varchar, 
    dt_updated datetime default current_timestamp());



CREATE OR REPLACE PROCEDURE generate_ai_description(
    model_name STRING,
    database_name STRING,
    schema_name STRING,
    table_name STRING ,
    column_name STRING ,
    action STRING,
    parallel_degree int,
    parallel_degree_table int,
    max_words_length_description int,
    show_sensitive_info boolean,
    sample_rate int,
    type_of_object string
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS

DECLARE sampled_values STRING;
generated_description STRING;
full_table_name STRING;
list_sql_execute ARRAY;
column_name_individual STRING;
table_name_individual STRING;
schema_name_individual STRING;
run_id datetime default current_timestamp();
desc_show_sensitive_info STRING DEFAULT 'Don''t show sensitive information in the description.';
type_of_object_individual STRING;
query STRING;
rs RESULTSET;

BEGIN
            
    IF (:show_sensitive_info) THEN
        desc_show_sensitive_info := 'You can show sensitive information in the description.';
    END IF;
        
    -- If a specific column is provided
    IF (:column_name IS NOT NULL) THEN
        -- Sample up to 200 values from the specified column
        full_table_name := (SELECT CONCAT(:database_name, '.', :schema_name, '.', :table_name)::STRING);
                
        query := 'SELECT LISTAGG(DISTINCT column_value, '', '') list_of_values
            FROM (
                SELECT distinct identifier(?) AS column_value
                FROM identifier(?) 
                LIMIT '||:sample_rate||'
            )';
            
        rs := (EXECUTE IMMEDIATE :query USING (column_name, full_table_name ));
        
        LET cur1 CURSOR FOR rs;

          FOR row_variable IN cur1 DO
            sampled_values := row_variable.list_of_values;
          END FOR;
          
        --sampled_values := rs::VARCHAR;

        -- Generate column description if action is 'GENERATE' or 'FULL'
        IF (:action IN ('GENERATE', 'FULL')) THEN
            generated_description := (
                SELECT REPLACE(
                    SNOWFLAKE.CORTEX.COMPLETE(
                    :model_name, 
                    'Generate a concise description for column ' || :column_name || 
                    ' in ' || :type_of_object ||' '|| :table_name || ' containing the following values: ' || :sampled_values||
                    ' . Don'' t use more than ' ||:max_words_length_description || ' words. '||
                    ' ' ||:desc_show_sensitive_info
                ),char(39),char(39)||char(39))
            );
            
            -- Store the generated description in metadata
            INSERT INTO metadata_schema.column_descriptions (run_id,model_name,database_name, schema_name, table_name, column_name, description)
            VALUES (:run_id, :model_name, :database_name, :schema_name, :table_name, :column_name, :generated_description);
        END IF;
        
        -- Apply the generated description to the column if action is 'APPLY' or 'FULL'
        IF (:action IN ('APPLY', 'FULL')) THEN
            IF (:action IN ('APPLY')) THEN
                generated_description := (
                    SELECT REPLACE(c.description,char(39),char(39)||char(39)) FROM metadata_schema.column_descriptions c
                    WHERE c.database_name = :database_name AND c.schema_name = :schema_name 
                          AND c.table_name = :table_name AND c.column_name = :column_name
                    ORDER BY DT_UPDATED DESC LIMIT 1
                );
            END IF;
            EXECUTE IMMEDIATE 'ALTER ' || :type_of_object || ' ' || :database_name || ' .' || :schema_name || '.' || :table_name || ' ALTER COLUMN ' || :column_name || ' COMMENT ' ||               char(39)||:generated_description||char(39);
        END IF;
    END IF;

    -- If a table is specified but not a column, retrieve all columns from ACCOUNT_USAGE.COLUMNS
    IF (:table_name IS NOT NULL AND :column_name IS NULL) THEN
        IF (:type_of_object IS NULL) THEN
            type_of_object := (
                SELECT MAX(case when table_type='BASE TABLE' then 'TABLE' else table_type end)
                FROM SNOWFLAKE.ACCOUNT_USAGE.tables t
                WHERE t.table_catalog = :database_name 
                  AND t.table_schema = :schema_name     
                  AND t.table_name = :table_name          
                  and deleted is null);
        END IF;
        
        list_sql_execute := (SELECT ARRAY_agg( 'call generate_ai_description('''||:model_name||''','''||:database_name||''','''||:schema_name||''','''||:table_name||''','''||column_name||''','''||:action||''','||:parallel_degree||','||:parallel_degree_table||','||:max_words_length_description||','||:show_sensitive_info||','||:sample_rate||','''||:type_of_object||''')')
            FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS t
            WHERE t.table_catalog = :database_name 
              AND t.table_schema = :schema_name 
              AND t.table_name = :table_name              
              and deleted is null);

        CALL EXECUTE_SQL_IN_PARALLELL(:list_sql_execute,:parallel_degree);
        
        -- Generate table-level description based on columns descriptions
        sampled_values := (
            SELECT LISTAGG(c.description, ' ') WITHIN GROUP (ORDER BY c.column_name)
            FROM metadata_schema.column_descriptions c
            WHERE c.database_name = :database_name AND c.schema_name = :schema_name AND c.table_name = :table_name
            and c.column_name is not null
        );

        IF (:action IN ('GENERATE', 'FULL')) THEN
            generated_description := (
                SELECT 
                REPLACE(
                    SNOWFLAKE.CORTEX.COMPLETE(
                        :model_name, 
                        'Generate a concise description for '|| :type_of_object ||' '|| :table_name || 
                        ' based on the following column descriptions: ' || :sampled_values||' but don''t list them in the description. '||
                        ' . Don'' t use more than ' ||:max_words_length_description ||' words'||
                        ' ' ||:desc_show_sensitive_info
                    )
                    ,char(39),char(39)||char(39))
            );
            
            -- Store table description in metadata
            INSERT INTO metadata_schema.column_descriptions (run_id,model_name,database_name, schema_name, table_name, description)
            VALUES (:run_id,:model_name, :database_name, :schema_name, :table_name, :generated_description);
        END IF;
        
        -- Apply table description if action is 'APPLY' or 'FULL'
        IF (:action IN ('APPLY', 'FULL')) THEN
            IF (:action IN ('APPLY')) THEN
                generated_description := (
                SELECT REPLACE(c.description,char(39),char(39)||char(39)) FROM metadata_schema.column_descriptions c
                WHERE c.database_name = :database_name AND c.schema_name = :schema_name 
                      AND c.table_name = :table_name
                      and c.column_name is null
                ORDER BY DT_UPDATED DESC LIMIT 1
            );
            END IF;
            
            EXECUTE IMMEDIATE 'COMMENT ON '||:type_of_object|| ' ' || :database_name || '.' || :schema_name || '.' || :table_name || ' IS ' || char(39)||:generated_description||char(39);
        END IF;
    END IF;

    -- If a schema is specified but not a table
    IF (:schema_name IS NOT NULL AND :table_name IS NULL) THEN
        
        list_sql_execute := (
            SELECT ARRAY_agg( 'call generate_ai_description('''||:model_name||''','''||:database_name||''','''||:schema_name||''','''
            ||table_name||''',NULL,'''||:action||''','||:parallel_degree
            ||','||:parallel_degree_table||','
            ||:max_words_length_description||','||:show_sensitive_info||','||:sample_rate
            ||','''||case when table_type='BASE TABLE' then 'TABLE' else table_type end||''')')
            FROM SNOWFLAKE.ACCOUNT_USAGE.tables t
            WHERE t.table_catalog = :database_name 
              AND t.table_schema = :schema_name          
              and deleted is null);

        CALL EXECUTE_SQL_IN_PARALLELL(:list_sql_execute,:parallel_degree_table);
        
        sampled_values := (
            SELECT LISTAGG(description, ' ') WITHIN GROUP (ORDER BY table_name)
            FROM metadata_schema.column_descriptions
            WHERE database_name = :database_name AND schema_name = :schema_name
            and table_name is not null
            and column_name is null
        );
       
        IF (:action IN ('GENERATE', 'FULL')) THEN
            generated_description := (
                SELECT 
                REPLACE(
                    SNOWFLAKE.CORTEX.COMPLETE(
                    :model_name, 
                    'Generate a concise description for schema ' || :schema_name || 
                    ' based on the following table descriptions: ' || :sampled_values || ' but don''t list them in the description. '||
                    ' . Don'' t use more than ' ||:max_words_length_description ||' words'||
                    ' ' ||:desc_show_sensitive_info
                )
                ,char(39),char(39)||char(39))
            );
            
            INSERT INTO metadata_schema.column_descriptions (run_id,model_name, database_name, schema_name, description)
            VALUES (:run_id,:model_name, :database_name, :schema_name, :generated_description);
        END IF;
        
        IF (:action IN ('APPLY', 'FULL')) THEN
            IF (:action IN ('APPLY')) THEN
                generated_description := (
                    SELECT c.description FROM metadata_schema.column_descriptions c
                    WHERE c.database_name = :database_name AND c.schema_name = :schema_name
                    and c.table_name is null
                    ORDER BY dt_updated DESC LIMIT 1
                );
            
            END IF;
            EXECUTE IMMEDIATE 'COMMENT ON SCHEMA ' || :database_name || '.' || :schema_name || ' IS ' || char(39)||:generated_description||char(39);
        END IF;
    END IF;

    -- If a database is specified but not a schema
    IF (:database_name IS NOT NULL AND :schema_name IS NULL) THEN
    
        LET res1 RESULTSET DEFAULT (
            SELECT s.schema_name s_name
            FROM SNOWFLAKE.ACCOUNT_USAGE.schemata s
            WHERE s.catalog_name = :database_name 
              and deleted is null    );
              
        LET cur1 CURSOR FOR res1;
        
        -- Iterate through each column and generate descriptions
        FOR schi IN cur1 DO
            schema_name_individual := schi.s_name;
            CALL generate_ai_description(:model_name, :database_name, :schema_name_individual, NULL, NULL, :action, :parallel_degree,:parallel_degree_table, 
                :max_words_length_description, :show_sensitive_info,:sample_rate, NULL);
        END FOR;

        sampled_values := (
            SELECT LISTAGG(d.description, ' ') WITHIN GROUP (ORDER BY d.schema_name)
            FROM metadata_schema.column_descriptions d 
            inner join SNOWFLAKE.ACCOUNT_USAGE.schemata s 
            on d.schema_name=s.schema_name
            WHERE d.database_name = :database_name 
            and d.table_name is null
            ORDER BY dt_updated desc limit 1
            
        );

        IF (:action IN ('GENERATE', 'FULL')) THEN
            generated_description := (
                SELECT 
                    REPLACE(
                            SNOWFLAKE.CORTEX.COMPLETE(
                            :model_name, 
                            'Generate a concise description for database ' || :database_name || 
                            ' based on the following schema descriptions: ' || :sampled_values|| ' but don''t list them in the description. '||
                            ' . Don'' t use more than ' ||:max_words_length_description ||' words'||
                            ' ' ||:desc_show_sensitive_info
                        )
                ,char(39),char(39)||char(39))
            );
            
            INSERT INTO metadata_schema.column_descriptions (run_id,model_name,database_name, description)
            VALUES (:run_id,:model_name, :database_name, :generated_description);
        END IF;
        
        IF (:action IN ('APPLY', 'FULL')) THEN
            IF (:action IN ('APPLY')) THEN
                generated_description := (
                    SELECT c.description FROM metadata_schema.column_descriptions c
                    WHERE c.database_name = :database_name
                    and c.schema_name is null
                    ORDER BY dt_updated DESC LIMIT 1
                );
            END IF;
            EXECUTE IMMEDIATE 'COMMENT ON DATABASE ' || :database_name || ' IS ' || char(39)||:generated_description||char(39);
        END IF;
    END IF;
    
    RETURN 'Process completed successfully';
END;
;

CREATE OR REPLACE PROCEDURE EXECUTE_SQL_IN_PARALLELL(
    sql_list ARRAY,
    parallell_degree INT
)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_in_parallel'
AS
$$
from concurrent.futures import ThreadPoolExecutor
from snowflake.snowpark import Session

def execute_in_parallel(session: Session, sql_list, parallell_degree):

    def run_query(sql_command):
        try:
            session.sql(sql_command).collect()
            return f"OK: {sql_command}"
        except Exception as e:
            return f"ERROR in: {sql_command} -> {str(e)}"

    results = []
    with ThreadPoolExecutor(max_workers=parallell_degree) as executor:
        futures = [executor.submit(run_query, sql_command) for sql_command in sql_list]
        for future in futures:
            results.append(future.result())
    return results
$$;


call generate_ai_description(
    'mistral-large', --MODEL
    'BD_EMPRESA', --DATABASE level
    'PUBLIC', --SCHEMA level
    NULL, --TABLE / VIEW level
    null, --COLUMN level
    'FULL', --TYPE OF ACTION GENERATE, APPLY, FULL(BOTH)
    5, --DEGREE OF PARALLELISM PROCESSING in columns
    2, --DEGREE OF PARALLELISM PROCESSING in tables
    100, --MAXIMUN NUMBER OF WORDS IN DESCRIPTION LENGTH
    false, --SHOW SENSITIVE INFORMATION
    200, --SAMPLE NUMBER RATE ROWS in COLUMN VALUES
    NULL --FOR INTERNAL USE SPECIFYING TYPE OF OBJECT MANAGED
);

select 
    NVL2( column_name, 'column',
        NVL2( table_name,'table', 
            NVL2( schema_name,'schema', 'database' ))
        ) lvl,
count(1) total_ai_descriptions
from metadata_schema.column_descriptions
GROUP BY ROLLUP (lvl);

select *
from metadata_schema.column_descriptions
order by 2,3,4,5,6,1;




