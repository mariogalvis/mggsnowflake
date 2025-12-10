COPY FILES INTO @docs
FROM @MGG_RAG
FILES = ('Cartilla practica para solicitud de credito.pdf');

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
TARGET_LAG = '1 minute'
as (
    select chunk,
        relative_path,
        file_url,
        category
    from docs_chunks_table
);

ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
