ALTER USER mggsnowflake2
SET DEFAULT_WAREHOUSE = 'VW_GENAI';

ALTER NOTEBOOK "Data Engineering Pipeline with pandas on Snowflake"
  SET IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 300; -- 5 minutos

ALTER NOTEBOOK "Extraccion de Notas"
  SET IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 300; -- 5 minutos

ALTER NOTEBOOK "Reconocimiento de Imagenes Llama 3_2"
  SET IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 300; -- 5 minutos
  
ALTER NOTEBOOK "Tableros de Control en Notebooks"
  SET IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 300; -- 5 minutos

  

ALTER NOTEBOOK "Data Engineering Pipeline with pandas on Snowflake"
  SET IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 300; -- 5 minutos

  
