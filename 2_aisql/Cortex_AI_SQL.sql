SELECT snowflake.cortex.complete('openai-gpt-5', 'que es ABC en Colombia?'); 
-- claude-4-sonnet - Documentación oficial: https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions

USE DATABASE BD_AI_CORTEX;
USE SCHEMA PUBLIC;

--CREATE OR REPLACE STAGE LLAMADAS ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

// De Audio a Texto desde SQL //
SELECT AI_TRANSCRIBE(TO_FILE('@LLAMADAS', '081725_1349.mp3'),{'timestamp_granularity': 'speaker'});

// Analítica de Sentimiento desde SQL //
WITH transcriptions AS
  ( SELECT TO_VARCHAR(AI_TRANSCRIBE(TO_FILE('@LLAMADAS',
      '081725_1349.mp3'))) AS transcribed_call )
SELECT
  AI_SENTIMENT(transcribed_call, ['Profesionalismo', 'Resolución',
      'Tiempo de Espera']) AS call_sentiment
FROM transcriptions;
 
SET prompt = '### De la transcripción, genera un formato JSON con el resumen traducido a español menos de 200 palabras, nombre del producto, defecto, sentimiento del cliente sólo decir malo, neutral, positivo y la solución al cliente todo en español ###';

SELECT snowflake.cortex.complete('llama3-70b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary FROM call_transcripts 
WHERE language = 'English' LIMIT 1;

SELECT snowflake.cortex.complete('claude-4-sonnet', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary FROM call_transcripts 
WHERE language = 'English' LIMIT 1;

-- Evaluación de sentimiento por categoría usando ENTITY_SENTIMENT
SELECT SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(
  'Devuelve el sentimiento del cliente para la siguiente reseña: 
   Abrieron una pizzería nueva en el barrio y obviamente tocaba darle la probada. 
   La pizza se demoró un huevo, pero el sabor... ¡una delicia total! 
   Casi que me sentí en Italia. Eso sí, el precio sí me dejó pensándolo. 
   Muy rica, pero toca guardarla pa cuando uno se quiera dar un gustico. 
   Quedé feliz, pero con el bolsillo apretado.',
  ['calidad_comida', 'sabor_comida', 'tiempo_espera', 'precio_comida']
) AS sentimiento_respuesta;

-- Extracción de entidades específicas usando el modelo Mistral con esquema JSON
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'mistral-large2',
  [
    {
      'role': 'user',
      'content': 'Extrae el nombre, apellido, nacionalidad, donde trabaja, y antiguedad de la siguiente oración: 
      "Mario Galvis es Colombiano y trabaja en Snowflake hace 2 años"'
    }
  ],
  {
    'temperature': 0,
    'max_tokens': 100,
    'response_format': {
      'type': 'json',
      'schema': {
        'type': 'object',
        'properties': {
          'nombre': {'type': 'string'},
          'apellido': {'type': 'string'},
          'nacionalidad': {'type': 'string'},
          'empresa_trabajo': {'type': 'string'},
          'antiguedad_trabajo': {'type': 'integer'}
        },
        'required': ['nombre', 'apellido', 'nacionalidad', 'empresa_trabajo', 'antiguedad_trabajo']
      }
    }
  }
);

-- ¿cuántas llamadas recibimos por tipo de daño?

-- Ejemplo de razonamiento lógico para evaluación de modelos Playground
-- Coloco una pelota en una taza. Coloco la taza dándole la vuelta sobre una mesa. Después de 1 minuto, tomo la taza y la coloco dentro del refrigerador.¿Dónde está la pelota?
