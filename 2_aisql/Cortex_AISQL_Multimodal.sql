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


-- Configuración del entorno de trabajo
USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;


-- Extracción de datos desde una imagen de cédula
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'pixtral-large',
    'Dame todos los datos de esta cédula, en este orden: número de cédula, nombre, apellido, nacionalidad, fecha de nacimiento, es mayor de edad? (si es mayor de 18 años sólo responde SI, o NO), Lugar de nacimiento, fecha de expedición, y si la cédula está vigente',
    TO_FILE('@myimages', 'cedula.jpg')
);

-- Clasificación de punto de referencia a partir de una imagen
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Clasifique el punto de referencia identificado en esta imagen. Responda en JSON solo con el nombre del punto de referencia',
    TO_FILE('@myimages', 'lugarb.jpg')
);

-- Análisis de imagen para reporte de accidente vehicular
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Responde. 
    1. Describe el incidente
    2. ¿Cuántos vehículos están involucrados en el incidente?
    3. ¿Dónde se produjo el impacto principal en el vehículo?
    4. ¿El daño es severo o superficial?
    5. ¿Qué parte del otro vehículo impactó al vehículo dañado?
    6. ¿Hay partes desprendidas o elementos faltantes en el vehículo afectado?
    7. ¿Las llantas están alineadas o presentan desplazamiento?
    8. ¿Se observa daño en las luces, espejos o defensa del vehículo?
    9. ¿Hay evidencia de fugas o manchas en el suelo?
    10. ¿Qué colores tienen los vehículos involucrados?
    11. ¿La imagen es clara y adecuada para radicar un siniestro?
    12. ¿Cuál es el número de la placa del vehículo?
    13. ¿Dame el nombre de la ciudad que aparece en la placa del vehículo? 
    Ponlo en formato JSON, solo dos columnas, con el número de pregunta y respuesta',
    TO_FILE('@myimages', 'choque3.png')
);
