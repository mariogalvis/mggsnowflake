select snowflake.cortex.complete('openai-gpt-5','Que es ABC en Colombia?');

USE DATABASE BD_AI_CORTEX;
USE SCHEMA PUBLIC;

SET prompt = '### De la transcripción, genera un formato JSON con el resumen traducido a español menos de 200 palabras, nombre del producto, defecto, sentimiento del cliente sólo decir malo, neutral, positivo y la solución al cliente todo en español ###';

SELECT snowflake.cortex.complete('llama3-70b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary FROM call_transcripts WHERE language = 'English' LIMIT 1;

SELECT snowflake.cortex.complete('claude-4-sonnet', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary FROM call_transcripts  WHERE language = 'English' LIMIT 1;

--AI Assistant - - ¿cuántas llamadas recibimos por tipo de daño?

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

-- Análisis de Imágenes desde SQL

USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;

-- Extracción de datos desde una imagen de cédula
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'pixtral-large',
    'Dame todos los datos de esta cédula, en este orden: número de cédula, nombre, apellido, nacionalidad, fecha de nacimiento, es mayor de edad? (si es mayor de 18 años sólo responde SI, o NO), Lugar de nacimiento, fecha de expedición, y si la cédula está vigente',
    TO_FILE('@myimages', 'cedula.jpg')
);

SELECT AI_EXTRACT(
  file => TO_FILE('@BD_EMPRESA.GOLD.MYIMAGES','358849121-Ejemplo-de-Facturas.pdf'),
  responseFormat => [['name', 'Cuál es el nombre del cliente?'], 
                     ['total', 'Cuál es el total de la factura?'],
                     ['numero', 'Cuál es el número de la factura?'],
                     ['fecha', 'Cuál es la fecha de la factura?']]
);

SELECT 
  relative_path,
  AI_EXTRACT(
    TO_FILE('@BD_EMPRESA.GOLD.MYIMAGES', relative_path),
    [ 'Como se llama el propietario?',
    'Cuál es la fecha de inicio de contrato? formato dd/mm/aaaa',
    'Cuál es la duración de meses del contrato? formato número',
    'Tiene cláusula de terminación anticipada? SI o NO',
    'Cuál es el canon de arrendamiento? sólo en formato número, sin decimales']
  ) AS extracted_info
FROM DIRECTORY(@BD_EMPRESA.GOLD.MYIMAGES)
WHERE relative_path LIKE 'contratos/%';


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

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Clasifique el punto de referencia identificado en esta imagen. Responda en JSON solo con el nombre del punto de referencia',
    TO_FILE('@myimages', 'lugarb.jpg')
);

USE DATABASE BD_AI_CORTEX;
USE SCHEMA PUBLIC;

-- De Audio a Texto desde SQL
SELECT AI_TRANSCRIBE(TO_FILE('@LLAMADAS', '081725_1349.mp3'),{'timestamp_granularity': 'speaker'});

-- Analítica de Sentimiento desde SQL 
WITH transcriptions AS
  ( SELECT TO_VARCHAR(AI_TRANSCRIBE(TO_FILE('@LLAMADAS',
      '081725_1349.mp3'))) AS transcribed_call )
SELECT
  AI_SENTIMENT(transcribed_call, ['Profesionalismo', 'Resolución',
      'Tiempo de Espera']) AS call_sentiment
FROM transcriptions;
