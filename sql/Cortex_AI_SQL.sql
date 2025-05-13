-- Configuración del entorno
USE WAREHOUSE VW_GENAI;
USE DATABASE BD_AI_CORTEX;
USE SCHEMA PUBLIC;

-- Ejemplo básico de uso de la función Cortex COMPLETE
SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', 'que es ABC en Colombia?');

-- Consulta básica para obtener un ejemplo de transcripción en español
SELECT * FROM Call_Transcripts_es 
WHERE language = 'Español' 
LIMIT 1;

-- Traducción de transcripciones del alemán al español
SELECT transcript, SNOWFLAKE.CORTEX.TRANSLATE(transcript, 'en', 'es')
FROM call_transcripts 
WHERE language = 'English';

-- Análisis de sentimiento para transcripciones en español
SELECT transcript, SNOWFLAKE.CORTEX.SENTIMENT(transcript) 
FROM call_transcripts_es 
WHERE language = 'Español';

-- Resumen de transcripciones en inglés
SELECT transcript, SNOWFLAKE.CORTEX.TRANSLATE(SNOWFLAKE.CORTEX.SUMMARIZE(transcript), 'en', 'es')
FROM call_transcripts_es 
WHERE language = 'Español' 
LIMIT 1;

-- Definición de prompt para resúmenes en español con análisis de sentimientos
SET prompt = 
'###
De la transcripción, genera un formato JSON con el resumen traducido a español (menos de 200 palabras), 
nombre del producto, defecto, sentimiento del cliente (solo malo, neutral, positivo) y la solución al cliente, 
todo en español.
###';

-- Generación de resúmenes usando diferentes modelos de lenguaje
SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-70b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-405b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

SELECT SNOWFLAKE.CORTEX.COMPLETE('gemma-7b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

-- Análisis de sentimiento detallado para reseñas, categorizando por diferentes aspectos
SELECT SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(
'Devuelve el sentimiento del cliente para la siguiente reseña: 
Abrieron una pizzería nueva en el barrio y obviamente tocaba darle la probada. 
La pizza se demoró un huevo, pero el sabor... ¡una delicia total! 
Casi que me sentí en Italia. Eso sí, el precio sí me dejó pensándolo. 
Muy rica, pero toca guardarla pa cuando uno se quiera dar un gustico. 
Quedé feliz, pero con el bolsillo apretado.',
['calidad_comida', 'sabor_comida', 'tiempo_espera', 'precio_comida']
) AS sentimiento_respuesta;

-- Creación de tabla para almacenar correos sin procesar
CREATE OR REPLACE TABLE EMAIL_RAW (
  id INT,
  email_contents STRING
);

-- Inserción de ejemplo de correo en la tabla
INSERT INTO EMAIL_RAW (id, email_contents)
VALUES (1, '
Fecha: 12 de abril de 2025, 10:22 a. m.
Asunto: Respuesta a notificación de retiro por seguridad

Estimado equipo de atención de AutoAndina Colombia:

Acabo de recibir una notificación sobre el retiro del sistema de airbags de mi camioneta Tucan X 2022 (adquirida en febrero de 2022), y me preocupa mucho este tema de seguridad. ¿Serían tan amables de ayudarme a resolver las siguientes inquietudes?

1. ¿Qué tan grave es el defecto en el sistema de airbags? ¿Es seguro seguir usando el vehículo hasta que se realice la reparación?
2. ¿Cuál es la primera cita disponible en su centro de servicio en la región de Cundinamarca?
3. ¿Cuánto tiempo estiman que tomará el proceso de reparación?

He sido un cliente fiel de AutoAndina y valoro mucho el buen desempeño que ha tenido mi vehículo hasta el momento. Por eso, quisiera asegurarme de que este asunto se resuelva de forma oportuna. Para mí, la seguridad es lo más importante.

Cordialmente,
Mario Galvis
');

-- Análisis de sentimiento de correos electrónicos, categorizando por 'Marca' y 'Calidad del producto'
SELECT *,
  SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(email_contents, 
    ['Marca', 'Calidad del producto']) AS sentimiento_respuesta
FROM EMAIL_RAW;

DROP TABLE EMAIL_RAW;

-- Ejemplo de extracción de entidades desde texto estructurado en JSON
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
  [
    {
      'role': 'user',
      'content': 'Extrae el nombre, apellido, nacionalidad, donde trabaja, y antigüedad de la siguiente oración: "Mario Galvis es Colombiano y trabaja en Snowflake hace 2 años"'
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
        'required': ['nombre', 'apellido', 'nacionalidad','empresa_trabajo', 'antiguedad_trabajo']
      }
    }
  }
);

-- Pregunta básica para obtener el número de llamadas por tipo de defecto
/* dime cuantas llamadas recibimos por tipo de defecto? */

//https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions
//ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

//Coloco una pelota en una taza. Coloco la taza dándole la vuelta sobre una mesa. Después de 1 minuto, tomo la taza y la coloco dentro del refrigerador. ¿Dónde está la pelota?

//En cada conversación identifique el nombre del cliente, producto, la razón de la llamada, el problema con el producto, genere un JSON, sin repetir el prompt mensaje

//Crea un código de streamlit donde se muestren dos Campos uno de texto libre y otro un combobox de selección con tres opciones y en la parte de abajo una gráfica que muestre los últimos 3 meses con mediciones aleatorias cada vez que yo hago un cambio en el combobox

