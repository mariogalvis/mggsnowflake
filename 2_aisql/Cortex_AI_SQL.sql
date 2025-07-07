-- Ejemplo de consulta simple usando el modelo Claude 4 Sonnet
SELECT snowflake.cortex.complete('claude-4-sonnet', 'que es ABC en Colombia?');

-- Notas y referencias
-- openai-gpt-4.1
-- Documentación oficial: https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions

-- Ejemplo de razonamiento lógico para evaluación de modelos
-- Coloco una pelota en una taza. Coloco la taza dándole la vuelta sobre una mesa. Después de 1 minuto, tomo la taza y la coloco dentro del refrigerador.¿Dónde está la pelota?

-- Instrucción general para cada conversación:
-- Identifique el nombre del cliente, producto, la razón de la llamada, el problema con el producto. Genere un JSON sin repetir el prompt mensaje.

-- Configuración de warehouse y esquema
USE WAREHOUSE VW_GENAI;
USE DATABASE BD_AI_CORTEX;
USE SCHEMA PUBLIC;

-- Ejemplo: consulta de transcripciones en español
SELECT date_created, country, transcript FROM Call_Transcripts_es WHERE language = 'Español' LIMIT 1;

-- Ajuste opcional del warehouse
-- ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Análisis de sentimiento de llamadas en español
SELECT transcript, 
       snowflake.cortex.sentiment(transcript) 
FROM call_transcripts_es 
WHERE language = 'Español';

-- Resumen automático de llamadas en inglés
SELECT transcript,
       snowflake.cortex.summarize(transcript) 
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

-- Prompt para generar JSON en español con resumen, producto, defecto, sentimiento y solución
SET prompt = '
### 
De la transcripción, genera un formato JSON con el resumen traducido a español menos de 200 palabras, 
nombre del producto, defecto, sentimiento del cliente sólo decir malo, neutral, positivo y la solución al cliente todo en español.
###';

-- Completions usando diferentes modelos LLM con el prompt anterior
SELECT snowflake.cortex.complete('llama3-70b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

SELECT snowflake.cortex.complete('llama3.1-405b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

SELECT snowflake.cortex.complete('claude-3-5-sonnet', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary
FROM call_transcripts 
WHERE language = 'English' 
LIMIT 1;

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

-- Creación de tabla de correos de clientes
CREATE OR REPLACE TABLE EMAIL_RAW (
  id INT,
  email_contents STRING
);

-- Inserción de ejemplo con queja real de cliente
INSERT INTO EMAIL_RAW (id, email_contents)
VALUES (1, '
Fecha: 12 de abril de 2025, 10:22 a. m.
Asunto: Respuesta a notificación de retiro por seguridad

Estimado equipo de atención de AutoAndina Colombia:

Acabo de recibir una notificación sobre el retiro del sistema de airbags de mi camioneta Tucan X 2022 (adquirida en febrero de 2022), y me preocupa mucho este tema de seguridad. ¿Serían tan amables de ayudarme a resolver las siguientes inquietudes?

1. ¿Qué tan grave es el defecto en el sistema de airbags? ¿Es seguro seguir usando el vehículo hasta que se realice la reparación?
2. ¿Cuál es la primera cita disponible en su centro de servicio en la región de Cundinamarca?
3. ¿Cuánto tiempo estiman que tomará el proceso de reparación?

He sido un cliente fiel de AutoAndina y valoro mucho el buen desempeño que ha tenido mi vehículo hasta el momento. 
Por eso, quisiera asegurarme de que este asunto se resuelva de forma oportuna. Para mí, la seguridad es lo más importante.

Cordialmente,
Mario Galvis
');

-- Análisis de sentimiento sobre atributos definidos del correo insertado
SELECT *,
       SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(email_contents, 
         ['Marca', 'Calidad del producto']) AS sentimiento_respuesta
FROM EMAIL_RAW;


-- ¿cuántas llamadas recibimos por tipo de defecto?

-- Tarea adicional con Streamlit (no SQL):
-- Crea un código de streamlit donde se muestren dos campos:
-- uno de texto libre y otro un combobox de selección con tres opciones.
-- En la parte de abajo, una gráfica que muestre los últimos 3 meses
-- con mediciones aleatorias cada vez que yo haga un cambio en el combobox.
