select snowflake.cortex.complete('claude-4-sonnet','que es ABC en Colombia?');

//openai-gpt-4.1
//https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions
//ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

//Coloco una pelota en una taza. Coloco la taza dándole la vuelta sobre una mesa. Después de 1 minuto, tomo la taza y la coloco dentro del refrigerador. ¿Dónde está la pelota?

//En cada conversación identifique el nombre del cliente, producto, la razón de la llamada, el problema con el producto, genere un JSON, sin repetir el prompt mensaje

//Crea un código de streamlit donde se muestren dos Campos uno de texto libre y otro un combobox de selección con tres opciones y en la parte de abajo una gráfica que muestre los últimos 3 meses con mediciones aleatorias cada vez que yo hago un cambio en el combobox

USE WAREHOUSE VW_GENAI;
USE DATABASE BD_AI_CORTEX;
USE SCHEMA PUBLIC;

select * from Call_Transcript_es where language = 'Español' limit 1;

//ALTER WAREHOUSE VW_GENAI SET WAREHOUSE_SIZE = 'X-LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

select transcript,snowflake.cortex.translate(transcript,'de_DE','es_XX')
from call_transcripts 
where language = 'German';

select transcript, snowflake.cortex.sentiment(transcript) 
from call_transcript_es 
where language = 'Español';

select transcript,snowflake.cortex.summarize(transcript) 
from call_transcripts 
where language = 'English' limit 1;

SET prompt = 
'### 
De la transcripción, genera un formato JSON con el resumen traducido a español menos de 200 palabras, nombre del producto, defecto, sentimiento del cliente sólo decir malo, neutral, positivo y la solución al cliente todo en español.
###';

select snowflake.cortex.complete('llama3-70b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

select snowflake.cortex.complete('llama3.1-405b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

select snowflake.cortex.complete('mixtral-8x7b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

select snowflake.cortex.complete('claude-3-5-sonnet',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

select snowflake.cortex.complete('gemma-7b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

/* https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions */

SELECT SNOWFLAKE.CORTEX.ENTITY_SENTIMENT('Devuelve el sentimiento del cliente para la siguiente reseña: Abrieron una pizzería nueva en el barrio y obviamente tocaba darle la probada. La pizza se demoró un huevo, pero el sabor... ¡una delicia total! Casi que me sentí en Italia. Eso sí, el precio sí me dejó pensándolo. Muy rica, pero toca guardarla pa cuando uno se quiera dar un gustico. Quedé feliz, pero con el bolsillo apretado.
', ['calidad_comida', 'sabor_comida', 'tiempo_espera', 'precio_comida']) AS sentimiento_respuesta;

CREATE OR REPLACE TABLE EMAIL_RAW (
  id INT,
  email_contents STRING
);

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

SELECT *,
  SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(email_contents, 
    ['Marca', 'Calidad del producto']) AS sentimiento_respuesta
FROM EMAIL_RAW;

SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
  [
    {
      'role': 'user',
      'content': 'Extrae el nombre, apellido, nacionalidad, donde trabaja, y antiguedad de la siguiente oración: "Mario Galvis es Colombiano y trabaja en Snowflake hace 2 años"'
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

/* dime cuantas llamadas recibimos por tipo de defecto? */
