select snowflake.cortex.complete('openai-gpt-5','Que es ABC en Colombia?');

USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;

Select * from CALL_TRANSCRIPTS limit 5;

SET prompt = '### De la transcripción, genera un formato JSON con el resumen traducido a español menos de 100 palabras, nombre del producto, defecto, sentimiento del cliente sólo decir malo, neutral, positivo y la solución al cliente todo en español ###';

SELECT snowflake.cortex.complete('llama3-70b', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary FROM call_transcripts LIMIT 2;

SELECT snowflake.cortex.complete('claude-4-sonnet', CONCAT('[INST]', $prompt, transcript, '[/INST]')) AS summary FROM call_transcripts LIMIT 2;





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

-- Extracción de datos desde una imagen de cédula
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'pixtral-large',
    'Dame todos los datos de esta cédula, en este orden: número de cédula, nombre, apellido, nacionalidad, fecha de nacimiento, es mayor de edad? (si es mayor de 18 años sólo responde SI, o NO), Lugar de nacimiento, fecha de expedición, y si la cédula está vigente',
    TO_FILE('@mgg_files', 'cedula.jpg')
);

SELECT AI_EXTRACT(
  file => TO_FILE('@BD_EMPRESA.GOLD.MGG_FILES','358849121-Ejemplo-de-Facturas.pdf'),
  responseFormat => [['name', 'Cuál es el nombre del cliente?'], 
                     ['total', 'Cuál es el total de la factura?'],
                     ['numero', 'Cuál es el número de la factura?'],
                     ['fecha', 'Cuál es la fecha de la factura?']]
);

SELECT AI_EXTRACT(
  file => TO_FILE('@BD_EMPRESA.GOLD.MGG_FILES','SARLAFT.pdf'),
  responseFormat => [['name', 'Cuál es el nombre?'], 
                     ['tipo', 'Es vinculación?'],
                     ['id', 'Cuál es el número de identificación?'],
                     ['telefono', 'Cuál es el teléfono?'],
                     ['email', 'Cuál es el correo electrónico?'],
                     ['ingresos_mensuales', 'Cuáles son los ingresos mensuales?']]
);

--INSERT INTO FORMULARIOS_SARLAFT (NOMBRE, TIPO_VINCULACION, IDENTIFICACION, TELEFONO, EMAIL, INGRESOS_MENSUALES)
SELECT
    RESULT:response:name::string                AS NOMBRE,
    RESULT:response:tipo::string                AS TIPO_VINCULACION,
    RESULT:response:id::string                  AS IDENTIFICACION,
    RESULT:response:telefono::string            AS TELEFONO,
    RESULT:response:email::string               AS EMAIL,
    RESULT:response:ingresos_mensuales::string  AS INGRESOS_MENSUALES
FROM (
    SELECT AI_EXTRACT(
        file => TO_FILE('@BD_EMPRESA.GOLD.MGG_FILES', 'SARLAFT.pdf'),
        responseFormat => [
            ['name', 'Cuál es el nombre?'], 
            ['tipo', 'Es vinculación?'],
            ['id', 'Cuál es el número de identificación?'],
            ['telefono', 'Cuál es el teléfono?'],
            ['email', 'Cuál es el correo electrónico?'],
            ['ingresos_mensuales', 'Cuáles son los ingresos mensuales?']
        ]
    ) AS RESULT
);

SELECT 
  relative_path,
  AI_EXTRACT(
    TO_FILE('@BD_EMPRESA.GOLD.MGG_FILES', relative_path),
    [ 'Como se llama el propietario?',
    'Cuál es la fecha de inicio de contrato? formato dd/mm/aaaa',
    'Cuál es la duración de meses del contrato? formato número',
    'Tiene cláusula de terminación anticipada? SI o NO',
    'Cuál es el canon de arrendamiento? sólo en formato número, sin decimales']
  ) AS extracted_info
FROM DIRECTORY(@BD_EMPRESA.GOLD.MGG_FILES)
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
    TO_FILE('@mgg_files', 'choque3.png')
);

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Clasifique el punto de referencia identificado en esta imagen. Responda en JSON solo con el nombre del punto de referencia',
    TO_FILE('@mgg_files', 'lugarb.jpg')
);

-- De Audio a Texto desde SQL
SELECT AI_TRANSCRIBE(TO_FILE('@mgg_files', '081725_1349.mp3'),{'timestamp_granularity': 'speaker'});

-- Analítica de Sentimiento desde SQL 
WITH transcriptions AS
  ( SELECT TO_VARCHAR(AI_TRANSCRIBE(TO_FILE('@mgg_files',
      '081725_1349.mp3'))) AS transcribed_call )
SELECT
  AI_SENTIMENT(transcribed_call, ['Profesionalismo', 'Resolución',
      'Tiempo de Espera']) AS call_sentiment
FROM transcriptions;

-- Structure Outputs 
SELECT AI_COMPLETE(
    model => 'mistral-large2',
    prompt => 'Devuelve el sentimiento del cliente para la siguiente transcripción de llamada: 
    Llamé porque mi factura llegó con un cobro adicional que no reconozco. 
    La agente fue muy amable y me explicó todo con paciencia, aunque tuve que esperar casi 10 minutos en línea. 
    Al final me resolvieron el problema, pero siento que el proceso fue demasiado lento.',
    response_format => {
        'type': 'json', 'schema': { 'type': 'object', 'properties': {'sentiment_categories': {'type': 'object','properties': {
                        'atencion_agente':        {'type': 'string'},
                        'tiempo_espera':          {'type': 'string'},
                        'resolucion_problema':    {'type': 'string'},
                        'satisfaccion_general':   {'type': 'string'}
                    },
                    'required': ['atencion_agente','tiempo_espera','resolucion_problema','satisfaccion_general']
}}}});

-- Opcional Creación de Tabla Sarlaft
CREATE OR REPLACE TABLE FORMULARIOS_SARLAFT (
    NOMBRE STRING,
    TIPO_VINCULACION STRING,
    IDENTIFICACION STRING,
    TELEFONO STRING,
    EMAIL STRING,
    INGRESOS_MENSUALES STRING,
    FECHA_EXTRACCION TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT * FROM FORMULARIOS_SARLAF;
