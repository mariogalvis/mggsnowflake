-- Configuración del entorno de trabajo
USE WAREHOUSE VW_GENAI;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;

-- Clasificación de punto de referencia a partir de una imagen
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Clasifique el punto de referencia identificado en esta imagen. Responda en JSON solo con el nombre del punto de referencia',
    TO_FILE('@myimages', 'lugarb.jpg')
);

-- Extracción de datos desde una imagen de cédula
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'pixtral-large',
    'Dame todos los datos de esta cédula, en este orden: número de cédula, nombre, apellido, nacionalidad, fecha de nacimiento, es mayor de edad? (si es mayor de 18 años sólo responde SI, o NO), Lugar de nacimiento, fecha de expedición, y si la cédula está vigente',
    TO_FILE('@myimages', 'cedula.jpg')
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

-- Identificación de electrodomésticos en una imagen de cocina
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Extraiga los electrodomésticos de cocina identificados en esta imagen. Responda en JSON solo con los electrodomésticos identificados',
    TO_FILE('@myimages', 'kitchen.png')
);

-- Análisis de imagen para proyección de inflación
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    '¿Qué país observará el mayor cambio de inflación en 2024 en comparación con 2023?',
    TO_FILE('@myimages', 'inflation-forecast.png')
);

-- Resumen de ideas de un gráfico circular
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Resuma las ideas de este gráfico circular en 100 palabras.',
    TO_FILE('@myimages', 'science-employment-slide.jpeg')
);

-- Comparación de dos imágenes para identificar audiencia
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    PROMPT('Compare esta imagen {0} con esta imagen {1} y describa la audiencia ideal para cada una en dos viñetas concisas de no más de 10 palabras.',
    TO_FILE('@myimages', 'adcreative_1.png'),
    TO_FILE('@myimages', 'adcreative_2.png'))
);
