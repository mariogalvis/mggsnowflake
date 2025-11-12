-- ============================================================================
-- Snowflake AISQL Demo - Script de Configuración de Base de Datos
-- ============================================================================
-- Este script crea la base de datos AISQL_Demo con todas las tablas,
-- stages y datos de ejemplo necesarios para la demostración temática de AISQL
-- con el tema Tasty Bytes.
-- ============================================================================

-- Crear warehouse para el demo (X-SMALL Gen2)
CREATE OR REPLACE WAREHOUSE AISQL_PLAYGROUND_ES_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse para la aplicación Streamlit del demo AISQL y ejecución de consultas';

-- Crear base de datos y esquema
CREATE DATABASE IF NOT EXISTS AISQL_PLAYGROUND_ES;
USE DATABASE AISQL_PLAYGROUND_ES;
USE WAREHOUSE AISQL_PLAYGROUND_ES_WH;
CREATE SCHEMA IF NOT EXISTS DEMO;
USE SCHEMA DEMO;

-- ============================================================================
-- CREAR STAGES INTERNOS PARA ARCHIVOS DE MEDIA
-- ============================================================================

-- Stage para archivos de audio (demos de transcripción)
CREATE OR REPLACE STAGE AUDIO_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Stage para archivos de documentos (demos de análisis y extracción)
CREATE OR REPLACE STAGE DOCUMENT_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Stage para documentos de facturas de proveedores (demos de AI_EXTRACT)
CREATE OR REPLACE STAGE SUPPLIER_DOCUMENTS_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- Stage para archivos de imágenes (demos de análisis de imágenes)
CREATE OR REPLACE STAGE IMAGE_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

-- ============================================================================
-- CREAR TABLAS PARA DATOS ESTRUCTURADOS
-- ============================================================================

-- Tabla de Reseñas de Clientes (para sentimiento, resumen, traducción)
CREATE OR REPLACE TABLE CUSTOMER_REVIEWS (
    review_id INT AUTOINCREMENT,
    customer_name VARCHAR(100),
    food_truck_name VARCHAR(100),
    menu_item VARCHAR(100),
    review_text TEXT,
    review_date DATE,
    rating INT,
    language VARCHAR(20),
    PRIMARY KEY (review_id)
);

-- Tabla de Elementos del Menú (para demos de traducción)
CREATE OR REPLACE TABLE MENU_ITEMS (
    menu_id INT AUTOINCREMENT,
    food_truck_name VARCHAR(100),
    item_name VARCHAR(100),
    description_english TEXT,
    description_spanish TEXT,
    description_french TEXT,
    description_german TEXT,
    description_japanese TEXT,
    price DECIMAL(10,2),
    category VARCHAR(50),
    PRIMARY KEY (menu_id)
);

-- Tabla de Food Trucks
CREATE OR REPLACE TABLE FOOD_TRUCKS (
    truck_id INT AUTOINCREMENT,
    truck_name VARCHAR(100),
    cuisine_type VARCHAR(50),
    city VARCHAR(100),
    operating_since DATE,
    description TEXT,
    PRIMARY KEY (truck_id)
);

-- Tabla de Tickets de Soporte al Cliente (para clasificación, filtrado - SIN PII)
CREATE OR REPLACE TABLE SUPPORT_TICKETS (
    ticket_id INT AUTOINCREMENT,
    customer_name VARCHAR(100),
    food_truck_name VARCHAR(100),
    issue_description TEXT,
    created_date DATE,
    status VARCHAR(20),
    urgency VARCHAR(20),
    PRIMARY KEY (ticket_id)
);

-- Tabla de Tickets de Soporte con PII (solo para demos de AI_REDACT)
CREATE OR REPLACE TABLE SUPPORT_TICKETS_PII (
    ticket_id INT AUTOINCREMENT,
    customer_name VARCHAR(100),
    food_truck_name VARCHAR(100),
    issue_description TEXT,
    created_date DATE,
    status VARCHAR(20),
    urgency VARCHAR(20),
    PRIMARY KEY (ticket_id)
);

-- Tablas de Análisis de Documentos (para AI_PARSE_DOCUMENT y Cortex Search)
CREATE OR REPLACE TABLE PARSE_DOC_RAW_TEXT (
    doc_id INT AUTOINCREMENT,
    file_name VARCHAR(500),
    file_url VARCHAR(2000),
    raw_text TEXT,
    parsed_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (doc_id)
);

CREATE OR REPLACE TABLE PARSE_DOC_CHUNKED_TEXT (
    chunk_id INT AUTOINCREMENT,
    file_name VARCHAR(500),
    file_url VARCHAR(2000),
    chunk_index INT,
    chunk_text TEXT,
    chunk_length INT,
    created_date TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (chunk_id)
);

-- Tabla de Detalles de Facturas de Proveedores (para demos de AI_EXTRACT)
CREATE OR REPLACE TABLE SUPPLIER_INVOICE_DETAILS (
    invoice_detail_id INT AUTOINCREMENT,
    file_name VARCHAR(500),
    file_url VARCHAR(2000),
    invoice_number VARCHAR(50),
    invoice_date DATE,
    supplier_name VARCHAR(200),
    supplier_address VARCHAR(500),
    supplier_phone VARCHAR(50),
    customer_name VARCHAR(200),
    customer_address VARCHAR(500),
    customer_phone VARCHAR(50),
    subtotal DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_terms VARCHAR(100),
    item_count INT,
    extraction_date DATE,
    raw_json VARIANT,
    PRIMARY KEY (invoice_detail_id)
);

-- ============================================================================
-- INSERTAR DATOS DE EJEMPLO - FOOD TRUCKS
-- ============================================================================

INSERT INTO FOOD_TRUCKS (truck_name, cuisine_type, city, operating_since, description) VALUES
('Guac n Roll', 'Mexicana', 'Austin', '2019-03-15', 'Tacos callejeros mexicanos auténticos y guacamole fresco'),
('Punto de Congelación', 'Postres', 'Boston', '2020-06-01', 'Helado artesanal y delicias congeladas'),
('Kitakata Ramen Bar', 'Japonesa', 'San Francisco', '2018-11-20', 'Ramen japonés tradicional con fideos hechos a mano'),
('Camión de Pekín', 'China', 'Nueva York', '2019-08-10', 'Cocina auténtica estilo Beijing y dumplings'),
('Venganza de los Quesos', 'Canadiense', 'Seattle', '2020-02-14', 'Poutine gourmet y comida reconfortante canadiense'),
('BBQ Ahumado', 'BBQ Americana', 'Denver', '2017-05-05', 'Carnes ahumadas lentamente y salsas BBQ auténticas'),
('El Rincón de las Crêpes', 'Francesa', 'Portland', '2021-01-20', 'Crêpes francesas dulces y saladas'),
('Palacio Vegetal', 'Vegana', 'Los Ángeles', '2020-09-15', 'Cocina innovadora a base de plantas'),
('Griego Descarado', 'Griega', 'Chicago', '2019-04-22', 'Gyros griegos tradicionales y sabores mediterráneos'),
('Cocina de Nani', 'India', 'Miami', '2018-07-30', 'Curries indios auténticos y especialidades tandoori');

-- ============================================================================
-- INSERTAR DATOS DE EJEMPLO - ELEMENTOS DEL MENÚ
-- ============================================================================

INSERT INTO MENU_ITEMS (food_truck_name, item_name, description_english, description_spanish, description_french, description_german, description_japanese, price, category) VALUES
('Guac n Roll', 'Taco de Carne Asada', 'Carne asada a la parrilla marinada con cilantro fresco, cebolla y salsa casera en tortilla de maíz suave', 'Carne asada a la parrilla con cilantro fresco, cebolla y salsa casera en tortilla de maíz suave', 'Steak grillé mariné avec coriandre fraîche, oignons et salsa maison sur tortilla de maïs moelleuse', 'Gegrilltes mariniertes Steak mit frischem Koriander, Zwiebeln und hausgemachter Salsa auf weicher Maistortilla', 'やわらかいコーントルティーヤに新鮮なコリアンダー、玉ねぎ、自家製サルサを添えたグリルマリネステーキ', 4.50, 'Tacos'),
('Guac n Roll', 'Super Burrito', 'Gran tortilla de harina rellena con tu elección de carne, arroz, frijoles, queso, guacamole y crema agria', 'Gran tortilla de harina rellena con tu elección de carne, arroz, frijoles, queso, guacamole y crema agria', 'Grande tortilla de farine garnie de votre choix de viande, riz, haricots, fromage, guacamole et crème sure', 'Große Weizentortilla gefüllt mit Fleisch nach Wahl, Reis, Bohnen, Käse, Guacamole und Sauerrahm', '大きな小麦粉トルティーヤにお好みの肉、ご飯、豆、チーズ、ワカモレ、サワークリームを詰めました', 9.99, 'Burritos'),
('Punto de Congelación', 'Remolino de Caramelo Salado', 'Helado de vainilla rico con cintas de caramelo salado y nueces pecanas confitadas', 'Helado de vainilla rico con cintas de caramelo salado y nueces pecanas confitadas', 'Crème glacée vanille riche avec des rubans de caramel salé et noix de pécan confites', 'Reichhaltiges Vanilleeis mit gesalzenem Karamell und kandierten Pekannüssen', '塩キャラメルとキャンディピーカンナッツのリボンを添えた濃厚なバニラアイスクリーム', 6.50, 'Helado'),
('Kitakata Ramen Bar', 'Ramen Tonkotsu', 'Caldo rico de hueso de cerdo con fideos hechos a mano, cerdo chashu, huevo pasado por agua y cebolletas', 'Caldo rico de hueso de cerdo con fideos hechos a mano, cerdo chashu, huevo pasado por agua y cebolletas', 'Bouillon riche d''os de porc avec nouilles faites main, porc chashu, œuf mollet et oignons verts', 'Reichhaltige Schweineknochenbrühe mit handgezogenen Nudeln, Chashu-Schweinefleisch, weichgekochtem Ei und Frühlingszwiebeln', '手打ち麺、チャーシュー、半熟卵、ねぎを添えた濃厚な豚骨スープ', 13.99, 'Ramen'),
('Camión de Pekín', 'Dumplings de Sopa', 'Delicados dumplings al vapor rellenos de cerdo sazonado y caldo sabroso', 'Delicados dumplings al vapor rellenos de cerdo sazonado y caldo sabroso', 'Délicats raviolis vapeur farcis de porc assaisonné et bouillon savoureux', 'Zarte gedämpfte Teigtaschen gefüllt mit gewürztem Schweinefleisch und herzhafter Brühe', '味付けした豚肉と旨味のあるスープを詰めた繊細な蒸し餃子', 8.50, 'Dumplings'),
('Venganza de los Quesos', 'Poutine Clásica', 'Papas fritas crujientes cubiertas con cuajada de queso y bañadas en salsa rica', 'Papas fritas crujientes cubiertas con cuajada de queso y bañadas en salsa rica', 'Frites croustillantes garnies de fromage en grains et nappées de sauce riche', 'Knusprige Pommes mit Käsebruch belegt und in reichhaltiger Soße ertränkt', 'チーズカードをトッピングし、濃厚なグレービーソースをかけたサクサクのフライドポテト', 7.99, 'Poutine'),
('BBQ Ahumado', 'Sándwich de Cerdo Desmenuzado', 'Cerdo desmenuzado ahumado lentamente con salsa BBQ picante en pan tostado', 'Cerdo desmenuzado ahumado lentamente con salsa BBQ picante en pan tostado', 'Porc effiloché fumé lentement avec sauce BBQ acidulée sur pain grillé', 'Langsam geräuchertes Pulled Pork mit würziger BBQ-Sauce auf geröstetem Brötchen', 'トーストしたバンズに濃厚なBBQソースをかけたスロースモークプルドポーク', 10.50, 'Sándwiches'),
('El Rincón de las Crêpes', 'Crêpe de Nutella y Plátano', 'Crêpe francés delgado relleno de Nutella y rodajas de plátano fresco', 'Crêpe francés delgado relleno de Nutella y rodajas de plátano fresco', 'Crêpe française fine garnie de Nutella et tranches de banane fraîche', 'Dünner französischer Crêpe gefüllt mit Nutella und frischen Bananenscheiben', 'ヌテラと新鮮なバナナスライスを詰めた薄いフレンチクレープ', 7.50, 'Postre'),
('Palacio Vegetal', 'Hamburguesa Beyond', 'Hamburguesa a base de plantas con lechuga, tomate, pepinillos y salsa especial', 'Hamburguesa a base de plantas con lechuga, tomate, pepinillos y salsa especial', 'Galette de burger à base de plantes avec laitue, tomate, cornichons et sauce spéciale', 'Pflanzliches Burger-Patty mit Salat, Tomate, Gurken und Spezialsoße', 'レタス、トマト、ピクルス、スペシャルソースを添えた植物ベースのバーガーパティ', 11.99, 'Hamburguesas'),
('Griego Descarado', 'Gyro de Cordero', 'Cordero sazonado con salsa tzatziki, tomates y cebollas en pan pita caliente', 'Cordero sazonado con salsa tzatziki, tomates y cebollas en pan pita caliente', 'Agneau assaisonné avec sauce tzatziki, tomates et oignons dans pain pita chaud', 'Gewürztes Lammfleisch mit Tzatziki-Sauce, Tomaten und Zwiebeln in warmem Pitabrot', '温かいピタパンにツァジキソース、トマト、玉ねぎを添えた味付けラム肉', 9.99, 'Gyros');

-- ============================================================================
-- INSERTAR DATOS DE EJEMPLO - RESEÑAS DE CLIENTES
-- ============================================================================

INSERT INTO CUSTOMER_REVIEWS (customer_name, food_truck_name, menu_item, review_text, review_date, rating, language) VALUES
('Sarah Johnson', 'Guac n Roll', 'Taco de Carne Asada', '¡Los tacos de carne asada fueron absolutamente increíbles! La carne estaba perfectamente sazonada y asada a la perfección. La salsa casera tenía justo el toque picante adecuado. Los mejores tacos que he probado en Austin. ¡Definitivamente volveré!', '2024-10-15', 5, 'Español'),
('Michael Chen', 'Kitakata Ramen Bar', 'Ramen Tonkotsu', 'Esto es lo más cercano al ramen japonés auténtico que puedes conseguir en San Francisco. El caldo era rico y cremoso, los fideos tenían la textura perfecta, y el cerdo chashu se derretía en mi boca. La única queja es el tiempo de espera largo, ¡pero valió la pena!', '2024-10-20', 4, 'Español'),
('Emily Rodriguez', 'Punto de Congelación', 'Remolino de Caramelo Salado', 'Buen helado pero un poco caro para el tamaño de la porción. El sabor de caramelo salado estaba delicioso sin embargo. Desearía que ofrecieran tamaños más grandes.', '2024-10-18', 3, 'Español'),
('David Williams', 'BBQ Ahumado', 'Sándwich de Cerdo Desmenuzado', 'Decepcionado con mi pedido de hoy. El cerdo desmenuzado estaba seco y la salsa BBQ no pudo salvarlo. El servicio también fue bastante lento. He tenido mejores experiencias aquí antes, así que tal vez fue solo un mal día.', '2024-10-22', 2, 'Español'),
('Jessica Martinez', 'El Rincón de las Crêpes', 'Crêpe de Nutella y Plátano', '¡Absolutamente divino! La crêpe era delgada y delicada, justo como en Francia. El Nutella era generoso y los plátanos estaban frescos. Perfecto para postre o un desayuno dulce. ¡Altamente recomendado!', '2024-10-25', 5, 'Español'),
('James Taylor', 'Palacio Vegetal', 'Hamburguesa Beyond', 'Como vegetariano, siempre busco buenas opciones a base de plantas. ¡Esta hamburguesa superó mis expectativas! Sabía increíble y ni siquiera pude notar que no era carne real. ¡Buen trabajo!', '2024-10-19', 5, 'Español'),
('Lisa Anderson', 'Venganza de los Quesos', 'Poutine Clásica', 'Las papas fritas estaban crujientes y la cuajada de queso estaba fresca, pero la salsa estaba demasiado salada para mi gusto. Además, la porción era más pequeña de lo esperado. Decente pero no excelente.', '2024-10-21', 3, 'Español'),
('Robert Brown', 'Camión de Pekín', 'Dumplings de Sopa', '¡Estos dumplings de sopa son los verdaderos! El caldo adentro era sabroso y caliente, la masa era delicada. Se puede notar que están hechos frescos. El mejor camión de comida china en NYC por mucho.', '2024-10-23', 5, 'Español'),
('María García', 'Griego Descarado', 'Gyro de Cordero', 'El gyro de cordero estaba delicioso. La carne estaba bien condimentada y la salsa tzatziki era fresca y cremosa. Definitivamente volveré por más.', '2024-10-24', 4, 'Español'),
('Thomas White', 'Kitakata Ramen Bar', 'Ramen Tonkotsu', 'Experiencia terrible. Encontré un cabello en mi ramen y cuando se lo dije al personal, fueron muy despectivos. Nunca volveré.', '2024-10-26', 1, 'Español'),
('Amanda Lee', 'Guac n Roll', 'Super Burrito', '¡Este burrito es ENORME y tan llenador! Todo adentro estaba fresco y sabroso. El guacamole claramente está hecho fresco. Gran valor por el precio. ¡Mi nuevo lugar favorito para el almuerzo!', '2024-10-27', 5, 'Español'),
('Christopher Davis', 'BBQ Ahumado', 'Sándwich de Cerdo Desmenuzado', 'El BBQ aquí nunca decepciona. La carne siempre está tierna y ahumada, y la salsa es picante y dulce. La ensalada de col al lado también es excelente. ¡Debes probarlo!', '2024-10-28', 5, 'Español'),
('Jennifer Wilson', 'Punto de Congelación', 'Remolino de Caramelo Salado', '¡Este helado es increíble! Cremoso, rico, y las cintas de caramelo salado son perfectas. Las nueces pecanas confitadas añaden un crujido agradable. Vale cada centavo.', '2024-10-29', 5, 'Español'),
('Daniel Moore', 'Palacio Vegetal', 'Hamburguesa Beyond', 'No está mal, pero he probado mejores hamburguesas a base de plantas. La hamburguesa estaba un poco seca y le faltaba condimento. Sin embargo, los acompañamientos estaban frescos.', '2024-10-30', 3, 'Español'),
('Michelle Taylor', 'El Rincón de las Crêpes', 'Crêpe de Nutella y Plátano', 'Demasiado dulce para mí. Apenas pude terminarla. Si tienes un gran diente dulce podrías gustarte, pero fue abrumador para mí.', '2024-11-01', 2, 'Español'),
('Kevin Martinez', 'Griego Descarado', 'Gyro de Cordero', '¡Gyro sólido! El cordero estaba tierno y bien sazonado. El pan pita estaba caliente y suave. Solo desearía que dieran más salsa tzatziki. En general, una gran comida.', '2024-11-02', 4, 'Español'),
('Laura Anderson', 'Camión de Pekín', 'Dumplings de Sopa', 'Ten cuidado al comer estos - ¡la sopa adentro está súper caliente! Pero son absolutamente deliciosos. El relleno de cerdo está perfectamente sazonado. ¡Amo este camión!', '2024-11-03', 5, 'Español'),
('Steven Jackson', 'Venganza de los Quesos', 'Poutine Clásica', 'Esta poutine es auténtica y deliciosa. Las papas fritas están crujientes y la salsa es perfecta. ¡Recomiendo encarecidamente!', '2024-11-04', 5, 'Español'),
('Karen Thomas', 'Kitakata Ramen Bar', 'Ramen Tonkotsu', 'Buen ramen pero nada especial. El caldo era sabroso pero los fideos estaban un poco sobrecocidos. Sin embargo, el servicio fue amigable.', '2024-11-05', 3, 'Español'),
('Patricia White', 'Guac n Roll', 'Taco de Carne Asada', 'Estos tacos me recuerdan a mi infancia en México. Auténticos y deliciosos. La salsa verde es particularmente buena. ¡Gracias!', '2024-11-06', 5, 'Español'),
('Brandon Cooper', 'BBQ Ahumado', 'Sándwich de Cerdo Desmenuzado', '¡La salsa BBQ en este sándwich es perfecta! Dulce, ahumada, y justo la cantidad adecuada de picante. La carne literalmente se derrite en tu boca. ¡Mejor BBQ en Denver!', '2024-11-07', 5, 'Español'),
('Rachel Green', 'Punto de Congelación', 'Remolino de Caramelo Salado', '¡Vengo aquí todas las semanas! El helado siempre está fresco y los sabores cambian según la temporada. El personal recuerda mi pedido habitual lo que me hace sentir especial.', '2024-11-08', 5, 'Español'),
('Tyler Johnson', 'Cocina de Nani', 'Pollo Tikka Masala', 'Cocina india absolutamente auténtica. Las especias están balanceadas perfectamente y el pan naan está hecho fresco. ¡Me recuerda a mi viaje a Mumbai!', '2024-11-09', 5, 'Español'),
('Samantha Blake', 'El Rincón de las Crêpes', 'Crêpe de Nutella y Plátano', '¡Es magnífico! Las crêpes son ligeras y deliciosas. ¡Me siento como si estuviera en París!', '2024-11-10', 5, 'Español'),
('Marcus Williams', 'Camión de Pekín', 'Dumplings de Sopa', 'Estos xiaolongbao son tan buenos como los que probé en Shanghai. La masa es delgada, el caldo es sabroso, y están hechos frescos al pedido. ¡Vale la pena la espera!', '2024-11-11', 5, 'Español'),
('Ashley Martinez', 'Palacio Vegetal', 'Hamburguesa Beyond', 'Como vegetariana de toda la vida, estoy tan feliz de tener opciones de calidad a base de plantas como esta. La hamburguesa es jugosa y sabrosa. ¡Mis amigos que comen carne no pudieron notar la diferencia!', '2024-11-12', 5, 'Español'),
('Eric Thompson', 'Venganza de los Quesos', 'Poutine Clásica', 'Esta poutine me lleva de vuelta a mis días universitarios en Montreal. La cuajada de queso cruje perfectamente y la salsa es rica sin ser demasiado pesada. ¡Comida reconfortante canadiense auténtica!', '2024-11-13', 5, 'Español'),
('Nicole Rodriguez', 'Griego Descarado', 'Gyro de Cordero', 'El cordero está perfectamente sazonado y las verduras siempre están frescas. La salsa tzatziki es casera y puedes notar la diferencia. ¡Altamente recomendado!', '2024-11-14', 4, 'Español'),
('Justin Lee', 'Kitakata Ramen Bar', 'Ramen Tonkotsu', '¡Finalmente encontré tonkotsu auténtico en SF! El caldo es rico y cremoso, claramente cocido a fuego lento durante horas. El cerdo chashu está tierno y el huevo es perfecto. ¡Esto es lo verdadero!', '2024-11-15', 5, 'Español'),
('Melissa Brown', 'Guac n Roll', 'Super Burrito', '¡Este burrito es masivo! Lleno de carne asada perfectamente sazonada, guacamole fresco, y todos los acompañamientos. Un burrito es fácilmente dos comidas. ¡Gran valor!', '2024-11-16', 5, 'Español'),
('Derek Wilson', 'BBQ Ahumado', 'Sándwich de Cerdo Desmenuzado', 'Buen BBQ pero he probado mejor. La carne estaba un poco seca hoy y podría usar más salsa. ¿Tal vez solo fue un mal día? Le daré otra oportunidad.', '2024-11-17', 3, 'Español'),
('Christina Davis', 'Punto de Congelación', 'Remolino de Caramelo Salado', 'Caro para el tamaño de la porción. El helado en sí está bien pero $8 por una bola pequeña es demasiado. Me quedaré con otros lugares.', '2024-11-18', 2, 'Español'),
('Andrew Miller', 'Palacio Vegetal', 'Hamburguesa Beyond', 'No impresionado. La hamburguesa se desmoronó y el pan estaba empapado. Amo la comida a base de plantas pero esto necesita mejoras.', '2024-11-19', 2, 'Español'),
('Victoria García', 'Griego Descarado', 'Gyro de Cordero', '¡Muy delicioso! La carne de cordero está perfectamente sazonada. Las porciones son generosas y el precio es justo. ¡Volveré pronto!', '2024-11-20', 5, 'Español'),
('Ryan Martinez', 'Camión de Pekín', 'Dumplings de Sopa', '¡Estos dumplings son increíbles! Advertencia - son adictivos. Pedí 8 y desearía haber pedido más. La salsa de jengibre para mojar es el complemento perfecto.', '2024-11-21', 5, 'Español'),
('Angela White', 'Kitakata Ramen Bar', 'Ramen Tonkotsu', 'El ramen estaba bien pero el tiempo de espera fue de más de 45 minutos. Para un camión de comida, eso es demasiado largo. El sabor estaba bien pero no valió la espera.', '2024-11-22', 3, 'Español'),
('Brian Taylor', 'Venganza de los Quesos', 'Poutine Clásica', 'Como canadiense viviendo en Seattle, puedo confirmar que esta es poutine auténtica! La receta de la salsa debe ser de Quebec. ¡Tan feliz de encontrar esta joya!', '2024-11-23', 5, 'Español'),
('Monica Harris', 'El Rincón de las Crêpes', 'Crêpe de Nutella y Plátano', 'La crêpe estaba deliciosa pero no había suficiente Nutella. Por el precio, esperaba más relleno. ¡Aún así sabrosa!', '2024-11-24', 3, 'Español'),
('Kevin Anderson', 'Cocina de Nani', 'Pollo Tikka Masala', '¡La mejor comida india que he probado de un camión de comida! El curry es rico y cremoso, el pollo está tierno, y el nivel de especias es perfecto. ¡Volveré semanalmente!', '2024-11-25', 5, 'Español'),
('Gregory Peterson', 'BBQ Ahumado', 'Sándwich de Cerdo Desmenuzado', '¡BBQ excepcional! El cerdo desmenuzado es increíblemente tierno y ahumado. La salsa BBQ tiene el equilibrio perfecto de dulce y picante. El tamaño de la porción es generoso y el servicio fue rápido. ¡Este es ahora mi lugar favorito para BBQ auténtico!', '2024-11-26', 5, 'Español');

-- ============================================================================
-- INSERTAR DATOS DE EJEMPLO - TICKETS DE SOPORTE (SIN PII)
-- ============================================================================

INSERT INTO SUPPORT_TICKETS (customer_name, food_truck_name, issue_description, created_date, status, urgency) VALUES
('Sarah Johnson', 'BBQ Ahumado', 'Pedí el sándwich de cerdo desmenuzado ayer pero recibí el artículo incorrecto. El pedido se suponía que era para entrega pero llegó incorrecto. Necesito que esto se resuelva lo antes posible.', '2024-10-15', 'Abierto', 'Alta'),
('Michael Chen', 'Guac n Roll', 'Me cobraron dos veces por mi pedido el sábado. Por favor procesen un reembolso por el cargo duplicado. Esto parece ser un error del sistema.', '2024-10-16', 'En Progreso', 'Alta'),
('Emily Rodriguez', 'Punto de Congelación', 'Mi pedido de helado llegó completamente derretido. Me gustaría un reembolso o reemplazo. Experiencia muy decepcionante.', '2024-10-17', 'Abierto', 'Media'),
('David Williams', 'El Rincón de las Crêpes', 'Solo quería felicitar sus increíbles crêpes! La calidad y el servicio fueron excepcionales. ¡Sigan con el gran trabajo!', '2024-10-18', 'Cerrado', 'Baja'),
('Jessica Martinez', 'Palacio Vegetal', 'Tengo una alergia severa a las nueces y necesito verificar si su Hamburguesa Beyond contiene nueces de árbol antes de pedir. Esto es urgente por razones de salud.', '2024-10-19', 'Abierto', 'Alta'),
('James Taylor', 'Camión de Pekín', '¡Sus dumplings de sopa fueron increíbles! Me encantaría reservar su camión para un evento corporativo. Por favor envíenme información sobre servicios de catering.', '2024-10-20', 'En Progreso', 'Media'),
('Lisa Anderson', 'Kitakata Ramen Bar', 'Encontré un cabello en mi ramen la semana pasada. Esto es inaceptable y espero un reembolso completo. Muy infeliz con esta experiencia.', '2024-10-21', 'Abierto', 'Alta'),
('Robert Brown', 'Griego Descarado', 'Estoy consultando sobre oportunidades de franquicia para su marca de camión de comida. Por favor envíenme información sobre requisitos y detalles de inversión.', '2024-10-22', 'En Progreso', 'Baja'),
('María García', 'Venganza de los Quesos', '¡Su poutine es increíble! Quiero pedir catering para mi fiesta de cumpleaños el próximo mes. Por favor envíen información de precios para grupos grandes.', '2024-10-23', 'Cerrado', 'Media'),
('Thomas White', 'Cocina de Nani', 'El pollo tikka masala llegó frío cuando fue entregado. Para el precio, esto no es aceptable. Me gustaría un reembolso.', '2024-10-24', 'En Progreso', 'Alta'),
('Amanda Lee', 'Punto de Congelación', 'Quería reportar un excelente servicio de su personal ayer! Hicieron más de lo necesario para ayudar. ¡Sigan con el trabajo increíble!', '2024-10-25', 'Cerrado', 'Baja'),
('Christopher Davis', 'Guac n Roll', 'El burrito no tenía guacamole a pesar de que pagué extra por él. Me cobraron el precio completo. Esto necesita ser corregido.', '2024-10-26', 'Abierto', 'Media'),
('Jennifer Wilson', 'BBQ Ahumado', '¡Su sándwich de cerdo desmenuzado hizo mi día! Los sabores fueron increíbles. Me encantaría obtener la receta si es posible. ¡Gracias por la comida increíble!', '2024-10-27', 'Cerrado', 'Baja'),
('Daniel Moore', 'Kitakata Ramen Bar', 'Su ramen me dio intoxicación alimentaria. Mi abogado los contactará. Este es un problema serio de salud y seguridad.', '2024-10-28', 'Abierto', 'Alta'),
('Michelle Taylor', 'El Rincón de las Crêpes', 'Estoy solicitando información de catering para un evento grande. Necesito precios para aproximadamente doscientas personas. Por favor respondan con disponibilidad.', '2024-10-29', 'En Progreso', 'Media'),
('Kevin Martinez', 'Griego Descarado', 'El gyro de cordero estaba poco cocido y me enfermé después de comerlo. Necesito un reembolso completo inmediatamente.', '2024-10-30', 'Abierto', 'Alta'),
('Laura Anderson', 'Palacio Vegetal', 'Estoy consultando sobre oportunidades de trabajo con su camión de comida. Tengo cinco años de experiencia en servicio de alimentos y estoy muy interesada en unirme a su equipo.', '2024-10-31', 'En Progreso', 'Baja'),
('Steven Jackson', 'Camión de Pekín', '¡Sus dumplings de sopa son los mejores! Mi doctor recomendó su comida para mi dieta. Gracias por proporcionar opciones saludables y deliciosas.', '2024-11-01', 'Cerrado', 'Baja'),
('Karen Thomas', 'Venganza de los Quesos', 'Necesito un recibo fiscal para un gasto de negocio. Mi pedido reciente necesita ser documentado adecuadamente para propósitos contables.', '2024-11-02', 'Abierto', 'Baja'),
('Patricia White', 'Cocina de Nani', 'Recientemente me uní a su programa de lealtad y amo su comida! Esperando ganar recompensas. ¡Sigan haciendo deliciosa comida india!', '2024-11-03', 'Cerrado', 'Baja');

-- ============================================================================
-- INSERTAR DATOS DE EJEMPLO - TICKETS DE SOPORTE CON PII (SOLO PARA AI_REDACT)
-- ============================================================================

INSERT INTO SUPPORT_TICKETS_PII (customer_name, food_truck_name, issue_description, created_date, status, urgency) VALUES
('Sarah Johnson', 'BBQ Ahumado', 'Hola, soy Sarah Johnson (SSN: 123-45-6789, FDN: 03/15/1985, Mujer) y pedí el sándwich de cerdo desmenuzado ayer pero estaba completamente incorrecto. Pueden contactarme en sarah.j@email.com o llamar al 555-123-4567. Necesito que esto se resuelva lo antes posible. Mi pedido se suponía que fuera entregado a 742 Evergreen Terrace, Austin, TX 78701. Mi tarjeta de crédito 4532-1234-5678-9012 fue cobrada.', '2024-10-15', 'Abierto', 'Alta'),
('Michael Chen', 'Guac n Roll', 'Este es Michael Chen, FDN 03/15/1985, Hombre, Licencia # D1234567, SSN 234-56-7890. Visité su camión el sábado y me cobraron dos veces en mi tarjeta que termina en 4532. Por favor reembolsen a michael.chen@gmail.com o llámenme al 555-234-5678. Mi dirección de facturación es 1428 Elm Street, Apt 5B, Portland, OR 97209.', '2024-10-16', 'En Progreso', 'Alta'),
('Emily Rodriguez', 'Punto de Congelación', 'Estoy escribiendo sobre mi pedido reciente. Mi nombre es Emily Rodriguez (Mujer, Edad 32, SSN: 345-67-8901), email emily.rodriguez@yahoo.com, teléfono 555-345-6789. Vivo en 3845 Oak Avenue, Miami, FL 33101. El helado que recibí estaba derretido. Mi tarjeta de pago 5412-7534-8901-2345 necesita ser reembolsada.', '2024-10-17', 'Abierto', 'Media'),
('David Williams', 'El Rincón de las Crêpes', 'Solo quería felicitar sus increíbles crêpes! Soy David Williams (Hombre, FDN: 07/22/1990, Licencia # D7654321) y pueden contactarme al 555-456-7890 o david.w@outlook.com si alguna vez necesitan un testimonial. Vivo cerca en 956 Pine Street, Portland, OR 97209. ¡Sigan con el gran trabajo!', '2024-10-18', 'Cerrado', 'Baja'),
('Jessica Martinez', 'Palacio Vegetal', 'Hola, esta es Jessica Martinez (Mujer, Edad 29, SSN: 456-78-9012, FDN: 08/30/1995) llamando desde 555-567-8901. Tengo una alergia severa a las nueces y necesito saber si su Hamburguesa Beyond contiene nueces de árbol. Mi email es jessica.m@email.com. Estoy ubicada en 2134 Maple Drive, Los Ángeles, CA 90028. Por favor respondan urgentemente.', '2024-10-19', 'Abierto', 'Alta'),
('James Taylor', 'Camión de Pekín', 'Hola, James Taylor aquí (Hombre, Pasaporte P12345678, SSN: 567-89-0123). Visité su camión ayer y los dumplings de sopa fueron increíbles! Me encantaría reservarlos para un evento corporativo. Por favor contáctenme en james.taylor@company.com o 555-678-9012. Nuestra oficina está en 675 Birch Lane, Nueva York, NY 10001.', '2024-10-20', 'En Progreso', 'Media'),
('Lisa Anderson', 'Kitakata Ramen Bar', 'Esta es Lisa Anderson, Mujer, nacida 11/30/1988, SSN: 678-90-1234. Pedí ramen la semana pasada y encontré un cabello en él. Esto es inaceptable. Mi información de contacto: lisa.a@email.net o 555-789-0123. Vivo en 1523 Cedar Court, San Francisco, CA 94102. Licencia # D8765432. Espero un reembolso completo.', '2024-10-21', 'Abierto', 'Alta'),
('Robert Brown', 'Griego Descarado', 'Robert Brown aquí, Hombre, Edad 42, SSN en archivo es 789-01-2345, Licencia # D2468135. Estoy consultando sobre oportunidades de franquicia. Por favor envíen información a robert.brown@business.com o llamen al 555-890-1234. Mi dirección actual es 789 Willow Way, Denver, CO 80201. Tarjeta de crédito 6011-2345-6789-0123.', '2024-10-22', 'En Progreso', 'Baja'),
('María García', 'Venganza de los Quesos', 'María García (Mujer, FDN 09/25/1985, SSN: 890-12-3456) llamando desde 555-901-2345. ¡Su poutine es increíble! Quiero pedir catering para mi fiesta de cumpleaños el 09/25. Envíenme un email a maria.garcia@party.com con precios. Ubicación del evento: 445 Sunset Boulevard, Seattle, WA 98101. Dirección IP 192.168.1.105.', '2024-10-23', 'Cerrado', 'Media'),
('Thomas White', 'Cocina de Nani', 'Este es Thomas White, Hombre, Edad 38, SSN: 901-23-4567, Pasaporte P87654321. He estado tratando de contactar a alguien sobre mi pedido. Mi teléfono es 555-012-3456 y email es thomas.white@email.com. Estoy ubicado en 234 River Road, Miami, FL 33101. El pollo tikka masala estaba frío cuando fue entregado. Tarjeta 3782-8224-6310-005. Necesito reembolso.', '2024-10-24', 'En Progreso', 'Alta'),
('Amanda Lee', 'Punto de Congelación', 'Amanda Lee aquí, Mujer, nacida 12/08/1992, SSN: 012-34-5678, Licencia # D1357924. Quería reportar un excelente servicio de su personal ayer! Pueden contactarme en amanda.lee@health.org o 555-123-4560. Vivo en 1867 Mountain View, Boston, MA 02101. ¡Sigan con el trabajo increíble!', '2024-10-25', 'Cerrado', 'Baja'),
('Christopher Davis', 'Guac n Roll', 'Christopher Davis (Hombre, Edad 44, FDN 04/12/1980, SSN: 123-45-6780) escribiendo para quejarme del servicio. Mi teléfono: 555-234-5670, email: c.davis@email.com. El burrito no tenía guacamole a pesar de que pagué extra. Mi tarjeta que termina en 6789 fue cobrada el precio completo. Dirección: 678 Valley Street, Phoenix, AZ 85001. Licencia # D9876543.', '2024-10-26', 'Abierto', 'Media'),
('Jennifer Wilson', 'BBQ Ahumado', 'Hola, Jennifer Wilson aquí (Mujer, FDN 06/14/1990, SSN: 234-56-7891, Pasaporte P23456789). ¡Su sándwich de cerdo desmenuzado hizo mi día! ¿Puedo obtener la receta? Contáctenme en j.wilson@foodie.com o 555-345-6781. Estoy en 891 School Lane, Austin, TX 78701. ¡Gracias por la comida increíble!', '2024-10-27', 'Cerrado', 'Baja'),
('Daniel Moore', 'Kitakata Ramen Bar', 'Daniel Moore (Hombre, Edad 47, SSN: 567-89-0123, Licencia # D5432167) presentando queja. Su ramen me dio intoxicación alimentaria. El abogado los contactará. Mi información: 555-456-7892, moore.legal@lawfirm.com. Dirección: 1122 Justice Drive, Dallas, TX 75201. Tarjeta de crédito 4111-1111-1111-1111. Esto es serio.', '2024-10-28', 'Abierto', 'Alta'),
('Michelle Taylor', 'El Rincón de las Crêpes', 'Michelle Taylor (Mujer, FDN 08/30/1985, SSN: 678-90-1235, Pasaporte P12345678) solicitando información de catering. Mi licencia de conducir es D3698521 si necesitan identificación para el lugar. Contacto: michelle.t@events.com o 555-567-8903. Lugar del evento: 445 Airport Road, Portland, OR 97209. Tarjeta de crédito 5555-4444-3333-2222. Necesito precios para 200 personas.', '2024-10-29', 'En Progreso', 'Media'),
('Kevin Martinez', 'Griego Descarado', 'Kevin Martinez aquí (Hombre, nacido 10/15/1991, SSN: 789-01-2346, Edad 33, Licencia # D7531598). El gyro de cordero estaba poco cocido y me enfermé. Mi tarjeta de crédito 3782-8224-6310-005 fue cobrada. Email: kevin.m@email.com, teléfono: 555-678-9014. Casa: 2233 Rental Avenue, Chicago, IL 60601. Pasaporte P98765432. Necesito reembolso completo.', '2024-10-30', 'Abierto', 'Alta'),
('Laura Anderson', 'Palacio Vegetal', 'Laura Anderson (Mujer, FDN 01/25/1995, Edad 29, SSN: 890-12-3457) consultando sobre oportunidades de trabajo. Currículum adjunto. Contacto: laura.anderson@jobseeker.com o 555-789-0125. Actualmente en 556 Career Path, Houston, TX 77001. Tengo 5 años de experiencia en servicio de alimentos. Licencia # D1928374. ¡Muy interesada!', '2024-10-31', 'En Progreso', 'Baja'),
('Steven Jackson', 'Camión de Pekín', 'Steven Jackson (Hombre, FDN 07/04/1970, Edad 54, SSN: 901-23-4568) con comentarios. ¡Sus dumplings de sopa son los mejores! Mi doctor al 555-890-1236 recomendó su comida para mi dieta. Email: steven.j@health.net. Dirección: 778 Wellness Street, Minneapolis, MN 55401. Pasaporte P11223344.', '2024-11-01', 'Cerrado', 'Baja'),
('Karen Thomas', 'Venganza de los Quesos', 'Karen Thomas (Mujer, FDN 12/18/1983, Edad 40, SSN: 012-34-5679, Licencia # D4826159) solicitando recibo fiscal para gasto de negocio. Dirección de negocio: 334 Revenue Road, Sacramento, CA 95814. Email a karen.thomas@business.com. Teléfono: 555-901-2347. El pedido fue de $156.80 en tarjeta 4532-8765-4321-9876.', '2024-11-02', 'Abierto', 'Baja'),
('Patricia White', 'Cocina de Nani', 'Patricia White aquí (Mujer, FDN 05/22/1989, Edad 35, SSN: 123-45-6791, Pasaporte P55667788). Me uní a su programa de lealtad. Tarjeta 4111-1111-1111-1111 en archivo. Envíen recompensas a 667 Member Lane, Miami, FL 33101. Contacto: patricia.white@email.org o 555-012-3458. Licencia # D9517530. ID de miembro: CLB-99887. ¡Amo su comida!', '2024-11-03', 'Cerrado', 'Baja');

-- ============================================================================
-- CREAR VISTAS PARA ANÁLISIS
-- ============================================================================

-- Vista para análisis de reseñas
CREATE OR REPLACE VIEW REVIEW_ANALYTICS AS
SELECT 
    food_truck_name,
    AVG(rating) as avg_rating,
    COUNT(*) as total_reviews,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) as negative_reviews
FROM CUSTOMER_REVIEWS
GROUP BY food_truck_name;

-- Vista para popularidad de elementos del menú
CREATE OR REPLACE VIEW POPULAR_ITEMS AS
SELECT 
    menu_item,
    food_truck_name,
    COUNT(*) as order_count,
    AVG(rating) as avg_rating
FROM CUSTOMER_REVIEWS
GROUP BY menu_item, food_truck_name
ORDER BY order_count DESC;

-- ============================================================================
-- OTORGAR PRIVILEGIOS (ajustar según sea necesario para su entorno)
-- ============================================================================

-- Otorgar uso en base de datos y esquema
GRANT USAGE ON DATABASE AISQL_PLAYGROUND_ES TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA AISQL_PLAYGROUND_ES.DEMO TO ROLE SYSADMIN;

-- Crear warehouse para Cortex Search (si no existe)
CREATE WAREHOUSE IF NOT EXISTS CORTEX_SEARCH_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Otorgar privilegios en todas las tablas
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA AISQL_PLAYGROUND_ES.DEMO TO ROLE SYSADMIN;

-- Otorgar privilegios en stages
GRANT READ, WRITE ON STAGE AISQL_PLAYGROUND_ES.DEMO.AUDIO_STAGE TO ROLE SYSADMIN;
GRANT READ, WRITE ON STAGE AISQL_PLAYGROUND_ES.DEMO.DOCUMENT_STAGE TO ROLE SYSADMIN;
GRANT READ, WRITE ON STAGE AISQL_PLAYGROUND_ES.DEMO.SUPPLIER_DOCUMENTS_STAGE TO ROLE SYSADMIN;
GRANT READ, WRITE ON STAGE AISQL_PLAYGROUND_ES.DEMO.IMAGE_STAGE TO ROLE SYSADMIN;

-- Otorgar privilegios en vistas
GRANT SELECT ON ALL VIEWS IN SCHEMA AISQL_PLAYGROUND_ES.DEMO TO ROLE SYSADMIN;

-- ============================================================================
-- CONSULTAS DE VERIFICACIÓN
-- ============================================================================

-- Verificar food trucks
SELECT COUNT(*) as food_truck_count FROM FOOD_TRUCKS;

-- Verificar elementos del menú
SELECT COUNT(*) as menu_item_count FROM MENU_ITEMS;

-- Verificar reseñas de clientes
SELECT COUNT(*) as review_count FROM CUSTOMER_REVIEWS;

-- Verificar tickets de soporte
SELECT COUNT(*) as ticket_count FROM SUPPORT_TICKETS;
SELECT COUNT(*) as ticket_pii_count FROM SUPPORT_TICKETS_PII;

-- Verificar tablas de análisis de documentos
SELECT COUNT(*) as raw_docs_count FROM PARSE_DOC_RAW_TEXT;
SELECT COUNT(*) as chunked_docs_count FROM PARSE_DOC_CHUNKED_TEXT;

-- Verificar tabla de detalles de facturas de proveedores
SELECT COUNT(*) as invoice_count FROM SUPPLIER_INVOICE_DETAILS;

-- Listar todos los stages
SHOW STAGES;

-- ============================================================================
-- INSTRUCCIONES DE ARCHIVOS DE EJEMPLO
-- ============================================================================

-- Después de ejecutar este script, necesitarás cargar manualmente archivos de ejemplo:
-- 
-- Opción 1: Usando la interfaz de Snowsight (Carga Manual)
-- Navega a Data > Databases > AISQL_PLAYGROUND_ES > DEMO > Stages
-- Selecciona el stage apropiado y haz clic en "+ Files" para cargar archivos
-- Documentación: https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui
--
-- Opción 2: Usando CLI de Snowflake (comando PUT)
-- 1. ARCHIVOS DE AUDIO (para ejemplos de AI_TRANSCRIBE):
--    PUT file:///ruta/a/audio.wav @AUDIO_STAGE AUTO_COMPRESS=FALSE;
--
-- 2. ARCHIVOS DE DOCUMENTOS (para ejemplos de AI_PARSE_DOCUMENT):
--    PUT file:///ruta/a/documento.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE;
--
-- 3. ARCHIVOS DE FACTURAS DE PROVEEDORES (para ejemplos de AI_EXTRACT):
--    PUT file://factura_proveedor_*.pdf @SUPPLIER_DOCUMENTS_STAGE AUTO_COMPRESS=FALSE;
--
-- 4. ARCHIVOS DE IMÁGENES (para ejemplos de AI_CLASSIFY y AI_COMPLETE con imágenes):
--    PUT file:///ruta/a/imagen.jpg @IMAGE_STAGE AUTO_COMPRESS=FALSE;

-- ============================================================================
-- FIN DEL SCRIPT DE CONFIGURACIÓN
-- ============================================================================

SELECT '¡Configuración de base de datos completada exitosamente!' as status;

