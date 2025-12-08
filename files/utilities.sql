CREATE OR REPLACE VIEW VISTA_CLIENTES_VIP AS
SELECT CC_NAME
FROM (SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER);

CREATE OR REPLACE FUNCTION EVALUAR_RIESGO_CREDITICIO()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return Object.getOwnPropertyNames(this);
$$;

CREATE OR REPLACE FUNCTION PREDECIR_FUGA_CLIENTES()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return Object.getOwnPropertyNames(this);
$$;

CREATE OR REPLACE FUNCTION VALIDAR_CUMPLIMIENTO_REGULATORIO()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return Object.getOwnPropertyNames(this);
$$;

CREATE OR REPLACE FUNCTION DETECTAR_FRAUDE_RECLAMACIONES()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return Object.getOwnPropertyNames(this);
$$;

CREATE OR REPLACE FUNCTION SEGMENTAR_USUARIOS_POR_PREFERENCIAS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return Object.getOwnPropertyNames(this);
$$;

-- ====================================================================
-- LINAJE - TABLAS BASE
-- ====================================================================

CREATE OR REPLACE TABLE Clientes_lin (
  cliente_id       INT PRIMARY KEY,
  nombre           VARCHAR(100),
  fecha_nacimiento DATE,
  genero           CHAR(1),
  pais             VARCHAR(50)
);

CREATE OR REPLACE TABLE Transacciones (
  transaccion_id   INT PRIMARY KEY,
  cliente_id       INT,
  fecha            DATE,
  monto            DECIMAL(10, 2),
  tipo_transaccion VARCHAR(20),
  FOREIGN KEY (cliente_id) REFERENCES Clientes_lin(cliente_id)
);

CREATE OR REPLACE TABLE Cuentas (
  cuenta_id   INT PRIMARY KEY,
  cliente_id  INT,
  tipo_cuenta VARCHAR(50),
  saldo       DECIMAL(10, 2),
  FOREIGN KEY (cliente_id) REFERENCES Clientes_lin(cliente_id)
);

-- ====================================================================
-- LINAJE - DATOS DE EJEMPLO
-- ====================================================================

INSERT INTO Clientes_lin (cliente_id, nombre, fecha_nacimiento, genero, pais)
VALUES
  (1, 'Juan Pérez', '1980-01-01', 'M', 'Colombia'),
  (2, 'Ana Gómez', '1990-05-10', 'F', 'Perú'),
  (3, 'Luis Torres', '1975-08-20', 'M', 'Chile');

INSERT INTO Transacciones (transaccion_id, cliente_id, fecha, monto, tipo_transaccion)
VALUES
  (1, 1, '2024-01-15', 5000.00, 'Depósito'),
  (2, 1, '2024-01-20', -200.00, 'Retiro'),
  (3, 2, '2024-01-18', 1000.00, 'Depósito'),
  (4, 2, '2024-01-22', -150.00, 'Pago Tarjeta'),
  (5, 3, '2024-01-19', 7000.00, 'Depósito');

INSERT INTO Cuentas (cuenta_id, cliente_id, tipo_cuenta, saldo)
VALUES
  (1, 1, 'Ahorros', 5200.00),
  (2, 1, 'Corriente', 300.00),
  (3, 2, 'Ahorros', 1150.00),
  (4, 3, 'Ahorros', 7000.00);

-- ====================================================================
-- LINAJE - VISTAS INTERMEDIAS
-- ====================================================================

CREATE OR REPLACE VIEW ResumenTransacciones AS
SELECT
  c.cliente_id,
  c.nombre,
  c.pais,
  SUM(CASE WHEN t.tipo_transaccion = 'Depósito' THEN t.monto ELSE 0 END) AS total_depositos,
  SUM(CASE WHEN t.tipo_transaccion = 'Retiro' THEN -t.monto ELSE 0 END) AS total_retiros,
  COUNT(t.transaccion_id) AS total_transacciones
FROM Clientes_lin c
JOIN Transacciones t ON c.cliente_id = t.cliente_id
GROUP BY c.cliente_id, c.nombre, c.pais;

CREATE OR REPLACE VIEW ReporteIngresosPais AS
SELECT
  c.pais,
  DATE_TRUNC('month', t.fecha) AS mes,
  SUM(CASE WHEN t.monto > 0 THEN t.monto ELSE 0 END) AS ingresos_mensuales,
  COUNT(t.transaccion_id) AS total_transacciones
FROM Transacciones t
JOIN Clientes_lin c ON t.cliente_id = c.cliente_id
GROUP BY c.pais, DATE_TRUNC('month', t.fecha);

CREATE OR REPLACE VIEW AlertasRiesgo AS
SELECT
  t.transaccion_id,
  t.cliente_id,
  c.nombre,
  t.fecha,
  t.monto,
  CASE
    WHEN t.monto > 5000 THEN 'Alta'
    WHEN t.monto > 1000 THEN 'Media'
    ELSE 'Baja'
  END AS nivel_riesgo
FROM Transacciones t
JOIN Clientes_lin c ON t.cliente_id = c.cliente_id
WHERE t.monto > 1000;

CREATE OR REPLACE VIEW ReporteRiesgoDetallado AS
SELECT
  a.transaccion_id,
  a.cliente_id,
  a.nombre AS cliente_nombre,
  a.fecha AS fecha_transaccion,
  a.monto AS monto_transaccion,
  a.nivel_riesgo,
  c.tipo_cuenta,
  c.saldo AS saldo_cuenta,
  cl.pais
FROM AlertasRiesgo a
JOIN Cuentas c ON a.cliente_id = c.cliente_id
JOIN Clientes_lin cl ON a.cliente_id = cl.cliente_id
WHERE a.nivel_riesgo = 'Alta';

-- ====================================================================
-- FUNCIONES SECURE
-- ====================================================================

CREATE OR REPLACE SECURE FUNCTION obtener_score_crediticio(id_cliente STRING)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
  // Usar arguments[0] para acceder al primer argumento que es el 'id_cliente'
  var input_id = arguments[0];

  // Generar un score crediticio ficticio basado en el ID
  var hash_value = 0;
  for (var i = 0; i < input_id.length; i++) {
    hash_value += input_id.charCodeAt(i) * (i + 1);
  }

  // Crear una fórmula ficticia para generar un score entre 300 y 850
  var score_ficticio = 300 + (hash_value % 550);

  return parseFloat(score_ficticio);
$$;

SELECT obtener_score_crediticio('801783645') AS score;

CREATE OR REPLACE SECURE FUNCTION datos_complementarios(n NUMBER)
RETURNS TABLE(col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING)
LANGUAGE SQL
AS
$$
  SELECT
    TO_CHAR(UNIFORM(1, 1000, RANDOM())) AS col1,
    TO_CHAR('A') AS col2,
    TO_CHAR(UNIFORM(1, 1000, RANDOM())) AS col3,
    TO_CHAR(110121) AS col4,
    TO_CHAR('CO') AS col5
$$;

SELECT * FROM TABLE(datos_complementarios(801783645));

-- ====================================================================
-- TABLA SEGMENTOS CLIENTES
-- ====================================================================

CREATE OR REPLACE TABLE segmentos_clientes (
  cliente_id                STRING,
  segmento                  STRING,
  antiguedad_cliente        INT,
  promedio_compra           FLOAT,
  frecuencia_compra         INT,
  nivel_ingreso_est         STRING,
  tipo_producto_preferido   STRING,
  canal_preferido           STRING,
  puntaje_satisfaccion      INT,
  probabilidad_recompra     FLOAT,
  riesgo_crediticio_est     STRING
);

-- ====================================================================
-- PROCEDIMIENTOS ALMACENADOS
-- ====================================================================

CREATE OR REPLACE PROCEDURE CALCULAR_BALANCE_GENERAL(ARGUMENT1 VARCHAR)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  var INPUT_ARGUMENT1 = ARGUMENT1;
  var result = `${INPUT_ARGUMENT1}`;
  return result;
$$;

CREATE OR REPLACE PROCEDURE PROCESAR_RECLAMACIONES_DE_SINIESTROS(ARGUMENT1 VARCHAR)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  var INPUT_ARGUMENT1 = ARGUMENT1;
  var result = `${INPUT_ARGUMENT1}`;
  return result;
$$;

CREATE OR REPLACE PROCEDURE ACTUALIZAR_LIMITES_DE_CREDITOS(ARGUMENT1 VARCHAR)
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  var INPUT_ARGUMENT1 = ARGUMENT1;
  var result = `${INPUT_ARGUMENT1}`;
  return result;
$$;

-- ====================================================================
-- SECUENCIAS, STREAMS Y TAREAS
-- ====================================================================

CREATE OR REPLACE SEQUENCE SEQ_01 START = 1 INCREMENT = 1;

CREATE OR REPLACE STREAM STREAM_A ON TABLE CLIENTES;

CREATE OR REPLACE PROCEDURE drop_db()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  var cmd = `DROP DATABASE "DEMO3B_DB";`;
  var sql = snowflake.createStatement({sqlText: cmd});
  var result = sql.execute();
  return 'Database has been successfully dropped';
$$;

CREATE OR REPLACE TASK tsk_wait_15
  WAREHOUSE = VW_GENAI
  SCHEDULE  = '11520 MINUTE'
AS
  CALL drop_db();

ALTER TASK tsk_wait_15 SUSPEND;