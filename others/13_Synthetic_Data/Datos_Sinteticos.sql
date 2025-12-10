-- ============================================
-- EJEMPLO: Clientes y Transacciones con Datos Sintéticos
-- ============================================
-- Datos sintéticos: mantienen propiedades estadísticas sin información real
-- Beneficios: privacidad, desarrollo rápido, compliance, sin dependencias externas

-- PARTE 0: Crear base de datos y esquema

CREATE OR REPLACE DATABASE DATOS_SINTETICOS_DEMO;
USE DATABASE DATOS_SINTETICOS_DEMO;

CREATE OR REPLACE SCHEMA DEMO;
USE SCHEMA DEMO;

-- PARTE 1: Crear tablas
CREATE OR REPLACE TABLE DATOS_SINTETICOS_DEMO.DEMO.CLIENTES (
    cliente_id NUMBER PRIMARY KEY,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    email VARCHAR(150),
    telefono VARCHAR(20),
    fecha_nacimiento DATE,
    ciudad VARCHAR(100),
    pais VARCHAR(100),
    salario_anual NUMBER(10,2),
    fecha_registro TIMESTAMP_NTZ,
    es_premium BOOLEAN,
    categoria VARCHAR(50)
);

CREATE OR REPLACE TABLE DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES (
    transaccion_id NUMBER PRIMARY KEY,
    cliente_id NUMBER,
    fecha_transaccion TIMESTAMP_NTZ,
    monto NUMBER(10,2),
    tipo_transaccion VARCHAR(50),
    metodo_pago VARCHAR(50),
    descripcion VARCHAR(200),
    estado VARCHAR(20),
    FOREIGN KEY (cliente_id) REFERENCES DATOS_SINTETICOS_DEMO.DEMO.CLIENTES(cliente_id)
);

-- PARTE 2: Insertar datos
INSERT INTO DATOS_SINTETICOS_DEMO.DEMO.CLIENTES VALUES
(1, 'María', 'González', 'maria.gonzalez@email.com', '+57 300 1234567', '1985-03-15', 'Bogotá', 'Colombia', 45000.00, '2020-01-15 10:30:00', TRUE, 'VIP'),
(2, 'Juan', 'Pérez', 'juan.perez@email.com', '+57 301 2345678', '1990-07-22', 'Medellín', 'Colombia', 38000.00, '2020-02-20 14:15:00', FALSE, 'Regular'),
(3, 'Ana', 'Rodríguez', 'ana.rodriguez@email.com', '+57 302 3456789', '1988-11-08', 'Cali', 'Colombia', 52000.00, '2020-03-10 09:00:00', TRUE, 'Premium'),
(4, 'Carlos', 'Martínez', 'carlos.martinez@email.com', '+57 303 4567890', '1992-05-30', 'Barranquilla', 'Colombia', 35000.00, '2020-04-05 16:45:00', FALSE, 'Regular'),
(5, 'Laura', 'López', 'laura.lopez@email.com', '+57 304 5678901', '1987-09-12', 'Bogotá', 'Colombia', 48000.00, '2020-05-12 11:20:00', TRUE, 'VIP'),
(6, 'Diego', 'Fernández', 'diego.fernandez@email.com', '+57 305 6789012', '1991-12-25', 'Medellín', 'Colombia', 42000.00, '2020-06-18 13:30:00', FALSE, 'Regular'),
(7, 'Sofía', 'Sánchez', 'sofia.sanchez@email.com', '+57 306 7890123', '1989-04-18', 'Bucaramanga', 'Colombia', 55000.00, '2020-07-22 08:15:00', TRUE, 'Premium'),
(8, 'Andrés', 'Ramírez', 'andres.ramirez@email.com', '+57 307 8901234', '1993-08-03', 'Pereira', 'Colombia', 32000.00, '2020-08-30 15:00:00', FALSE, 'Regular'),
(9, 'Valentina', 'Torres', 'valentina.torres@email.com', '+57 308 9012345', '1986-01-20', 'Bogotá', 'Colombia', 49000.00, '2021-01-10 10:00:00', TRUE, 'VIP'),
(10, 'Sebastián', 'García', 'sebastian.garcia@email.com', '+57 309 0123456', '1994-06-14', 'Cartagena', 'Colombia', 36000.00, '2021-02-15 12:30:00', FALSE, 'Regular'),
(11, 'Camila', 'Vargas', 'camila.vargas@email.com', '+57 310 1234567', '1990-10-28', 'Manizales', 'Colombia', 51000.00, '2021-03-20 09:45:00', TRUE, 'Premium'),
(12, 'Nicolás', 'Jiménez', 'nicolas.jimenez@email.com', '+57 311 2345678', '1988-02-07', 'Bogotá', 'Colombia', 44000.00, '2021-04-25 14:20:00', FALSE, 'Regular'),
(13, 'Isabella', 'Herrera', 'isabella.herrera@email.com', '+57 312 3456789', '1992-12-11', 'Medellín', 'Colombia', 47000.00, '2021-05-30 11:10:00', TRUE, 'VIP'),
(14, 'Mateo', 'Castro', 'mateo.castro@email.com', '+57 313 4567890', '1987-07-05', 'Cali', 'Colombia', 40000.00, '2021-06-15 16:00:00', FALSE, 'Regular'),
(15, 'Emma', 'Morales', 'emma.morales@email.com', '+57 314 5678901', '1991-03-19', 'Ibagué', 'Colombia', 53000.00, '2021-07-22 08:30:00', TRUE, 'Premium');

INSERT INTO DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES VALUES
(1, 1, '2023-01-15 10:30:00', 1250.50, 'Compra', 'Tarjeta Crédito', 'Compra en tienda online', 'Completada'),
(2, 1, '2023-02-20 14:20:00', 3200.00, 'Compra', 'Tarjeta Débito', 'Pago de servicios', 'Completada'),
(3, 2, '2023-01-25 09:15:00', 850.75, 'Compra', 'Efectivo', 'Restaurante', 'Completada'),
(4, 2, '2023-03-10 16:45:00', 1500.00, 'Transferencia', 'Transferencia Bancaria', 'Pago a proveedor', 'Completada'),
(5, 3, '2023-02-05 11:30:00', 4500.25, 'Compra', 'Tarjeta Crédito', 'Electrodomésticos', 'Completada'),
(6, 3, '2023-04-12 13:20:00', 2100.00, 'Compra', 'Tarjeta Débito', 'Supermercado', 'Completada'),
(7, 4, '2023-01-18 15:00:00', 680.50, 'Compra', 'Efectivo', 'Farmacia', 'Completada'),
(8, 4, '2023-03-22 10:15:00', 1200.00, 'Compra', 'Tarjeta Crédito', 'Ropa', 'Completada'),
(9, 5, '2023-02-28 12:40:00', 3500.75, 'Compra', 'Tarjeta Crédito', 'Viaje', 'Completada'),
(10, 5, '2023-05-05 09:30:00', 1850.00, 'Transferencia', 'Transferencia Bancaria', 'Pago de alquiler', 'Completada'),
(11, 6, '2023-03-15 14:50:00', 950.25, 'Compra', 'Efectivo', 'Gasolina', 'Completada'),
(12, 6, '2023-04-20 11:20:00', 2200.00, 'Compra', 'Tarjeta Débito', 'Muebles', 'Completada'),
(13, 7, '2023-02-10 16:30:00', 5200.00, 'Compra', 'Tarjeta Crédito', 'Electrónica', 'Completada'),
(14, 7, '2023-05-15 10:00:00', 2800.50, 'Transferencia', 'Transferencia Bancaria', 'Inversión', 'Completada'),
(15, 8, '2023-01-30 13:15:00', 750.00, 'Compra', 'Efectivo', 'Comida rápida', 'Completada'),
(16, 8, '2023-04-08 15:45:00', 1350.00, 'Compra', 'Tarjeta Crédito', 'Libros', 'Completada'),
(17, 9, '2023-03-25 11:00:00', 4100.25, 'Compra', 'Tarjeta Crédito', 'Decoración', 'Completada'),
(18, 9, '2023-05-22 14:30:00', 1950.00, 'Transferencia', 'Transferencia Bancaria', 'Pago de facturas', 'Completada'),
(19, 10, '2023-02-12 09:20:00', 1100.75, 'Compra', 'Tarjeta Débito', 'Gimnasio', 'Completada'),
(20, 10, '2023-04-18 12:50:00', 2400.00, 'Compra', 'Tarjeta Crédito', 'Tecnología', 'Completada'),
(21, 11, '2023-01-22 15:30:00', 4800.50, 'Compra', 'Tarjeta Crédito', 'Viaje internacional', 'Completada'),
(22, 11, '2023-05-10 10:15:00', 2650.00, 'Transferencia', 'Transferencia Bancaria', 'Ahorro', 'Completada'),
(23, 12, '2023-03-05 13:40:00', 920.25, 'Compra', 'Efectivo', 'Café', 'Completada'),
(24, 12, '2023-04-28 16:20:00', 1750.00, 'Compra', 'Tarjeta Débito', 'Ropa deportiva', 'Completada'),
(25, 13, '2023-02-18 10:50:00', 3900.00, 'Compra', 'Tarjeta Crédito', 'Hogar', 'Completada'),
(26, 13, '2023-05-25 14:00:00', 2100.75, 'Transferencia', 'Transferencia Bancaria', 'Préstamo', 'Completada'),
(27, 14, '2023-01-28 11:30:00', 1050.50, 'Compra', 'Efectivo', 'Transporte', 'Completada'),
(28, 14, '2023-04-12 09:45:00', 2300.00, 'Compra', 'Tarjeta Crédito', 'Entretenimiento', 'Completada'),
(29, 15, '2023-03-20 15:15:00', 5500.25, 'Compra', 'Tarjeta Crédito', 'Lujo', 'Completada'),
(30, 15, '2023-05-30 12:00:00', 3000.00, 'Transferencia', 'Transferencia Bancaria', 'Inversión premium', 'Completada');

-- PARTE 3: Verificar datos insertados

SELECT 'CLIENTES' as tabla, COUNT(*) as total_registros FROM DATOS_SINTETICOS_DEMO.DEMO.CLIENTES
UNION ALL
SELECT 'TRANSACCIONES' as tabla, COUNT(*) as total_registros FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES;


-- PARTE 3.1: Visualizar transacciones con información de clientes

SELECT 
    t.transaccion_id,
    t.fecha_transaccion,
    c.cliente_id,
    c.nombre || ' ' || c.apellido as cliente_nombre,
    c.email,
    c.ciudad,
    c.categoria as categoria_cliente,
    t.monto,
    t.tipo_transaccion,
    t.metodo_pago,
    t.descripcion,
    t.estado
FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES t
INNER JOIN DATOS_SINTETICOS_DEMO.DEMO.CLIENTES c
    ON t.cliente_id = c.cliente_id
ORDER BY t.fecha_transaccion DESC, t.transaccion_id;

SELECT 
    c.cliente_id,
    c.nombre || ' ' || c.apellido as cliente_nombre,
    c.categoria,
    COUNT(t.transaccion_id) as total_transacciones,
    SUM(t.monto) as monto_total,
    AVG(t.monto) as monto_promedio,
    MIN(t.monto) as monto_minimo,
    MAX(t.monto) as monto_maximo
FROM DATOS_SINTETICOS_DEMO.DEMO.CLIENTES c
LEFT JOIN DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES t
    ON c.cliente_id = t.cliente_id
GROUP BY c.cliente_id, c.nombre, c.apellido, c.categoria
ORDER BY total_transacciones DESC, monto_total DESC;

-- PARTE 4: Generar datos sintéticos
-- Requisitos: Snowflake 8.41+, aceptar términos Anaconda (Admin > Billing & Terms)
-- Nota: Columnas STRING con >50% valores únicos aparecen como "redacted" por privacidad

CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
    'datasets':[
        {
          'input_table': 'DATOS_SINTETICOS_DEMO.DEMO.CLIENTES',
          'output_table': 'DATOS_SINTETICOS_DEMO.DEMO.CLIENTES_SINTETICOS',
          'columns': {
            'ciudad': {'categorical': True},
            'categoria': {'categorical': True}
          }
        },
        {
          'input_table': 'DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES',
          'output_table': 'DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES_SINTETICAS',
          'columns': {
            'cliente_id': {'join_key': True},
            'tipo_transaccion': {'categorical': True},
            'metodo_pago': {'categorical': True}
          }
        }
      ],
      'replace_output_tables': True
});

-- PARTE 5: Verificar datos sintéticos generados

SELECT 'CLIENTES_SINTETICOS' as tabla, COUNT(*) as total_registros 
FROM DATOS_SINTETICOS_DEMO.DEMO.CLIENTES_SINTETICOS
UNION ALL
SELECT 'TRANSACCIONES_SINTETICAS' as tabla, COUNT(*) as total_registros 
FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES_SINTETICAS;

-- PARTE 6: Comparar originales vs sintéticos
SELECT 
  'Original' as tipo,
  'CLIENTES' as tabla,
  COUNT(*) as total_filas,
  COUNT(DISTINCT cliente_id) as clientes_unicos,
  AVG(salario_anual) as salario_promedio,
  COUNT(CASE WHEN es_premium = TRUE THEN 1 END) as clientes_premium
FROM DATOS_SINTETICOS_DEMO.DEMO.CLIENTES
UNION ALL
SELECT 
  'Sintético' as tipo,
  'CLIENTES' as tabla,
  COUNT(*) as total_filas,
  COUNT(DISTINCT cliente_id) as clientes_unicos,
  AVG(salario_anual) as salario_promedio,
  COUNT(CASE WHEN es_premium = TRUE THEN 1 END) as clientes_premium
FROM DATOS_SINTETICOS_DEMO.DEMO.CLIENTES_SINTETICOS;

SELECT 
  'Original' as tipo,
  COUNT(*) as total_transacciones,
  AVG(monto) as monto_promedio,
  SUM(monto) as monto_total,
  COUNT(DISTINCT cliente_id) as clientes_con_transacciones
FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES
UNION ALL
SELECT 
  'Sintético' as tipo,
  COUNT(*) as total_transacciones,
  AVG(monto) as monto_promedio,
  SUM(monto) as monto_total,
  COUNT(DISTINCT cliente_id) as clientes_con_transacciones
FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES_SINTETICAS;

-- PARTE 7: Verificar relaciones entre tablas

SELECT 
  COUNT(DISTINCT t.cliente_id) as clientes_en_transacciones,
  COUNT(DISTINCT c.cliente_id) as clientes_en_clientes,
  CASE 
    WHEN COUNT(DISTINCT t.cliente_id) = COUNT(DISTINCT c.cliente_id) THEN 'Consistentes' 
    ELSE 'Inconsistentes' 
  END as estado_relaciones
FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES_SINTETICAS t
FULL OUTER JOIN DATOS_SINTETICOS_DEMO.DEMO.CLIENTES_SINTETICOS c
  ON t.cliente_id = c.cliente_id;

-- PARTE 8: Mostrar ejemplos de datos sintéticos

SELECT * FROM DATOS_SINTETICOS_DEMO.DEMO.CLIENTES_SINTETICOS LIMIT 5;
SELECT * FROM DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES_SINTETICAS LIMIT 5;

-- PARTE 9: Limpieza (descomentar para eliminar todo)

/*
-- Eliminar tablas sintéticas
DROP TABLE IF EXISTS DATOS_SINTETICOS_DEMO.DEMO.CLIENTES_SINTETICOS;
DROP TABLE IF EXISTS DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES_SINTETICAS;

-- Eliminar tablas originales
DROP TABLE IF EXISTS DATOS_SINTETICOS_DEMO.DEMO.TRANSACCIONES;
DROP TABLE IF EXISTS DATOS_SINTETICOS_DEMO.DEMO.CLIENTES;

-- Eliminar esquema
DROP SCHEMA IF EXISTS DATOS_SINTETICOS_DEMO.DEMO;

-- Eliminar base de datos
DROP DATABASE IF EXISTS DATOS_SINTETICOS_DEMO;
*/
