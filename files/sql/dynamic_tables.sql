USE WAREHOUSE VW_ADVANCED_ANALYTICS;
USE DATABASE BD_EMPRESA;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE CUST_INFO (
  CUSTID     NUMBER(10, 0),
  CNAME      VARCHAR(100),
  SPENDLIMIT NUMBER(10, 2)
);

INSERT INTO cust_info VALUES
  (1001, 'John Villarreal', 8187.42),
  (1002, 'Austin Carroll', 4933.94),
  (1003, 'Connie Curtis', 5217.71),
  (1004, 'Michael Harrison', 3003.78),
  (1005, 'Amanda Mendoza', 3382.70);

CREATE OR REPLACE TABLE PROD_STOCK_INV (
  PID        NUMBER(10, 0),
  PNAME      VARCHAR(100),
  STOCK      NUMBER(10, 2),
  STOCKDATE  DATE
);

INSERT INTO prod_stock_inv VALUES
  (101, 'Wooden pegs', 500, '2023-12-15'),
  (102, 'Automated desk', 600, '2023-12-15'),
  (103, 'Multi-layered widtgh', 300, '2023-12-15'),
  (104, 'Quantum tester', 100, '2023-12-15'),
  (105, 'Vision-oriented product', 700, '2023-12-15');

CREATE OR REPLACE TABLE SALESDATA (
  CUSTID    NUMBER(10, 0),
  PURCHASE  VARIANT
);

INSERT INTO salesdata
SELECT 1001, PARSE_JSON('{"prodid": 101,"purchase_amount": 919.8,"purchase_date": "2023-12-20","quantity": 4}');

INSERT INTO salesdata
SELECT 1001, PARSE_JSON('{"prodid": 102,"purchase_amount": 505.05,"purchase_date": "2023-12-21","quantity": 4}');

INSERT INTO salesdata
SELECT 1002, PARSE_JSON('{"prodid": 103,"purchase_amount": 898.92,"purchase_date": "2023-12-21","quantity": 3}');

INSERT INTO salesdata
SELECT 1004, PARSE_JSON('{"prodid": 104,"purchase_amount": 852.52,"purchase_date": "2023-12-20","quantity": 5}');

INSERT INTO salesdata
SELECT 1003, PARSE_JSON('{"prodid": 105,"purchase_amount": 546.43,"purchase_date": "2023-12-21","quantity": 2}');

CREATE OR REPLACE DYNAMIC TABLE customer_sales_data_history
  LAG       = 'DOWNSTREAM'
  WAREHOUSE = VW_ADVANCED_ANALYTICS
AS
SELECT
  s.custid AS customer_id,
  c.cname AS customer_name,
  s.purchase:"prodid"::number(5) AS product_id,
  s.purchase:"purchase_amount"::number(10) AS saleprice,
  s.purchase:"quantity"::number(5) AS quantity,
  s.purchase:"purchase_date"::date AS salesdate
FROM cust_info c
INNER JOIN salesdata s ON c.custid = s.custid;

CREATE OR REPLACE DYNAMIC TABLE salesreport
  LAG       = '90 DAYS'
  WAREHOUSE = VW_ADVANCED_ANALYTICS
AS
SELECT
  t1.customer_id,
  t1.customer_name,
  t1.product_id,
  p.pname AS product_name,
  t1.saleprice,
  t1.quantity,
  (t1.saleprice / t1.quantity) AS unitsalesprice,
  t1.salesdate AS CreationTime,
  customer_id || '-' || t1.product_id || '-' || t1.salesdate AS CUSTOMER_SK,
  LEAD(CreationTime) OVER (PARTITION BY t1.customer_id ORDER BY CreationTime ASC) AS END_TIME
FROM customer_sales_data_history t1
INNER JOIN prod_stock_inv p ON t1.product_id = p.pid;

-- Agregar nuevos registros
INSERT INTO salesdata
SELECT 1002, PARSE_JSON('{"prodid": 105,"purchase_amount": 200.8,"purchase_date": "2023-12-25","quantity": 6}');

INSERT INTO salesdata
SELECT 1005, PARSE_JSON('{"prodid": 102,"purchase_amount": 500,"purchase_date": "2023-12-25","quantity": 3}');
