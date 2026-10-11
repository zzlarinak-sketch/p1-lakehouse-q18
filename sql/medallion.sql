-- Практическая 1, вариант 3: TPC-H Q18 "Large Volume Customer"
-- Медальон bronze -> silver -> gold, масштаб sf10.
-- Этот же файл выполняет scripts/medallion.py (он ещё собирает строки/МБ/время по таблицам).
-- Руками: docker exec -i trino trino < sql/medallion.sql

CREATE SCHEMA IF NOT EXISTS bronze.lakehouse;
CREATE SCHEMA IF NOT EXISTS silver.lakehouse;
CREATE SCHEMA IF NOT EXISTS gold.lakehouse;

DROP TABLE IF EXISTS gold.lakehouse.mart_large_orders;
DROP TABLE IF EXISTS silver.lakehouse.fct_orders;
DROP TABLE IF EXISTS silver.lakehouse.dim_customer;
DROP TABLE IF EXISTS bronze.lakehouse.raw_customer;
DROP TABLE IF EXISTS bronze.lakehouse.raw_orders;
DROP TABLE IF EXISTS bronze.lakehouse.raw_lineitem;
DROP TABLE IF EXISTS bronze.lakehouse.raw_nation;

-- ===== BRONZE: все колонки как в источнике + когда и откуда загружено =====
SET SESSION bronze.compression_codec = 'GZIP';

CREATE TABLE bronze.lakehouse.raw_customer WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.customer;

CREATE TABLE bronze.lakehouse.raw_orders WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.orders;

CREATE TABLE bronze.lakehouse.raw_lineitem WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.lineitem;

-- справочник стран (25 строк) - для обогащения клиентов в silver
CREATE TABLE bronze.lakehouse.raw_nation WITH (format = 'PARQUET') AS
SELECT *, current_timestamp AS loaded_at, 'tpch.sf10' AS source_system
FROM tpch.sf10.nation;

-- ===== SILVER: очистка, нормализация, обогащение =====
SET SESSION silver.compression_codec = 'ZSTD';

-- клиенты: один ряд на custkey (дедупликация по последней загрузке),
-- обрезаем пробелы, приводим сегмент к верхнему регистру, добавляем название страны
CREATE TABLE silver.lakehouse.dim_customer WITH (format = 'PARQUET') AS
SELECT c.custkey,
       trim(c.name)             AS name,
       c.nationkey,
       trim(n.name)             AS nation_name,
       upper(trim(c.mktsegment)) AS mktsegment,
       CAST(c.acctbal AS decimal(12, 2)) AS acctbal
FROM (
  SELECT *, row_number() OVER (PARTITION BY custkey ORDER BY loaded_at DESC) AS rn
  FROM bronze.lakehouse.raw_customer
  WHERE custkey IS NOT NULL
) c
LEFT JOIN bronze.lakehouse.raw_nation n ON n.nationkey = c.nationkey
WHERE c.rn = 1;

-- заказы, обогащённые итогами по позициям: сколько товара и сколько строк в заказе.
-- Это и есть "подзапрос" Q18 (sum(quantity) по orderkey), только посчитанный один раз
-- для всех заказов. Битые позиции (quantity <= 0 или NULL) отбрасываем.
CREATE TABLE silver.lakehouse.fct_orders WITH (format = 'PARQUET') AS
WITH li AS (
  SELECT orderkey,
         sum(quantity) AS total_quantity,
         count(*)      AS line_cnt
  FROM bronze.lakehouse.raw_lineitem
  WHERE quantity > 0
  GROUP BY orderkey
)
SELECT o.orderkey,
       o.custkey,
       o.orderdate,
       o.orderstatus,
       CAST(o.totalprice AS decimal(12, 2)) AS totalprice,
       CAST(li.total_quantity AS integer)   AS total_quantity,
       CAST(li.line_cnt AS integer)         AS line_cnt
FROM bronze.lakehouse.raw_orders o
JOIN li ON li.orderkey = o.orderkey
WHERE o.orderkey IS NOT NULL
  AND o.custkey IS NOT NULL;

-- ===== GOLD: витрина под запрос - топ-100 крупных заказов (больше 300 единиц товара) =====
SET SESSION gold.compression_codec = 'SNAPPY';

CREATE TABLE gold.lakehouse.mart_large_orders WITH (format = 'PARQUET') AS
SELECT c.name,
       c.custkey,
       o.orderkey,
       o.orderdate,
       o.totalprice,
       o.total_quantity,
       c.nation_name
FROM silver.lakehouse.fct_orders o
JOIN silver.lakehouse.dim_customer c ON c.custkey = o.custkey
WHERE o.total_quantity > 300
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;

-- ответ на вопрос варианта
SELECT * FROM gold.lakehouse.mart_large_orders
ORDER BY totalprice DESC, orderdate;

-- ===== проверка: gold совпадает с оригинальным Q18 по сырому tpch.sf10 =====
WITH orig AS (
  SELECT c.name, c.custkey, o.orderkey, o.orderdate,
         CAST(o.totalprice AS decimal(12, 2)) AS totalprice,
         CAST(sum(l.quantity) AS integer) AS total_quantity
  FROM tpch.sf10.customer c
  JOIN tpch.sf10.orders   o ON c.custkey = o.custkey
  JOIN tpch.sf10.lineitem l ON o.orderkey = l.orderkey
  WHERE o.orderkey IN (
    SELECT orderkey FROM tpch.sf10.lineitem
    GROUP BY orderkey HAVING sum(quantity) > 300
  )
  GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
  ORDER BY o.totalprice DESC, o.orderdate
  LIMIT 100
),
gold AS (
  SELECT name, custkey, orderkey, orderdate, totalprice, total_quantity
  FROM gold.lakehouse.mart_large_orders
)
-- один FULL JOIN по всем 6 колонкам: строка без пары = расхождение
-- (через EXCEPT orig считался бы 3 раза, и Trino на 8 ГБ падал по памяти)
SELECT count(o.orderkey)                                  AS rows_orig,
       count(g.orderkey)                                  AS rows_gold,
       count_if(g.orderkey IS NULL)                       AS only_in_orig,
       count_if(o.orderkey IS NULL)                       AS only_in_gold,
       sum(o.total_quantity)                              AS sum_qty_orig,
       sum(g.total_quantity)                              AS sum_qty_gold
FROM orig o
FULL JOIN gold g
  ON  o.orderkey = g.orderkey AND o.custkey = g.custkey AND o.name = g.name
  AND o.orderdate = g.orderdate AND o.totalprice = g.totalprice
  AND o.total_quantity = g.total_quantity;
