#!/usr/bin/env python3
# п.6.2: Spark подключается к lakehouse и работает с Bronze.
# Spark ходит в тот же каталог Lakekeeper (warehouse bronze) и сам читает parquet из MinIO,
# Trino ему для этого не нужен.

import time

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("p1-bronze-q18")
    # в образе по умолчанию прописан каталог demo, которого у нас нет -> основной каталог bronze
    .config("spark.sql.defaultCatalog", "bronze")
    .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.bronze.type", "rest")
    .config("spark.sql.catalog.bronze.uri", "http://host.docker.internal:8181/catalog")
    .config("spark.sql.catalog.bronze.warehouse", "bronze")
    .config("spark.sql.catalog.bronze.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.bronze.s3.endpoint", "http://host.docker.internal:9000")
    .config("spark.sql.catalog.bronze.s3.path-style-access", "true")
    .config("spark.sql.catalog.bronze.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.bronze.s3.secret-access-key", "minioadmin")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("### 1. Таблицы, которые Spark видит в Bronze")
spark.sql("SHOW TABLES IN bronze.lakehouse").show(truncate=False)

print("### 2. Строк в каждой таблице")
for tb in ("raw_customer", "raw_orders", "raw_lineitem", "raw_nation"):
    print(f"  {tb}: {spark.table(f'bronze.lakehouse.{tb}').count():,}")

print("\n### 3. Схема raw_orders (служебные loaded_at и source_system на месте)")
spark.table("bronze.lakehouse.raw_orders").printSchema()

print("### 4. Снапшоты raw_lineitem (метаданные Iceberg видны и из Spark)")
spark.sql("SELECT snapshot_id, operation, committed_at FROM bronze.lakehouse.raw_lineitem.snapshots"
          ).show(truncate=False)

print("### 5. Запрос варианта 3 (Q18) в Spark прямо по Bronze")
t0 = time.time()
rows = spark.sql("""
SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM bronze.lakehouse.raw_customer c
JOIN bronze.lakehouse.raw_orders   o ON c.custkey = o.custkey
JOIN bronze.lakehouse.raw_lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey FROM bronze.lakehouse.raw_lineitem GROUP BY orderkey HAVING sum(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100
""").collect()
print(f"  строк: {len(rows)}, время: {time.time() - t0:.1f} с")
print(f"  сумма total_quantity: {sum(r['total_quantity'] for r in rows):.0f}")
print(f"  сумма totalprice: {sum(r['totalprice'] for r in rows):.2f}")
for r in rows[:5]:
    print(f"  {r['name']}  order {r['orderkey']}  {r['orderdate']}  {r['totalprice']}  qty {r['total_quantity']:.0f}")

spark.stop()
