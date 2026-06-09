# Databricks notebook source
import time
from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
variant_name = dbutils.widgets.get("variant_name")
run_timestamp = dbutils.widgets.get("run_timestamp")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

query = """INSERT OVERWRITE TABLE result_query_collate
SELECT o.region COLLATE UTF8_BINARY_RTRIM, c.segment COLLATE UTF8_BINARY_RTRIM, c.country COLLATE UTF8_BINARY_RTRIM, o.product COLLATE UTF8_BINARY_RTRIM,
       sum(o.amount) AS total_amount,
       count(*) AS order_count,
       count(DISTINCT o.customer_key COLLATE UTF8_BINARY_RTRIM) AS distinct_customers
FROM orders_default o
JOIN customers_default c ON o.customer_key COLLATE UTF8_BINARY_RTRIM = c.customer_key COLLATE UTF8_BINARY_RTRIM
WHERE o.status COLLATE UTF8_BINARY_RTRIM IN ('PLACED', 'SHIPPED', 'DELIVERED')
  AND o.region COLLATE UTF8_BINARY_RTRIM IN ('NA', 'EMEA', 'APAC')
  AND c.segment COLLATE UTF8_BINARY_RTRIM <> 'GOV'
GROUP BY o.region COLLATE UTF8_BINARY_RTRIM, c.segment COLLATE UTF8_BINARY_RTRIM, c.country COLLATE UTF8_BINARY_RTRIM, o.product COLLATE UTF8_BINARY_RTRIM
ORDER BY total_amount DESC"""

spark.sql("""CREATE TABLE IF NOT EXISTS result_query_collate (
  region STRING, segment STRING, country STRING, product STRING,
  total_amount DECIMAL(38,2), order_count BIGINT, distinct_customers BIGINT)""")

times = []
for i in range(5):
    start = time.time()
    spark.sql(query)
    times.append(round(time.time() - start, 2))

(spark.createDataFrame([(variant_name, i + 1, s) for i, s in enumerate(times)], ["variant_name", "run", "seconds"])
    .withColumn("run_timestamp", F.lit(run_timestamp).cast("timestamp"))
    .write.mode("append").saveAsTable("collation_perf_results"))