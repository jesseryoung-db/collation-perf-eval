# Databricks notebook source
import time
from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
variant_name = dbutils.widgets.get("variant_name")
run_timestamp = dbutils.widgets.get("run_timestamp")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

query = """INSERT OVERWRITE TABLE result_rtrim
SELECT o.region, c.segment, c.country, o.product,
       sum(o.amount) AS total_amount,
       count(*) AS order_count,
       count(DISTINCT o.customer_key) AS distinct_customers
FROM orders_rtrim o
JOIN customers_rtrim c ON o.customer_key = c.customer_key
WHERE o.status IN ('PLACED', 'SHIPPED', 'DELIVERED')
  AND o.region IN ('NA', 'EMEA', 'APAC')
  AND c.segment <> 'GOV'
GROUP BY o.region, c.segment, c.country, o.product
ORDER BY total_amount DESC"""

spark.sql("""CREATE TABLE IF NOT EXISTS result_rtrim (
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