# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql("""CREATE TABLE IF NOT EXISTS collation_perf_results (
  variant_name STRING, run BIGINT, seconds DOUBLE, run_timestamp TIMESTAMP)""")

tables = ["orders_rtrim", "customers_rtrim", "orders_default", "customers_default"]
if all(spark.catalog.tableExists(t) for t in tables):
    dbutils.notebook.exit("skipped: data already exists")

N = 500_000_000
PART = 512

orders = spark.range(0, N, 1, PART).selectExpr(
    "concat('ORD_', lpad(id, 10, '0')) AS order_id",
    "concat('CUST_', lpad(id, 10, '0')) AS customer_key",
    "element_at(array('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED'), cast(rand() * 5 as int) + 1) AS status",
    "element_at(array('NA', 'EMEA', 'APAC', 'LATAM'), cast(rand() * 4 as int) + 1) AS region",
    "concat('PROD_', lpad(cast(rand() * 2000 as int), 5, '0')) AS product",
    "cast(round(rand() * 10000, 2) as decimal(12,2)) AS amount",
    "timestamp_seconds(1672531200 + cast(rand() * 63072000 as bigint)) AS order_ts")

customers = spark.range(0, N, 1, PART).selectExpr(
    "concat('CUST_', lpad(id, 10, '0')) AS customer_key",
    "concat('Name_', cast(id as string)) AS name",
    "concat('user', cast(id as string), '@example.com') AS email",
    "element_at(array('Seattle', 'Austin', 'Boston', 'Denver', 'Miami', 'Chicago', 'Dublin', 'Berlin', 'Tokyo', 'Sydney'), cast(rand() * 10 as int) + 1) AS city",
    "element_at(array('US', 'IE', 'DE', 'JP', 'AU', 'BR', 'IN', 'FR', 'CA', 'UK'), cast(rand() * 10 as int) + 1) AS country",
    "element_at(array('ENTERPRISE', 'SMB', 'CONSUMER', 'GOV'), cast(rand() * 4 as int) + 1) AS segment")

spark.sql("""CREATE TABLE orders_rtrim (
  order_id STRING, customer_key STRING, status STRING, region STRING,
  product STRING, amount DECIMAL(12,2), order_ts TIMESTAMP
) USING DELTA DEFAULT COLLATION UTF8_BINARY_RTRIM""")
orders.write.mode("append").saveAsTable("orders_rtrim")

spark.sql("""CREATE TABLE customers_rtrim (
  customer_key STRING, name STRING, email STRING, city STRING, country STRING, segment STRING
) USING DELTA DEFAULT COLLATION UTF8_BINARY_RTRIM""")
customers.write.mode("append").saveAsTable("customers_rtrim")

spark.sql("""CREATE TABLE orders_default AS SELECT
  order_id COLLATE UTF8_BINARY AS order_id,
  customer_key COLLATE UTF8_BINARY AS customer_key,
  status COLLATE UTF8_BINARY AS status,
  region COLLATE UTF8_BINARY AS region,
  product COLLATE UTF8_BINARY AS product,
  amount, order_ts FROM orders_rtrim""")

spark.sql("""CREATE TABLE customers_default AS SELECT
  customer_key COLLATE UTF8_BINARY AS customer_key,
  name COLLATE UTF8_BINARY AS name,
  email COLLATE UTF8_BINARY AS email,
  city COLLATE UTF8_BINARY AS city,
  country COLLATE UTF8_BINARY AS country,
  segment COLLATE UTF8_BINARY AS segment FROM customers_rtrim""")

for t in tables:
    spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")

dbutils.notebook.exit("generated")
