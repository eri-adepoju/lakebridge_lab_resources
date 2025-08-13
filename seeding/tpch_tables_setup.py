# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-H Tables Setup for demo
# MAGIC
# MAGIC This notebook creates the TPC-H tables used in the Lakebridge labs
# MAGIC ## Parameters
# MAGIC - `catalog_name`: The catalog to create tables in (default: "lakebridge_lab_resources")
# MAGIC - `schema_name`: The schema to create tables in (default: "tpch")
# MAGIC - `row_count`: Number of rows to insert (default: 1000)
# MAGIC
# MAGIC ## Tables Created
# MAGIC - `customer`: Customer information
# MAGIC - `orders`: Order information  
# MAGIC - `lineitem`: Line item details
# MAGIC - `supplier`: Supplier information
# MAGIC - `part`: Part/product information
# MAGIC - `nation`: Nation/country information
# MAGIC - `region`: Region information

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets for Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lakebridge_lab_resources", "Catalog Name")
dbutils.widgets.text("schema_name", "tpch", "Schema Name")
dbutils.widgets.text("row_count", "1000", "Number of Rows to Insert")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Parameter Values

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
row_count = int(dbutils.widgets.get("row_count"))

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Row Count: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{schema_name}")

print(f"Created catalog: {catalog_name}")
print(f"Created schema: {catalog_name}.{schema_name} and hive_metastore.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Region Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.region (
    r_regionkey INT PRIMARY KEY,
    r_name STRING,
    r_comment STRING
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.region (
    r_regionkey INT,
    r_name STRING,
    r_comment STRING
)
""")

print("Created region table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Nation Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.nation (
    n_nationkey INT PRIMARY KEY,
    n_name STRING,
    n_regionkey INT,
    n_comment STRING,
    FOREIGN KEY (n_regionkey) REFERENCES {catalog_name}.{schema_name}.region(r_regionkey)
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.nation (
    n_nationkey INT,
    n_name STRING,
    n_regionkey INT,
    n_comment STRING
)
""")

print("Created nation table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Supplier Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.supplier (
    s_suppkey INT PRIMARY KEY,
    s_name STRING,
    s_address STRING,
    s_nationkey INT,
    s_phone STRING,
    s_acctbal DECIMAL(15,2),
    s_comment STRING,
    FOREIGN KEY (s_nationkey) REFERENCES {catalog_name}.{schema_name}.nation(n_nationkey)
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.supplier (
    s_suppkey INT,
    s_name STRING,
    s_address STRING,
    s_nationkey INT,
    s_phone STRING,
    s_acctbal DECIMAL(15,2),
    s_comment STRING
)
""")

print("Created supplier table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Part Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.part (
    p_partkey INT PRIMARY KEY,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DECIMAL(15,2),
    p_comment STRING
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.part (
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DECIMAL(15,2),
    p_comment STRING
)
""")

print("Created part table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Customer Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.customer (
    c_custkey INT PRIMARY KEY,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DECIMAL(15,2),
    c_mktsegment STRING,
    c_comment STRING,
    FOREIGN KEY (c_nationkey) REFERENCES {catalog_name}.{schema_name}.nation(n_nationkey)
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.customer (
    c_custkey INT,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DECIMAL(15,2),
    c_mktsegment STRING,
    c_comment STRING
)
""")

print("Created customer table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Orders Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.orders (
    o_orderkey INT PRIMARY KEY,
    o_custkey INT,
    o_orderstatus STRING,
    o_totalprice DECIMAL(15,2),
    o_orderdate DATE,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING,
    o_timestamp TIMESTAMP,
    FOREIGN KEY (o_custkey) REFERENCES {catalog_name}.{schema_name}.customer(c_custkey)
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.orders (
    o_orderkey INT,
    o_custkey INT,
    o_orderstatus STRING,
    o_totalprice DECIMAL(15,2),
    o_orderdate DATE,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING,
    o_timestamp TIMESTAMP
)
""")

print("Created orders table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Lineitem Table

# COMMAND ----------

# Create in Unity Catalog
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.lineitem (
    l_orderkey INT,
    l_partkey INT,
    l_suppkey INT,
    l_linenumber INT,
    l_quantity DECIMAL(15,2),
    l_extendedprice DECIMAL(15,2),
    l_discount DECIMAL(15,2),
    l_tax DECIMAL(15,2),
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING,
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_orderkey) REFERENCES {catalog_name}.{schema_name}.orders(o_orderkey),
    FOREIGN KEY (l_partkey) REFERENCES {catalog_name}.{schema_name}.part(p_partkey),
    FOREIGN KEY (l_suppkey) REFERENCES {catalog_name}.{schema_name}.supplier(s_suppkey)
)
""")

# Create in Hive Metastore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS hive_metastore.{schema_name}.lineitem (
    l_orderkey INT,
    l_partkey INT,
    l_suppkey INT,
    l_linenumber INT,
    l_quantity DECIMAL(15,2),
    l_extendedprice DECIMAL(15,2),
    l_discount DECIMAL(15,2),
    l_tax DECIMAL(15,2),
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING
)
""")

print("Created lineitem table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Region Data

# COMMAND ----------

from pyspark.sql.functions import lit, monotonically_increasing_id, rand, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

# Create region data
regions_data = [
    (0, "AFRICA", "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to "),
    (1, "AMERICA", "hs use ironic, even requests. s"),
    (2, "ASIA", "ges. thinly even pinto beans ca"),
    (3, "EUROPE", "ly final courts cajole furiously final excuse"),
    (4, "MIDDLE EAST", "uickly special accounts cajole carefully blithely close customers. carefully final asymptotes haggle furiousl")
]

regions_df = spark.createDataFrame(regions_data, ["r_regionkey", "r_name", "r_comment"])

# Insert into Unity Catalog
regions_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.region")

# Insert into Hive Metastore
regions_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.region")

print(f"Inserted {regions_df.count()} rows into region table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Nation Data

# COMMAND ----------

# Create nation data
nations_data = [
    (0, "ALGERIA", 0, "a final accounts. bold, regular pinto beans use alongside the pending requests. carefully unusual deposits are slyly "),
    (1, "ARGENTINA", 1, "al foxes promise slyly according to the regular accounts. bold requests alon"),
    (2, "BRAZIL", 1, "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special "),
    (3, "CANADA", 1, "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold"),
    (4, "EGYPT", 4, "y above the slyly ironic theodolites. slyly bold accounts among the closely regular packages use fluffily bold requ"),
    (5, "ETHIOPIA", 0, "ven packages wake quickly. quic"),
    (6, "FRANCE", 3, "refully final requests. regular, ironi"),
    (7, "GERMANY", 3, "l platelets. regular accounts x-ray: unusual, regular acco"),
    (8, "INDIA", 2, "ss excuses cajole slyly across the packages. deposits print a"),
    (9, "INDONESIA", 2, " slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull"),
    (10, "IRAN", 4, "efully alongside of the slyly final dependencies. "),
    (11, "IRAQ", 4, "nic accounts boost carefully pending requests. blithely pending packages use fluffily. pending, express requests cajo"),
    (12, "JAPAN", 2, "ously. final, express gifts cajole a"),
    (13, "JORDAN", 4, "ic deposits are blithely about the carefully regular pa"),
    (14, "KENYA", 0, " pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t"),
    (15, "MOROCCO", 0, "rns. blithely bold courts among the closely regular packages use furiously bold platelets?"),
    (16, "MOZAMBIQUE", 0, "s. ironic, daring asymptotes haggle furiously. carefully final accounts cajole requests. furious"),
    (17, "PERU", 1, "platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun"),
    (18, "CHINA", 2, "c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos"),
    (19, "ROMANIA", 3, "ular asymptotes are about the furious multipliers. express, bold pinto beans cajole slyly blithely regular hockey "),
    (20, "SAUDI ARABIA", 4, "ts. silent requests haggle. closely express packages sleep across the blithely"),
    (21, "VIETNAM", 2, "hely enticingly express accounts. even, final "),
    (22, "RUSSIA", 3, " requests against the platelets use never according to the quickly regular pint"),
    (23, "UNITED KINGDOM", 3, "y final packages. fluffily bold customers in"),
    (24, "UNITED STATES", 1, "yielding the ironic, silent, thin pinto beans. quickly final packagers promise quickly. quickly silent theodolites cajole slyly; carefully express accounts sleep blithely. carefully final ideas cannot express ")
]

nations_df = spark.createDataFrame(nations_data, ["n_nationkey", "n_name", "n_regionkey", "n_comment"])

# Insert into Unity Catalog
nations_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.nation")

# Insert into Hive Metastore
nations_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.nation")

print(f"Inserted {nations_df.count()} rows into nation table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customer Data

# COMMAND ----------

from pyspark.sql.functions import col, rand, when, expr, concat, lit
import random

# Generate customer data
customer_count = row_count
customer_df = spark.range(customer_count).select(
    col("id").alias("c_custkey"),
    concat(lit("Customer#"), col("id")).alias("c_name"),
    concat(lit("Address#"), col("id")).alias("c_address"),
    (col("id") % 25).alias("c_nationkey"),
    concat(lit("Phone#"), col("id")).alias("c_phone"),
    (rand() * 10000 + 1000).alias("c_acctbal"),
    when(col("id") % 5 == 0, "AUTOMOBILE")
    .when(col("id") % 5 == 1, "BUILDING")
    .when(col("id") % 5 == 2, "FURNITURE")
    .when(col("id") % 5 == 3, "MACHINERY")
    .otherwise("HOUSEHOLD").alias("c_mktsegment"),
    concat(lit("Comment#"), col("id")).alias("c_comment")
)

# Insert into Unity Catalog
customer_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.customer")

# Insert into Hive Metastore
customer_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.customer")

print(f"Inserted {customer_df.count()} rows into customer table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Supplier Data

# COMMAND ----------

# Generate supplier data
supplier_count = row_count // 10  # Fewer suppliers than customers
supplier_df = spark.range(supplier_count).select(
    col("id").alias("s_suppkey"),
    concat(lit("Supplier#"), col("id")).alias("s_name"),
    concat(lit("Address#"), col("id")).alias("s_address"),
    (col("id") % 25).alias("s_nationkey"),
    concat(lit("Phone#"), col("id")).alias("s_phone"),
    (rand() * 10000 + 1000).alias("s_acctbal"),
    concat(lit("Comment#"), col("id")).alias("s_comment")
)

# Insert into Unity Catalog
supplier_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.supplier")

# Insert into Hive Metastore
supplier_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.supplier")

print(f"Inserted {supplier_df.count()} rows into supplier table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Part Data

# COMMAND ----------

# Generate part data
part_count = row_count // 5  # Fewer parts than customers
part_df = spark.range(part_count).select(
    col("id").alias("p_partkey"),
    concat(lit("Part#"), col("id")).alias("p_name"),
    when(col("id") % 5 == 0, "Manufacturer#1")
    .when(col("id") % 5 == 1, "Manufacturer#2")
    .when(col("id") % 5 == 2, "Manufacturer#3")
    .when(col("id") % 5 == 3, "Manufacturer#4")
    .otherwise("Manufacturer#5").alias("p_mfgr"),
    when(col("id") % 10 == 0, "Brand#10")
    .when(col("id") % 10 == 1, "Brand#11")
    .when(col("id") % 10 == 2, "Brand#12")
    .when(col("id") % 10 == 3, "Brand#13")
    .when(col("id") % 10 == 4, "Brand#14")
    .when(col("id") % 10 == 5, "Brand#15")
    .when(col("id") % 10 == 6, "Brand#16")
    .when(col("id") % 10 == 7, "Brand#17")
    .when(col("id") % 10 == 8, "Brand#18")
    .otherwise("Brand#19").alias("p_brand"),
    when(col("id") % 25 == 0, "STANDARD")
    .when(col("id") % 25 == 1, "SMALL")
    .when(col("id") % 25 == 2, "MEDIUM")
    .when(col("id") % 25 == 3, "LARGE")
    .when(col("id") % 25 == 4, "ECONOMY")
    .when(col("id") % 25 == 5, "PROMO")
    .when(col("id") % 25 == 6, "STANDARD")
    .when(col("id") % 25 == 7, "SMALL")
    .when(col("id") % 25 == 8, "MEDIUM")
    .when(col("id") % 25 == 9, "LARGE")
    .when(col("id") % 25 == 10, "ECONOMY")
    .when(col("id") % 25 == 11, "PROMO")
    .when(col("id") % 25 == 12, "STANDARD")
    .when(col("id") % 25 == 13, "SMALL")
    .when(col("id") % 25 == 14, "MEDIUM")
    .when(col("id") % 25 == 15, "LARGE")
    .when(col("id") % 25 == 16, "ECONOMY")
    .when(col("id") % 25 == 17, "PROMO")
    .when(col("id") % 25 == 18, "STANDARD")
    .when(col("id") % 25 == 19, "SMALL")
    .when(col("id") % 25 == 20, "MEDIUM")
    .when(col("id") % 25 == 21, "LARGE")
    .when(col("id") % 25 == 22, "ECONOMY")
    .when(col("id") % 25 == 23, "PROMO")
    .otherwise("STANDARD").alias("p_type"),
    (col("id") % 50 + 1).alias("p_size"),
    when(col("id") % 6 == 0, "SM CASE")
    .when(col("id") % 6 == 1, "SM BOX")
    .when(col("id") % 6 == 2, "SM PACK")
    .when(col("id") % 6 == 3, "SM PKG")
    .when(col("id") % 6 == 4, "LG CASE")
    .otherwise("LG BOX").alias("p_container"),
    (rand() * 1000 + 100).alias("p_retailprice"),
    concat(lit("Comment#"), col("id")).alias("p_comment")
)

# Insert into Unity Catalog
part_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.part")

# Insert into Hive Metastore
part_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.part")

print(f"Inserted {part_df.count()} rows into part table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Orders Data

# COMMAND ----------

# Generate orders data
orders_count = row_count * 2  # More orders than customers
orders_df = spark.range(orders_count).select(
    col("id").alias("o_orderkey"),
    (col("id") % customer_count).alias("o_custkey"),
    when(col("id") % 3 == 0, "F")
    .when(col("id") % 3 == 1, "O")
    .otherwise("P").alias("o_orderstatus"),
    (rand() * 100000 + 1000).alias("o_totalprice"),
    expr("date_add('1992-01-01', cast(rand() * 1000 as int))").alias("o_orderdate"),
    when(col("id") % 5 == 0, "1-URGENT")
    .when(col("id") % 5 == 1, "2-HIGH")
    .when(col("id") % 5 == 2, "3-MEDIUM")
    .when(col("id") % 5 == 3, "4-NOT SPECIFIED")
    .otherwise("5-LOW").alias("o_orderpriority"),
    concat(lit("Clerk#"), col("id")).alias("o_clerk"),
    (col("id") % 5 + 1).alias("o_shippriority"),
    concat(lit("Comment#"), col("id")).alias("o_comment"),
    # Add timestamp with some null values (every 7th order will have null timestamp)
    when(col("id") % 7 == 0, lit(None).cast("timestamp"))
    .otherwise(expr("date_add(SECOND, cast(rand() * 1000 - 500 as int), current_timestamp())")).alias("o_timestamp")
)

# Insert into Unity Catalog
orders_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.orders")

# Insert into Hive Metastore
orders_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.orders")

print(f"Inserted {orders_df.count()} rows into orders table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Lineitem Data

# COMMAND ----------

# Generate lineitem data
lineitem_count = row_count * 4  # More lineitems than orders
lineitem_df = spark.range(lineitem_count).select(
    (col("id") % orders_count).alias("l_orderkey"),
    (col("id") % part_count).alias("l_partkey"),
    (col("id") % supplier_count).alias("l_suppkey"),
    (col("id") % 7 + 1).alias("l_linenumber"),
    (rand() * 50 + 1).alias("l_quantity"),
    (rand() * 10000 + 100).alias("l_extendedprice"),
    (rand() * 0.1).alias("l_discount"),
    (rand() * 0.08).alias("l_tax"),
    when(col("id") % 3 == 0, "A")
    .when(col("id") % 3 == 1, "N")
    .otherwise("R").alias("l_returnflag"),
    when(col("id") % 2 == 0, "F")
    .otherwise("O").alias("l_linestatus"),
    expr("date_add('1992-01-01', cast(rand() * 1000 as int))").alias("l_shipdate"),
    expr("date_add('1992-01-01', cast(rand() * 1000 as int))").alias("l_commitdate"),
    expr("date_add('1992-01-01', cast(rand() * 1000 as int))").alias("l_receiptdate"),
    when(col("id") % 4 == 0, "DELIVER IN PERSON")
    .when(col("id") % 4 == 1, "COLLECT COD")
    .when(col("id") % 4 == 2, "TAKE BACK RETURN")
    .otherwise("NONE").alias("l_shipinstruct"),
    when(col("id") % 7 == 0, "AIR")
    .when(col("id") % 7 == 1, "AIR REG")
    .when(col("id") % 7 == 2, "RAIL")
    .when(col("id") % 7 == 3, "SHIP")
    .when(col("id") % 7 == 4, "TRUCK")
    .when(col("id") % 7 == 5, "MAIL")
    .otherwise("FOB").alias("l_shipmode"),
    concat(lit("Comment#"), col("id")).alias("l_comment")
)

# Insert into Unity Catalog
lineitem_df.write.mode("overwrite").insertInto(f"{catalog_name}.{schema_name}.lineitem")

# Insert into Hive Metastore
lineitem_df.write.mode("overwrite").insertInto(f"hive_metastore.{schema_name}.lineitem")

print(f"Inserted {lineitem_df.count()} rows into lineitem table in both Unity Catalog and Hive Metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Table Row Counts

# COMMAND ----------

tables = ["region", "nation", "customer", "supplier", "part", "orders", "lineitem"]

print("Unity Catalog Tables:")
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table}").collect()[0]["count"]
    print(f"  {table}: {count} rows")

print("\nHive Metastore Tables:")
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as count FROM hive_metastore.{schema_name}.{table}").collect()[0]["count"]
    print(f"  {table}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data Preview

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer Sample - Unity Catalog

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.customer LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer Sample - Hive Metastore

# COMMAND ----------

display(spark.sql(f"SELECT * FROM hive_metastore.{schema_name}.customer LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Sample - Unity Catalog

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.orders LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Sample - Hive Metastore

# COMMAND ----------

display(spark.sql(f"SELECT * FROM hive_metastore.{schema_name}.orders LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lineitem Sample - Unity Catalog

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.lineitem LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lineitem Sample - Hive Metastore

# COMMAND ----------

display(spark.sql(f"SELECT * FROM hive_metastore.{schema_name}.lineitem LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("TPC-H TABLES SETUP COMPLETE")
print("=" * 60)
print(f"Unity Catalog: {catalog_name}.{schema_name}")
print(f"Hive Metastore: hive_metastore.{schema_name}")
print(f"Tables created in both locations: {', '.join(tables)}")