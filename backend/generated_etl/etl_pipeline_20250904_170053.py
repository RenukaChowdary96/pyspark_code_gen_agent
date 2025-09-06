# Oracle to Databricks ETL Pipeline - Generated Code
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import logging
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Pipeline")

# Load config from environment
ORACLE_HOST = os.environ.get('ORACLE_HOST', 'localhost')
ORACLE_PORT = os.environ.get('ORACLE_PORT', '1521')
ORACLE_SERVICE = os.environ.get('ORACLE_SERVICE', 'XE')
ORACLE_USERNAME = os.environ.get('ORACLE_USERNAME')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')

# Validate credentials
if not ORACLE_USERNAME or not ORACLE_PASSWORD:
    raise ValueError("Oracle credentials not found in environment variables")

ORACLE_URL = f"jdbc:oracle:thin:@{ORACLE_HOST}:{ORACLE_PORT}:{ORACLE_SERVICE}"
DELTA_LAKE_LOCATION = os.environ.get('DELTA_LAKE_LOCATION', '/tmp/delta-lake')

# Create SparkSession with optimizations
spark = SparkSession.builder \
    .appName("Customer Product Monthly Sales") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

logger.info("Spark session created with Delta Lake support")

# Oracle connection properties
oracle_props = {
    "user": ORACLE_USERNAME,
    "password": ORACLE_PASSWORD,
    "driver": "oracle.jdbc.driver.OracleDriver",
    "fetchsize": "10000"
}


# ==================== MAIN ETL LOGIC ====================

import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull

# Load YAML configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Create SparkSession with Delta Lake extensions
spark = SparkSession.builder \
    .appName("Sales ETL") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("INFO")

# Load environment variables
SALES_DB_URL = os.environ['SALES_DB_URL']
SALES_DB_USERNAME = os.environ['SALES_DB_USERNAME']
SALES_DB_PASSWORD = os.environ['SALES_DB_PASSWORD']
DELTA_LAKE_PATH = os.environ['DELTA_LAKE_PATH']

# Define database and table names
DATABASE = config['metadata']['database']
CUSTOMERS_TABLE = config['metadata']['tables']['customers']
PRODUCTS_TABLE = config['metadata']['tables']['products']
SALES_TABLE = config['metadata']['tables']['sales']

try:
    # Read customers table with predicate pushdown
    customers_df = spark.read.format("jdbc") \
        .option("url", SALES_DB_URL) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"{DATABASE}.{CUSTOMERS_TABLE}") \
        .option("user", SALES_DB_USERNAME) \
        .option("password", SALES_DB_PASSWORD) \
        .option("query", f"SELECT * FROM {DATABASE}.{CUSTOMERS_TABLE} WHERE STATUS = 'ACTIVE'") \
        .load()

    # Read products table
    products_df = spark.read.format("jdbc") \
        .option("url", SALES_DB_URL) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"{DATABASE}.{PRODUCTS_TABLE}") \
        .option("user", SALES_DB_USERNAME) \
        .option("password", SALES_DB_PASSWORD) \
        .load()

    # Read sales table with predicate pushdown
    sales_df = spark.read.format("jdbc") \
        .option("url", SALES_DB_URL) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", f"{DATABASE}.{SALES_TABLE}") \
        .option("user", SALES_DB_USERNAME) \
        .option("password", SALES_DB_PASSWORD) \
        .option("query", f"SELECT * FROM {DATABASE}.{SALES_TABLE} WHERE QUANTITY > 0 AND TOTAL_AMOUNT > 0") \
        .load()

    # Perform data quality checks
    print("Customers Data Quality Check:")
    print(customers_df.select(count(col("CUSTOMER_ID")).alias("count"), count(isnull(col("CUSTOMER_ID"))).alias("null_count")).collect())
    print("Products Data Quality Check:")
    print(products_df.select(count(col("PRODUCT_ID")).alias("count"), count(isnull(col("PRODUCT_ID"))).alias("null_count")).collect())
    print("Sales Data Quality Check:")
    print(sales_df.select(count(col("SALE_ID")).alias("count"), count(isnull(col("SALE_ID"))).alias("null_count")).collect())

    # Join sales with customers and products using broadcast
    sales_with_customers_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner")
    sales_with_customers_products_df = sales_with_customers_df.join(broadcast(products_df), "PRODUCT_ID", "inner")

    # Perform monthly aggregation
    aggregated_df = sales_with_customers_products_df.groupBy(col("CUSTOMER_ID"), col("PRODUCT_ID"), col("SALE_DATE").substr(1, 7).alias("MONTH")) \
        .agg(count(col("SALE_ID")).alias("SALES_COUNT"), sum(col("TOTAL_AMOUNT")).alias("TOTAL_SALES"))

    # Write aggregated data to Delta Lake
    aggregated_df.write.format("delta") \
        .option("path", DELTA_LAKE_PATH) \
        .option("mergeSchema", "true") \
        .partitionBy("MONTH") \
        .save()

    print("ETL pipeline completed successfully.")

except Exception as e:
    print(f"Error occurred: {str(e)}")
    spark.stop()

finally:
    spark.stop()

# ============================================================
# VALIDATION REPORT
# ============================================================
# SparkSession: PASS
#   Details: SparkSession properly initialized
# Delta Lake: PASS
#   Details: Delta Lake format detected
# Environment Variables: PASS
#   Details: Uses environment variables
# No Hardcoded Creds: PASS
#   Details: No hardcoded credentials found
# Predicate Pushdown: PASS
#   Details: Database-level filtering detected
# Broadcast Joins: PASS
#   Details: Broadcast joins implemented
# Error Handling: PASS
#   Details: Exception handling present
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: PASS
#   Details: Data quality checks present
