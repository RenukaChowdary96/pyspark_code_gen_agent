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
    .appName("Sales ETL Pipeline") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set environment variables for credentials
db_username = os.environ['DB_USERNAME']
db_password = os.environ['DB_PASSWORD']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_database = config['metadata']['database']

# Create logger
logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# Load data from database
try:
    logger.info("Loading data from database...")
    customers_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "CUSTOMERS") \
        .option("user", db_username) \
        .option("password", db_password) \
        .load() \
        .filter(col("STATUS") == "ACTIVE")

    products_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "PRODUCTS") \
        .option("user", db_username) \
        .option("password", db_password) \
        .load()

    sales_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_database}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "SALES") \
        .option("user", db_username) \
        .option("password", db_password) \
        .load() \
        .filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))

    logger.info("Data loaded successfully.")
except Exception as e:
    logger.error(f"Error loading data: {str(e)}")
    spark.stop()
    exit(1)

# Perform data quality checks
try:
    logger.info("Performing data quality checks...")
    customers_null_count = customers_df.select(count(isnull("CUSTOMER_ID")).alias("null_count")).collect()[0].null_count
    products_null_count = products_df.select(count(isnull("PRODUCT_ID")).alias("null_count")).collect()[0].null_count
    sales_null_count = sales_df.select(count(isnull("SALE_ID")).alias("null_count")).collect()[0].null_count

    logger.info(f"Customers null count: {customers_null_count}")
    logger.info(f"Products null count: {products_null_count}")
    logger.info(f"Sales null count: {sales_null_count}")

    if customers_null_count > 0 or products_null_count > 0 or sales_null_count > 0:
        logger.error("Data quality checks failed. Null values found.")
        spark.stop()
        exit(1)
except Exception as e:
    logger.error(f"Error performing data quality checks: {str(e)}")
    spark.stop()
    exit(1)

# Join sales with customers and products using broadcast
try:
    logger.info("Joining sales with customers and products...")
    sales_joined_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner") \
        .join(broadcast(products_df), "PRODUCT_ID", "inner")

    logger.info("Join successful.")
except Exception as e:
    logger.error(f"Error joining sales with customers and products: {str(e)}")
    spark.stop()
    exit(1)

# Perform monthly aggregation
try:
    logger.info("Performing monthly aggregation...")
    aggregated_df = sales_joined_df.groupBy(col("CUSTOMER_ID"), col("PRODUCT_ID"), col("SALE_DATE").substr(1, 7).alias("MONTH")) \
        .agg(count("SALE_ID").alias("SALES_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))

    logger.info("Aggregation successful.")
except Exception as e:
    logger.error(f"Error performing monthly aggregation: {str(e)}")
    spark.stop()
    exit(1)

# Write output to Delta Lake format with partitioning
try:
    logger.info("Writing output to Delta Lake format...")
    aggregated_df.write.format("delta") \
        .option("path", config['target']['path']) \
        .option("mergeSchema", "true") \
        .partitionBy("MONTH") \
        .save()

    logger.info("Output written successfully.")
except Exception as e:
    logger.error(f"Error writing output to Delta Lake format: {str(e)}")
    spark.stop()
    exit(1)

# Stop SparkSession
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
