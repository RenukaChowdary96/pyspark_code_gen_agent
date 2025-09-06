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

# Set environment variables
SALES_DB_USERNAME = os.environ['SALES_DB_USERNAME']
SALES_DB_PASSWORD = os.environ['SALES_DB_PASSWORD']
SALES_DB_HOST = os.environ['SALES_DB_HOST']
SALES_DB_PORT = os.environ['SALES_DB_PORT']

# Create SparkSession with Delta Lake extensions
spark = SparkSession.builder \
    .appName("Sales ETL Pipeline") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("INFO")

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
# Predicate Pushdown: FAIL (Performance)
#   Details: No predicate pushdown optimization
# Broadcast Joins: FAIL (Performance)
#   Details: No broadcast join optimization
# Error Handling: FAIL (Important)
#   Details: Missing try/except blocks
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: FAIL (Best Practice)
#   Details: No data quality checks
