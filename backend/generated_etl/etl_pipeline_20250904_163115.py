import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(file_path):
    """Load YAML configuration file."""
    with open(file_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_spark_session():
    """Create SparkSession with Delta Lake extensions."""
    spark = SparkSession.builder \
        .appName("Sales ETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_data(spark, config):
    """Load data from source database."""
    source_url = config['source']['url']
    username = os.environ['SALES_DB_USERNAME']
    password = os.environ['SALES_DB_PASSWORD']
    database = config['metadata']['database']
    tables = config['metadata']['tables']

    # Load CUSTOMERS table
    customers_df = spark.read.format("jdbc") \
        .option("url", source_url) \
        .option("username", username) \
        .option("password", password) \
        .option("dbtable", f"{database}.CUSTOMERS") \
        .option("predicate", "STATUS = 'ACTIVE'") \
        .load()

    # Load PRODUCTS table
    products_df = spark.read.format("jdbc") \
        .option("url", source_url) \
        .option("username", username) \
        .option("password", password) \
        .option("dbtable", f"{database}.PRODUCTS") \
        .load()

    # Load SALES table
    sales_df = spark.read.format("jdbc") \
        .option("url", source_url) \
        .option("username", username) \
        .option("password", password) \
        .option("dbtable", f"{database}.SALES") \
        .option("predicate", "QUANTITY > 0 AND TOTAL_AMOUNT > 0") \
        .load()

    return customers_df, products_df, sales_df

def transform_data(customers_df, products_df, sales_df):
    """Transform data by joining and aggregating."""
    # Broadcast joins for dimension tables
    sales_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID") \
        .join(broadcast(products_df), "PRODUCT_ID")

    # Monthly aggregation by customer and product
    aggregated_df = sales_df.groupBy(col("CUSTOMER_ID"), col("CUSTOMER_NAME"), col("PRODUCT_ID"), col("PRODUCT_NAME"), month("SALE_DATE").alias("MONTH"), year("SALE_DATE").alias("YEAR")) \
        .agg({"TOTAL_AMOUNT": "sum", "QUANTITY": "sum"})

    return aggregated_df

def load_data_to_delta(spark, aggregated_df, config):
    """Load data to Delta Lake."""
    target_url = config['target']['url']
    format = config['target']['format']

    # Write data to Delta Lake with partitioning
    aggregated_df.write.format(format) \
        .partitionBy("YEAR", "MONTH") \
        .save(target_url)

def data_quality_checks(aggregated_df):
    """Perform data quality checks."""
    # Check for null values
    null_counts = aggregated_df.select([count(when(isnull(c), c)).alias(c) for c in aggregated_df.columns])
    logger.info("Null counts:")
    null_counts.show()

    # Check for duplicate values
    duplicate_counts = aggregated_df.groupBy("CUSTOMER_ID", "CUSTOMER_NAME", "PRODUCT_ID", "PRODUCT_NAME", "MONTH", "YEAR").count()
    logger.info("Duplicate counts:")
    duplicate_counts.show()

def main():
    # Load configuration
    config_file = "config.yaml"
    config = load_config(config_file)

    # Create SparkSession
    spark = create_spark_session()

    try:
        # Load data from source database
        customers_df, products_df, sales_df = load_data(spark, config)
        logger.info("Data loaded from source database.")

        # Transform data
        aggregated_df = transform_data(customers_df, products_df, sales_df)
        logger.info("Data transformed.")

        # Perform data quality checks
        data_quality_checks(aggregated_df)
        logger.info("Data quality checks completed.")

        # Load data to Delta Lake
        load_data_to_delta(spark, aggregated_df, config)
        logger.info("Data loaded to Delta Lake.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")

    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main()

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
# Broadcast Joins: PASS
#   Details: Broadcast joins implemented
# Error Handling: PASS
#   Details: Exception handling present
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: PASS
#   Details: Data quality checks present
