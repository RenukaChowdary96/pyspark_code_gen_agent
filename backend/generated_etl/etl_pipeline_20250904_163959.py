import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, month, year
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file):
    """Load YAML configuration file"""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_spark_session():
    """Create SparkSession with Delta Lake extensions"""
    spark = SparkSession.builder \
        .appName("Sales ETL Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_data(spark, config):
    """Load data from source database"""
    source_db = config['source']['database']
    source_username = os.environ.get('SALES_DB_USERNAME')
    source_password = os.environ.get('SALES_DB_PASSWORD')
    source_host = os.environ.get('SALES_DB_HOST')
    source_port = os.environ.get('SALES_DB_PORT')

    customers_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{source_host}:{source_port}/{source_db}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "CUSTOMERS") \
        .option("user", source_username) \
        .option("password", source_password) \
        .load()

    products_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{source_host}:{source_port}/{source_db}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "PRODUCTS") \
        .option("user", source_username) \
        .option("password", source_password) \
        .load()

    sales_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{source_host}:{source_port}/{source_db}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "SALES") \
        .option("user", source_username) \
        .option("password", source_password) \
        .load()

    return customers_df, products_df, sales_df

def filter_data(customers_df, products_df, sales_df):
    """Filter data based on business rules"""
    filtered_customers_df = customers_df.filter(col("STATUS") == "ACTIVE")
    filtered_sales_df = sales_df.filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))

    return filtered_customers_df, filtered_sales_df

def aggregate_data(filtered_customers_df, filtered_sales_df, products_df):
    """Aggregate data by customer and product"""
    aggregated_df = filtered_sales_df.join(filtered_customers_df, on="CUSTOMER_ID", how="inner") \
        .join(products_df, on="PRODUCT_ID", how="inner") \
        .groupBy(month("SALE_DATE").alias("month"), year("SALE_DATE").alias("year"), "CUSTOMER_ID", "PRODUCT_ID") \
        .agg(count("SALE_ID").alias("num_sales"), sum("TOTAL_AMOUNT").alias("total_amount"))

    return aggregated_df

def write_data_to_delta_lake(aggregated_df, config):
    """Write data to Delta Lake"""
    delta_lake_path = config['target']['delta_lake_path']
    aggregated_df.write.format("delta") \
        .option("path", delta_lake_path) \
        .option("mergeSchema", "true") \
        .partitionBy("year", "month") \
        .save()

def data_quality_checks(aggregated_df):
    """Perform data quality checks"""
    # Check for null values
    null_counts = aggregated_df.select([count(when(isnull(c), c)).alias(c) for c in aggregated_df.columns])
    logger.info("Null counts:")
    logger.info(null_counts.collect())

    # Check for duplicate values
    duplicate_counts = aggregated_df.groupBy("CUSTOMER_ID", "PRODUCT_ID", "year", "month").count()
    logger.info("Duplicate counts:")
    logger.info(duplicate_counts.collect())

def main():
    config_file = "config.yaml"
    config = load_config(config_file)

    spark = create_spark_session()

    try:
        customers_df, products_df, sales_df = load_data(spark, config)
        logger.info("Data loaded successfully")

        filtered_customers_df, filtered_sales_df = filter_data(customers_df, products_df, sales_df)
        logger.info("Data filtered successfully")

        aggregated_df = aggregate_data(filtered_customers_df, filtered_sales_df, products_df)
        logger.info("Data aggregated successfully")

        write_data_to_delta_lake(aggregated_df, config)
        logger.info("Data written to Delta Lake successfully")

        data_quality_checks(aggregated_df)
        logger.info("Data quality checks completed successfully")

    except Exception as e:
        logger.error("Error occurred: ", e)

    finally:
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
# Predicate Pushdown: PASS
#   Details: Database-level filtering detected
# Broadcast Joins: FAIL (Performance)
#   Details: No broadcast join optimization
# Error Handling: PASS
#   Details: Exception handling present
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: PASS
#   Details: Data quality checks present
