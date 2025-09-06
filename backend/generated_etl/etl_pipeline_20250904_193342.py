# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull
import os
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Create a SparkSession with Delta Lake extensions.
    """
    spark = SparkSession.builder \
        .appName("Sales ETL Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_config(spark):
    """
    Load configuration from environment variables.
    """
    db_username = os.environ.get("DB_USERNAME")
    db_password = os.environ.get("DB_PASSWORD")
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    delta_lake_path = os.environ.get("DELTA_LAKE_PATH")

    # Create a dictionary to store the configuration
    config = {
        "db_username": db_username,
        "db_password": db_password,
        "db_host": db_host,
        "db_port": db_port,
        "db_name": db_name,
        "delta_lake_path": delta_lake_path
    }
    return config

def load_data(spark, config):
    """
    Load data from the database into DataFrames.
    """
    try:
        # Load customers data
        customers_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['db_host']}:{config['db_port']}/{config['db_name']}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES_DB.CUSTOMERS") \
            .option("user", config["db_username"]) \
            .option("password", config["db_password"]) \
            .load()

        # Filter customers by status
        customers_df = customers_df.filter(col("STATUS") == "ACTIVE")

        # Load products data
        products_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['db_host']}:{config['db_port']}/{config['db_name']}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES_DB.PRODUCTS") \
            .option("user", config["db_username"]) \
            .option("password", config["db_password"]) \
            .load()

        # Load sales data
        sales_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['db_host']}:{config['db_port']}/{config['db_name']}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES_DB.SALES") \
            .option("user", config["db_username"]) \
            .option("password", config["db_password"]) \
            .load()

        # Filter sales by quantity and total amount
        sales_df = sales_df.filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))

        return customers_df, products_df, sales_df

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def perform_etl(spark, customers_df, products_df, sales_df):
    """
    Perform the ETL pipeline.
    """
    try:
        # Perform joins using broadcast
        sales_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner")
        sales_df = sales_df.join(broadcast(products_df), "PRODUCT_ID", "inner")

        # Perform monthly aggregation
        aggregated_df = sales_df.groupBy(col("CUSTOMER_ID"), col("PRODUCT_ID"), col("SALE_DATE").substr(1, 7).alias("MONTH")) \
            .agg(count("*").alias("SALES_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))

        # Perform data quality checks
        null_counts = aggregated_df.select([count(isnull(c)).alias(c) for c in aggregated_df.columns])
        logger.info(f"Null counts: {null_counts.collect()}")

        return aggregated_df

    except Exception as e:
        logger.error(f"Error performing ETL: {str(e)}")
        raise

def write_to_delta_lake(spark, aggregated_df, config):
    """
    Write the aggregated data to Delta Lake.
    """
    try:
        aggregated_df.write.format("delta") \
            .option("path", config["delta_lake_path"]) \
            .option("mergeSchema", "true") \
            .partitionBy("MONTH") \
            .save()

        logger.info("Data written to Delta Lake successfully")

    except Exception as e:
        logger.error(f"Error writing to Delta Lake: {str(e)}")
        raise

def main():
    spark = create_spark_session()
    config = load_config(spark)
    customers_df, products_df, sales_df = load_data(spark, config)
    aggregated_df = perform_etl(spark, customers_df, products_df, sales_df)
    write_to_delta_lake(spark, aggregated_df, config)

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
# Broadcast Joins: PASS
#   Details: Broadcast joins implemented
# Error Handling: PASS
#   Details: Exception handling present
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: PASS
#   Details: Data quality checks present
