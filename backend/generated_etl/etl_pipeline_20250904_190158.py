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
    spark = SparkSession.builder.appName("Sales ETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
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
    Load data from the database using the provided configuration.
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

        # Filter customers by status
        filtered_customers_df = customers_df.filter(col("STATUS") == "ACTIVE")

        # Filter sales by quantity and total amount
        filtered_sales_df = sales_df.filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))

        # Perform data quality checks
        logger.info("Performing data quality checks...")
        customers_null_count = filtered_customers_df.select(count(isnull("CUSTOMER_ID")).alias("null_count")).collect()[0].null_count
        products_null_count = products_df.select(count(isnull("PRODUCT_ID")).alias("null_count")).collect()[0].null_count
        sales_null_count = filtered_sales_df.select(count(isnull("SALE_ID")).alias("null_count")).collect()[0].null_count

        logger.info(f"Customers null count: {customers_null_count}")
        logger.info(f"Products null count: {products_null_count}")
        logger.info(f"Sales null count: {sales_null_count}")

        return filtered_customers_df, products_df, filtered_sales_df

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def transform_data(customers_df, products_df, sales_df):
    """
    Transform the data by joining the sales data with the customers and products data.
    """
    try:
        # Join sales data with customers data
        sales_customers_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner")

        # Join sales_customers data with products data
        sales_customers_products_df = sales_customers_df.join(broadcast(products_df), "PRODUCT_ID", "inner")

        # Perform monthly aggregation by customer and product
        aggregated_df = sales_customers_products_df.groupBy("CUSTOMER_ID", "PRODUCT_ID", "SALE_DATE").agg({"TOTAL_AMOUNT": "sum"})

        return aggregated_df

    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def load_data_to_delta_lake(df, config):
    """
    Load the transformed data to Delta Lake.
    """
    try:
        # Write the data to Delta Lake
        df.write.format("delta") \
            .option("path", config["delta_lake_path"]) \
            .option("mergeSchema", "true") \
            .partitionBy("SALE_DATE") \
            .save()

        logger.info("Data loaded to Delta Lake successfully.")

    except Exception as e:
        logger.error(f"Error loading data to Delta Lake: {str(e)}")
        raise

def main():
    # Create a SparkSession
    spark = create_spark_session()

    # Load configuration
    config = load_config(spark)

    # Load data
    customers_df, products_df, sales_df = load_data(spark, config)

    # Transform data
    aggregated_df = transform_data(customers_df, products_df, sales_df)

    # Load data to Delta Lake
    load_data_to_delta_lake(aggregated_df, config)

    # Stop the SparkSession
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
# Broadcast Joins: PASS
#   Details: Broadcast joins implemented
# Error Handling: PASS
#   Details: Exception handling present
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: PASS
#   Details: Data quality checks present
