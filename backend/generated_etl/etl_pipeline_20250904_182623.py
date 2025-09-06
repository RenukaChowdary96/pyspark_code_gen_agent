import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

# Create SparkSession with Delta Lake extensions
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Sales ETL Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

# Load data from database
def load_data(spark):
    try:
        # Load customers table
        customers_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES_DB.CUSTOMERS") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .load() \
            .filter(col("STATUS") == "ACTIVE")

        # Load products table
        products_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES_DB.PRODUCTS") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .load()

        # Load sales table
        sales_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES_DB.SALES") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .load() \
            .filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))

        logger.info("Data loaded successfully")
        return customers_df, products_df, sales_df

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

# Perform joins and aggregations
def transform_data(customers_df, products_df, sales_df):
    try:
        # Join sales with customers and products using broadcast
        sales_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner") \
            .join(broadcast(products_df), "PRODUCT_ID", "inner")

        # Perform monthly aggregation
        aggregated_df = sales_df.groupBy(col("CUSTOMER_ID"), col("PRODUCT_ID"), col("SALE_DATE").substr(1, 7).alias("MONTH")) \
            .agg(count("*").alias("SALES_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))

        logger.info("Data transformed successfully")
        return aggregated_df

    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

# Write data to Delta Lake
def write_data(aggregated_df):
    try:
        # Write data to Delta Lake with partitioning
        aggregated_df.write.format("delta") \
            .partitionBy("MONTH") \
            .save("delta://sales_data")

        logger.info("Data written to Delta Lake successfully")
    except Exception as e:
        logger.error(f"Error writing data: {str(e)}")
        raise

# Perform data quality checks
def check_data_quality(aggregated_df):
    try:
        # Check for null values
        null_counts = aggregated_df.select([count(isnull(c)).alias(c) for c in aggregated_df.columns]).collect()[0]

        # Log null counts
        for c, count in null_counts.asDict().items():
            logger.info(f"Null count for {c}: {count}")

        logger.info("Data quality checks completed")
    except Exception as e:
        logger.error(f"Error performing data quality checks: {str(e)}")
        raise

# Main function
def main():
    spark = create_spark_session()
    customers_df, products_df, sales_df = load_data(spark)
    aggregated_df = transform_data(customers_df, products_df, sales_df)
    write_data(aggregated_df)
    check_data_quality(aggregated_df)

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
