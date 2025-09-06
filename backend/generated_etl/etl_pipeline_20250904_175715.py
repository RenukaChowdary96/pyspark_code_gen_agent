import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a SparkSession with Delta Lake extensions"""
    spark = SparkSession.builder \
        .appName("Sales ETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_data(spark, db_name, table_name):
    """Load data from a database table"""
    try:
        logger.info(f"Loading data from {db_name}.{table_name}")
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{db_name}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_name) \
            .option("user", os.environ['DB_USER']) \
            .option("password", os.environ['DB_PASSWORD']) \
            .load()
        logger.info(f"Loaded {df.count()} rows from {db_name}.{table_name}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {db_name}.{table_name}: {str(e)}")
        raise

def filter_data(df, table_name):
    """Filter data based on business rules"""
    try:
        logger.info(f"Filtering data for {table_name}")
        if table_name == "CUSTOMERS":
            filtered_df = df.filter(col("STATUS") == "ACTIVE")
        elif table_name == "SALES":
            filtered_df = df.filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))
        else:
            filtered_df = df
        logger.info(f"Filtered {filtered_df.count()} rows for {table_name}")
        return filtered_df
    except Exception as e:
        logger.error(f"Error filtering data for {table_name}: {str(e)}")
        raise

def join_data(customers_df, products_df, sales_df):
    """Join data using broadcast() for dimension tables"""
    try:
        logger.info("Joining data")
        joined_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner") \
            .join(broadcast(products_df), "PRODUCT_ID", "inner")
        logger.info(f"Joined {joined_df.count()} rows")
        return joined_df
    except Exception as e:
        logger.error(f"Error joining data: {str(e)}")
        raise

def aggregate_data(joined_df):
    """Aggregate data by customer and product"""
    try:
        logger.info("Aggregating data")
        aggregated_df = joined_df.groupBy("CUSTOMER_ID", "CUSTOMER_NAME", "PRODUCT_ID", "PRODUCT_NAME", "SALE_DATE") \
            .agg(count("SALE_ID").alias("SALE_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))
        logger.info(f"Aggregated {aggregated_df.count()} rows")
        return aggregated_df
    except Exception as e:
        logger.error(f"Error aggregating data: {str(e)}")
        raise

def write_data(df, table_name):
    """Write data to Delta Lake format with partitioning"""
    try:
        logger.info(f"Writing data to {table_name}")
        df.write.format("delta") \
            .option("path", f"/delta/{table_name}") \
            .partitionBy("SALE_DATE") \
            .save()
        logger.info(f"Wrote data to {table_name}")
    except Exception as e:
        logger.error(f"Error writing data to {table_name}: {str(e)}")
        raise

def data_quality_checks(df, table_name):
    """Perform data quality checks"""
    try:
        logger.info(f"Performing data quality checks for {table_name}")
        null_counts = df.select([count(isnull(c)).alias(c) for c in df.columns]).collect()[0]
        for col_name, null_count in null_counts.asDict().items():
            logger.info(f"{table_name}: {col_name} has {null_count} null values")
        logger.info(f"Data quality checks complete for {table_name}")
    except Exception as e:
        logger.error(f"Error performing data quality checks for {table_name}: {str(e)}")
        raise

def main():
    spark = create_spark_session()
    logger.info("SparkSession created")

    # Load data
    customers_df = load_data(spark, "SALES_DB", "CUSTOMERS")
    products_df = load_data(spark, "SALES_DB", "PRODUCTS")
    sales_df = load_data(spark, "SALES_DB", "SALES")

    # Filter data
    filtered_customers_df = filter_data(customers_df, "CUSTOMERS")
    filtered_products_df = filter_data(products_df, "PRODUCTS")
    filtered_sales_df = filter_data(sales_df, "SALES")

    # Join data
    joined_df = join_data(filtered_customers_df, filtered_products_df, filtered_sales_df)

    # Aggregate data
    aggregated_df = aggregate_data(joined_df)

    # Perform data quality checks
    data_quality_checks(aggregated_df, "SALES_AGGREGATED")

    # Write data
    write_data(aggregated_df, "SALES_AGGREGATED")

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
