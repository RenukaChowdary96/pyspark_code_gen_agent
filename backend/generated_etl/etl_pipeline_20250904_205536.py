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
    db_name = os.environ.get("DB_NAME")
    customers_table = os.environ.get("CUSTOMERS_TABLE")
    products_table = os.environ.get("PRODUCTS_TABLE")
    sales_table = os.environ.get("SALES_TABLE")
    delta_path = os.environ.get("DELTA_PATH")
    return db_name, customers_table, products_table, sales_table, delta_path

def load_data(spark, db_name, customers_table, products_table, sales_table):
    """
    Load data from tables using predicate pushdown.
    """
    try:
        # Load customers data with filter
        customers_df = spark.read.format("parquet") \
            .option("path", f"{db_name}.{customers_table}") \
            .load() \
            .filter(col("STATUS") == "ACTIVE")
        
        # Load products data
        products_df = spark.read.format("parquet") \
            .option("path", f"{db_name}.{products_table}") \
            .load()
        
        # Load sales data with filter
        sales_df = spark.read.format("parquet") \
            .option("path", f"{db_name}.{sales_table}") \
            .load() \
            .filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))
        
        return customers_df, products_df, sales_df
    
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def perform_join(spark, customers_df, products_df, sales_df):
    """
    Perform join operations using broadcast() function.
    """
    try:
        # Join sales with customers using broadcast()
        sales_customers_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner")
        
        # Join sales_customers with products using broadcast()
        sales_customers_products_df = sales_customers_df.join(broadcast(products_df), "PRODUCT_ID", "inner")
        
        return sales_customers_products_df
    
    except Exception as e:
        logger.error(f"Error performing join: {str(e)}")
        raise

def perform_aggregation(spark, sales_customers_products_df):
    """
    Perform monthly aggregation by customer and product.
    """
    try:
        # Perform aggregation
        aggregated_df = sales_customers_products_df.groupBy(col("CUSTOMER_ID"), col("PRODUCT_ID"), col("SALE_DATE").substr(1, 7).alias("MONTH")) \
            .agg(count("*").alias("SALES_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))
        
        return aggregated_df
    
    except Exception as e:
        logger.error(f"Error performing aggregation: {str(e)}")
        raise

def write_to_delta(spark, aggregated_df, delta_path):
    """
    Write aggregated data to Delta Lake format with partitioning.
    """
    try:
        # Write to Delta Lake
        aggregated_df.write.format("delta") \
            .option("path", delta_path) \
            .option("mergeSchema", "true") \
            .partitionBy("MONTH") \
            .save()
        
        logger.info(f"Data written to Delta Lake: {delta_path}")
    
    except Exception as e:
        logger.error(f"Error writing to Delta Lake: {str(e)}")
        raise

def perform_data_quality_checks(spark, aggregated_df):
    """
    Perform data quality checks using isNull() and count().
    """
    try:
        # Check for null values
        null_counts = aggregated_df.select([count(isnull(c)).alias(c) for c in aggregated_df.columns])
        
        # Log null counts
        logger.info(f"Null counts: {null_counts.collect()[0].asDict()}")
        
        # Check for total count
        total_count = aggregated_df.count()
        
        # Log total count
        logger.info(f"Total count: {total_count}")
    
    except Exception as e:
        logger.error(f"Error performing data quality checks: {str(e)}")
        raise

def main():
    # Create SparkSession
    spark = create_spark_session()
    
    # Load configuration
    db_name, customers_table, products_table, sales_table, delta_path = load_config(spark)
    
    # Load data
    customers_df, products_df, sales_df = load_data(spark, db_name, customers_table, products_table, sales_table)
    
    # Perform join
    sales_customers_products_df = perform_join(spark, customers_df, products_df, sales_df)
    
    # Perform aggregation
    aggregated_df = perform_aggregation(spark, sales_customers_products_df)
    
    # Perform data quality checks
    perform_data_quality_checks(spark, aggregated_df)
    
    # Write to Delta Lake
    write_to_delta(spark, aggregated_df, delta_path)

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
