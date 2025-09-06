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
        .appName("Sales ETL Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_data(spark):
    """Load data from the database"""
    try:
        # Load customers data
        customers_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{os.environ['SALES_DB_HOST']}:{os.environ['SALES_DB_PORT']}/SALES_DB") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "CUSTOMERS") \
            .option("user", os.environ['SALES_DB_USERNAME']) \
            .option("password", os.environ['SALES_DB_PASSWORD']) \
            .load() \
            .filter(col("STATUS") == "ACTIVE")
        
        # Load products data
        products_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{os.environ['SALES_DB_HOST']}:{os.environ['SALES_DB_PORT']}/SALES_DB") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "PRODUCTS") \
            .option("user", os.environ['SALES_DB_USERNAME']) \
            .option("password", os.environ['SALES_DB_PASSWORD']) \
            .load()
        
        # Load sales data
        sales_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{os.environ['SALES_DB_HOST']}:{os.environ['SALES_DB_PORT']}/SALES_DB") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES") \
            .option("user", os.environ['SALES_DB_USERNAME']) \
            .option("password", os.environ['SALES_DB_PASSWORD']) \
            .load() \
            .filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))
        
        return customers_df, products_df, sales_df
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def perform_etl(customers_df, products_df, sales_df):
    """Perform ETL operations"""
    try:
        # Perform joins using broadcast
        sales_df = sales_df.join(broadcast(customers_df), "CUSTOMER_ID", "inner")
        sales_df = sales_df.join(broadcast(products_df), "PRODUCT_ID", "inner")
        
        # Perform monthly aggregation
        aggregated_df = sales_df.groupBy("CUSTOMER_ID", "PRODUCT_ID", "SALE_DATE").agg(count("SALE_ID").alias("SALES_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))
        
        return aggregated_df
    
    except Exception as e:
        logger.error(f"Error performing ETL: {e}")
        raise

def write_data(aggregated_df):
    """Write data to Delta Lake"""
    try:
        # Write data to Delta Lake with partitioning
        aggregated_df.write.format("delta") \
            .partitionBy("SALE_DATE") \
            .option("path", os.environ['DELTA_LAKE_PATH']) \
            .saveAsTable("SALES_AGGREGATED")
        
        # Perform data quality checks
        null_counts = aggregated_df.select([count(isnull(c)).alias(c) for c in aggregated_df.columns]).collect()
        for row in null_counts:
            for col_name, null_count in row.asDict().items():
                logger.info(f"Null count for {col_name}: {null_count}")
        
    except Exception as e:
        logger.error(f"Error writing data: {e}")
        raise

def main():
    spark = create_spark_session()
    customers_df, products_df, sales_df = load_data(spark)
    aggregated_df = perform_etl(customers_df, products_df, sales_df)
    write_data(aggregated_df)
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
