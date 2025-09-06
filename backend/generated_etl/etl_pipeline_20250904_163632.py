import os
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year

# Load YAML configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a SparkSession with Delta Lake extensions"""
    spark = SparkSession.builder \
        .appName("Sales ETL Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_data(spark, config):
    """Load data from source database"""
    logger.info("Loading data from source database")
    try:
        # Load CUSTOMERS table
        customers_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['source']['host']}:{config['source']['port']}/{config['metadata']['database']}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "CUSTOMERS") \
            .option("user", config['source']['username']) \
            .option("password", config['source']['password']) \
            .load()
        
        # Load PRODUCTS table
        products_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['source']['host']}:{config['source']['port']}/{config['metadata']['database']}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "PRODUCTS") \
            .option("user", config['source']['username']) \
            .option("password", config['source']['password']) \
            .load()
        
        # Load SALES table
        sales_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{config['source']['host']}:{config['source']['port']}/{config['metadata']['database']}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "SALES") \
            .option("user", config['source']['username']) \
            .option("password", config['source']['password']) \
            .load()
        
        return customers_df, products_df, sales_df
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def filter_data(customers_df, products_df, sales_df):
    """Filter data based on business rules"""
    logger.info("Filtering data")
    try:
        # Filter CUSTOMERS by STATUS = 'ACTIVE'
        filtered_customers_df = customers_df.filter(col("STATUS") == "ACTIVE")
        
        # Filter SALES by QUANTITY > 0 AND TOTAL_AMOUNT > 0
        filtered_sales_df = sales_df.filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))
        
        return filtered_customers_df, filtered_sales_df
    
    except Exception as e:
        logger.error(f"Error filtering data: {e}")
        raise

def join_data(filtered_customers_df, products_df, filtered_sales_df):
    """Join data using broadcast joins"""
    logger.info("Joining data")
    try:
        # Broadcast join CUSTOMERS and SALES
        joined_sales_df = filtered_sales_df.join(broadcast(filtered_customers_df), on="CUSTOMER_ID")
        
        # Broadcast join PRODUCTS and SALES
        joined_sales_df = joined_sales_df.join(broadcast(products_df), on="PRODUCT_ID")
        
        return joined_sales_df
    
    except Exception as e:
        logger.error(f"Error joining data: {e}")
        raise

def aggregate_data(joined_sales_df):
    """Aggregate data by customer and product"""
    logger.info("Aggregating data")
    try:
        # Aggregate data by customer and product
        aggregated_df = joined_sales_df.groupBy("CUSTOMER_ID", "PRODUCT_ID", month("SALE_DATE").alias("MONTH"), year("SALE_DATE").alias("YEAR")) \
            .agg({"TOTAL_AMOUNT": "sum", "QUANTITY": "sum"}) \
            .withColumnRenamed("sum(TOTAL_AMOUNT)", "TOTAL_AMOUNT") \
            .withColumnRenamed("sum(QUANTITY)", "QUANTITY")
        
        return aggregated_df
    
    except Exception as e:
        logger.error(f"Error aggregating data: {e}")
        raise

def write_data(aggregated_df, config):
    """Write data to Delta Lake"""
    logger.info("Writing data to Delta Lake")
    try:
        # Write data to Delta Lake
        aggregated_df.write.format("delta") \
            .option("path", config['target']['delta_lake_path']) \
            .option("mergeSchema", "true") \
            .partitionBy("MONTH", "YEAR") \
            .save()
        
    except Exception as e:
        logger.error(f"Error writing data: {e}")
        raise

def data_quality_checks(aggregated_df):
    """Perform data quality checks"""
    logger.info("Performing data quality checks")
    try:
        # Check for null values
        null_counts = aggregated_df.select([count(when(isnull(c), c)).alias(c) for c in aggregated_df.columns]).collect()
        logger.info(f"Null counts: {null_counts}")
        
        # Check for duplicate values
        duplicate_counts = aggregated_df.groupBy("CUSTOMER_ID", "PRODUCT_ID", "MONTH", "YEAR").count().filter("count > 1").collect()
        logger.info(f"Duplicate counts: {duplicate_counts}")
        
    except Exception as e:
        logger.error(f"Error performing data quality checks: {e}")
        raise

def main():
    # Create SparkSession
    spark = create_spark_session()
    
    # Load configuration
    global config
    config = yaml.safe_load(open('config.yaml', 'r'))
    
    # Load data
    customers_df, products_df, sales_df = load_data(spark, config)
    
    # Filter data
    filtered_customers_df, filtered_sales_df = filter_data(customers_df, products_df, sales_df)
    
    # Join data
    joined_sales_df = join_data(filtered_customers_df, products_df, filtered_sales_df)
    
    # Aggregate data
    aggregated_df = aggregate_data(joined_sales_df)
    
    # Perform data quality checks
    data_quality_checks(aggregated_df)
    
    # Write data to Delta Lake
    write_data(aggregated_df, config)
    
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
# Environment Variables: FAIL (Important)
#   Details: No environment variables detected
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
