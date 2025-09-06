import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file):
    """Load YAML configuration file"""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_spark_session(config):
    """Create SparkSession with Delta Lake extensions"""
    spark = SparkSession.builder \
        .appName("Sales ETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_data(spark, config):
    """Load data from source database"""
    metadata = config['metadata']
    source = config['source']
    database = metadata['database']
    tables = metadata['tables']

    customers_df = spark.read.format("jdbc") \
        .option("url", source['url']) \
        .option("driver", source['driver']) \
        .option("dbtable", f"{database}.{tables['customers']}") \
        .option("user", os.environ['DB_USERNAME']) \
        .option("password", os.environ['DB_PASSWORD']) \
        .load()

    products_df = spark.read.format("jdbc") \
        .option("url", source['url']) \
        .option("driver", source['driver']) \
        .option("dbtable", f"{database}.{tables['products']}") \
        .option("user", os.environ['DB_USERNAME']) \
        .option("password", os.environ['DB_PASSWORD']) \
        .load()

    sales_df = spark.read.format("jdbc") \
        .option("url", source['url']) \
        .option("driver", source['driver']) \
        .option("dbtable", f"{database}.{tables['sales']}") \
        .option("user", os.environ['DB_USERNAME']) \
        .option("password", os.environ['DB_PASSWORD']) \
        .load()

    return customers_df, products_df, sales_df

def filter_data(customers_df, products_df, sales_df):
    """Apply business rules to filter data"""
    filtered_customers_df = customers_df.filter(col("STATUS") == "ACTIVE")
    filtered_sales_df = sales_df.filter((col("QUANTITY") > 0) & (col("TOTAL_AMOUNT") > 0))

    return filtered_customers_df, filtered_sales_df

def join_data(filtered_customers_df, products_df, filtered_sales_df):
    """Join data using broadcast() function for dimension tables"""
    joined_df = filtered_sales_df.join(broadcast(filtered_customers_df), "CUSTOMER_ID", "inner") \
        .join(broadcast(products_df), "PRODUCT_ID", "inner")

    return joined_df

def aggregate_data(joined_df):
    """Aggregate data by customer and product"""
    aggregated_df = joined_df.groupBy(col("CUSTOMER_ID"), col("PRODUCT_ID"), col("SALE_DATE").substr(1, 7).alias("MONTH")) \
        .agg(count("*").alias("SALES_COUNT"), sum("TOTAL_AMOUNT").alias("TOTAL_SALES"))

    return aggregated_df

def write_data(aggregated_df, config):
    """Write data to Delta Lake format with partitioning"""
    target = config['target']
    database = target['database']
    table = target['table']

    aggregated_df.write.format("delta") \
        .option("path", f"{database}/{table}") \
        .option("mergeSchema", "true") \
        .partitionBy("MONTH") \
        .saveAsTable(f"{database}.{table}")

def data_quality_checks(aggregated_df):
    """Perform data quality checks"""
    null_counts = aggregated_df.select([count(isnull(c)).alias(c) for c in aggregated_df.columns]).collect()
    logger.info("Data quality checks:")
    for row in null_counts:
        for col_name, null_count in row.asDict().items():
            logger.info(f"Null count for {col_name}: {null_count}")

def main():
    config_file = "config.yaml"
    config = load_config(config_file)

    spark = create_spark_session(config)

    try:
        customers_df, products_df, sales_df = load_data(spark, config)
        logger.info("Data loaded successfully")

        filtered_customers_df, filtered_sales_df = filter_data(customers_df, products_df, sales_df)
        logger.info("Data filtered successfully")

        joined_df = join_data(filtered_customers_df, products_df, filtered_sales_df)
        logger.info("Data joined successfully")

        aggregated_df = aggregate_data(joined_df)
        logger.info("Data aggregated successfully")

        write_data(aggregated_df, config)
        logger.info("Data written to Delta Lake successfully")

        data_quality_checks(aggregated_df)
        logger.info("Data quality checks completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        spark.stop()

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
# Broadcast Joins: PASS
#   Details: Broadcast joins implemented
# Error Handling: PASS
#   Details: Exception handling present
# Logging: PASS
#   Details: Logging implemented
# Data Quality Checks: PASS
#   Details: Data quality checks present
