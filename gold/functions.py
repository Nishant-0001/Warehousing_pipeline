# functions.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from loguru import logger
import os
import sys
import config

def setup_logging():
    os.makedirs(config.LOG_DIRECTORY, exist_ok=True)
    
    # Remove default logger (stdout) to customize handlers
    logger.remove()

    # Log to both file and stdout
    logger.add(f"{config.LOG_DIRECTORY}/gold_layer.log", rotation="500 MB")
    logger.add(sys.stdout, level="INFO")

def initialize_spark():
    spark = SparkSession.builder.appName(config.SPARK_APP_NAME).getOrCreate()
    logger.info("Spark session initialized.")
    return spark

def load_silver_data(spark):
    data = spark.read.parquet(config.SILVER_LAYER_INPUT_PATH)
    logger.info("Data loaded successfully from Silver layer.")
    return data

def create_dimension_tables(data):
    customer_dim = data.select("CustomerName", "PhoneNumber", "Location", "Country").dropDuplicates().withColumn("CustomerID", col("CustomerName"))
    product_dim = data.select("Product").dropDuplicates().withColumn("ProductID", col("Product"))
    store_dim = data.select("StoreCode").dropDuplicates().withColumn("StoreID", col("StoreCode"))
    date_dim = data.select("Date").dropDuplicates().withColumn("DateID", col("Date"))
    logger.info("Dimension tables created successfully.")
    return customer_dim, product_dim, store_dim, date_dim

def write_dimension_tables_to_mysql(customer_dim, product_dim, store_dim, date_dim):
    customer_dim.write.jdbc(config.JDBC_URL, "customer_dim", mode="append", properties=config.CONNECTION_PROPERTIES)
    product_dim.write.jdbc(config.JDBC_URL, "product_dim", mode="append", properties=config.CONNECTION_PROPERTIES)
    store_dim.write.jdbc(config.JDBC_URL, "store_dim", mode="append", properties=config.CONNECTION_PROPERTIES)
    date_dim.write.jdbc(config.JDBC_URL, "date_dim", mode="append", properties=config.CONNECTION_PROPERTIES)
    logger.info("Dimension tables written successfully to MySQL.")

def write_dimension_tables_to_disk(customer_dim, product_dim, store_dim, date_dim):
    customer_dim.write.parquet(config.GOLD_LAYER_OUTPUT_PATHS["customer_dim"], mode="overwrite")
    product_dim.write.parquet(config.GOLD_LAYER_OUTPUT_PATHS["product_dim"], mode="overwrite")
    store_dim.write.parquet(config.GOLD_LAYER_OUTPUT_PATHS["store_dim"], mode="overwrite")
    date_dim.write.parquet(config.GOLD_LAYER_OUTPUT_PATHS["date_dim"], mode="overwrite")
    logger.info("Dimension tables written successfully to disk.")

def create_fact_table(data, customer_dim, product_dim, store_dim, date_dim):
    fact_table = data.join(customer_dim, ["CustomerName", "PhoneNumber", "Location", "Country"]) \
                     .join(product_dim, "Product") \
                     .join(store_dim, "StoreCode") \
                     .join(date_dim, "Date") \
                     .select("OrderID", "CustomerID", "ProductID", "StoreID", "DateID", "Quantity", "Price")
    logger.info("Fact table created successfully.")
    return fact_table

def write_fact_table_to_mysql(fact_table):
    fact_table.write.jdbc(config.JDBC_URL, "fact_table", mode="append", properties=config.CONNECTION_PROPERTIES)
    logger.info("Fact table written successfully to MySQL.")

def write_fact_table_to_disk(fact_table):
    fact_table.write.parquet(config.GOLD_LAYER_OUTPUT_PATHS["fact_table"], mode="overwrite")
    logger.info("Fact table written successfully to disk.")
