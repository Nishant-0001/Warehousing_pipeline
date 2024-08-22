# functions.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from loguru import logger
import os
import sys
import config

def setup_logging():
    os.makedirs(config.LOG_DIRECTORY, exist_ok=True)
    logger.add(f"{config.LOG_DIRECTORY}/silver_layer.log", rotation="500 MB")

def initialize_spark():
    spark = SparkSession.builder.appName(config.SPARK_APP_NAME).getOrCreate()
    logger.info("Spark session initialized.")
    return spark

def load_bronze_data(spark):
    data = spark.read.parquet(config.BRONZE_LAYER_INPUT_PATH)
    logger.info("Data loaded successfully from Bronze layer.")
    return data

def standardize_date(data):
    return data.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))

def write_silver_data(data):
    data.write.parquet(config.SILVER_LAYER_OUTPUT_PATH, mode="append")
    logger.info("Data written successfully to Silver layer.")
