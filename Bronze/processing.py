# processing.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat
from config import SALT, EXPECTED_SCHEMA, BRONZE_OUTPUT_DIRECTORY
from utils import logger, move_file_to_failed, mark_file_as_processed

def process_file(file_path, failed_folder, checkpoint_path):
    """Process a single file."""
    try:
        # Initialize Spark session locally
        spark = SparkSession.builder \
            .appName("Bronze Layer Processing") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

        # Load data
        data = spark.read.csv(file_path, header=True, inferSchema=True)
        logger.info(f"Data loaded successfully from {file_path}.")

        # Validate schema
        if data.schema != EXPECTED_SCHEMA:
            logger.error(f"Schema mismatch in {file_path}. Expected: {EXPECTED_SCHEMA}, Got: {data.schema}")
            
            # Move file to failed folder
            failed_file_path = os.path.join(failed_folder, os.path.basename(file_path))
            move_file_to_failed(file_path, failed_file_path)
            logger.info(f"File {file_path} moved to failed folder: {failed_file_path}")
            return False

        logger.info(f"Schema validation passed for {file_path}.")

        # Hash sensitive data with salt
        hashed_data = data.withColumn("HashedCreditCardNumber", sha2(concat(col("CreditCardNumber"), lit(SALT)), 256)) \
                          .withColumn("HashedExpiryDate", sha2(concat(col("ExpiryDate"), lit(SALT)), 256)) \
                          .drop("CreditCardNumber", "ExpiryDate")
        hashed_data = hashed_data.withColumn("Quantity", col("Quantity").cast("int"))

        logger.info(f"Sensitive data hashed successfully for {file_path}.")

        # Write to Bronze layer locally
        output_file_path = os.path.join(BRONZE_OUTPUT_DIRECTORY, f"processed_{os.path.basename(file_path)}.parquet")
        hashed_data.write.parquet(output_file_path, mode="overwrite")
        logger.info(f"Data written successfully to Bronze layer at {output_file_path}.")

        # Mark file as processed
        mark_file_as_processed(os.path.basename(file_path), checkpoint_path)
        return True

    except Exception as e:
        logger.error(f"Error occurred while processing {file_path}: {str(e)}")
        return False

    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped.")
