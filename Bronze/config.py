# config.py
import datetime
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, DoubleType,DateType
# Salt for hashing sensitive data
SALT = "some_random_salt_value"

# Local file paths and directories
BRONZE_INPUT_DIRECTORY = "bronze/raw/Data_Ingestion"
BRONZE_OUTPUT_DIRECTORY = "bronze/bronze_layer_output"
FAILED_FOLDER = "bronze/raw/failed"
CHECKPOINT_PATH = "bronze/bronze_layer_processed_files.txt"

# Logging configuration
LOG_PATH = "bronze/logs/bronze_layer.log"

# Expected schema for the data
EXPECTED_SCHEMA = StructType([
    StructField("OrderID", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("PhoneNumber", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("StoreCode", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", DoubleType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Date", DateType(), True),
    StructField("CreditCardNumber", StringType(), True),
    StructField("ExpiryDate", StringType(), True)
])
