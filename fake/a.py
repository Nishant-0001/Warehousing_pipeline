from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import os
import random
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GenerateDataset") \
    .getOrCreate()

# Define the schema
schema = StructType([
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

# Sample data generation functions
def generate_order_id():
    return f"ORD{random.randint(1, 9999)}"

def generate_customer_name():
    first_names = ["John", "Jane", "Alex", "Emily", "Michael"]
    last_names = ["Doe", "Smith", "Johnson", "Brown", "Lee"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def generate_phone_number():
    return f"+1-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

def generate_location():
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    return random.choice(cities)

def generate_country():
    return "USA"

def generate_store_code():
    return f"STR{random.randint(100, 999)}"

def generate_product():
    products = ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch"]
    return random.choice(products)

def generate_quantity():
    return float(random.randint(1, 10))

def generate_price():
    return round(random.uniform(10, 1000), 2)

def generate_date():
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 1, 1)
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date.date()  # Use date object for DateType

def generate_credit_card_number():
    return f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"

def generate_expiry_date():
    return f"{random.randint(1, 12)}/{random.randint(22, 28)}"

# Generate a list of sample data
data = [(generate_order_id(), generate_customer_name(), generate_phone_number(), generate_location(), generate_country(),
         generate_store_code(), generate_product(), generate_quantity(), generate_price(), generate_date(),
         generate_credit_card_number(), generate_expiry_date()) for _ in range(100)]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Define output paths
parquet_output_path = "output/dataset.parquet"
csv_output_path = "output/dataset.csv"

# Write DataFrame to Parquet (intermediate storage)
df.write.parquet(parquet_output_path, mode='overwrite')

# Read from Parquet and write to CSV
df_parquet = spark.read.parquet(parquet_output_path)
df_parquet.write.csv(csv_output_path, header=True, mode='overwrite', sep=',')

# Stop the Spark session
spark.stop()
