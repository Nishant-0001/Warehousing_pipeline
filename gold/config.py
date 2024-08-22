# config.py

# Logging configuration
LOG_DIRECTORY = "warehousing/gold/logs"

# Spark configuration
SPARK_APP_NAME = "GoldLayerProcessing"

# File paths for data
SILVER_LAYER_INPUT_PATH = "./data/silver/Silver_Layer_Sample_Data.parquet"

# Database connection configuration for MySQL
JDBC_URL = "jdbc:mysql://localhost:3306/retail"
CONNECTION_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Output file paths for Parquet data
GOLD_LAYER_OUTPUT_PATHS = {
    "customer_dim": "./data/gold/customer_dim.parquet",
    "product_dim": "./data/gold/product_dim.parquet",
    "store_dim": "./data/gold/store_dim.parquet",
    "date_dim": "./data/gold/date_dim.parquet",
    "fact_table": "./data/gold/fact_table.parquet"
}
