# main.py

from functions import setup_logging, initialize_spark, load_bronze_data, standardize_date, write_silver_data
import sys
from loguru import logger

def main():
    setup_logging()
    
    try:
        spark = initialize_spark()
        data = load_bronze_data(spark)
        silver_data = standardize_date(data)
        silver_data.show()
        write_silver_data(silver_data)

    except Exception as e:
        logger.error(f"Error occurred in Silver layer: {str(e)}")
        sys.exit(1)  # Exit the script with a failure code

    logger.info("Silver layer processing completed.")

if __name__ == "__main__":
    main()
