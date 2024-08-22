# main.py

from functions import (
    setup_logging, 
    initialize_spark, 
    load_silver_data, 
    create_dimension_tables, 
    write_dimension_tables_to_mysql, 
    write_dimension_tables_to_disk, 
    create_fact_table, 
    write_fact_table_to_mysql, 
    write_fact_table_to_disk
)
import sys
from loguru import logger

def main():
    setup_logging()
    
    try:
        spark = initialize_spark()
        data = load_silver_data(spark)
        
        # Create Dimension Tables
        customer_dim, product_dim, store_dim, date_dim = create_dimension_tables(data)
        
        # Write Dimension Tables to MySQL and disk
        write_dimension_tables_to_mysql(customer_dim, product_dim, store_dim, date_dim)
        write_dimension_tables_to_disk(customer_dim, product_dim, store_dim, date_dim)
        
        # Create Fact Table
        fact_table = create_fact_table(data, customer_dim, product_dim, store_dim, date_dim)
        fact_table.show()
        
        # Write Fact Table to MySQL and disk
        write_fact_table_to_mysql(fact_table)
        write_fact_table_to_disk(fact_table)

    except Exception as e:
        logger.error(f"Error occurred in Gold layer: {str(e)}")
        sys.exit(1)  # Exit the script with a failure code

    logger.info("Gold layer processing completed.")

if __name__ == "__main__":
    main()
