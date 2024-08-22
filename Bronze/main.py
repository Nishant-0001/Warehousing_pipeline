# main.py
import os
import sys
from config import BRONZE_INPUT_DIRECTORY, FAILED_FOLDER, CHECKPOINT_PATH
from utils import list_files_in_directory, file_already_processed, logger
from processing import process_file

# Main execution
try:
    logger.info("Starting Bronze layer processing.")

    # Create necessary directories
    os.makedirs(BRONZE_INPUT_DIRECTORY, exist_ok=True)
    os.makedirs(FAILED_FOLDER, exist_ok=True)

    # List files in the directory
    files = list_files_in_directory(BRONZE_INPUT_DIRECTORY)
    logger.info(f"Found files: {files}")

    # Process each file until a valid one is found
    for file_path in files:
        file_name = os.path.basename(file_path)
        if not file_already_processed(file_name, CHECKPOINT_PATH):
            if process_file(file_path, FAILED_FOLDER, CHECKPOINT_PATH):
                break
    else:
        logger.error("No valid files found in the directory. Exiting.")
        sys.exit(1)  # Exit if no valid file is processed

except Exception as e:
    logger.error(f"Error occurred in Bronze layer: {str(e)}")
    sys.exit(1)  # Exit the script with a failure code
finally:
    logger.info("Bronze layer processing completed.")
