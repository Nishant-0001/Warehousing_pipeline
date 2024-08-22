import subprocess
import sys
from loguru import logger
import config

def run_script(script_path):
    try:
        # Run the script using subprocess
        result = subprocess.run(["python", script_path], check=True)
        logger.info(f"{script_path} ran successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred while running {script_path}: {str(e)}")
        sys.exit(1)  # Exit the script with a failure code

def main():
    logger.info("Starting the data pipeline...")

    # List of scripts to run, using paths from config.py
    scripts = [config.BRONZE_SCRIPT_PATH, config.SILVER_SCRIPT_PATH, config.GOLD_SCRIPT_PATH]

    for script in scripts:
        run_script(script)

    logger.info("Data pipeline completed successfully.")

if __name__ == "__main__":
    main()
