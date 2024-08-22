# utils.py
import os
import shutil
from loguru import logger
from config import LOG_PATH

# Setup logging
logger.add(LOG_PATH, rotation="500 MB")

def list_files_in_directory(directory):
    """List files in a directory."""
    return [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

def file_already_processed(file_name, checkpoint_path):
    """Check if the file is already processed."""
    try:
        with open(checkpoint_path, 'r') as f:
            processed_files = f.read().splitlines()
        return file_name in processed_files
    except FileNotFoundError:
        return False

def mark_file_as_processed(file_name, checkpoint_path):
    """Mark a file as processed."""
    with open(checkpoint_path, 'a') as f:
        f.write(file_name + '\n')

def move_file_to_failed(src_path, dest_path):
    """Move file to the failed directory."""
    shutil.move(src_path, dest_path)
