"""
Utility functions for the reconciliation system.

This module contains helper functions that are used across the system but
are not directly related to transaction processing or reconciliation.
"""

import os
import pathlib
import logging

logger = logging.getLogger(__name__)

def setup_logging(log_level=logging.INFO):
    """
    Set up logging configuration.
    
    Args:
        log_level (int): Logging level (default: logging.INFO)
    
    Uses LOG_FILE environment variable to determine log file location.
    If LOG_FILE is not set, logs to stderr.
    """
    # Set root logger level
    logging.getLogger().setLevel(log_level)
    
    log_file = os.getenv('LOG_FILE')
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            
        logging.basicConfig(
            filename=log_file,
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Create an empty log file if it doesn't exist
        if not os.path.exists(log_file):
            open(log_file, 'a').close()
    else:
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

def ensure_directory(dir_type):
    """
    Verify a directory exists and create it if necessary.
    
    Args:
        dir_type (str): Type of directory ("archive" or "logs")
    
    Returns:
        str: Full path to the directory
    
    Raises:
        ValueError: If dir_type is not "archive" or "logs"
    """
    if dir_type not in ["archive", "logs"]:
        raise ValueError("dir_type must be 'archive' or 'logs'")
        
    script_dir = pathlib.Path(__file__).parent.absolute()
    target_dir = os.path.join(script_dir, dir_type)
    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)
    return target_dir

def create_output_directories(output_dir):
    """
    Create output directories for reconciled and unmatched transactions.
    
    Args:
        output_dir (str or pathlib.Path): Base directory for output files
    
    Returns:
        None
    
    Side Effects:
        - Creates output_dir if it doesn't exist
        - Creates reconciled and unmatched subdirectories
    """
    logger.info(f"Creating output directories in {output_dir}")
    
    # Convert to Path object if string
    if isinstance(output_dir, str):
        output_dir = pathlib.Path(output_dir)
    
    # Create base directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories
    (output_dir / "reconciled").mkdir(exist_ok=True)
    (output_dir / "unmatched").mkdir(exist_ok=True) 