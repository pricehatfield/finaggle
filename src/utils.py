"""
Utility functions for the reconciliation system.

This module contains helper functions that are used across the system but
are not directly related to transaction processing or reconciliation.
"""

import os
import pathlib
import logging

logger = logging.getLogger(__name__)

def setup_logging(debug=False, log_level='info'):
    """Configure logging for the application."""
    # Determine log level
    if debug:
        level = logging.DEBUG
    else:
        level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure basic logging
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Get log file path from environment or use default
    log_file = os.getenv('LOG_FILE', 'debug.log')
    
    # Create log directory if needed
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Set up logging to file and console
    logging.basicConfig(
        level=level,
        format=format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return log_file

def ensure_directory(dir_type):
    """Ensure required directories exist.
    
    Args:
        dir_type (str): Type of directory ('archive', 'logs', etc.)
        
    Returns:
        pathlib.Path: Path to the directory
        
    Raises:
        ValueError: If dir_type is invalid
    """
    # Validate directory type
    valid_dir_types = ['archive', 'logs', 'output', 'data']
    if dir_type not in valid_dir_types:
        raise ValueError(f"Invalid directory type: {dir_type}. Expected one of: {valid_dir_types}")
    
    base_dir = os.getenv('DATA_DIR', os.getcwd())
    dir_path = pathlib.Path(base_dir) / dir_type
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path

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