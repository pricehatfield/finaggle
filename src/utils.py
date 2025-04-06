"""
Utility functions for the reconciliation system.

This module contains helper functions that are used across the system but
are not directly related to transaction processing or reconciliation.
"""

import os
import pathlib
import logging

logger = logging.getLogger(__name__)

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