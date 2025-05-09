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
    level = getattr(logging, log_level.upper())
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=level, format=format)

def ensure_directory(directory):
    """Ensure a directory exists, creating it if necessary."""
    if not os.path.exists(directory):
        os.makedirs(directory)

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