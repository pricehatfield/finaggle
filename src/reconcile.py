"""
Transaction Reconciliation System

This system standardizes transaction data from various financial institutions into a common format
for reconciliation purposes. The standardized format contains only the essential fields needed for
matching transactions across different sources.

Standardized Format:
- Transaction Date: Date of the transaction (YYYY-MM-DD)
- Post Date: Date the transaction posted (YYYY-MM-DD)
- Description: Transaction description (preserved exactly as-is)
- Amount: Transaction amount (negative for debits, positive for credits)
- Category: Transaction category
- source_file: Origin of the transaction

Reconciliation Keys:
- P: prefix for Post Date matches (from aggregator)
- T: prefix for Transaction Date matches (from detail records)
- U: prefix for unmatched records (both aggregator and detail)
- Format: {prefix}:{date}_{amount}

Design Principles:
1. Simplicity: Single, clear transformation path
2. Consistency: Same output format regardless of input source
3. Maintainability: Clear expectations for data structure
4. Debugging: Original files serve as source of truth
5. Matching: Clear distinction between Post Date and Transaction Date matches
"""

import pandas as pd
from datetime import datetime
import numpy as np
import os
import logging
import pathlib
import re
import csv
from src.utils import ensure_directory, create_output_directories, setup_logging
import argparse
from typing import Tuple

logger = logging.getLogger(__name__)

# Required columns for standardized output
required_columns = [
    'Date',
    'YearMonth',
    'Account',
    'Description',
    'Category',
    'Tags',
    'Amount',
    'reconciled_key',
    'Matched'
]

def standardize_date(date_str):
    """
    Convert various date formats to YYYY-MM-DD (ISO8601).
    
    Args:
        date_str (str, pd.Series, or None): Date string to standardize
        
    Returns:
        str or None: Standardized date in YYYY-MM-DD format, or None if date is invalid
        
    Raises:
        ValueError: If date is null, not a string, or invalid format
    """
    if isinstance(date_str, pd.Series):
        return date_str.apply(standardize_date)
        
    if pd.isna(date_str):
        raise ValueError("Date cannot be null")
        
    if not isinstance(date_str, str):
        raise ValueError(f"Date must be a string, got {type(date_str)}")
        
    # Remove quotes and extra whitespace
    date_str = date_str.strip().strip('"\'')
    logger.debug(f"Processing date string: {date_str}")
    
    # Check if the string looks like a date (contains at least one digit and one separator)
    if not re.search(r'\d+[/-]\d+[/-]\d+', date_str):
        raise ValueError(f"Invalid date format: {date_str}")
    
    # Try different date formats
    formats = [
        '%m/%d/%Y',  # US (Chase format)
        '%Y-%m-%d',  # ISO
        '%Y-%m-%d %H:%M:%S',  # ISO with time
        '%m-%d-%Y',  # US with dashes
        '%Y%m%d',    # Compact
        '%m%d%Y',    # Compact US
        '%m/%d/%y'   # Short year
    ]
    
    for fmt in formats:
        try:
            logger.debug(f"Trying format {fmt} on {date_str}")
            dt = datetime.strptime(date_str, fmt)
            if dt.year < 1900 or dt.year > 2100:
                raise ValueError(f"Invalid date year: {dt.year}")
            result = dt.strftime('%Y-%m-%d')
            logger.debug(f"Successfully converted {date_str} to {result} using format {fmt}")
            return result
        except ValueError:
            continue
            
    # If we get here, the date format is invalid
    raise ValueError(f"Invalid date format: {date_str}")

def clean_amount(amount):
    """Clean and standardize amount values.
    
    Args:
        amount (str or float): Amount to clean
        
    Returns:
        float: Cleaned amount (negative for debits, positive for credits)
        
    Raises:
        ValueError: If amount cannot be converted to float
    """
    if pd.isna(amount):
        return 0.0
        
    if isinstance(amount, (int, float)):
        return float(amount)
        
    if not isinstance(amount, str):
        raise ValueError(f"Amount must be string or number, got {type(amount)}")
    
    # Remove currency symbols, commas, and whitespace
    cleaned = re.sub(r'[$,]', '', amount.strip())
    
    # Handle parentheses for negative numbers
    if cleaned.startswith('(') and cleaned.endswith(')'):
        cleaned = '-' + cleaned[1:-1]
    
    try:
        # Convert to float and ensure negative for debits, positive for credits
        result = float(cleaned)
        return result
    except ValueError:
        raise ValueError(f"Invalid amount format: {amount}")

def standardize_category(category):
    """
    Standardize transaction categories to a common format.
    
    Args:
        category (str): Raw transaction category
        
    Returns:
        str: Standardized category
    """
    if pd.isna(category) or not isinstance(category, str) or not category.strip():
        return 'Uncategorized'
    
    # Standardize case and remove extra spaces
    category = category.strip()
    
    # Define category mappings
    category_map = {
        'Supermarkets': 'Groceries',
        'Merchandise': 'Shopping',
        'Services': 'Entertainment',
        'Telephone': 'Utilities',
        'Cable/Satellite': 'Utilities',
        'Travel/ Entertainment': 'Entertainment',
        'Payments and Credits': 'Transfers',
        'Payment/Credit': 'Transfers',
        'Healthcare/Medical': 'Healthcare',
        'Electronics': 'Shopping'
    }
    
    # Return mapped category or original if no mapping exists
    return category_map.get(category, category)

def is_valid_amount(x):
    """
    Validate if a value can be converted to a float amount.
    
    Args:
        x: Value to validate
        
    Returns:
        bool: True if value can be converted to float, False otherwise
        
    Notes:
        - Handles currency symbols ($)
        - Handles commas in numbers
        - Handles parentheses for negative numbers
        - NaN values are considered invalid
        - Empty strings are considered valid (for optional amounts)
    """
    if pd.isna(x):
        return True  # Allow NaN values
    if isinstance(x, (int, float)):
        return True
    if isinstance(x, str):
        if x == '':
            return True  # Allow empty strings
        try:
            # Remove currency symbols and commas
            x = x.replace('$', '').replace(',', '')
            # Handle parentheses for negative numbers
            if x.startswith('(') and x.endswith(')'):
                x = '-' + x[1:-1]
            float(x)
            return True
        except:
            return False
    return False

def standardize_description(description):
    """
    Standardize transaction descriptions by stripping newlines while preserving content.
    
    Args:
        description (str): Raw transaction description
        
    Returns:
        str: Standardized description with newlines stripped
        
    Notes:
        - Preserves original content exactly
        - Replaces newlines with spaces
        - Handles multiple consecutive newlines
        - Preserves leading/trailing spaces
    """
    if pd.isna(description) or not isinstance(description, str):
        return description
        
    # Replace newlines with spaces, handling multiple consecutive newlines
    result = re.sub(r'\n+', ' ', description)
    logger.debug(f"Standardized description: {repr(description)} -> {repr(result)}")
    return result

def process_discover_format(df, source_file):
    """Process Discover transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        source_file (str): Source file name
        
    Returns:
        pd.DataFrame: Standardized transaction data
    """
    # Validate required columns
    required_columns = ['Trans. Date', 'Post Date', 'Description', 'Amount', 'Category']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Trans. Date'].apply(standardize_date)
    result['Post Date'] = df['Post Date'].apply(standardize_date)
    
    # Validate date order
    if (result['Post Date'] < result['Transaction Date']).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    # Discover uses positive for debits, so we need to invert the sign
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    # Standardize category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file
    result['source_file'] = source_file
    
    return result

def process_capital_one_format(df: pd.DataFrame) -> pd.DataFrame:
    """Process Capital One transactions into standardized format."""
    # Validate required columns
    required_columns = ['Transaction Date', 'Posted Date', 'Description', 'Debit', 'Credit']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Transaction Date'].apply(standardize_date)
    result['Post Date'] = df['Posted Date'].apply(standardize_date)
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Clean amounts first, then combine Debit and Credit into single Amount column
    debit = df['Debit'].apply(clean_amount)
    credit = df['Credit'].apply(clean_amount)
    result['Amount'] = df.apply(lambda row: -debit[row.name] if pd.notna(debit[row.name]) else credit[row.name], axis=1)
    
    return result

def process_chase_format(df: pd.DataFrame) -> pd.DataFrame:
    """Process Chase transactions into standardized format."""
    # Validate required columns
    required_columns = ['Posting Date', 'Description', 'Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Log columns and first few rows for debugging
    logger.debug(f"Processing Chase format with columns: {df.columns.tolist()}")
    logger.debug(f"First row: {df.iloc[0].to_dict()}")
    
    # Use posting date for both transaction and post dates
    posting_dates = df['Posting Date']
    logger.debug(f"Posting Date values: {posting_dates.head().tolist()}")
    
    # Apply date standardization with error handling
    try:
        result['Transaction Date'] = posting_dates.apply(standardize_date)
        result['Post Date'] = posting_dates.apply(standardize_date)
    except ValueError as e:
        logger.error(f"Date standardization error: {str(e)}")
        # Check for problematic values
        for idx, val in posting_dates.items():
            try:
                standardize_date(val)
            except ValueError as e:
                logger.error(f"Invalid date at index {idx}: {val} - {str(e)}")
        raise
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Preserve Type field as separate transaction classification
    if 'Type' in df.columns:
        result['Type'] = df['Type']
    
    # Preserve Check or Slip # field if present
    if 'Check or Slip #' in df.columns:
        result['Check or Slip #'] = df['Check or Slip #']
    
    return result

def process_amex_format(df, source_file):
    """Process American Express transactions into standardized format."""
    # Validate required columns
    required_columns = ['Date', 'Description', 'Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Use transaction date for both dates
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    # Amex uses positive for debits, so we need to invert the sign
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    return result

def process_aggregator_format(df: pd.DataFrame) -> pd.DataFrame:
    """Process aggregator transactions into standardized format."""
    # Validate required columns
    required_columns = ['Date', 'Description', 'Amount', 'Account']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Use date for both transaction and post dates
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Clean and preserve amount
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Preserve Account (required field)
    result['Account'] = df['Account']
    
    # Preserve Category if present
    if 'Category' in df.columns:
        result['Category'] = df['Category']
    
    # Preserve Tags if present
    if 'Tags' in df.columns:
        result['Tags'] = df['Tags']
    
    return result

def process_alliant_checking_format(df, source_file):
    """Process Alliant Checking transactions into standardized format."""
    # Validate required columns
    required_columns = ['Date', 'Description', 'Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Use transaction date for both dates
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    return result

def process_alliant_visa_format(df: pd.DataFrame) -> pd.DataFrame:
    """Process Alliant Visa transactions into standardized format."""
    # Validate required columns
    required_columns = ['Date', 'Description', 'Amount', 'Post Date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Post Date'].apply(standardize_date)
    
    # Standardize description (strip newlines)
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    return result

def reconcile_transactions(aggregator_df, detail_dfs):
    """Reconcile transactions between aggregator and detail DataFrames.
    Args:
        aggregator_df (pd.DataFrame): Aggregator transactions (fixed format)
        detail_dfs (list): List of detail transaction DataFrames (intermediary format)
    Returns:
        tuple: (matched_df, unmatched_df)
    """
    matched = []
    unmatched = []
    matched_detail_keys = set()  # Use a set instead of a dictionary
    matched_agg_keys = set()     # Use a set instead of a dictionary

    # Build detail key index for fast lookup
    detail_key_index = {}
    for detail_df_idx, detail_df in enumerate(detail_dfs):
        for idx, row in detail_df.iterrows():
            # Try both Post Date and Transaction Date for detail records
            keys = []
            if pd.notna(row.get('Post Date', None)):
                keys.append(f"P:{row['Post Date']}_{abs(row['Amount']):.2f}")
            if pd.notna(row.get('Transaction Date', None)):
                keys.append(f"T:{row['Transaction Date']}_{abs(row['Amount']):.2f}")
            for key in keys:
                detail_key_index.setdefault(key, []).append((detail_df_idx, idx, row))

    # Match aggregator records to detail records
    for agg_idx, agg_row in aggregator_df.iterrows():
        # Generate keys for matching - try Post Date first if available, then Transaction Date
        agg_keys = []
        if pd.notna(agg_row.get('Post Date', None)):
            agg_keys.append(f"P:{agg_row['Post Date']}_{abs(agg_row['Amount']):.2f}")
        # Always include Transaction Date as a fallback
        agg_keys.append(f"P:{agg_row['Transaction Date']}_{abs(agg_row['Amount']):.2f}")
            
        match_found = False
        # Try each key for matching
        for agg_key in agg_keys:
            if match_found:
                break
                
            if agg_key in detail_key_index:
                for detail_df_idx, idx, detail_row in detail_key_index[agg_key]:
                    # Only match if not already matched
                    if (detail_df_idx, idx) not in matched_detail_keys:
                        # Prioritize aggregator fields, only use detail fields if aggregator field is null/empty
                        matched_record = {
                            'Transaction Date': agg_row['Transaction Date'],
                            'YearMonth': agg_row['Transaction Date'][:7],
                            'Account': agg_row.get('Account', detail_row.get('source_file', '')),
                            'Description': agg_row.get('Description') if pd.notna(agg_row.get('Description')) else detail_row.get('Description', ''),
                            'Category': agg_row.get('Category') if pd.notna(agg_row.get('Category')) else detail_row.get('Category', ''),
                            'Tags': agg_row.get('Tags', ''),
                            'Amount': agg_row.get('Amount') if pd.notna(agg_row.get('Amount')) else detail_row.get('Amount', 0),
                            'reconciled_key': agg_key,
                            'Matched': True
                        }
                        matched.append(matched_record)
                        matched_detail_keys.add((detail_df_idx, idx))
                        matched_agg_keys.add(agg_idx)
                        match_found = True
                        break
                        
        if not match_found:
            # Unmatched aggregator record - use the first key generated
            unmatched_key = agg_keys[0] if agg_keys else f"U:{agg_row['Transaction Date']}_{abs(agg_row['Amount']):.2f}"
            unmatched_record = {
                'Transaction Date': agg_row['Transaction Date'],
                'YearMonth': agg_row['Transaction Date'][:7],
                'Account': agg_row.get('Account', agg_row.get('source_file', '')),
                'Description': agg_row['Description'],
                'Category': agg_row.get('Category', ''),
                'Tags': agg_row.get('Tags', ''),
                'Amount': agg_row['Amount'],
                'reconciled_key': unmatched_key.replace('P:', 'U:').replace('T:', 'U:'),
                'Matched': False
            }
            unmatched.append(unmatched_record)

    # Add unmatched detail records
    for detail_df_idx, detail_df in enumerate(detail_dfs):
        for idx, row in detail_df.iterrows():
            if (detail_df_idx, idx) not in matched_detail_keys:
                # Prefer Post Date for unmatched key if available
                if pd.notna(row.get('Post Date', None)):
                    key = f"U:{row['Post Date']}_{abs(row['Amount']):.2f}"
                    date = row['Post Date']
                else:
                    key = f"U:{row['Transaction Date']}_{abs(row['Amount']):.2f}"
                    date = row['Transaction Date']
                unmatched_record = {
                    'Transaction Date': date,
                    'YearMonth': date[:7],
                    'Account': row.get('source_file', ''),
                    'Description': row['Description'],  # Preserve original description
                    'Category': row.get('Category', ''),
                    'Tags': row.get('Tags', ''),  # Ensure Tags field exists but is empty by default
                    'Amount': row['Amount'],  # Preserve original amount
                    'reconciled_key': key,
                    'Matched': False
                }
                unmatched.append(unmatched_record)

    # Create DataFrames with consistent columns, even if empty
    columns = ['Transaction Date', 'YearMonth', 'Account', 'Description', 'Category', 
               'Tags', 'Amount', 'reconciled_key', 'Matched']
    
    if matched:
        matched_df = pd.DataFrame(matched)
    else:
        matched_df = pd.DataFrame(columns=columns)
    
    if unmatched:
        unmatched_df = pd.DataFrame(unmatched)
    else:
        unmatched_df = pd.DataFrame(columns=columns)
        
    # Ensure Tags field exists in all DataFrames
    if 'Tags' not in matched_df.columns:
        matched_df['Tags'] = ''
    if 'Tags' not in unmatched_df.columns:
        unmatched_df['Tags'] = ''
    
    return matched_df, unmatched_df

def identify_format(df):
    """Identify the format of a DataFrame based on its columns.
    
    Args:
        df (pd.DataFrame): DataFrame to identify
        
    Returns:
        str: Format identifier ('discover', 'capital_one', 'chase', 'amex', 'alliant_checking', 'alliant_visa', 'aggregator')
        
    Raises:
        ValueError: If format cannot be identified
    """
    logger.info("Identifying file format")
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    
    # Ensure column names are strings and strip whitespace
    df.columns = df.columns.str.strip()
    
    def has_required_columns(df_cols, required_cols):
        """Check if DataFrame has all required columns."""
        return all(col in df_cols for col in required_cols)
    
    # Define format signatures
    format_signatures = {
        'discover': ['Trans. Date', 'Post Date', 'Description', 'Amount', 'Category'],
        'capital_one': ['Transaction Date', 'Posted Date', 'Card No.', 'Description', 'Category', 'Debit', 'Credit'],
        'chase': ['Details', 'Posting Date', 'Description', 'Amount', 'Type', 'Balance', 'Check or Slip #'],  # Exact match to README
        'amex': ['Date', 'Description', 'Card Member', 'Account #', 'Amount'],
        'alliant_checking': ['Date', 'Description', 'Amount', 'Balance'],
        'alliant_visa': ['Date', 'Description', 'Amount', 'Balance', 'Post Date'],
        'aggregator': ['Date', 'Account', 'Description', 'Category', 'Tags', 'Amount']
    }
    
    # Check each format
    for format_name, required_cols in format_signatures.items():
        if has_required_columns(df.columns, required_cols):
            logger.info(f"Identified format: {format_name}")
            return format_name
    
    # If we get here, the format is unknown
    raise ValueError(f"Unknown file format: {df.columns.tolist()}")

def import_csv(file_path):
    """Import a CSV file and process it based on its format.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pd.DataFrame: Processed transaction data in standardized format
        
    Raises:
        ValueError: If file cannot be read or format is unknown
    """
    try:
        logger.debug(f"Reading file: {file_path}")
        
        # Try different encodings
        encodings = ['utf-8', 'utf-8-sig', 'cp1252']
        df = None
        for encoding in encodings:
            try:
                # Special handling for Chase files with unquoted header and quoted data
                if os.path.basename(file_path).lower().startswith('chase'):
                    import csv as pycsv
                    with open(file_path, 'r', encoding=encoding, newline='') as f:
                        reader = pycsv.reader(f, delimiter=',', quotechar='"', quoting=pycsv.QUOTE_MINIMAL)
                        header_cols = next(reader)
                        rows = []
                        for idx, row in enumerate(reader):
                            # Skip empty rows
                            if not any(cell.strip() for cell in row):
                                continue
                            if len(row) == len(header_cols):
                                rows.append(row)
                            elif len(row) == len(header_cols) + 1 and row[-1].strip() == '':
                                # Accept row with trailing comma (extra empty column)
                                rows.append(row[:-1])
                            else:
                                print(f"[Chase CSV Import] Skipping malformed row {idx+2}: {row} (len={len(row)})")
                                continue
                    if not rows:
                        raise ValueError("No valid data rows found in Chase file")
                    df = pd.DataFrame(rows, columns=[col.strip() for col in header_cols])
                    print("[Chase CSV Import] First 3 rows after import:")
                    print(df.head(3).to_string())
                else:
                    df = pd.read_csv(
                        file_path,
                        header=0,  # First row is header
                        dtype=str,  # Read all columns as strings initially
                        skipinitialspace=True,  # Skip spaces after delimiter
                        encoding=encoding
                    )
                logger.debug(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise ValueError("Could not read file with any encoding")
        
        logger.debug("===================================")
        logger.debug(f"File: {file_path}")
        logger.debug(f"Shape: {df.shape}")
        logger.debug(f"Columns: {list(df.columns)}")
        logger.debug(f"Columns (repr): {[repr(col) for col in df.columns]}")
        logger.debug(f"First row: {df.iloc[0].to_dict()}")
        logger.debug("===================================")
        
        # Get source file name
        source_file = os.path.basename(file_path)
        
        # Identify format based on structure
        format_type = identify_format(df)
        logger.debug(f"Identified format: {format_type}")
        
        # Process based on identified format
        if format_type == 'chase':
            result = process_chase_format(df)
        elif format_type == 'discover':
            result = process_discover_format(df, source_file)
        elif format_type == 'capital_one':
            result = process_capital_one_format(df)
        elif format_type == 'alliant_checking':
            result = process_alliant_checking_format(df, source_file)
        elif format_type == 'alliant_visa':
            result = process_alliant_visa_format(df)
        elif format_type == 'amex':
            result = process_amex_format(df, source_file)
        elif format_type == 'aggregator':
            result = process_aggregator_format(df)
        else:
            raise ValueError(f"Unknown format: {format_type}")
            
        return result
        
    except Exception as e:
        raise ValueError(f"Error processing {file_path}: {str(e)}")

def import_folder(folder_path):
    """
    Import all transaction files from a folder.
    
    Args:
        folder_path (str or Path): Path to the folder containing transaction files
        
    Returns:
        list: List of DataFrames containing transaction data from each file
        
    Raises:
        ValueError: If folder does not exist or no valid files found
    """
    # Convert to Path object if string
    if isinstance(folder_path, str):
        folder_path = pathlib.Path(folder_path)
    
    # Check if folder exists
    if not folder_path.exists():
        raise ValueError(f"Folder not found: {folder_path}")
    
    # Check if path is a directory
    if not folder_path.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")
    
    # Get list of CSV and Excel files (case-insensitive)
    files = set()  # Use a set to avoid duplicates
    for pattern in ['*.csv', '*.CSV', '*.xlsx', '*.XLSX']:
        files.update(folder_path.glob(pattern))
    
    if not files:
        raise ValueError(f"No CSV or Excel files found in {folder_path}")
    
    logger.info(f"Importing folder: {folder_path}")
    
    # Import each file
    dfs = []
    for file_path in sorted(files):  # Sort for consistent order
        try:
            df = import_csv(file_path)
            if isinstance(df, pd.DataFrame) and not df.empty:
                dfs.append(df)
            else:
                logger.warning(f"Skipping empty file: {file_path}")
        except Exception as e:
            logger.error(f"Error importing {file_path}: {str(e)}")
            raise ValueError(f"Error importing {file_path}: {str(e)}")
    
    # Return list of DataFrames
    if not dfs:
        raise ValueError(f"No valid data found in {folder_path}")
    
    return dfs

def save_reconciliation_results(matched_df, unmatched_df, output_path):
    """Save reconciliation results to CSV file.
    
    Args:
        matched_df (pd.DataFrame): DataFrame containing matched transactions
        unmatched_df (pd.DataFrame): DataFrame containing unmatched transactions
        output_path (str or Path): Path to output file
    """
    # Create a copy to avoid modifying the originals
    matched_result = matched_df.copy() if not matched_df.empty else pd.DataFrame()
    unmatched_result = unmatched_df.copy() if not unmatched_df.empty else pd.DataFrame()
    
    # Process matched transactions
    if not matched_result.empty:
        # Add YearMonth column if not present
        if 'YearMonth' not in matched_result.columns:
            matched_result['YearMonth'] = pd.to_datetime(matched_result['Transaction Date']).dt.strftime('%Y-%m')
        
        # Add reconciled_key if not present
        if 'reconciled_key' not in matched_result.columns:
            matched_result['reconciled_key'] = matched_result.apply(
                lambda row: f"P:{row['Transaction Date']}_{abs(row['Amount']):.2f}",
                axis=1
            )
        
        # Add Matched column if not present
        if 'Matched' not in matched_result.columns:
            matched_result['Matched'] = "True"
        
        # Rename Transaction Date to Date if needed
        if 'Transaction Date' in matched_result.columns and 'Date' not in matched_result.columns:
            matched_result = matched_result.rename(columns={'Transaction Date': 'Date'})
    
    # Process unmatched transactions
    if not unmatched_result.empty:
        # Add YearMonth column if not present
        if 'YearMonth' not in unmatched_result.columns:
            unmatched_result['YearMonth'] = pd.to_datetime(unmatched_result['Transaction Date']).dt.strftime('%Y-%m')
        
        # Add reconciled_key if not present
        if 'reconciled_key' not in unmatched_result.columns:
            unmatched_result['reconciled_key'] = unmatched_result.apply(
                lambda row: f"U:{row['Transaction Date']}_{abs(row['Amount']):.2f}",
                axis=1
            )
        
        # Add Matched column if not present
        if 'Matched' not in unmatched_result.columns:
            unmatched_result['Matched'] = "False"
        
        # Rename Transaction Date to Date if needed
        if 'Transaction Date' in unmatched_result.columns and 'Date' not in unmatched_result.columns:
            unmatched_result = unmatched_result.rename(columns={'Transaction Date': 'Date'})
    
    # Combine results
    result = pd.concat([matched_result, unmatched_result], ignore_index=True)
    
    # Ensure all required columns are present
    required_columns = [
        'Date', 'YearMonth', 'Account', 'Description',
        'Category', 'Tags', 'Amount', 'reconciled_key', 'Matched'
    ]
    for col in required_columns:
        if col not in result.columns:
            result[col] = ''
    
    # Reorder columns
    result = result[required_columns]
    
    # Convert to Path object if string
    output_path = pathlib.Path(output_path)
    
    # If output_path is a directory, create all_transactions.csv in that directory
    if output_path.is_dir() or not output_path.suffix:
        output_path = output_path / "all_transactions.csv"
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to CSV with all fields quoted
    result.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)

def format_report_summary(matched_df, unmatched_df):
    """Format a summary of reconciliation results.
    
    Args:
        matched_df (pd.DataFrame): DataFrame of matched transactions
        unmatched_df (pd.DataFrame): DataFrame of unmatched transactions
        
    Returns:
        str: Formatted summary text
    """
    total_transactions = len(matched_df) + len(unmatched_df)
    total_amount = abs(matched_df['Amount'].sum()) + abs(unmatched_df['Amount'].sum())
    matched_amount = abs(matched_df['Amount'].sum())
    unmatched_amount = abs(unmatched_df['Amount'].sum())
    
    summary = [
        f"Total Transactions: {total_transactions}",
        f"Matched Transactions: {len(matched_df)}",
        f"Unmatched Transactions: {len(unmatched_df)}",
        f"Total Amount: ${total_amount:.2f}",
        f"Matched Amount: ${matched_amount:.2f}",
        f"Unmatched Amount: ${unmatched_amount:.2f}"
    ]
    
    return "\n".join(summary)

def generate_reconciliation_report(matched_df, unmatched_df, output_dir):
    """Generate a reconciliation report.
    
    Args:
        matched_df (pd.DataFrame): Matched transactions
        unmatched_df (pd.DataFrame): Unmatched transactions
        output_dir (pathlib.Path): Output directory
    """
    # Create report text
    report = format_report_summary(matched_df, unmatched_df)
    
    # Save report
    output_path = output_dir / "reconciliation_report.txt"
    with open(output_path, 'w') as f:
        f.write(report)

def main():
    """Main execution function."""
    try:
        # Set up argument parser
        parser = argparse.ArgumentParser(description='Reconcile financial transactions')
        parser.add_argument('--statements', type=str, default='data/2025/details',
                          help='Path to statement files')
        parser.add_argument('--aggregator', type=str, default='data/2025/empower_2025.csv',
                          help='Path to aggregator file')
        parser.add_argument('--output', type=str, default='output',
                          help='Output directory')
        parser.add_argument('--debug', action='store_true',
                          help='Enable debug logging')
        args = parser.parse_args()
        
        # Set up logging
        setup_logging(debug=args.debug)
        logger.info("Starting reconciliation process")
        
        # Import and process data
        statements_dfs = import_folder(args.statements)
        aggregator_df = import_csv(args.aggregator)
        
        if not statements_dfs:
            raise ValueError("No statement files found")
        
        # Combine all statement DataFrames
        statements_df = pd.concat(statements_dfs, ignore_index=True)
        
        # Reconcile transactions
        matched_df, unmatched_df = reconcile_transactions(aggregator_df, [statements_df])
        
        # Create output directory
        logger.debug("Creating output directory...")
        output_dir = pathlib.Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save results
        save_reconciliation_results(matched_df, unmatched_df, output_dir)
        
        # Generate report
        generate_reconciliation_report(matched_df, unmatched_df, output_dir)
        
    except Exception as e:
        logger.error(f"Error during reconciliation: {str(e)}")
        raise

if __name__ == '__main__':
    main()
