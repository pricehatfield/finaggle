"""
Transaction Reconciliation System

This system standardizes transaction data from various financial institutions into a common format
for reconciliation purposes. The standardized format contains only the essential fields needed for
matching transactions across different sources.

Standardized Format:
- Transaction Date: Date of the transaction (YYYY-MM-DD)
- Post Date: Date the transaction posted (YYYY-MM-DD)
- Description: Transaction description (cleaned)
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
    
    # Try different date formats
    formats = [
        '%Y-%m-%d',  # ISO
        '%Y-%m-%d %H:%M:%S',  # ISO with time
        '%m/%d/%Y',  # US
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

def clean_amount(amount_str):
    """
    Convert amount strings to float and handle currency symbols.
    
    Args:
        amount_str (str, pd.Series, or numeric): Amount to clean
        
    Returns:
        float: Cleaned amount as a float
        
    Raises:
        ValueError: If amount format is invalid and cannot be parsed
    """
    if isinstance(amount_str, pd.Series):
        return amount_str.apply(clean_amount)
        
    if pd.isna(amount_str):
        raise ValueError("Amount cannot be null")
        
    if isinstance(amount_str, (int, float)):
        return float(amount_str)
        
    if not isinstance(amount_str, str):
        raise ValueError("Invalid amount format")
        
    # Remove currency symbols and commas
    amount_str = amount_str.replace('$', '').replace(',', '').strip()
    
    # Handle parentheses for negative numbers
    if amount_str.startswith('(') and amount_str.endswith(')'):
        amount_str = '-' + amount_str[1:-1]
    
    # Handle explicit negative signs
    if amount_str.startswith('-'):
        amount_str = amount_str[1:]
        is_negative = True
    else:
        is_negative = False
        
    try:
        amount = float(amount_str)
        return -amount if is_negative else amount
    except ValueError:
        raise ValueError("Invalid amount format")

def standardize_description(description):
    """
    Standardize transaction descriptions by removing noise while preserving case and special characters.
    
    Args:
        description (str): Raw transaction description
        
    Returns:
        str: Standardized description
        
    Raises:
        ValueError: If description is empty or invalid
    """
    if pd.isna(description):
        raise ValueError("Description cannot be empty")
        
    if not isinstance(description, str):
        raise ValueError("Description cannot be empty")
        
    # Remove extra spaces
    description = ' '.join(description.split())
    
    if not description:
        raise ValueError("Description cannot be empty")
        
    # Remove "Ending in XXXX" suffix if present
    description = re.sub(r'\s*-\s*Ending in \d{4}$', '', description)
    
    return description

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

def process_discover_format(df):
    """Process Discover transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
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
    
    # Standardize description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    # Discover uses positive for debits, so we need to invert the sign
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    # Standardize category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file
    result['source_file'] = 'discover_2025.csv'
    
    return result

def process_capital_one_format(df):
    """Process Capital One transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
    """
    # Validate required columns
    required_columns = ['Transaction Date', 'Posted Date', 'Card No.', 'Description', 'Category', 'Debit', 'Credit']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Transaction Date'].apply(standardize_date)
    result['Post Date'] = df['Posted Date'].apply(standardize_date)
    
    # Validate date order
    if (result['Post Date'] < result['Transaction Date']).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Standardize description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = df.apply(
        lambda row: -clean_amount(row['Debit']) if pd.notna(row['Debit']) and row['Debit'] != '' else clean_amount(row['Credit']),
        axis=1
    )
    
    # Standardize category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file
    result['source_file'] = 'capital_one_2025.csv'
    
    return result

def process_chase_format(df):
    """Process Chase transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
    """
    # Validate required columns
    required_columns = ['Details', 'Posting Date', 'Description', 'Amount', 'Type', 'Balance', 'Check or Slip #']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Posting Date'].apply(standardize_date)  # Chase only provides posting date
    result['Post Date'] = df['Posting Date'].apply(standardize_date)
    
    # Standardize description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = df['Amount'].apply(clean_amount)  # Chase already uses negative for debits
    
    # Standardize category (Chase doesn't provide categories)
    result['Category'] = 'Uncategorized'
    
    # Add source file
    result['source_file'] = 'chase_2025.csv'
    
    return result

def process_amex_format(df, source_file=None):
    """Process American Express transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        source_file (str, optional): Source file name. Defaults to None.
        
    Returns:
        pd.DataFrame: Standardized transaction data
    """
    # Validate required columns
    required_columns = ['Date', 'Description', 'Card Member', 'Account #', 'Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = result['Transaction Date']  # AMEX only provides transaction date
    
    # Standardize description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (invert sign - positive for charges becomes negative)
    result['Amount'] = -df['Amount'].apply(clean_amount)
    
    # Standardize category
    result['Category'] = df['Category'].apply(standardize_category) if 'Category' in df.columns else 'Uncategorized'
    
    # Add source file
    result['source_file'] = source_file if source_file else 'amex_2025.csv'
    
    return result

def process_aggregator_format(df):
    """Process aggregator transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
    """
    # Validate required columns
    required_columns = ['Date', 'Description', 'Amount', 'Category']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create standardized DataFrame
    result = pd.DataFrame()
    
    # Standardize dates
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = result['Transaction Date']  # Use same date for both
    
    # Clean descriptions and preserve account information
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Handle account information
    if 'Account' in df.columns:
        result['Account'] = df['Account']
    else:
        # If no Account column, try to extract from Description
        result['Account'] = df['Description'].where(
            df['Description'].str.contains('Ending in'),
            df['Description']  # Fallback to full description if no account number
        )
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Standardize category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Preserve metadata if present
    result['Tags'] = df['Tags'] if 'Tags' in df.columns else ''
    
    # Add source file
    result['source_file'] = 'aggregator.csv'
    
    return result

def process_alliant_checking_format(df, source_file=None):
    """Process Alliant checking format.

    Args:
        df (pd.DataFrame): DataFrame with Alliant checking format
        source_file (str, optional): Source file name. Defaults to None.

    Returns:
        pd.DataFrame: Standardized DataFrame with required columns
    """
    # Validate required columns
    required_cols = ['Date', 'Description', 'Amount']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Expected: {required_cols}")

    # Validate description is not empty
    if df['Description'].isna().any() or (df['Description'] == '').any():
        raise ValueError("Description cannot be empty")

    # Create standardized DataFrame
    result = pd.DataFrame()

    # Map date columns and standardize to YYYY-MM-DD
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)  # Alliant doesn't have separate post date

    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")

    # Map description
    result['Description'] = df['Description']

    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)

    # Map category if available, otherwise use Uncategorized
    result['Category'] = df['Category'] if 'Category' in df.columns else 'Uncategorized'

    # Add source file information
    result['source_file'] = source_file if source_file else 'alliant_checking'

    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_alliant_visa_format(df, source_file=None):
    """Process Alliant Visa transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        source_file (str, optional): Source file name. Defaults to None.
        
    Returns:
        pd.DataFrame: Standardized transaction data
    """
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
    
    # Validate date order
    if (result['Post Date'] < result['Transaction Date']).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Standardize description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Standardize amount (negative for debits, positive for credits)
    result['Amount'] = -df['Amount'].apply(clean_amount)  # Invert sign since Alliant shows debits as positive
    
    # Standardize category
    result['Category'] = df['Category'].apply(standardize_category) if 'Category' in df.columns else 'Uncategorized'
    
    # Add source file
    result['source_file'] = source_file if source_file else 'alliant_visa_2025.csv'
    
    return result

def reconcile_transactions(source_df, target_dfs):
    """Reconcile transactions between source and target DataFrames.
    
    Args:
        source_df (pd.DataFrame): Source transactions
        target_dfs (list): List of target transaction DataFrames
        
    Returns:
        tuple: (matched_df, unmatched_df)
    """
    # Initialize results
    matched = []
    unmatched = []
    
    # Track matched target transactions
    matched_target_keys = {}  # key -> count of matches
    matched_source_keys = {}  # key -> count of matches
    
    # Process each source transaction
    for _, source_row in source_df.iterrows():
        # Create reconciled key using transaction date
        source_key = f"P:{source_row['Transaction Date']}_{abs(source_row['Amount']):.2f}"
        
        # Check for matches in each target DataFrame
        matched_in_target = False
        for target_df in target_dfs:
            for _, target_row in target_df.iterrows():
                # Create target key using transaction date
                target_key = f"P:{target_row['Transaction Date']}_{abs(target_row['Amount']):.2f}"
                
                if source_key == target_key:
                    # Count matches for both source and target
                    source_matches = matched_source_keys.get(source_key, 0)
                    target_matches = matched_target_keys.get(target_key, 0)
                    
                    # Only match if we haven't exceeded the count on either side
                    source_count = len(source_df[
                        (source_df['Transaction Date'] == source_row['Transaction Date']) &
                        (source_df['Amount'] == source_row['Amount'])
                    ])
                    target_count = len(target_df[
                        (target_df['Transaction Date'] == target_row['Transaction Date']) &
                        (target_df['Amount'] == target_row['Amount'])
                    ])
                    
                    if source_matches < source_count and target_matches < target_count:
                        # Add to matched results
                        matched.append({
                            'Transaction Date': source_row['Transaction Date'],
                            'Post Date': source_row['Post Date'],
                            'Description': source_row['Description'],
                            'Amount': source_row['Amount'],
                            'Category': source_row['Category'],
                            'Tags': source_row.get('Tags', ''),
                            'reconciled_key': source_key,
                            'Matched': True,
                            'source_file': source_row.get('source_file', '')
                        })
                        matched_in_target = True
                        matched_target_keys[target_key] = target_matches + 1
                        matched_source_keys[source_key] = source_matches + 1
                        break
            
            if matched_in_target:
                break
        
        if not matched_in_target:
            # Add to unmatched results
            unmatched.append({
                'Transaction Date': source_row['Transaction Date'],
                'Post Date': source_row['Post Date'],
                'Description': source_row['Description'],
                'Amount': source_row['Amount'],
                'Category': source_row['Category'],
                'Tags': source_row.get('Tags', ''),
                'reconciled_key': f"U:{source_row['Transaction Date']}_{abs(source_row['Amount']):.2f}",
                'Matched': False,
                'source_file': source_row.get('source_file', '')
            })
    
    # Add unmatched target transactions
    for target_df in target_dfs:
        for _, target_row in target_df.iterrows():
            target_key = f"P:{target_row['Transaction Date']}_{abs(target_row['Amount']):.2f}"
            target_matches = matched_target_keys.get(target_key, 0)
            target_count = len(target_df[
                (target_df['Transaction Date'] == target_row['Transaction Date']) &
                (target_df['Amount'] == target_row['Amount'])
            ])
            
            if target_matches < target_count:
                unmatched.append({
                    'Transaction Date': target_row['Transaction Date'],
                    'Post Date': target_row['Post Date'],
                    'Description': target_row['Description'],
                    'Amount': target_row['Amount'],
                    'Category': target_row['Category'],
                    'Tags': target_row.get('Tags', ''),
                    'reconciled_key': f"U:{target_row['Transaction Date']}_{abs(target_row['Amount']):.2f}",
                    'Matched': False,
                    'source_file': target_row.get('source_file', '')
                })
                matched_target_keys[target_key] = target_matches + 1
    
    # Convert results to DataFrames
    matched_df = pd.DataFrame(matched)
    unmatched_df = pd.DataFrame(unmatched)
    
    return matched_df, unmatched_df

def import_csv(file_path):
    """
    Import transactions from a file.
    
    Decision Hierarchy:
    1. Check if file exists
    2. Check if it's a directory
    3. Check if it's a supported file type (case-insensitive)
       - CSV files: .csv or .CSV
       - Excel files: .xlsx or .XLSX
    4. Check filename pattern for format processor
    5. For unknown formats, check required columns
    
    Args:
        file_path (str or Path): Path to the file to import
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If file format is not supported or data is malformed
    """
    # Convert to Path object if string
    if isinstance(file_path, str):
        file_path = pathlib.Path(file_path)
    
    # 1. Check if file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # 2. Check if path is a directory
    if file_path.is_dir():
        raise ValueError(f"Path is a directory: {file_path}")
    
    # 3. Check if it's a supported file type (case-insensitive)
    ext = file_path.suffix.lower()
    if ext == '.csv':
        try:
            # Try different encodings
            encodings = ['utf-8-sig', 'utf-8', 'cp1252']
            df = None
            for encoding in encodings:
                try:
                    # First try with pandas directly
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.debug(f"Successfully read CSV with pandas using encoding {encoding}")
                    break
                except UnicodeDecodeError:
                    logger.debug(f"Failed to read with pandas encoding {encoding}, trying next...")
                    continue
                except Exception as e:
                    logger.debug(f"Error reading with pandas encoding {encoding}: {str(e)}")
                    continue
            
            if df is None:
                raise ValueError("Could not read CSV file with any supported encoding")
                
            if df.empty:
                raise ValueError("No data could be read from the CSV file")
                
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"Columns in CSV: {df.columns}")
            logger.debug(f"First few rows:\n{df.head()}")
        except Exception as e:
            raise ValueError(f"Error reading CSV file {file_path}: {str(e)}")
    elif ext == '.xlsx':
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            raise ValueError(f"Error reading Excel file {file_path}: {str(e)}")
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}. Supported formats: .csv, .CSV, .xlsx, .XLSX")
    
    # 4. Check filename pattern for format processor
    filename = file_path.stem  # Get filename without extension
    filename_lower = filename.lower()
    
    try:
        if 'discover' in filename_lower:
            result = process_discover_format(df)
            result['source_file'] = filename
            return result
        elif 'capital_one' in filename_lower:
            result = process_capital_one_format(df)
            result['source_file'] = filename
            return result
        elif 'chase' in filename_lower:
            result = process_chase_format(df)
            result['source_file'] = filename
            return result
        elif 'alliant_checking' in filename_lower:
            result = process_alliant_checking_format(df)
            result['source_file'] = filename
            return result
        elif 'alliant_visa' in filename_lower:
            result = process_alliant_visa_format(df)
            result['source_file'] = filename
            return result
        elif 'amex' in filename_lower:
            result = process_amex_format(df)
            result['source_file'] = filename
            return result
        elif 'empower' in filename_lower or 'aggregator' in filename_lower:
            result = process_aggregator_format(df)
            result['source_file'] = filename
            return result
        
        # 5. For unknown formats, check required columns
        required_columns = ['Transaction Date', 'Description', 'Amount']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing required columns in {file_path}. Required: {required_columns}")
        
        df['source_file'] = filename
        return df
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

def generate_reconciliation_report(matches_df, unmatched_df, report_path):
    """
    Generate a reconciliation report and save it to a text file.
    
    Args:
        matches_df (pd.DataFrame): DataFrame containing matched transactions
        unmatched_df (pd.DataFrame): DataFrame containing unmatched transactions
        report_path (str or Path): Path where the report should be saved
    """
    # Create report content
    report_content = []
    
    # Add summary section
    report_content.append("=== Reconciliation Report ===\n")
    report_content.append(format_report_summary(matches_df, unmatched_df))
    
    # Add matched transactions section
    report_content.append("\n=== Matched Transactions ===\n")
    if not matches_df.empty:
        for _, row in matches_df.iterrows():
            report_content.append(
                f"Date: {row['Date']} | "
                f"Description: {row['Description']} | "
                f"Amount: ${abs(row['Amount']):.2f} | "
                f"Category: {row['Category']}\n"
            )
    else:
        report_content.append("No matched transactions found.\n")
    
    # Add unmatched transactions section
    report_content.append("\n=== Unmatched Transactions ===\n")
    if not unmatched_df.empty:
        for _, row in unmatched_df.iterrows():
            report_content.append(
                f"Date: {row['Date']} | "
                f"Description: {row['Description']} | "
                f"Amount: ${abs(row['Amount']):.2f} | "
                f"Category: {row['Category']} | "
                f"Source: {row['Account']}\n"
            )
    else:
        report_content.append("No unmatched transactions found.\n")
    
    # Write report to file
    with open(report_path, 'w') as f:
        f.writelines(report_content)

def save_reconciliation_results(matched_df, unmatched_df, output_path):
    """Save reconciliation results to CSV or Excel file.
    
    Args:
        matched_df (pd.DataFrame): DataFrame containing matched transactions
        unmatched_df (pd.DataFrame): DataFrame containing unmatched transactions
        output_path (str or Path): Path to output file (CSV or Excel)
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
            matched_result['Matched'] = True
        
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
            unmatched_result['Matched'] = False
        
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
    
    # Save to file based on extension
    if output_path.suffix.lower() == '.xlsx':
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            result.to_excel(writer, sheet_name='All Transactions', index=False)
    else:
        result.to_csv(output_path, index=False)

def format_report_summary(matches_df, unmatched_df):
    """
    Format a summary of the reconciliation results.
    
    Args:
        matches_df (pd.DataFrame): DataFrame containing matched transactions
        unmatched_df (pd.DataFrame): DataFrame containing unmatched transactions
        
    Returns:
        str: Formatted summary text
        
    Raises:
        ValueError: If required columns are missing
    """
    # Validate required columns
    required_columns = ['Date', 'Description', 'Amount']
    for df in [matches_df, unmatched_df]:
        if not df.empty and not all(col in df.columns for col in required_columns):
            raise ValueError("Missing required columns in DataFrame")
    
    summary = []
    
    # Count statistics
    total_matches = len(matches_df)
    total_unmatched = len(unmatched_df)
    total_transactions = total_matches + total_unmatched
    
    # Calculate match rate
    match_rate = (total_matches / total_transactions * 100) if total_transactions > 0 else 0
    
    # Calculate total amounts
    matched_amount = matches_df['Amount'].sum() if not matches_df.empty else 0
    unmatched_amount = unmatched_df['Amount'].sum() if not unmatched_df.empty else 0
    total_amount = matched_amount + unmatched_amount
    
    # Format summary
    summary.append(f"Total Transactions: {total_transactions}\n")
    summary.append(f"Matched Transactions: {total_matches} ({match_rate:.1f}%)\n")
    summary.append(f"Unmatched Transactions: {total_unmatched}\n")
    summary.append(f"\nTotal Amount: ${abs(total_amount):.2f}\n")
    summary.append(f"Matched Amount: ${abs(matched_amount):.2f}\n")
    summary.append(f"Unmatched Amount: ${abs(unmatched_amount):.2f}\n")
    
    return "".join(summary)

def import_data(statements_path: str, aggregator_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Import and process data from statements and aggregator files.
    
    Args:
        statements_path: Path to statements directory or file
        aggregator_path: Path to aggregator file
        
    Returns:
        Tuple of (statements_df, aggregator_df)
        
    Raises:
        ValueError: If paths are invalid or data cannot be loaded
    """
    try:
        # Import statements
        if os.path.isdir(statements_path):
            statements_dfs = import_folder(statements_path)
            if not statements_dfs:
                raise ValueError("No data loaded from statements")
            statements_df = pd.concat(statements_dfs, ignore_index=True)
        else:
            statements_df = import_csv(statements_path)
            
        if statements_df.empty:
            raise ValueError("No data loaded from statements")
            
        # Import aggregator
        aggregator_df = import_csv(aggregator_path)
        if aggregator_df.empty:
            raise ValueError("No data loaded from aggregator")
            
        return statements_df, aggregator_df
        
    except Exception as e:
        raise ValueError(f"Error importing data: {str(e)}")

def main():
    """
    Main entry point for the reconciliation process.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Reconcile transaction files')
    parser.add_argument('--statements', default='data/2025/details',
                      help='Directory containing statement files')
    parser.add_argument('--aggregator', default='data/2025/aggregator.csv',
                      help='Path to aggregator file')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    parser.add_argument('--log-level', choices=['debug', 'info', 'warning', 'error'],
                      default='info', help='Set logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=log_level,
                      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    try:
        logger.info("Starting reconciliation process")
        logger.debug(f"Using statements directory: {args.statements}")
        logger.debug(f"Using aggregator file: {args.aggregator}")
        
        # Import data
        statements_df, aggregator_df = import_data(args.statements, args.aggregator)
        
        if statements_df.empty or aggregator_df.empty:
            logger.error("No data to process")
            return
            
        # Process and match transactions
        matched, unmatched = reconcile_transactions(aggregator_df, [statements_df])
        
        # Save results
        save_reconciliation_results(matched, unmatched, "output/reconciliation_results.csv")
        
        # Generate report
        generate_reconciliation_report(matched, unmatched, "output/reconciliation_report.txt")
        
        logger.info("Reconciliation complete")
        
    except Exception as e:
        logger.error(f"Error during reconciliation: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main()
