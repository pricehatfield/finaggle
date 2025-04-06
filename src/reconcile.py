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

logger = logging.getLogger(__name__)

def setup_logging(log_level=logging.ERROR):
    """
    Configure logging to output to both file and console with different levels.
    
    Args:
        log_level (int): Minimum logging level for console output. Defaults to ERROR.
                        File logging is always set to DEBUG.
    
    Returns:
        None
    
    Side Effects:
        - Creates a logs directory if it doesn't exist
        - Sets up console and file handlers
        - Configures logging format
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler - set to WARNING or user specified level
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(max(logging.WARNING, log_level))
    logger.addHandler(console_handler)
    
    # File handler - always set to DEBUG for diagnostics
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(workspace_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"reconciliation_{timestamp}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

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

def standardize_date(date_str):
    """
    Convert various date formats to YYYY-MM-DD (ISO8601).
    
    Args:
        date_str (str, pd.Series, or None): Date string to standardize
        
    Returns:
        str or None: Standardized date in YYYY-MM-DD format, or None if invalid
        
    Supported Formats:
        - YYYY-MM-DD (ISO)
        - MM/DD/YYYY (US)
        - YYYYMMDD (Compact)
        - MMDDYYYY (Compact US)
        - M/D/YY (Short year)
    """
    logger.debug(f"standardize_date called with date_str: {date_str}")
    
    if isinstance(date_str, pd.Series):
        return date_str.apply(standardize_date)
        
    try:
        if pd.isna(date_str):
            return None
            
        if isinstance(date_str, str):
            # Already in ISO format
            if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
                dt = pd.to_datetime(date_str[:10])  # Take just the date part if there's a time component
                if dt.year > 1900 and dt.year < 2100:  # Basic validation
                    return dt.strftime('%Y-%m-%d')
                return None
                
            # US format MM/DD/YYYY
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                dt = pd.to_datetime(date_str, format='%m/%d/%Y')
                if dt.year > 1900 and dt.year < 2100:
                    return dt.strftime('%Y-%m-%d')
                return None
                
            # Compact format YYYYMMDD or MMDDYYYY
            if re.match(r'^\d{8}$', date_str):
                try:
                    # Try YYYYMMDD first
                    try:
                        dt = pd.to_datetime(date_str, format='%Y%m%d')
                    except:
                        # Try MMDDYYYY
                        dt = pd.to_datetime(date_str, format='%m%d%Y')
                    if dt.year > 1900 and dt.year < 2100:
                        return dt.strftime('%Y-%m-%d')
                except:
                    return None
                
            # Short year format M/D/YY
            if re.match(r'^\d{1,2}/\d{1,2}/\d{2}$', date_str):
                try:
                    dt = pd.to_datetime(date_str, format='%m/%d/%y')
                    if dt.year > 1900 and dt.year < 2100:
                        return dt.strftime('%Y-%m-%d')
                except:
                    return None
        
        # For any other format, let pandas try to parse it
        try:
            dt = pd.to_datetime(date_str)
            if dt.year > 1900 and dt.year < 2100:
                return dt.strftime('%Y-%m-%d')
        except:
            pass
        
        return None
    except:
        return None

def clean_amount(amount_str):
    """
    Convert amount strings to float and handle currency symbols.
    
    Args:
        amount_str (str, pd.Series, or numeric): Amount to clean
        
    Returns:
        float: Cleaned amount as a float
        
    Notes:
        - Removes currency symbols ($) and commas
        - Returns 0.0 for invalid inputs
        - Handles pandas Series by applying the function to each element
        - Handles debit/credit columns by making debits negative
    """
    logger.debug(f"clean_amount called with amount_str: {amount_str}")
    
    if isinstance(amount_str, pd.Series):
        return amount_str.apply(clean_amount)
    
    if pd.isna(amount_str):
        return 0.0
        
    if isinstance(amount_str, str):
        # Remove currency symbols and commas
        amount_str = amount_str.replace('$', '').replace(',', '')
        # Handle parentheses for negative numbers
        if amount_str.startswith('(') and amount_str.endswith(')'):
            amount_str = '-' + amount_str[1:-1]
    try:
        return float(amount_str)
    except:
        return 0.0

def process_discover_format(df):
    """
    Process Discover credit card transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Discover transactions with columns:
            - Trans. Date or Transaction Date: Date of the transaction
            - Post Date: Date the transaction posted
            - Description: Transaction description
            - Amount: Transaction amount (with $ and commas)
            - Category: Transaction category (optional)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'discover'
    """
    result = df.copy()
    
    # Handle both possible transaction date column names
    trans_date_col = 'Trans. Date' if 'Trans. Date' in df.columns else 'Transaction Date'
    result['Transaction Date'] = result[trans_date_col].apply(standardize_date)
    
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('discover', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_capital_one_format(df):
    """
    Process Capital One transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Capital One transactions with columns:
            - Transaction Date: Date of the transaction
            - Posted Date: Date the transaction posted
            - Description: Transaction description
            - Debit: Debit amount (if applicable)
            - Credit: Credit amount (if applicable)
            - Category: Transaction category (optional)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'capital_one'
    """
    result = df.copy()
    result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
    result['Post Date'] = result['Posted Date'].apply(standardize_date)
    
    # Combine Debit and Credit columns into Amount
    result['Amount'] = result.apply(
        lambda row: -clean_amount(row['Debit']) if pd.notna(row['Debit']) else clean_amount(row['Credit']),
        axis=1
    )
    
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('capital_one', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_chase_format(df):
    """
    Process Chase transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Chase transactions with columns:
            - Details: Transaction type
            - Posting Date: Date the transaction posted
            - Description: Transaction description
            - Amount: Transaction amount (with $ and commas)
            - Type: Transaction type
            - Balance: Account balance
            - Check or Slip #: Check number
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'chase'
    """
    result = df.copy()
    result['Transaction Date'] = result['Posting Date'].apply(standardize_date)  # Chase only provides posting date
    result['Post Date'] = result['Posting Date'].apply(standardize_date)
    result['Amount'] = result['Amount'].apply(clean_amount)  # Chase amounts are already negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('chase', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_amex_format(df):
    """
    Process American Express credit card transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Amex transactions with columns:
            - Date: Date of the transaction (MM/DD/YYYY)
            - Description: Transaction description
            - Card Member: Name of card member
            - Account #: Account number
            - Amount: Transaction amount (positive for debits, negative for credits)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Empty string (Amex doesn't provide categories)
            - source_file: Set to 'amex'
    """
    result = df.copy()
    
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Transaction Date']  # Amex doesn't provide post date
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = ''  # Amex doesn't provide categories
    result['source_file'] = result.get('source_file', pd.Series('amex', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_aggregator_format(df):
    """
    Process Empower (aggregator) transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Empower transactions with columns:
            - Date: Date of the transaction (YYYY-MM-DD)
            - Account: Account name
            - Description: Transaction description
            - Category: Transaction category
            - Tags: Transaction tags
            - Amount: Transaction amount (negative for debits, positive for credits)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'empower'
    """
    result = df.copy()
    
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Transaction Date']  # Empower doesn't provide post date
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('empower', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_post_date_format(df):
    """
    Process transactions with only post dates into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing transactions with columns:
            - Post Date: Date the transaction posted
            - Description: Transaction description
            - Amount: Transaction amount (with $ and commas)
            - Category: Transaction category (optional)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'post_date'
    """
    result = df.copy()
    result['Transaction Date'] = result['Post Date'].apply(standardize_date)  # Use post date for both
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('post_date', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_date_format(df):
    """
    Process transactions with only transaction dates into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing transactions with columns:
            - Transaction Date: Date of the transaction
            - Description: Transaction description
            - Amount: Transaction amount (with $ and commas)
            - Category: Transaction category (optional)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'date'
    """
    result = df.copy()
    result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
    result['Post Date'] = result['Transaction Date'].apply(standardize_date)  # Use transaction date for both
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('date', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_debit_credit_format(df):
    """
    Process transactions with separate debit and credit columns into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing transactions with columns:
            - Date: Transaction date
            - Description: Transaction description
            - Debit: Debit amount (if applicable)
            - Credit: Credit amount (if applicable)
            - Category: Transaction category (optional)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'debit_credit'
    """
    result = df.copy()
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)  # Use transaction date for both
    
    # Combine Debit and Credit columns into Amount
    result['Amount'] = result.apply(
        lambda row: -clean_amount(row['Debit']) if pd.notna(row['Debit']) else clean_amount(row['Credit']),
        axis=1
    )
    
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('debit_credit', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_alliant_checking_format(df):
    """
    Process Alliant checking transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Alliant checking transactions with columns:
            - Date: Transaction date
            - Description: Transaction description
            - Amount: Transaction amount (with $ and commas)
            - Balance: Account balance
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'alliant_checking'
    """
    result = df.copy()
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)  # Alliant only provides one date
    result['Amount'] = result['Amount'].apply(clean_amount)  # Alliant amounts are already negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('alliant_checking', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_alliant_visa_format(df):
    """
    Process Alliant Visa transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Alliant Visa transactions with columns:
            - Date: Transaction date
            - Description: Transaction description
            - Amount: Transaction amount (with $ and commas)
            - Balance: Account balance
            - Type: Transaction type
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'alliant_visa'
    """
    result = df.copy()
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)  # Alliant only provides one date
    result['Amount'] = result['Amount'].apply(clean_amount)  # Alliant amounts are already negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('alliant_visa', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def reconcile_transactions(aggregator_df, detail_records):
    """
    Reconcile transactions between aggregator and detail records.
    
    Args:
        aggregator_df (pd.DataFrame): DataFrame containing aggregator transactions
        detail_records (list): List of DataFrames containing detail transactions
    
    Returns:
        tuple: (matches_df, unmatched_df) containing matched and unmatched transactions
    """
    # Process all detail records into standardized format
    processed_details = []
    for df in detail_records:
        if 'Transaction Date' in df.columns and 'Post Date' in df.columns:
            processed_details.append(process_date_format(df))
        elif 'Post Date' in df.columns:
            processed_details.append(process_post_date_format(df))
        elif 'Debit' in df.columns and 'Credit' in df.columns:
            processed_details.append(process_debit_credit_format(df))
        else:
            processed_details.append(process_aggregator_format(df))
    
    # Combine all detail records
    detail_df = pd.concat(processed_details, ignore_index=True)
    
    # Process aggregator data
    aggregator_df = process_aggregator_format(aggregator_df)
    
    # Find matches
    matches = []
    unmatched_aggregator = []
    unmatched_detail = []
    
    # Match by post date
    for _, agg_row in aggregator_df.iterrows():
        detail_matches = detail_df[
            (detail_df['Post Date'] == agg_row['Post Date']) &
            (detail_df['Amount'] == agg_row['Amount'])
        ]
        
        if len(detail_matches) > 0:
            for _, detail_row in detail_matches.iterrows():
                matches.append({
                    'Transaction Date': detail_row['Transaction Date'],
                    'Post Date': agg_row['Post Date'],
                    'Description': detail_row['Description'],
                    'Amount': agg_row['Amount'],
                    'Category': detail_row['Category'],
                    'source_file': detail_row['source_file'],
                    'match_type': 'P'  # Post date match
                })
        else:
            unmatched_aggregator.append(agg_row)
    
    # Match by transaction date
    for _, detail_row in detail_df.iterrows():
        if detail_row['Transaction Date'] is not None:
            agg_matches = aggregator_df[
                (aggregator_df['Transaction Date'] == detail_row['Transaction Date']) &
                (aggregator_df['Amount'] == detail_row['Amount'])
            ]
            
            if len(agg_matches) == 0:
                unmatched_detail.append(detail_row)
    
    # Convert to DataFrames
    matches_df = pd.DataFrame(matches)
    unmatched_aggregator_df = pd.DataFrame(unmatched_aggregator)
    unmatched_detail_df = pd.DataFrame(unmatched_detail)
    
    # Combine unmatched records
    unmatched_df = pd.concat([unmatched_aggregator_df, unmatched_detail_df], ignore_index=True)
    
    return matches_df, unmatched_df

def import_csv(file_path):
    """
    Import and process a CSV file based on its format.
    
    Args:
        file_path (str or pathlib.Path): Path to the CSV file
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If the file format cannot be determined
    """
    logger.debug(f"Importing CSV file: {file_path}")
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Determine the format based on column names
    columns = set(df.columns)
    
    # Discover format
    if {'Trans. Date', 'Post Date', 'Description', 'Amount'}.issubset(columns) or \
       {'Transaction Date', 'Post Date', 'Description', 'Amount'}.issubset(columns):
        return process_discover_format(df)
    
    # Capital One format
    if {'Transaction Date', 'Posted Date', 'Description', 'Debit', 'Credit'}.issubset(columns):
        return process_capital_one_format(df)
    
    # Chase format
    if {'Posting Date', 'Description', 'Amount', 'Type', 'Balance', 'Check or Slip #'}.issubset(columns):
        return process_chase_format(df)
    
    # Alliant Checking format
    if {'Date', 'Description', 'Amount', 'Balance'}.issubset(columns):
        return process_alliant_checking_format(df)
    
    # Alliant Visa format
    if {'Date', 'Description', 'Amount', 'Balance', 'Type'}.issubset(columns):
        return process_alliant_visa_format(df)
    
    # Amex format
    if {'Date', 'Description', 'Card Member', 'Account #', 'Amount'}.issubset(columns):
        return process_amex_format(df)
    
    # Aggregator format
    if {'Date', 'Description', 'Amount', 'Category'}.issubset(columns):
        return process_aggregator_format(df)
    
    # Post date format
    if {'Post Date', 'Description', 'Amount'}.issubset(columns):
        return process_post_date_format(df)
    
    # Transaction date format
    if {'Transaction Date', 'Description', 'Amount'}.issubset(columns):
        return process_date_format(df)
    
    raise ValueError(f"Could not determine format for file: {file_path}\nColumns: {columns}")

def import_folder(folder_path):
    """
    Import all CSV files from a folder and combine them into a single DataFrame.
    
    Args:
        folder_path (str): Path to the folder containing CSV files
    
    Returns:
        pd.DataFrame: Combined DataFrame containing all imported data
    """
    logger.info(f"Importing folder: {folder_path}")
    
    try:
        files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    except Exception as e:
        logger.error(f"Error listing files in folder {folder_path}: {e}")
        raise
    
    dfs = []
    for file in files:
        try:
            df = import_csv(os.path.join(folder_path, file))
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error importing file {file}: {e}")
            continue
    
    if not dfs:
        logger.warning(f"No valid CSV files found in {folder_path}")
        return pd.DataFrame()
    
    return pd.concat(dfs, ignore_index=True)

def export_reconciliation(results_df, output_path):
    """
    Export reconciliation results to a CSV file.
    
    Args:
        results_df (pd.DataFrame): DataFrame containing reconciliation results
        output_path (str): Path to save the output file
    
    Returns:
        None
    """
    logger.info(f"Exporting reconciliation results to {output_path}")
    
    try:
        results_df.to_csv(output_path, index=False)
    except Exception as e:
        logger.error(f"Error exporting results to {output_path}: {e}")
        raise

def generate_reconciliation_report(reconciliation_results):
    """
    Generate a human-readable reconciliation report.
    
    Args:
        reconciliation_results (dict): Dictionary containing reconciliation results
    
    Returns:
        str: Formatted report text
    """
    logger.info("Generating reconciliation report")
    
    report = []
    report.append("Reconciliation Report")
    report.append("=" * 50)
    report.append("")
    
    # Matched Transactions
    report.append("Matched Transactions")
    report.append("-" * 50)
    if len(reconciliation_results['matched']) > 0:
        for _, row in reconciliation_results['matched'].iterrows():
            report.append(f"Date: {row['Transaction Date']}")
            report.append(f"Description: {row['Description']}")
            report.append(f"Amount: {row['Amount']}")
            report.append(f"Source: {row['source_file']}")
            report.append("")
    else:
        report.append("No matched transactions")
        report.append("")
    
    # Unmatched Transactions
    report.append("Unmatched Transactions")
    report.append("-" * 50)
    if len(reconciliation_results['unmatched']) > 0:
        for _, row in reconciliation_results['unmatched'].iterrows():
            report.append(f"Date: {row['Transaction Date']}")
            report.append(f"Description: {row['Description']}")
            report.append(f"Amount: {row['Amount']}")
            report.append(f"Source: {row['source_file']}")
            report.append("")
    else:
        report.append("No unmatched transactions")
        report.append("")
    
    # Summary
    report.append("Summary")
    report.append("-" * 50)
    report.append(f"Total Matched Transactions: {len(reconciliation_results['matched'])}")
    report.append(f"Total Unmatched Transactions: {len(reconciliation_results['unmatched'])}")
    report.append(f"Total Amount Matched: {reconciliation_results['matched']['Amount'].sum():.2f}")
    report.append(f"Total Amount Unmatched: {reconciliation_results['unmatched']['Amount'].sum():.2f}")
    
    return "\n".join(report)

def main():
    """
    Main function to run the reconciliation process.
    """
    # Set up logging
    setup_logging()
    
    # Import data
    aggregator_df = import_csv("data/2025/transactions_2025.xlsx")
    detail_records = []
    for file in os.listdir("data/2025/details"):
        if file.endswith('.csv'):
            detail_records.append(import_csv(os.path.join("data/2025/details", file)))
    
    # Reconcile transactions
    matches, unmatched = reconcile_transactions(aggregator_df, detail_records)
    
    # Export results
    export_reconciliation(matches, "output/matched.csv")
    export_reconciliation(unmatched, "output/unmatched.csv")
    
    # Generate report
    report = generate_reconciliation_report({
        'matched': matches,
        'unmatched': unmatched
    })
    
    with open("output/report.txt", "w") as f:
        f.write(report)

if __name__ == "__main__":
    main()
