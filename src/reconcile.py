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
        - DD-MM-YYYY (UK)
        - YYYYMMDD (Compact)
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
                return date_str[:10]  # Take just the date part if there's a time component
                
            # US format MM/DD/YYYY
            if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_str):
                return pd.to_datetime(date_str, format='%m/%d/%Y').strftime('%Y-%m-%d')
                
            # UK format DD-MM-YYYY
            if re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', date_str):
                return pd.to_datetime(date_str, format='%d-%m-%Y').strftime('%Y-%m-%d')
                
            # Compact format YYYYMMDD
            if re.match(r'^\d{8}$', date_str):
                return pd.to_datetime(date_str, format='%Y%m%d').strftime('%Y-%m-%d')
                
            # Short year format M/D/YY
            if re.match(r'^\d{1,2}/\d{1,2}/\d{2}$', date_str):
                return pd.to_datetime(date_str, format='%m/%d/%y').strftime('%Y-%m-%d')
        
        # For any other format, let pandas try to parse it
        return pd.to_datetime(date_str).strftime('%Y-%m-%d')
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
    """
    logger.debug(f"clean_amount called with amount_str: {amount_str}")
    
    if isinstance(amount_str, pd.Series):
        return amount_str.apply(clean_amount)
    
    if pd.isna(amount_str):
        return 0.0
        
    if isinstance(amount_str, str):
        amount_str = amount_str.replace('$', '').replace(',', '')
    try:
        return float(amount_str)
    except:
        return 0.0

def process_discover_format(df):
    """
    Process Discover credit card transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Discover transactions with columns:
            - Transaction Date: Date of the transaction
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
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('discover', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_amex_format(df):
    """
    Process American Express transactions into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing Amex transactions with columns:
            - Date: Transaction date
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
            - source_file: Set to 'amex'
    """
    result = df.copy()
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)  # AMEX only provides one date
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('amex', index=result.index))
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
    
    # Combine Debit and Credit columns
    result['Amount'] = result.apply(
        lambda row: -clean_amount(row['Debit']) if row['Debit'] else clean_amount(row['Credit']),
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
            - Amount: Transaction amount (negative for debits)
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
    result['Amount'] = result['Amount'].apply(clean_amount)  # Chase already uses negative for debits
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('chase', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_aggregator_format(df):
    """
    Process aggregator format into standardized format.
    
    Args:
        df (pd.DataFrame): DataFrame containing aggregator transactions with columns:
            - Transaction Date: Date of the transaction
            - Post Date: Date the transaction posted
            - Description: Transaction description
            - Amount: Transaction amount (negative for debits)
            - Category: Transaction category (optional)
            - Tags: Transaction tags (optional)
            - Account: Account name (optional)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Set to 'aggregator'
    """
    result = df.copy()
    result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    result['Amount'] = result['Amount'].apply(clean_amount)
    result['Description'] = result['Description'].str.strip()
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('aggregator', index=result.index))
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_post_date_format(df):
    """
    Process detail format that uses Post Date column for date and single Amount column.
    
    Args:
        df (pd.DataFrame): DataFrame containing transactions with columns:
            - Post Date: Date the transaction posted
            - Description: Transaction description
            - Amount: Transaction amount (negative for debits)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Empty string
            - source_file: Set to 'unknown'
    """
    logger.debug(f"process_post_date_format called with DataFrame shape: {df.shape}")
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df['Transaction Date'] = df['Post Date'].apply(standardize_date)
    df['Post Date'] = df['Post Date'].apply(standardize_date)
    df['Amount'] = df['Amount'].apply(clean_amount)
    df['Amount'] = -df['Amount']  # Invert amount
    df['source_file'] = 'unknown'  # Add source_file column
    df['Category'] = ''  # Add empty Category column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_date_format(df):
    """
    Process detail format that uses Date column for date and single Amount column.
    
    Args:
        df (pd.DataFrame): DataFrame containing transactions with columns:
            - Date: Transaction date
            - Description: Transaction description
            - Amount: Transaction amount (negative for debits)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Empty string
            - source_file: Set to 'unknown'
    """
    logger.debug(f"process_date_format called with DataFrame shape: {df.shape}")
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df['Transaction Date'] = df['Date'].apply(standardize_date)
    df['Post Date'] = df['Date'].apply(standardize_date)
    df['Amount'] = df['Amount'].apply(clean_amount)
    df['Amount'] = -df['Amount']  # Invert amount
    df['source_file'] = 'unknown'  # Add source_file column
    df['Category'] = ''  # Add empty Category column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def process_debit_credit_format(df):
    """
    Process detail format that uses Posted Date for date and separate Debit/Credit columns.
    
    Args:
        df (pd.DataFrame): DataFrame containing transactions with columns:
            - Posted Date: Date the transaction posted
            - Description: Transaction description
            - Debit: Debit amount (if applicable)
            - Credit: Credit amount (if applicable)
    
    Returns:
        pd.DataFrame: Standardized transaction data with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits, positive for credits)
            - Category: Empty string
            - source_file: Set to 'unknown'
    """
    logger.debug(f"process_debit_credit_format called with DataFrame shape: {df.shape}")
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df['Transaction Date'] = df['Posted Date'].apply(standardize_date)
    df['Post Date'] = df['Posted Date'].apply(standardize_date)
    
    # Handle Debit/Credit columns with explicit logging
    df['Amount'] = df.apply(
        lambda x: (
            -clean_amount(x['Debit']) if pd.notna(x['Debit'])  # Debit (outgoing) becomes negative
            else clean_amount(x['Credit'])  # Credit (incoming) stays positive
        ),
        axis=1
    )
    
    # Log the conversion for debugging
    for _, row in df.iterrows():
        if pd.notna(row['Debit']):
            logger.debug(f"Converted Debit {row['Debit']} to Amount {row['Amount']}")
        else:
            logger.debug(f"Converted Credit {row['Credit']} to Amount {row['Amount']}")
    
    df['source_file'] = 'unknown'  # Add source_file column
    df['Category'] = ''  # Add empty Category column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def reconcile_transactions(aggregator_df, detail_records):
    """
    Reconcile transactions between aggregator and detail records.
    
    Args:
        aggregator_df (pd.DataFrame): DataFrame containing aggregator transactions
        detail_records (list): List of DataFrames containing detail transactions
    
    Returns:
        pd.DataFrame: Reconciled transactions with columns:
            - Transaction Date: Standardized date (YYYY-MM-DD)
            - Post Date: Standardized date (YYYY-MM-DD)
            - Description: Cleaned description
            - Amount: Numeric amount (negative for debits)
            - Category: Transaction category
            - source_file: Origin of the transaction
            - reconciled_key: Key used for matching (P: or T: prefix)
            - Matched: Boolean indicating if transaction was matched
    
    Notes:
        - Post Date matches take priority over Transaction Date matches
        - Unmatched records are marked with U: prefix
        - Each transaction is matched at most once
    """
    # Process aggregator records
    aggregator_processed = process_aggregator_format(aggregator_df)

    # Process detail records
    detail_processed = []
    for df in detail_records:
        if isinstance(df, pd.DataFrame):
            detail_processed.append(df)

    # Create reconciliation keys for aggregator records
    aggregator_processed['reconciled_key'] = (
        'P:' + aggregator_processed['Transaction Date'].astype(str) + '_' +
        aggregator_processed['Amount'].astype(str)
    )

    # Create reconciliation keys for detail records
    for df in detail_processed:
        df['reconciled_key'] = (
            'T:' + df['Transaction Date'].astype(str) + '_' +
            df['Amount'].astype(str)
        )

    # Initialize lists for matched and unmatched records
    matched_records = []
    unmatched_aggregator = []
    unmatched_detail = []

    # Track which detail records have been matched
    matched_detail_keys = set()

    # Match transactions
    for _, agg_row in aggregator_processed.iterrows():
        matched = False
        for df in detail_processed:
            # Try to match on Post Date first
            post_date_matches = df[
                (df['Post Date'] == agg_row['Post Date']) &
                (df['Amount'] == agg_row['Amount']) &
                ~df['reconciled_key'].isin(matched_detail_keys)
            ]

            if not post_date_matches.empty:
                # Use the first match
                detail_row = post_date_matches.iloc[0]
                matched_records.append({
                    'Transaction Date': agg_row['Transaction Date'],
                    'Post Date': agg_row['Post Date'],
                    'Description': agg_row['Description'],
                    'Amount': agg_row['Amount'],
                    'Category': agg_row['Category'],
                    'Tags': agg_row.get('Tags', ''),
                    'Account': agg_row.get('Account', ''),
                    'reconciled_key': agg_row['reconciled_key'],
                    'Matched': True
                })
                matched_detail_keys.add(detail_row['reconciled_key'])
                matched = True
                break

            # If no Post Date match, try Transaction Date
            trans_date_matches = df[
                (df['Transaction Date'] == agg_row['Transaction Date']) &
                (df['Amount'] == agg_row['Amount']) &
                ~df['reconciled_key'].isin(matched_detail_keys)
            ]

            if not trans_date_matches.empty:
                # Use the first match
                detail_row = trans_date_matches.iloc[0]
                matched_records.append({
                    'Transaction Date': agg_row['Transaction Date'],
                    'Post Date': agg_row['Post Date'],
                    'Description': agg_row['Description'],
                    'Amount': agg_row['Amount'],
                    'Category': agg_row['Category'],
                    'Tags': agg_row.get('Tags', ''),
                    'Account': agg_row.get('Account', ''),
                    'reconciled_key': agg_row['reconciled_key'],
                    'Matched': True
                })
                matched_detail_keys.add(detail_row['reconciled_key'])
                matched = True
                break

        if not matched:
            # Add to unmatched aggregator records
            unmatched_aggregator.append({
                'Transaction Date': agg_row['Transaction Date'],
                'Post Date': agg_row['Post Date'],
                'Description': agg_row['Description'],
                'Amount': agg_row['Amount'],
                'Category': agg_row['Category'],
                'Tags': agg_row.get('Tags', ''),
                'Account': agg_row.get('Account', ''),
                'reconciled_key': 'U:' + agg_row['Transaction Date'] + '_' + str(agg_row['Amount']),
                'Matched': False
            })

    # Add unmatched detail records
    for df in detail_processed:
        for _, detail_row in df.iterrows():
            if detail_row['reconciled_key'] not in matched_detail_keys:
                # Add to unmatched detail records
                unmatched_detail.append({
                    'Transaction Date': detail_row['Transaction Date'],
                    'Post Date': detail_row['Post Date'],
                    'Description': detail_row['Description'],
                    'Amount': detail_row['Amount'],
                    'Category': detail_row['Category'],
                    'Tags': '',
                    'Account': '',
                    'reconciled_key': 'U:' + detail_row['Transaction Date'] + '_' + str(detail_row['Amount']),
                    'Matched': False
                })

    # Combine all records
    all_records = pd.DataFrame(matched_records + unmatched_aggregator + unmatched_detail)
    
    # Add YearMonth column
    all_records['YearMonth'] = pd.to_datetime(all_records['Transaction Date']).dt.strftime('%Y-%m')
    
    return all_records

def import_csv(file_path):
    """
    Import and process a CSV file containing transaction data.
    
    Args:
        file_path (str): Path to the CSV file to import
    
    Returns:
        pd.DataFrame: Processed transaction data in standardized format
    
    Raises:
        ValueError: If the file format is not recognized
        FileNotFoundError: If the file does not exist
        pd.errors.EmptyDataError: If the file is empty
    """
    logger.info(f"Importing CSV file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            logger.warning(f"Empty file: {file_path}")
            return pd.DataFrame()
        
        # Determine file format and process accordingly
        if 'Post Date' in df.columns and 'Amount' in df.columns:
            return process_post_date_format(df)
        elif 'Date' in df.columns and 'Amount' in df.columns:
            return process_date_format(df)
        elif 'Posted Date' in df.columns and ('Debit' in df.columns or 'Credit' in df.columns):
            return process_debit_credit_format(df)
        else:
            raise ValueError(f"Unknown file format: {file_path}")
    except Exception as e:
        logger.error(f"Error importing {file_path}: {str(e)}")
        raise

def import_folder(folder_path):
    """
    Import and process all CSV files in a folder.
    
    Args:
        folder_path (str): Path to the folder containing CSV files
    
    Returns:
        list: List of processed DataFrames in standardized format
    
    Raises:
        FileNotFoundError: If the folder does not exist
        ValueError: If no CSV files are found in the folder
    """
    logger.info(f"Importing folder: {folder_path}")
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    processed_files = []
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            try:
                df = import_csv(os.path.join(folder_path, file))
                if not df.empty:
                    processed_files.append(df)
            except Exception as e:
                logger.error(f"Error processing {file}: {str(e)}")
                continue
    
    if not processed_files:
        raise ValueError(f"No valid CSV files found in {folder_path}")
    
    return processed_files

def export_reconciliation(results_df, output_path):
    """
    Export reconciliation results to a CSV file.
    
    Args:
        results_df (pd.DataFrame): DataFrame containing reconciliation results
        output_path (str): Path where the CSV file should be saved
    
    Returns:
        str: Path to the exported file
    
    Raises:
        ValueError: If the results DataFrame is empty
        PermissionError: If unable to write to the output path
    """
    logger.info(f"Exporting reconciliation results to: {output_path}")
    if results_df.empty:
        raise ValueError("Cannot export empty results DataFrame")
    
    try:
        results_df.to_csv(output_path, index=False)
        logger.info(f"Successfully exported results to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error exporting results: {str(e)}")
        raise

def generate_reconciliation_report(reconciliation_results):
    """Generate a reconciliation report from the reconciliation results.
    
    Args:
        reconciliation_results (dict): Dictionary containing:
            - matched: DataFrame of matched transactions
            - unmatched_detail: DataFrame of unmatched detail transactions
            - unmatched_aggregator: DataFrame of unmatched aggregator transactions
            
    Returns:
        dict: Report containing:
            - summary: Dictionary of summary statistics
            - matched_list: List of matched transactions
            - unmatched_detail_list: List of unmatched detail transactions
            - unmatched_aggregator_list: List of unmatched aggregator transactions
            - timestamp: Report generation timestamp
    """
    # Calculate summary statistics
    total_transactions = len(reconciliation_results['matched']) + \
                        len(reconciliation_results['unmatched_detail']) + \
                        len(reconciliation_results['unmatched_aggregator'])
    matched_transactions = len(reconciliation_results['matched'])
    unmatched_detail = len(reconciliation_results['unmatched_detail'])
    unmatched_aggregator = len(reconciliation_results['unmatched_aggregator'])
    match_rate = matched_transactions / total_transactions if total_transactions > 0 else 0.0
    
    # Create summary
    summary = {
        'total_transactions': total_transactions,
        'matched_transactions': matched_transactions,
        'unmatched_detail': unmatched_detail,
        'unmatched_aggregator': unmatched_aggregator,
        'match_rate': match_rate
    }
    
    # Convert DataFrames to lists of dictionaries
    matched_list = reconciliation_results['matched'].to_dict('records')
    unmatched_detail_list = reconciliation_results['unmatched_detail'].to_dict('records')
    unmatched_aggregator_list = reconciliation_results['unmatched_aggregator'].to_dict('records')
    
    # Create report
    report = {
        'summary': summary,
        'matched_list': matched_list,
        'unmatched_detail_list': unmatched_detail_list,
        'unmatched_aggregator_list': unmatched_aggregator_list,
        'timestamp': pd.Timestamp.now().isoformat()
    }
    
    return report

def main():
    """
    Main function to run the reconciliation process.
    
    This function:
    1. Sets up logging
    2. Imports aggregator and detail files
    3. Performs reconciliation
    4. Exports results
    
    Returns:
        int: 0 if successful, 1 if an error occurred
    """
    try:
        setup_logging()
        logger.info("Starting reconciliation process")
        
        # Import files
        aggregator_df = import_csv('data/aggregator.csv')
        detail_files = import_folder('data/details')
        
        # Perform reconciliation
        results = reconcile_transactions(aggregator_df, detail_files)
        
        # Export results
        export_reconciliation(results, 'output/reconciliation_results.csv')
        
        logger.info("Reconciliation process completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
