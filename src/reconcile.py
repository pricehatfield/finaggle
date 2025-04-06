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
from src.utils import ensure_directory, create_output_directories

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

def standardize_date(date_str):
    """
    Convert various date formats to YYYY-MM-DD (ISO8601).
    
    Args:
        date_str (str, pd.Series, or None): Date string to standardize
        
    Returns:
        str or None: Standardized date in YYYY-MM-DD format, or None if invalid
    """
    if isinstance(date_str, pd.Series):
        return date_str.apply(standardize_date)
        
    if pd.isna(date_str):
        return None
        
    if not isinstance(date_str, str):
        return None
        
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
            dt = datetime.strptime(date_str, fmt)
            if dt.year < 1900 or dt.year > 2100:
                return None
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
            
    return None

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
        
    try:
        return float(amount_str)
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
        
    return category.strip()

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
    """
    Process Discover transactions into standardized format.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Trans. Date'].apply(standardize_date)
    result['Post Date'] = df['Post Date'].apply(standardize_date)
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
        
    # Validate post date is not before transaction date
    if (pd.to_datetime(result['Post Date']) < pd.to_datetime(result['Transaction Date'])).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file information
    result['source_file'] = 'discover'
    
    return result

def process_capital_one_format(df):
    """Process Capital One format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Transaction Date'].apply(standardize_date)
    result['Post Date'] = df['Posted Date'].apply(standardize_date)
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
        
    # Validate post date is not before transaction date
    if (pd.to_datetime(result['Post Date']) < pd.to_datetime(result['Transaction Date'])).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float and ensure debits are negative
    debit = df['Debit'].apply(lambda x: -clean_amount(x) if pd.notna(x) and x != '' else 0)
    credit = df['Credit'].apply(lambda x: clean_amount(x) if pd.notna(x) and x != '' else 0)
    result['Amount'] = debit + credit
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file information
    result['source_file'] = 'capital_one'
    
    return result

def process_chase_format(df):
    """Process Chase format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Posting Date'].apply(standardize_date)  # Chase only provides posting date
    result['Post Date'] = df['Posting Date'].apply(standardize_date)
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Map category (Chase doesn't provide categories)
    result['Category'] = 'Uncategorized'
    
    # Add source file information
    result['source_file'] = 'chase'
    
    return result

def process_amex_format(df):
    """Process American Express format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)  # AMEX only provides transaction date
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file information
    result['source_file'] = 'amex'
    
    return result

def process_aggregator_format(df):
    """Process Aggregator (Empower) format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)  # Aggregator only provides transaction date
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float (Aggregator amounts are already correctly signed)
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file information
    result['source_file'] = 'aggregator'
    
    return result

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
    """Process Alliant Checking format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Date'].apply(standardize_date)  # Alliant only provides transaction date
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file information
    result['source_file'] = 'alliant_checking'
    
    return result

def process_alliant_visa_format(df):
    """Process Alliant Visa format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Post Date'].apply(standardize_date)
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
        
    # Validate post date is not before transaction date
    if (pd.to_datetime(result['Post Date']) < pd.to_datetime(result['Transaction Date'])).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Add source file information
    result['source_file'] = 'alliant_visa'
    
    return result

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
    Import a CSV file and detect its format.
    
    Args:
        file_path (str or Path): Path to the CSV file
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        FileNotFoundError: If the file does not exist
        pd.errors.EmptyDataError: If the file is empty
        ValueError: If file format cannot be detected or processed
    """
    logger.debug(f"Importing CSV file: {file_path}")

    # Read CSV file
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        raise  # Let FileNotFoundError pass through
    except pd.errors.EmptyDataError:
        raise  # Let EmptyDataError pass through
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {str(e)}")

    logger.debug(f"Columns in CSV: {df.columns}")

    # Normalize column names
    df.columns = df.columns.str.strip()
    
    # Handle both 'Trans. Date' and 'Transaction Date'
    if 'Transaction Date' in df.columns and 'Trans. Date' not in df.columns:
        df['Trans. Date'] = df['Transaction Date']

    # Detect format based on columns
    if all(col in df.columns for col in ['Trans. Date', 'Post Date', 'Description', 'Amount']):
        logger.debug("Detected Discover format")
        return process_discover_format(df)
    elif all(col in df.columns for col in ['Date', 'Description', 'Card Member', 'Account #', 'Amount']):
        logger.debug("Detected Amex format")
        return process_amex_format(df)
    elif all(col in df.columns for col in ['Transaction Date', 'Posted Date', 'Card No.', 'Description', 'Category', 'Debit', 'Credit']):
        logger.debug("Detected Capital One format")
        return process_capital_one_format(df)
    elif all(col in df.columns for col in ['Details', 'Posting Date', 'Description', 'Amount', 'Type', 'Balance', 'Check or Slip #']):
        logger.debug("Detected Chase format")
        return process_chase_format(df)
    elif all(col in df.columns for col in ['Date', 'Description', 'Amount', 'Category', 'Post Date']):
        logger.debug("Detected Alliant Visa format")
        return process_alliant_visa_format(df)
    elif all(col in df.columns for col in ['Date', 'Description', 'Amount', 'Category']):
        logger.debug("Detected Alliant Checking format")
        return process_alliant_checking_format(df)
    elif all(col in df.columns for col in ['Date', 'Account', 'Description', 'Category', 'Tags', 'Amount']):
        logger.debug("Detected Aggregator format")
        return process_aggregator_format(df)
    else:
        raise ValueError("Could not detect file format based on columns")

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
