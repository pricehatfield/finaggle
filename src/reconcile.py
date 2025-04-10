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

def process_amex_format(df, source_file=None):
    """Process American Express format.
    
    Args:
        df (pd.DataFrame): DataFrame with Amex format
        source_file (str, optional): Source file name. Defaults to None.
    
    Returns:
        pd.DataFrame: Standardized DataFrame with required columns
    """
    # Validate required columns
    required_cols = ['Date', 'Description', 'Card Member', 'Account #', 'Amount']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Expected: {required_cols}")
    
    # Create result DataFrame
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
    
    # Add metadata
    result['Card Member'] = df['Card Member']
    result['Account #'] = df['Account #']
    
    # Map category if available, otherwise use Uncategorized
    result['Category'] = df['Category'].apply(standardize_category) if 'Category' in df.columns else 'Uncategorized'
    result['source_file'] = source_file if source_file else 'amex'
    
    return result

def process_aggregator_format(df):
    """Process aggregator format transactions.
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Standardized transaction data
        
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Create result DataFrame with standardized columns
    result = pd.DataFrame()
    
    # Map date columns - handle both 'Date' and 'Transaction Date'/'Post Date' cases
    if 'Date' in df.columns:
        result['Transaction Date'] = df['Date'].apply(standardize_date)
        result['Post Date'] = df['Date'].apply(standardize_date)  # Use same date for both
    else:
        result['Transaction Date'] = df['Transaction Date'].apply(standardize_date)
        result['Post Date'] = df['Transaction Date'].apply(standardize_date)  # Use transaction date for post date
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Extract account numbers and clean descriptions
    if 'Description' not in df.columns:
        raise ValueError("Description cannot be empty")
        
    # Validate descriptions
    if df['Description'].isna().any() or (df['Description'] == '').any():
        raise ValueError("Description cannot be empty")
    
    # Handle Account field - use explicit column if present, otherwise extract from description
    if 'Account' in df.columns:
        result['Account'] = df['Account']
    else:
        # Extract account numbers from descriptions ending with "- Ending in XXXX"
        account_pattern = r'Ending\s+in\s+(\d+)'
        account_matches = df['Description'].str.extract(account_pattern, expand=False)
        result['Account'] = account_matches.fillna('')
    
    # Clean descriptions by removing the account number part
    result['Description'] = df['Description'].str.replace(r'\s*-\s*Ending\s+in\s+\d+\s*$', '', regex=True)
    result['Description'] = result['Description'].apply(standardize_description)
    
    # Convert amount to float
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category) if 'Category' in df.columns else 'Uncategorized'
    
    # Preserve additional metadata
    if 'Tags' in df.columns:
        result['Tags'] = df['Tags']
    
    # Add source file information
    result['source_file'] = df.get('source_file', 'aggregator')
    
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
            
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Validate required columns
    required_cols = ['Post Date', 'Description', 'Amount']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Expected: {required_cols}")

    # Create result DataFrame
    result = df.copy()
    
    # Map and validate dates
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Use post date for transaction date
    result['Transaction Date'] = result['Post Date']
    
    # Map and validate description
    result['Description'] = result['Description'].apply(standardize_description)
    
    # Convert and validate amount
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    
    # Map category
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
            
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Validate required columns
    required_cols = ['Transaction Date', 'Description', 'Amount']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Expected: {required_cols}")

    # Create result DataFrame
    result = df.copy()
    
    # Map and validate dates
    result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Use transaction date for post date
    result['Post Date'] = result['Transaction Date']
    
    # Map and validate description
    result['Description'] = result['Description'].apply(standardize_description)
    
    # Convert and validate amount
    result['Amount'] = result['Amount'].apply(clean_amount) * -1  # Convert to negative for debits
    
    # Map category
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
            
    Raises:
        ValueError: If any required field is invalid or missing
    """
    # Validate required columns
    required_cols = ['Date', 'Description', 'Debit', 'Credit']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Expected: {required_cols}")

    # Create result DataFrame
    result = df.copy()
    
    # Map and validate dates
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Use transaction date for post date
    result['Post Date'] = result['Transaction Date']
    
    # Map and validate description
    result['Description'] = result['Description'].apply(standardize_description)
    
    # Convert and validate amounts
    result['Amount'] = result.apply(
        lambda row: -clean_amount(row['Debit']) if pd.notna(row['Debit']) and row['Debit'] != '' else clean_amount(row['Credit']),
        axis=1
    )
    
    # Map category
    result['Category'] = result.get('Category', pd.Series('', index=result.index))
    result['source_file'] = result.get('source_file', pd.Series('debit_credit', index=result.index))
    
    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

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
    """Process Alliant visa format.

    Args:
        df (pd.DataFrame): DataFrame with Alliant visa format
        source_file (str, optional): Source file name. Defaults to None.

    Returns:
        pd.DataFrame: Standardized DataFrame with required columns
    """
    # Validate required columns
    required_cols = ['Date', 'Description', 'Amount', 'Post Date']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns. Expected: {required_cols}")

    # Validate description is not empty
    if df['Description'].isna().any() or (df['Description'] == '').any():
        raise ValueError("Description cannot be empty")

    # Create standardized DataFrame
    result = pd.DataFrame()

    # Map date columns and standardize to YYYY-MM-DD
    result['Transaction Date'] = df['Date'].apply(standardize_date)
    result['Post Date'] = df['Post Date'].apply(standardize_date)

    # Validate dates
    if result['Transaction Date'].isna().any() or result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
        
    # Validate post date is not before transaction date
    if (result['Post Date'] < result['Transaction Date']).any():
        raise ValueError("Post date cannot be before transaction date")

    # Map description
    result['Description'] = df['Description']

    # Convert amount to float and ensure debits are negative
    result['Amount'] = df['Amount'].apply(clean_amount).apply(lambda x: -abs(x) if x > 0 else x)

    # Map category if available, otherwise use Uncategorized
    result['Category'] = df['Category'] if 'Category' in df.columns else 'Uncategorized'

    # Add source file information
    result['source_file'] = source_file if source_file else 'alliant_visa'

    return result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def reconcile_transactions(aggregator_df, detail_records):
    """Reconcile transactions between aggregator and detail records.
    
    Args:
        aggregator_df (pd.DataFrame): Aggregator transactions
        detail_records (list): List of detail record DataFrames
        
    Returns:
        tuple: (matches_df, unmatched_df) containing matched and unmatched transactions
    """
    # Initialize results
    matches = []
    unmatched = []
    
    # Process each detail record
    for detail_df in detail_records:
        # Get source file name
        source_file = detail_df['source_file'].iloc[0] if 'source_file' in detail_df.columns else 'unknown'
        
        # Match on post date and amount
        for _, agg_row in aggregator_df.iterrows():
            # Find matching detail record
            match = detail_df[
                (detail_df['Post Date'] == agg_row['Post Date']) &
                (detail_df['Amount'] == agg_row['Amount'])
            ]
            
            if not match.empty:
                # Add match to results
                match_row = match.iloc[0].copy()
                match_row['Date'] = match_row['Transaction Date']
                match_row['YearMonth'] = match_row['Date'][:7]  # YYYY-MM
                match_row['Account'] = f"Matched - {source_file}"
                match_row['Tags'] = agg_row.get('Tags', '')  # Preserve tags from aggregator
                match_row['reconciled_key'] = match_row['Date']
                match_row['Matched'] = True
                matches.append(match_row)
                
                # Remove matched record from detail_df
                detail_df = detail_df.drop(match.index)
            else:
                # Try matching on transaction date and amount
                match = detail_df[
                    (detail_df['Transaction Date'] == agg_row['Transaction Date']) &
                    (detail_df['Amount'] == agg_row['Amount'])
                ]
                
                if not match.empty:
                    # Add match to results
                    match_row = match.iloc[0].copy()
                    match_row['Date'] = match_row['Transaction Date']
                    match_row['YearMonth'] = match_row['Date'][:7]  # YYYY-MM
                    match_row['Account'] = f"Matched - {source_file}"
                    match_row['Tags'] = agg_row.get('Tags', '')  # Preserve tags from aggregator
                    match_row['reconciled_key'] = match_row['Date']
                    match_row['Matched'] = True
                    matches.append(match_row)
                    
                    # Remove matched record from detail_df
                    detail_df = detail_df.drop(match.index)
                else:
                    # Add unmatched aggregator record
                    unmatched_row = agg_row.copy()
                    unmatched_row['Date'] = unmatched_row['Transaction Date']
                    unmatched_row['YearMonth'] = unmatched_row['Date'][:7]  # YYYY-MM
                    unmatched_row['Account'] = f"Unreconciled - {source_file}"
                    unmatched_row['Tags'] = agg_row.get('Tags', '')  # Preserve tags from aggregator
                    unmatched_row['reconciled_key'] = unmatched_row['Date']
                    unmatched_row['Matched'] = False
                    unmatched.append(unmatched_row)
        
        # Add remaining detail records to unmatched
        for _, row in detail_df.iterrows():
            unmatched_row = row.copy()
            unmatched_row['Date'] = unmatched_row['Transaction Date']
            unmatched_row['YearMonth'] = unmatched_row['Date'][:7]  # YYYY-MM
            unmatched_row['Account'] = f"Unreconciled - {source_file}"
            unmatched_row['Tags'] = ''  # Detail records don't have tags
            unmatched_row['reconciled_key'] = unmatched_row['Date']
            unmatched_row['Matched'] = False
            unmatched.append(unmatched_row)
    
    # Convert results to DataFrames
    matches_df = pd.DataFrame(matches)
    unmatched_df = pd.DataFrame(unmatched)
    
    return matches_df, unmatched_df

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
        df = pd.read_csv(file_path)
    elif ext == '.xlsx':
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}. Supported formats: .csv, .CSV, .xlsx, .XLSX")
    
    # 4. Check filename pattern for format processor
    filename = file_path.stem  # Get filename without extension
    filename_lower = filename.lower()
    
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
    elif 'empower' in filename_lower:
        result = process_aggregator_format(df)
        result['source_file'] = filename
        return result
    
    # 5. For unknown formats, check required columns
    required_columns = ['Transaction Date', 'Description', 'Amount']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing required columns in {file_path}. Required: {required_columns}")
    
    df['source_file'] = filename
    return df

def import_folder(folder_path):
    """
    Import all CSV files from a folder.
    
    Args:
        folder_path (str or Path): Path to folder containing CSV files
        
    Returns:
        list: List of DataFrames containing standardized transaction data
        
    Raises:
        FileNotFoundError: If folder does not exist
        ValueError: If no CSV files found or if any file cannot be processed
    """
    logger.info(f"Importing folder: {folder_path}")
    
    # Convert to Path object if string
    if isinstance(folder_path, str):
        folder_path = pathlib.Path(folder_path)
    
    # Check if folder exists
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    # Find all CSV files
    csv_files = list(folder_path.glob('*.csv'))
    if not csv_files:
        raise ValueError(f"No CSV files found in {folder_path}")
    
    # Import each file
    results = []
    for file_path in csv_files:
        try:
            df = import_csv(file_path)
            results.append(df)
        except Exception as e:
            logger.error(f"Error importing {file_path}: {str(e)}")
            raise ValueError(f"Error importing {file_path}: {str(e)}")
    
    return results

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

def save_reconciliation_results(matches_df, unmatched_df, output_path):
    """
    Save reconciliation results to a single CSV file.
    
    Args:
        matches_df (pd.DataFrame): DataFrame containing matched transactions
        unmatched_df (pd.DataFrame): DataFrame containing unmatched transactions
        output_path (str or Path): Path where results should be saved
    """
    # Transform matched transactions
    matches_transformed = matches_df.copy()
    matches_transformed['Date'] = matches_transformed['Transaction Date']
    matches_transformed['YearMonth'] = pd.to_datetime(matches_transformed['Date']).dt.strftime('%Y-%m-%d').str[:7]
    matches_transformed['Account'] = 'Matched - ' + matches_transformed['source_file']
    matches_transformed['reconciled_key'] = matches_transformed['Date']
    matches_transformed['Matched'] = True
    
    # Transform unmatched transactions
    unmatched_transformed = unmatched_df.copy()
    unmatched_transformed['Date'] = unmatched_transformed['Transaction Date']
    unmatched_transformed['YearMonth'] = pd.to_datetime(unmatched_transformed['Date']).dt.strftime('%Y-%m-%d').str[:7]
    unmatched_transformed['Account'] = 'Unreconciled - ' + unmatched_transformed['source_file']
    unmatched_transformed['reconciled_key'] = unmatched_transformed['Date']
    unmatched_transformed['Matched'] = False
    
    # Select and order columns
    columns = [
        'Date', 'YearMonth', 'Account', 'Description', 'Category',
        'Tags', 'Amount', 'reconciled_key', 'Matched'
    ]
    
    # Fill NaN values with empty strings for string columns
    string_columns = ['Tags', 'Category', 'Description']
    matches_transformed[string_columns] = matches_transformed[string_columns].fillna('')
    unmatched_transformed[string_columns] = unmatched_transformed[string_columns].fillna('')
    
    # Combine matched and unmatched transactions
    all_transactions = pd.concat([
        matches_transformed[columns],
        unmatched_transformed[columns]
    ], ignore_index=True)
    
    # Sort by date and amount
    all_transactions = all_transactions.sort_values(['Date', 'Amount'])
    
    # Save to a single CSV file
    output_path = str(output_path)
    if output_path.endswith('.xlsx'):
        # Save as Excel with a single sheet
        with pd.ExcelWriter(output_path) as writer:
            all_transactions.to_excel(writer, sheet_name='All Transactions', index=False)
    else:
        # Save as CSV file
        os.makedirs(output_path, exist_ok=True)
        all_transactions_path = os.path.join(output_path, "all_transactions.csv")
        all_transactions.to_csv(all_transactions_path, index=False)
        logger.info(f"Saved all transactions to {all_transactions_path}")

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

def main():
    """Main function to run the reconciliation process."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Reconcile transaction data')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--log-level', choices=['debug', 'info', 'warning', 'error'], 
                       default='info', help='Set logging level')
    args = parser.parse_args()
    
    # Set up logging
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)
    
    logger.info("Starting reconciliation process")
    
    # Create output directories
    create_output_directories("output")
    
    # Import and process data
    try:
        # Import aggregator data
        logger.info("Importing aggregator data")
        aggregator_df = import_csv("data/2025/empower_2025.csv")
        
        # Import and process detail data
        logger.info("Importing and processing detail data")
        detail_dfs = []
        for file in os.listdir("data/2025/details"):
            if file.endswith(".csv"):
                logger.info(f"Processing {file}")
                df = import_csv(os.path.join("data/2025/details", file))
                detail_dfs.append(df)
        
        # Reconcile transactions
        logger.info("Reconciling transactions")
        reconciled_df, unmatched_df = reconcile_transactions(aggregator_df, detail_dfs)
        
        # Save results
        logger.info("Saving results")
        save_reconciliation_results(reconciled_df, unmatched_df, "output")
        
        logger.info("Reconciliation complete")
        
    except Exception as e:
        logger.error(f"Error during reconciliation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
