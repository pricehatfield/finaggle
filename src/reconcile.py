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

logger = logging.getLogger(__name__)

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
        result['Post Date'] = df['Post Date'].apply(standardize_date)
    
    # Validate dates
    if result['Transaction Date'].isna().any():
        raise ValueError("Invalid date format")
    if result['Post Date'].isna().any():
        raise ValueError("Invalid date format")
    
    # Map description
    result['Description'] = df['Description'].apply(standardize_description)
    
    # Convert amount to float
    result['Amount'] = df['Amount'].apply(clean_amount)
    
    # Map category
    result['Category'] = df['Category'].apply(standardize_category)
    
    # Preserve additional metadata
    if 'Tags' in df.columns:
        result['Tags'] = df['Tags']
    if 'Account' in df.columns:
        result['Account'] = df['Account']
    
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
    
    # Convert amount to float, removing $ symbol
    try:
        df['Amount'] = df['Amount'].str.replace('$', '').astype(float)
    except (ValueError, TypeError):
        raise ValueError("Invalid amount format")
    
    # Make debits negative
    df['Amount'] = -df['Amount']
    
    # Validate dates
    try:
        trans_dates = pd.to_datetime(df['Date'])
    except (ValueError, TypeError):
        raise ValueError("Invalid date format")
    
    # Standardize column names and add required columns
    result = pd.DataFrame({
        'Transaction Date': trans_dates.dt.strftime('%Y-%m-%d'),
        'Post Date': trans_dates.dt.strftime('%Y-%m-%d'),  # Same as transaction date for checking
        'Description': df['Description'],
        'Amount': df['Amount'],
        'Category': 'Uncategorized',
        'source_file': source_file if source_file else 'unknown'
    })
    
    return result

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
    
    # Convert amount to float, removing $ symbol
    try:
        df['Amount'] = df['Amount'].str.replace('$', '').astype(float)
    except (ValueError, TypeError):
        raise ValueError("Invalid amount format")
    
    # Make debits negative
    df['Amount'] = -df['Amount']
    
    # Validate dates
    try:
        trans_dates = pd.to_datetime(df['Date'])
        post_dates = pd.to_datetime(df['Post Date'])
    except (ValueError, TypeError):
        raise ValueError("Invalid date format")
    
    # Validate post date is not before transaction date
    if (post_dates < trans_dates).any():
        raise ValueError("Post date cannot be before transaction date")
    
    # Standardize column names and add required columns
    result = pd.DataFrame({
        'Transaction Date': trans_dates.dt.strftime('%Y-%m-%d'),
        'Post Date': post_dates.dt.strftime('%Y-%m-%d'),
        'Description': df['Description'],
        'Amount': df['Amount'],
        'Category': 'Uncategorized',
        'source_file': source_file if source_file else 'unknown'
    })
    
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
        if not {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category'}.issubset(df.columns):
            if 'Transaction Date' in df.columns and 'Post Date' in df.columns:
                processed_details.append(process_date_format(df))
            elif 'Post Date' in df.columns:
                processed_details.append(process_post_date_format(df))
            elif 'Debit' in df.columns and 'Credit' in df.columns:
                processed_details.append(process_debit_credit_format(df))
            else:
                processed_details.append(process_aggregator_format(df))
        else:
            processed_details.append(df)
    
    # Combine all detail records
    detail_df = pd.concat(processed_details, ignore_index=True)
    
    # Process aggregator data if not already in standardized format
    if not {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category'}.issubset(aggregator_df.columns):
        aggregator_df = process_aggregator_format(aggregator_df)
    
    # Find matches
    matches = []
    unmatched_aggregator = []
    unmatched_detail = []
    
    # Track which detail records have been matched to avoid duplicate matches
    matched_detail_indices = set()
    
    # Match by post date
    for _, agg_row in aggregator_df.iterrows():
        # Find all potential matches
        potential_matches = detail_df[
            (detail_df['Post Date'] == agg_row['Post Date']) &
            (detail_df['Amount'] == agg_row['Amount'])
        ]
        
        # Filter out already matched records
        available_matches = potential_matches[~potential_matches.index.isin(matched_detail_indices)]
        
        if len(available_matches) > 0:
            # Take the first available match
            detail_row = available_matches.iloc[0]
            matches.append({
                'Transaction Date': detail_row['Transaction Date'],
                'Post Date': agg_row['Post Date'],
                'Description': detail_row['Description'],
                'Amount': agg_row['Amount'],
                'Category': detail_row['Category'],
                'source_file': detail_row.get('source_file', 'unknown'),
                'match_type': 'P'  # Post date match
            })
            matched_detail_indices.add(detail_row.name)
        else:
            unmatched_aggregator.append(agg_row)
    
    # Match by transaction date for unmatched detail records
    for _, detail_row in detail_df.iterrows():
        if detail_row.name not in matched_detail_indices and detail_row['Transaction Date'] is not None:
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
                f"Date: {row['Transaction Date']} | "
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
                f"Date: {row['Transaction Date']} | "
                f"Description: {row['Description']} | "
                f"Amount: ${abs(row['Amount']):.2f} | "
                f"Category: {row['Category']} | "
                f"Source: {row['source_file']}\n"
            )
    else:
        report_content.append("No unmatched transactions found.\n")
    
    # Write report to file
    with open(report_path, 'w') as f:
        f.writelines(report_content)

def save_reconciliation_results(matches_df, unmatched_df, output_dir):
    """
    Save reconciliation results to CSV files.
    
    Args:
        matches_df (pd.DataFrame): DataFrame containing matched transactions
        unmatched_df (pd.DataFrame): DataFrame containing unmatched transactions
        output_dir (str or Path): Directory where the files should be saved
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save matched transactions
    matches_path = os.path.join(output_dir, "matched.csv")
    matches_df.to_csv(matches_path, index=False)
    
    # Save unmatched transactions
    unmatched_path = os.path.join(output_dir, "unmatched.csv")
    unmatched_df.to_csv(unmatched_path, index=False)

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
    required_columns = ['Transaction Date', 'Post Date', 'Description', 'Amount']
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
    """
    Main function to run the reconciliation process.
    """
    # Set up logging
    setup_logging()
    
    # Import data
    aggregator_df = import_csv("data/2025/empower_2025.csv")
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
    generate_reconciliation_report(matches, unmatched, "output/report.txt")

if __name__ == "__main__":
    main()
