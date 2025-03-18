import pandas as pd
from datetime import datetime
import numpy as np
import os
import logging
import pathlib
import re
import csv

logger = logging.getLogger(__name__)

# Set up logging
def setup_logging(log_level=logging.ERROR):
    """Configure logging to output to both file and console with different levels"""
    logger = logging.getLogger()
    # Set root logger to lowest level we'll use (DEBUG)
    logger.setLevel(logging.DEBUG)
    
    # Create formatters and handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler - set to WARNING or user specified level
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(max(logging.WARNING, log_level))  # Never go below WARNING for console
    logger.addHandler(console_handler)
    
    # File handler - always set to DEBUG for diagnostics
    log_dir = ensure_directory("logs")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"reconciliation_{timestamp}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)  # Always capture all details in file
    logger.addHandler(file_handler)

def ensure_directory(dir_type):
    """
    Verify a directory exists and create it if necessary.
    Args:
        dir_type: Type of directory ("archive" or "logs")
    Returns the full path to the directory.
    """
    script_dir = pathlib.Path(__file__).parent.absolute()
    target_dir = os.path.join(script_dir, dir_type)
    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)
    return target_dir

def standardize_date(date_str):
    """Convert various date formats to YYYY-MM-DD (ISO8601)"""
    logger.debug(f"standardize_date called with date_str: {date_str}")
    try:
        if not date_str or pd.isna(date_str):
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
    """Convert amount strings to float and handle currency symbols"""
    logger.debug(f"clean_amount called with amount_str: {amount_str}")
    if isinstance(amount_str, str):
        amount_str = amount_str.replace('$', '').replace(',', '')
    try:
        return float(amount_str)
    except:
        return 0.0

def process_aggregator_format(df):
    """Process aggregator format DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with aggregator format columns
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    logger.debug(f"process_aggregator_format called with DataFrame shape: {df.shape}")
    logger.debug(f"Initial columns: {df.columns.tolist()}")
    logger.debug(f"Sample data:\n{df.head()}")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # If the DataFrame already has Transaction Date and Post Date, use those
    if 'Transaction Date' in result.columns and 'Post Date' in result.columns:
        result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
        result['Post Date'] = result['Post Date'].apply(standardize_date)
    else:
        # Otherwise, use the Date column
        result['Transaction Date'] = result['Date'].apply(standardize_date)
        result['Post Date'] = result['Date'].apply(standardize_date)
    
    # Clean amounts
    result['Amount'] = result['Amount'].apply(clean_amount)
    
    # Add source file column
    result['source_file'] = 'aggregator'
    
    # Select and order columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']]
    
    logger.debug(f"Processed amounts:\n{result[['Description', 'Amount']]}")
    logger.debug(f"Returning {len(result)} processed aggregator records")
    
    return result

def process_discover_format(df):
    """Process Discover format DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with Discover format columns
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    logger.debug(f"process_discover_format called with DataFrame shape: {df.shape}")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Trans. Date'].apply(standardize_date)
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    
    # Clean amounts and convert to negative for debits
    result['Amount'] = result['Amount'].apply(clean_amount)
    result.loc[result['Amount'] > 0, 'Amount'] = -result.loc[result['Amount'] > 0, 'Amount']
    
    # Add source file column
    result['source_file'] = 'discover'
    
    # Select and order columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    return result

def process_amex_format(df):
    """Process Amex format DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with Amex format columns
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    logger.debug(f"process_amex_format called with DataFrame shape: {df.shape}")
    logger.debug(f"Initial columns: {df.columns.tolist()}")
    logger.debug(f"Sample data:\n{df.head()}")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)
    
    # Clean amounts and convert to negative for debits
    result['Amount'] = result['Amount'].apply(clean_amount)
    result.loc[result['Amount'] > 0, 'Amount'] = -result.loc[result['Amount'] > 0, 'Amount']
    
    # Add source file column
    result['source_file'] = 'amex'
    
    # Select and order columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']]
    
    logger.debug(f"Processed amounts:\n{result[['Description', 'Amount']]}")
    logger.debug(f"Returning {len(result)} processed Amex records")
    
    return result

def process_capital_one_format(df):
    """Process Capital One format DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with Capital One format columns
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    logger.debug(f"process_capital_one_format called with DataFrame shape: {df.shape}")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
    result['Post Date'] = result['Posted Date'].apply(standardize_date)
    
    # Clean amounts and combine Debit and Credit
    result['Amount'] = result.apply(lambda row: -clean_amount(row['Debit']) if pd.notna(row['Debit']) else clean_amount(row['Credit']), axis=1)
    
    # Add source file column
    result['source_file'] = 'capital_one'
    
    # Select and order columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    return result

def process_post_date_format(df):
    """Process detail format that uses Post Date column for date and single Amount column.
    Sign convention: negative for debits (money out), positive for credits (money in)
    """
    logger.debug(f"process_post_date_format called with DataFrame shape: {df.shape}")
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df['Transaction Date'] = df['Post Date'].apply(standardize_date)
    df['Post Date'] = df['Post Date'].apply(standardize_date)
    df['Amount'] = df['Amount'].apply(clean_amount)
    df['Amount'] = -df['Amount']  # Invert amount
    df['source_file'] = 'unknown'  # Add source_file column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']]

def process_date_format(df):
    """Process detail format that uses Date column for date and single Amount column.
    Sign convention: negative for debits (money out), positive for credits (money in)
    """
    logger.debug(f"process_date_format called with DataFrame shape: {df.shape}")
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df['Transaction Date'] = df['Date'].apply(standardize_date)
    df['Post Date'] = df['Date'].apply(standardize_date)
    df['Amount'] = df['Amount'].apply(clean_amount)
    df['Amount'] = -df['Amount']  # Invert amount
    df['source_file'] = 'unknown'  # Add source_file column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']]

def process_debit_credit_format(df):
    """Process detail format that uses Posted Date for date and separate Debit/Credit columns.
    Sign convention:
    - Debits (outgoing money) become negative amounts
    - Credits (incoming money) become positive amounts
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
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']]

def process_alliant_format(df):
    """Process Alliant format DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with Alliant format columns
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    logger.debug(f"process_alliant_format called with DataFrame shape: {df.shape}")
    logger.debug(f"Initial columns: {df.columns.tolist()}")
    logger.debug(f"Sample data:\n{df.head()}")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    
    # Clean amounts and convert to negative for debits
    result['Amount'] = result['Amount'].apply(clean_amount)
    result.loc[result['Amount'] > 0, 'Amount'] = -result.loc[result['Amount'] > 0, 'Amount']
    
    # Add source file column
    result['source_file'] = 'alliant'
    
    # Select and order columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']]
    
    logger.debug(f"Processed amounts:\n{result[['Description', 'Amount']]}")
    logger.debug(f"Returning {len(result)} processed Alliant records")
    
    return result

def process_chase_format(df):
    """Process Chase format DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with Chase format columns
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized columns
    """
    logger.debug(f"process_chase_format called with DataFrame shape: {df.shape}")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Posting Date'].apply(standardize_date)
    result['Post Date'] = result['Posting Date'].apply(standardize_date)
    
    # Clean amounts (Chase already has correct sign)
    result['Amount'] = result['Amount'].apply(clean_amount)
    
    # Add source file column
    result['source_file'] = 'chase'
    
    # Select and order columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']]
    
    return result

def reconcile_transactions(aggregator_df, detail_dfs):
    """Reconcile transactions between aggregator and detail records.
    
    Args:
        aggregator_df (pd.DataFrame): DataFrame with aggregator records
        detail_dfs (list): List of DataFrames with detail records
        
    Returns:
        pd.DataFrame: Reconciled DataFrame with columns:
            - Date: Transaction date
            - YearMonth: Year and month of transaction
            - Account: Account name
            - Description: Transaction description
            - Category: Transaction category
            - Tags: Transaction tags
            - Amount: Transaction amount
            - reconciled_key: Unique key for the transaction
            - Matched: Whether the transaction was matched
    """
    # Process aggregator records first
    agg_df = process_aggregator_format(aggregator_df)
    
    # Initialize result DataFrame with required columns
    result_df = pd.DataFrame(columns=['Date', 'YearMonth', 'Account', 'Description', 'Category', 'Tags', 'Amount', 'reconciled_key', 'Matched'])
    
    # Create compound key for matching
    agg_df['compound_key'] = agg_df['Transaction Date'] + '_' + agg_df['Amount'].astype(str)
    
    # Process each detail DataFrame
    for detail_df in detail_dfs:
        if not isinstance(detail_df, pd.DataFrame):
            continue
            
        # Create compound key for matching
        detail_df['compound_key'] = detail_df['Transaction Date'] + '_' + detail_df['Amount'].astype(str)
        
        # Find matches between detail and aggregator records
        matched_mask = detail_df['compound_key'].isin(agg_df['compound_key'])
        matched_records = detail_df[matched_mask]
        unmatched_records = detail_df[~matched_mask]
        
        # Process matched records
        for _, record in matched_records.iterrows():
            agg_record = agg_df[agg_df['compound_key'] == record['compound_key']].iloc[0]
            result_record = pd.DataFrame({
                'Date': [pd.to_datetime(record['Transaction Date'])],
                'YearMonth': [pd.to_datetime(record['Transaction Date']).strftime('%Y-%m')],
                'Account': [agg_record['Account']],
                'Description': [record['Description']],
                'Category': [agg_record['Category'] if pd.notna(agg_record['Category']) else record.get('Category', '')],
                'Tags': [agg_record['Tags']],
                'Amount': [record['Amount']],
                'reconciled_key': [f"M:{record['compound_key']}"],
                'Matched': [True]
            })
            result_df = pd.concat([result_df, result_record], ignore_index=True)
        
        # Add unmatched records with default values
        for _, record in unmatched_records.iterrows():
            result_record = pd.DataFrame({
                'Date': [pd.to_datetime(record['Transaction Date'])],
                'YearMonth': [pd.to_datetime(record['Transaction Date']).strftime('%Y-%m')],
                'Account': [''],
                'Description': [record['Description']],
                'Category': [record.get('Category', '')],
                'Tags': [''],
                'Amount': [record['Amount']],
                'reconciled_key': [f"U:{record['compound_key']}"],
                'Matched': [False]
            })
            result_df = pd.concat([result_df, result_record], ignore_index=True)
    
    # Add remaining unmatched aggregator records
    unmatched_agg_mask = ~agg_df['compound_key'].isin(detail_df['compound_key'])
    unmatched_agg_records = agg_df[unmatched_agg_mask]
    
    for _, record in unmatched_agg_records.iterrows():
        result_record = pd.DataFrame({
            'Date': [pd.to_datetime(record['Transaction Date'])],
            'YearMonth': [pd.to_datetime(record['Transaction Date']).strftime('%Y-%m')],
            'Account': [record['Account']],
            'Description': [record['Description']],
            'Category': [record['Category']],
            'Tags': [record['Tags']],
            'Amount': [record['Amount']],
            'reconciled_key': [f"U:{record['compound_key']}"],
            'Matched': [False]
        })
        result_df = pd.concat([result_df, result_record], ignore_index=True)
    
    # Drop the compound_key column if it exists
    if 'compound_key' in result_df.columns:
        result_df = result_df.drop('compound_key', axis=1)
    
    return result_df

def import_csv(filepath):
    """Import a CSV file and process it according to its format.
    Returns a DataFrame with standardized columns: Transaction Date, Post Date, Description, Amount
    Amount convention: negative for debits (money out), positive for credits (money in)
    """
    logger.debug(f"import_csv called with filepath: {filepath}")
    df = pd.read_csv(filepath)
    columns = set(df.columns)
    
    processed_df = None
    # Check formats in order from most specific to least specific
    if {'Date', 'Description', 'Amount', 'Balance', 'Post Date'} <= columns:
        processed_df = process_alliant_format(df)  # Alliant format
    elif {'Trans. Date', 'Post Date', 'Description', 'Amount'} <= columns:
        processed_df = process_discover_format(df)  # Discover format
    elif {'Details', 'Posting Date', 'Description', 'Amount', 'Type', 'Balance'} <= columns:  # Chase format
        processed_df = process_chase_format(df)  # Chase format
    elif {'Date', 'Description', 'Amount'} <= columns:
        processed_df = process_amex_format(df)  # Amex format
    elif {'Transaction Date', 'Posted Date', 'Description', 'Debit', 'Credit'} <= columns:
        processed_df = process_capital_one_format(df)  # Capital One format
    
    if processed_df is not None:
        processed_df.loc[:, 'source_file'] = os.path.basename(filepath)  # Use .loc to avoid warning
        
    return processed_df

def import_folder(folder_path):
    """
    Import all CSV files from a folder and combine them into a single DataFrame.
    Returns a combined DataFrame of all recognized CSV formats.
    """
    logger.debug(f"import_folder called with folder_path: {folder_path}")
    all_dfs = []
    
    # Iterate through all files in folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            filepath = os.path.join(folder_path, filename)
            df = import_csv(filepath)
            
            if df is not None:
                all_dfs.append(df)
    
    # Combine all DataFrames if any were found
    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return None

def main(aggregator_path, details_folder, log_level=logging.ERROR):
    """
    Main function to process transaction reconciliation.
    
    Args:
        aggregator_path: Path to the aggregator CSV file
        details_folder: Path to folder containing detail CSV files
        log_level: Logging level to use (affects console output only)
    """
    # Set up logging
    setup_logging(log_level)
    logger.debug(f"main called with aggregator_path: {aggregator_path}, details_folder: {details_folder}, log_level: {log_level}")
    
    # Import aggregator file with validation
    logger.debug(f"Reading aggregator file: {aggregator_path}")
    try:
        # First read the entire file to check for issues
        with open(aggregator_path, 'r', encoding='utf-8-sig') as f:  # Handle BOM character
            lines = f.readlines()
            
        logger.debug(f"Total lines in file: {len(lines)}")
        header = lines[0].strip()
        logger.debug(f"CSV Header: {header}")
        
        # Check for empty lines
        empty_lines = [i for i, line in enumerate(lines, 1) if not line.strip()]
        if empty_lines:
            logger.warning(f"Found empty lines at positions: {empty_lines}")
        
        # Now try reading with pandas with different options
        try:
            # First attempt - with proper quote handling
            aggregator_df = pd.read_csv(
                aggregator_path,
                encoding='utf-8-sig',  # Handle BOM character
                quoting=csv.QUOTE_MINIMAL,  # Handle quoted strings
                quotechar='"',  # Specify quote character
                thousands=',',  # Handle thousands separators in amounts
                na_values=[''],  # Empty strings should be NaN
                keep_default_na=True
            )
            logger.debug(f"Successfully read file with quote handling. Shape: {aggregator_df.shape}")
        except Exception as e:
            logger.warning(f"Quote-aware read failed: {str(e)}")
            try:
                # Second attempt - handle bad lines
                aggregator_df = pd.read_csv(
                    aggregator_path,
                    encoding='utf-8-sig',
                    on_bad_lines='warn'
                )
                logger.debug("Successfully read file with bad lines handling")
            except Exception as e:
                logger.error(f"Failed to read file even with bad lines handling: {str(e)}")
                return
        
        # Remove any completely empty rows
        original_rows = len(aggregator_df)
        aggregator_df = aggregator_df.dropna(how='all')
        if len(aggregator_df) != original_rows:
            logger.warning(f"Removed {original_rows - len(aggregator_df)} completely empty rows")
        
        # Log the first few rows
        logger.debug("First few rows of aggregator file:")
        logger.debug(f"\n{aggregator_df.head().to_string()}")
        
        # Check for partially empty rows
        partial_rows = aggregator_df.isna().any(axis=1)
        if partial_rows.any():
            logger.warning(f"Found {partial_rows.sum()} rows with some empty values")
            logger.debug("Sample of rows with some empty values:")
            problem_sample = aggregator_df[partial_rows].head()
            logger.debug(f"\n{problem_sample.to_string()}")
            
            # Show which columns have nulls in these rows
            null_columns = aggregator_df[partial_rows].isna().sum()
            logger.warning("Null counts by column in partial rows:")
            for col, count in null_columns[null_columns > 0].items():
                if col != 'Tags':  # Ignore Tags column as it's expected to be mostly empty
                    logger.warning(f"Column '{col}': {count} nulls")
        
        # Validate expected columns
        expected_columns = {'Date', 'Account', 'Description', 'Category', 'Tags', 'Amount'}
        missing_columns = expected_columns - set(aggregator_df.columns)
        if missing_columns:
            logger.warning(f"Missing expected columns: {missing_columns}")
        
        # Show value counts for each column to spot patterns
        for col in aggregator_df.columns:
            if col != 'Tags':  # Skip Tags column in reporting
                null_count = aggregator_df[col].isna().sum()
                if null_count > 0:
                    logger.info(f"Column '{col}' has {null_count} null values")
        
    except Exception as e:
        logger.error(f"Error reading aggregator file: {str(e)}")
        return
    
    # Import and combine all detail files
    details_df = import_folder(details_folder)
    
    if details_df is None:
        logger.error("No valid detail files found in folder")
        return
        
    # Reconcile transactions
    reconciled_df = reconcile_transactions(aggregator_df, details_df)
    
    # Ensure archive directory exists and get path
    archive_dir = ensure_directory("archive")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(archive_dir, f"reconciliation_report_{timestamp}.xlsx")
    
    # Write to Excel file
    reconciled_df.to_excel(output_file, index=False)
    logger.info(f"Reconciliation complete. Results written to {output_file}")

if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("aggregator_file", help="Path to aggregator CSV file")
    parser.add_argument("details_folder", help="Path to folder containing detail CSV files")
    parser.add_argument("--log-level", default="WARNING",  # Changed default to WARNING
                      choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                      help="Set the logging level (default: WARNING)")
    
    args = parser.parse_args()
    
    # Convert string log level to logging constant
    log_level = getattr(logging, args.log_level.upper())
    
    main(args.aggregator_file, args.details_folder, log_level)
