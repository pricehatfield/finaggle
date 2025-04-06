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
            
        if isinstance(date_str, pd.Series):
            return date_str.apply(standardize_date)
            
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
    if pd.isna(amount_str):
        return 0.0
    if isinstance(amount_str, str):
        amount_str = amount_str.replace('$', '').replace(',', '')
    try:
        return float(amount_str)
    except:
        return 0.0

def process_discover_format(df):
    """Process Discover format data"""
    logger.debug("Processing Discover format data")
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Trans. Date'].apply(standardize_date)
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    
    # Clean and standardize amounts
    result['Amount'] = result['Amount'].apply(clean_amount)
    # Discover shows positive amounts for debits, so we invert them
    result['Amount'] = -result['Amount']
    
    # Standardize description
    result['Description'] = result['Description'].str.upper()
    
    # Add source_file
    result['source_file'] = 'discover'
    
    # Add Category if not present
    if 'Category' not in result.columns:
        result['Category'] = ''
    
    # Select and rename columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    logger.debug(f"Processed {len(result)} Discover records")
    return result

def process_amex_format(df):
    """Process Amex format data"""
    logger.debug("Processing Amex format data")
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)
    
    # Clean and standardize amounts
    result['Amount'] = result['Amount'].apply(clean_amount)
    # Amex shows positive amounts for debits and negative for credits, so we invert all amounts
    result['Amount'] = -result['Amount']
    
    # Standardize description
    result['Description'] = result['Description'].str.upper()
    
    # Add source_file and Category
    result['source_file'] = 'amex'
    result['Category'] = ''  # Amex doesn't provide categories
    
    # Select and rename columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    logger.debug(f"Processed {len(result)} Amex records")
    return result

def process_capital_one_format(df):
    """Process Capital One format data"""
    logger.debug("Processing Capital One format data")
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Transaction Date'].apply(standardize_date)
    result['Post Date'] = result['Posted Date'].apply(standardize_date)
    
    # Handle separate Debit/Credit columns
    result['Debit'] = result['Debit'].fillna('0').apply(clean_amount)
    result['Credit'] = result['Credit'].fillna('0').apply(clean_amount)
    result['Amount'] = -result['Debit'] + result['Credit']  # Negate debits, keep credits positive
    
    # Standardize description
    result['Description'] = result['Description'].str.upper()
    
    # Add source_file and Category
    result['source_file'] = 'capital_one'
    if 'Category' not in result.columns:
        result['Category'] = ''
    
    # Select and rename columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    logger.debug(f"Processed {len(result)} Capital One records")
    return result

def process_alliant_format(df):
    """Process Alliant format data"""
    logger.debug("Processing Alliant format data")
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Post Date'].apply(standardize_date)
    
    # Clean and standardize amounts (remove $ symbol and convert to float)
    result['Amount'] = result['Amount'].str.replace('$', '').apply(clean_amount)
    # Alliant shows positive amounts with $ symbol, so we invert them
    result['Amount'] = -result['Amount']
    
    # Standardize description
    result['Description'] = result['Description'].str.upper()
    
    # Add source_file and Category
    result['source_file'] = 'alliant'
    if 'Category' not in result.columns:
        result['Category'] = ''
    
    # Select and rename columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    logger.debug(f"Processed {len(result)} Alliant records")
    return result

def process_chase_format(df):
    """Process Chase format data"""
    logger.debug("Processing Chase format data")
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Posting Date'].apply(standardize_date)
    result['Post Date'] = result['Posting Date'].apply(standardize_date)
    
    # Clean and standardize amounts
    result['Amount'] = result['Amount'].apply(clean_amount)
    # Chase shows positive amounts for debits, so we invert them
    result['Amount'] = -result['Amount']
    
    # Standardize description
    result['Description'] = result['Description'].str.upper()
    
    # Add source_file and Category
    result['source_file'] = 'chase'
    if 'Category' not in result.columns:
        result['Category'] = ''
    
    # Select and rename columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    logger.debug(f"Processed {len(result)} Chase records")
    return result

def process_aggregator_format(df):
    """Process aggregator format data"""
    logger.debug("Processing aggregator format data")
    result = df.copy()
    
    # Standardize dates
    result['Transaction Date'] = result['Date'].apply(standardize_date)
    result['Post Date'] = result['Date'].apply(standardize_date)
    
    # Clean and standardize amounts
    result['Amount'] = result['Amount'].apply(clean_amount)
    
    # Standardize description
    result['Description'] = result['Description'].str.upper()
    
    # Add source_file
    result['source_file'] = 'aggregator'
    
    # Keep Category from input
    result['Category'] = df['Category']
    
    # Select and rename columns
    result = result[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]
    
    logger.debug(f"Processed {len(result)} aggregator records")
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
    df['Category'] = ''  # Add empty Category column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

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
    df['Category'] = ''  # Add empty Category column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

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
    df['Category'] = ''  # Add empty Category column
    return df[['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']]

def reconcile_transactions(aggregator_df, detail_dfs):
    """Reconcile transactions between aggregator and detail records
    Args:
        aggregator_df: DataFrame with aggregator records (already processed)
        detail_dfs: List of DataFrames with detail records or single DataFrame
    Returns:
        DataFrame with reconciled records
    """
    if not isinstance(detail_dfs, list):
        detail_dfs = [detail_dfs]

    # Process and combine detail records
    detail_processed = pd.concat([df for df in detail_dfs], ignore_index=True)
    
    # Process aggregator records to match detail format
    aggregator_processed = process_aggregator_format(aggregator_df)
    
    # Initialize result DataFrame
    result = pd.DataFrame(columns=['Date', 'YearMonth', 'Account', 'Description', 'Category', 'Tags', 'Amount', 'reconciled_key', 'Matched'])
    
    # Try to match on Post Date first
    post_date_matches = detail_processed.merge(
        aggregator_processed,
        left_on=['Post Date', 'Amount', 'Description'],
        right_on=['Post Date', 'Amount', 'Description'],
        how='outer',
        indicator=True,
        suffixes=('_detail', '_agg')
    )
    
    # Handle matched records
    matched = post_date_matches[post_date_matches['_merge'] == 'both']
    if not matched.empty:
        result = pd.concat([result, pd.DataFrame({
            'Date': matched['Post Date'],
            'YearMonth': matched['Post Date'].str[:7],
            'Account': '',
            'Description': matched['Description'],
            'Category': matched['Category_agg'],  # Use aggregator's category
            'Tags': '',
            'Amount': matched['Amount'],
            'reconciled_key': 'P:' + matched['source_file_detail'],  # Use detail's source_file
            'Matched': True
        })], ignore_index=True)
    
    # Handle unmatched detail records
    unmatched_detail = post_date_matches[post_date_matches['_merge'] == 'left_only']
    if not unmatched_detail.empty:
        result = pd.concat([result, pd.DataFrame({
            'Date': unmatched_detail['Post Date'],
            'YearMonth': unmatched_detail['Post Date'].str[:7],
            'Account': '',
            'Description': unmatched_detail['Description'],
            'Category': unmatched_detail['Category_detail'],  # Use detail's category
            'Tags': '',
            'Amount': unmatched_detail['Amount'],
            'reconciled_key': 'D:' + unmatched_detail['source_file_detail'],  # Use detail's source_file
            'Matched': False
        })], ignore_index=True)
    
    # Handle unmatched aggregator records
    unmatched_agg = post_date_matches[post_date_matches['_merge'] == 'right_only']
    if not unmatched_agg.empty:
        result = pd.concat([result, pd.DataFrame({
            'Date': unmatched_agg['Post Date'],
            'YearMonth': unmatched_agg['Post Date'].str[:7],
            'Account': '',
            'Description': unmatched_agg['Description'],
            'Category': unmatched_agg['Category_agg'],  # Use aggregator's category
            'Tags': '',
            'Amount': unmatched_agg['Amount'],
            'reconciled_key': 'U:' + unmatched_agg['source_file_agg'],  # Use aggregator's source_file
            'Matched': False
        })], ignore_index=True)
    
    # Ensure Matched column is boolean
    result['Matched'] = result['Matched'].astype(bool)
    
    return result

def import_csv(filepath):
    """Import a CSV file and return a processed DataFrame"""
    logger.debug(f"import_csv called with filepath: {filepath}")
    try:
        # Read CSV without automatic date parsing and handle quoted strings
        df = pd.read_csv(
            filepath,
            parse_dates=False,
            dtype=str,  # Read all columns as strings initially
            na_values=['nan', 'NaN', ''],
            keep_default_na=True,
            quoting=csv.QUOTE_MINIMAL
        )
        columns = set(df.columns)
        
        # Add source_file column first
        df['source_file'] = os.path.basename(filepath)
        
        processed_df = None
        if {'Trans. Date', 'Post Date', 'Description', 'Amount', 'Category'} <= columns:  # Discover format
            processed_df = process_discover_format(df)  # Discover format
        elif {'Details', 'Posting Date', 'Description', 'Amount', 'Type', 'Balance'}.issubset(columns):  # Chase format
            # For Chase format, we need to handle the Description column specially
            df['Description'] = df['Description'].fillna('')
            processed_df = process_chase_format(df)  # Chase format
        elif {'Date', 'Description', 'Card Member', 'Account #', 'Amount'} <= columns:  # Amex format
            processed_df = process_amex_format(df)  # Amex format
        elif {'Transaction Date', 'Posted Date', 'Description', 'Debit', 'Credit'} <= columns:  # Capital One format
            processed_df = process_capital_one_format(df)  # Capital One format
        elif {'Date', 'Description', 'Amount', 'Balance', 'Post Date'} <= columns:  # Alliant format
            processed_df = process_alliant_format(df)  # Alliant format
        elif {'Date', 'Description', 'Amount', 'Category', 'Tags', 'Account'} <= columns:  # Aggregator format
            processed_df = process_aggregator_format(df)  # Aggregator format
        
        if processed_df is not None:
            # Ensure source_file is preserved
            processed_df['source_file'] = df['source_file']
            
        logger.debug(f"Columns found in file: {columns}")
        return processed_df
    except Exception as e:
        logger.error(f"Error processing {filepath}: {str(e)}")
        return None

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
                # Ensure consistent column structure
                required_columns = ['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']
                for col in required_columns:
                    if col not in df.columns:
                        logger.warning(f"Missing required column {col} in {filename}")
                        continue
                
                # Add Category column if missing
                if 'Category' not in df.columns:
                    df['Category'] = ''
                else:
                    df['Category'] = df['Category'].fillna('')
                
                # Add Tags column if missing
                if 'Tags' not in df.columns:
                    df['Tags'] = ''
                else:
                    df['Tags'] = df['Tags'].fillna('')
                
                # Set source_file to the actual filename
                df['source_file'] = filename
                
                all_dfs.append(df)
    
    # Combine all DataFrames if any were found
    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        # Ensure final DataFrame has all required columns
        required_columns = ['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file', 'Category', 'Tags']
        for col in required_columns:
            if col not in result.columns:
                result[col] = ''
            else:
                result[col] = result[col].fillna('')
        return result[required_columns]  # Return only the required columns in the correct order
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
        # Use import_csv to read the aggregator file
        aggregator_df = import_csv(aggregator_path)
        if aggregator_df is None:
            logger.error("Failed to process aggregator file format")
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
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        return

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
