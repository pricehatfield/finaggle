import pytest
import pandas as pd
import numpy as np
from src.reconcile import (
    standardize_date,
    clean_amount,
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_format,
    process_chase_format,
    process_aggregator_format
)

def test_standardize_date():
    """Test date standardization function with various formats"""
    test_cases = {
        '2025-03-17': '2025-03-17',  # ISO format
        '03/17/2025': '2025-03-17',  # US format
        '17-03-2025': '2025-03-17',  # UK format
        '20250317': '2025-03-17',    # Compact format
        '3/17/25': '2025-03-17',     # Short year format
        None: None,                   # None handling
        '': None,                     # Empty string
        np.nan: None,                 # NaN handling
    }
    
    for input_date, expected in test_cases.items():
        assert standardize_date(input_date) == expected

def test_clean_amount():
    """Test amount cleaning function with various formats"""
    test_cases = {
        '$123.45': 123.45,           # Dollar sign
        '123.45': 123.45,            # Decimal
        '-123.45': -123.45,          # Negative
        '$-123.45': -123.45,         # Dollar sign with negative
        '1,234.56': 1234.56,         # Thousands separator
        '$1,234.56': 1234.56,        # Dollar sign with thousands
        '': 0.0,                     # Empty string
        None: 0.0,                   # None handling
        np.nan: 0.0,                 # NaN handling
    }
    
    for input_amount, expected in test_cases.items():
        assert clean_amount(input_amount) == expected

def test_process_discover_format():
    """Test Discover format processing"""
    input_data = {
        'Trans. Date': ['03/17/2025', '03/16/2025'],
        'Post Date': ['03/18/2025', '03/17/2025'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Amount': ['123.45', '67.89'],
        'Category': ['Shopping', 'Groceries'],
    }
    df = pd.DataFrame(input_data)
    df['source_file'] = 'discover_test.csv'
    
    result = process_discover_format(df)
    
    assert list(result.columns) == ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']
    assert result['Transaction Date'].iloc[0] == '2025-03-17'
    assert result['Post Date'].iloc[0] == '2025-03-18'
    assert result['Description'].iloc[0] == 'AMAZON.COM'
    assert result['Amount'].iloc[0] == -123.45  # Discover amounts are inverted
    assert result['Category'].iloc[0] == 'Shopping'
    assert result['Account'].iloc[0] == 'Discover'

def test_process_amex_format():
    """Test Amex format processing"""
    input_data = {
        'Date': ['03/17/2025', '03/16/2025'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Card Member': ['JOHN DOE', 'JOHN DOE'],
        'Account #': ['1234', '1234'],
        'Amount': ['123.45', '67.89'],
    }
    df = pd.DataFrame(input_data)
    df['source_file'] = 'amex_test.csv'
    
    result = process_amex_format(df)
    
    assert list(result.columns) == ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']
    assert result['Transaction Date'].iloc[0] == '2025-03-17'
    assert result['Description'].iloc[0] == 'AMAZON.COM'
    assert result['Amount'].iloc[0] == -123.45  # Amex amounts are inverted
    assert result['Account'].iloc[0] == 'Amex'

def test_process_capital_one_format():
    """Test Capital One format processing"""
    input_data = {
        'Transaction Date': ['03/17/2025', '03/16/2025'],
        'Posted Date': ['03/18/2025', '03/17/2025'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Debit': ['123.45', ''],
        'Credit': ['', '67.89'],
        'Category': ['Shopping', 'Groceries'],
    }
    df = pd.DataFrame(input_data)
    df['source_file'] = 'capital_one_test.csv'
    
    result = process_capital_one_format(df)
    
    assert list(result.columns) == ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']
    assert result['Transaction Date'].iloc[0] == '2025-03-17'
    assert result['Post Date'].iloc[0] == '2025-03-18'
    assert result['Description'].iloc[0] == 'AMAZON.COM'
    assert result['Amount'].iloc[0] == -123.45  # Debit becomes negative
    assert result['Amount'].iloc[1] == 67.89    # Credit stays positive
    assert result['Category'].iloc[0] == 'Shopping'
    assert result['Account'].iloc[0] == 'Capital One'

def test_process_alliant_format():
    """Test Alliant format processing"""
    input_data = {
        'Date': ['03/17/2025', '03/16/2025'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Amount': ['$123.45', '$67.89'],
        'Balance': ['$1000.00', '$876.55'],
        'Post Date': ['03/18/2025', '03/17/2025'],
    }
    df = pd.DataFrame(input_data)
    df['source_file'] = 'alliant_test.csv'
    
    result = process_alliant_format(df)
    
    assert list(result.columns) == ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']
    assert result['Transaction Date'].iloc[0] == '2025-03-17'
    assert result['Post Date'].iloc[0] == '2025-03-18'
    assert result['Description'].iloc[0] == 'AMAZON.COM'
    assert result['Amount'].iloc[0] == -123.45  # Amounts are inverted
    assert result['Account'].iloc[0] == 'Alliant'

def test_process_chase_format():
    """Test Chase format processing"""
    input_data = {
        'Details': ['03/17/2025', '03/16/2025'],
        'Posting Date': ['AMAZON.COM', 'WALMART'],
        'Description': ['-123.45', '-67.89'],
        'Amount': ['0.00', '0.00'],
        'Type': ['DEBIT', 'DEBIT'],
        'Balance': ['1000.00', '876.55'],
    }
    df = pd.DataFrame(input_data)
    df['source_file'] = 'chase_test.csv'
    
    result = process_chase_format(df)
    
    assert list(result.columns) == ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']
    assert result['Transaction Date'].iloc[0] == '2025-03-17'
    assert result['Description'].iloc[0] == 'AMAZON.COM'
    assert result['Amount'].iloc[0] == -123.45  # Amount from Description column
    assert result['Account'].iloc[0] == 'Chase'

def test_process_aggregator_format():
    """Test Aggregator format processing"""
    input_data = {
        'Date': ['03/17/2025', '03/16/2025'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Amount': ['-123.45', '-67.89'],
        'Account': ['Chase', 'Discover'],
        'Category': ['Shopping', 'Groceries'],
        'Tags': ['Online', 'In-Store']
    }
    df = pd.DataFrame(input_data)
    df['source_file'] = 'aggregator_test.csv'
    
    result = process_aggregator_format(df)
    
    assert list(result.columns) == ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'source_file']
    assert result['Transaction Date'].iloc[0] == '2025-03-17'
    assert result['Description'].iloc[0] == 'AMAZON.COM'
    assert result['Amount'].iloc[0] == -123.45  # Amount preserved
    assert result['Account'].iloc[0] == 'Chase'
    assert result['Category'].iloc[0] == 'Shopping'  # Category preserved
    assert result['Tags'].iloc[0] == 'Online'      # Tags preserved 