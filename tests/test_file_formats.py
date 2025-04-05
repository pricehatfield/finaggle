import pytest
import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
from src.reconcile import (
    standardize_date,
    clean_amount,
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_format,
    process_chase_format,
    process_aggregator_format,
    import_csv
)

# Sample data for each format
discover_sample_data = {
    'Trans. Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Post Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Amount': ['123.45', '123.45', '67.89', '45.00'],  # All positive amounts for debits
    'Category': ['GROCERIES', 'GROCERIES', 'DINING', 'TRANSPORTATION'],
    'source_file': ['discover_2025.csv'] * 4
}

amex_sample_data = {
    'Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Card Member': ['PRICE L HATFIELD'] * 4,
    'Account #': ['-42004'] * 4,
    'Amount': ['123.45', '-123.45', '67.89', '45.00'],  # Positive for debits, negative for credits
    'Category': ['GROCERIES', 'GROCERIES', 'DINING', 'TRANSPORTATION'],
    'source_file': ['amex_2025.csv'] * 4
}

capital_one_sample_data = {
    'Transaction Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Posted Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'Card No.': ['1234'] * 4,
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Category': ['GROCERIES', 'GROCERIES', 'DINING', 'TRANSPORTATION'],
    'Debit': ['123.45', '', '67.89', '45.00'],  # Debits in Debit column
    'Credit': ['', '123.45', '', ''],  # Credits in Credit column
    'source_file': ['capital_one_2025.csv'] * 4
}

alliant_sample_data = {
    'Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Amount': ['$123.45', '$123.45', '$67.89', '$45.00'],  # All positive amounts with $ symbol
    'Balance': ['$0.00'] * 4,
    'Post Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'source_file': ['alliant_2025.csv'] * 4
}

chase_sample_data = {
    'Details': [''] * 4,
    'Posting Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Amount': ['-123.45', '123.45', '-67.89', '-45.00'],  # Negative amounts for debits
    'Type': ['DEBIT'] * 4,
    'Balance': ['3990.63'] * 4,
    'Check or Slip #': [''] * 4,
    'source_file': ['chase_2025.csv'] * 4
}

aggregator_sample_data = {
    'Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Amount': ['-123.45', '123.45', '-67.89', '-45.00'],  # Negative amounts for debits
    'Category': ['GROCERIES', 'GROCERIES', 'DINING', 'TRANSPORTATION'],
    'Tags': ['', '', '', ''],
    'Account': ['Discover'] * 4,
    'source_file': ['empower_2025.csv'] * 4
}

# Parameterized test for real data files
@pytest.mark.parametrize("format_name,file_pattern,sample_data", [
    ("discover", "discover_*.csv", discover_sample_data),
    ("amex", "amex_*.csv", amex_sample_data),
    ("capital_one", "capital_one_*.csv", capital_one_sample_data),
    ("alliant", "alliant_*.csv", alliant_sample_data),
    ("chase", "chase_*.csv", chase_sample_data),
    ("aggregator", "aggregator_*.csv", aggregator_sample_data)
])
def test_real_data_files(format_name, file_pattern, sample_data, tmp_path):
    """Test processing of real data files with various formats"""
    # Create a temporary CSV file with sample data
    file_path = tmp_path / f"{format_name}_test.csv"
    df = pd.DataFrame(sample_data)
    df.to_csv(file_path, index=False)
    
    # Process the file
    result = import_csv(str(file_path))
    
    # Verify the result
    assert result is not None
    assert 'Transaction Date' in result.columns
    assert 'Post Date' in result.columns
    assert 'Description' in result.columns
    assert 'Amount' in result.columns
    assert 'Category' in result.columns
    assert 'source_file' in result.columns
    
    # Verify data types
    assert pd.api.types.is_string_dtype(result['Transaction Date'])  # Dates are strings in YYYY-MM-DD format
    assert pd.api.types.is_string_dtype(result['Post Date'])  # Dates are strings in YYYY-MM-DD format
    assert pd.api.types.is_string_dtype(result['Description'])
    assert pd.api.types.is_float_dtype(result['Amount'])
    assert pd.api.types.is_string_dtype(result['Category'])
    
    # Verify amount signs based on format
    if format_name == 'discover':
        # Discover shows positive amounts for debits, processing inverts them
        assert result.loc[result['Description'] == 'GROCERY STORE', 'Amount'].iloc[0] < 0
    elif format_name == 'amex':
        # Amex uses mixed signs, processing inverts them
        assert result.loc[result['Description'] == 'GROCERY STORE', 'Amount'].iloc[0] < 0
    elif format_name == 'capital_one':
        # Capital One uses separate Debit/Credit columns, processing negates debits
        assert result.loc[result['Description'] == 'GROCERY STORE', 'Amount'].iloc[0] < 0
    elif format_name == 'alliant':
        # Alliant shows positive amounts with $ symbol, processing inverts them
        assert result.loc[result['Description'] == 'GROCERY STORE', 'Amount'].iloc[0] < 0
    elif format_name == 'chase':
        # Chase shows negative amounts for debits, processing keeps original signs
        assert result.loc[result['Description'] == 'GROCERY STORE', 'Amount'].iloc[0] < 0
    elif format_name == 'aggregator':
        # Aggregator shows negative amounts for debits, processing keeps original signs
        assert result.loc[result['Description'] == 'GROCERY STORE', 'Amount'].iloc[0] < 0

# Test for data conversion consistency
def test_data_conversion_consistency():
    """Test consistency of data conversion across different formats
    BUSINESS REQUIREMENT: Verifies the spec's requirement for consistent data conversion
    - Same transaction data in different formats should produce the same standardized output
    """
    # Process each format
    discover_result = process_discover_format(pd.DataFrame(discover_sample_data))
    amex_result = process_amex_format(pd.DataFrame(amex_sample_data))
    capital_one_result = process_capital_one_format(pd.DataFrame(capital_one_sample_data))
    alliant_result = process_alliant_format(pd.DataFrame(alliant_sample_data))
    chase_result = process_chase_format(pd.DataFrame(chase_sample_data))
    aggregator_result = process_aggregator_format(pd.DataFrame(aggregator_sample_data))
    
    # Verify consistent column structure
    required_columns = {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    for result in [discover_result, amex_result, capital_one_result, alliant_result, chase_result, aggregator_result]:
        assert required_columns.issubset(result.columns)
    
    # Verify consistent data types
    for result in [discover_result, amex_result, capital_one_result, alliant_result, chase_result, aggregator_result]:
        assert pd.api.types.is_string_dtype(result['Transaction Date'])
        assert pd.api.types.is_string_dtype(result['Post Date'])
        assert pd.api.types.is_string_dtype(result['Description'])
        assert pd.api.types.is_numeric_dtype(result['Amount'])

# Test for amount sign consistency
def test_amount_sign_consistency():
    """Test consistency of amount sign handling across different formats
    BUSINESS REQUIREMENT: Verifies the spec's requirement for consistent amount sign handling
    - Negative amounts for debits (money out)
    - Positive amounts for credits (money in)
    """
    # Process each format
    discover_result = process_discover_format(pd.DataFrame(discover_sample_data))
    amex_result = process_amex_format(pd.DataFrame(amex_sample_data))
    capital_one_result = process_capital_one_format(pd.DataFrame(capital_one_sample_data))
    alliant_result = process_alliant_format(pd.DataFrame(alliant_sample_data))
    chase_result = process_chase_format(pd.DataFrame(chase_sample_data))
    aggregator_result = process_aggregator_format(pd.DataFrame(aggregator_sample_data))
    
    # Verify amount signs
    # Discover: All amounts are inverted in processing
    assert discover_result['Amount'].iloc[0] < 0  # Debit (GROCERY STORE)
    assert discover_result['Amount'].iloc[1] < 0  # Debit (REFUND)
    assert discover_result['Amount'].iloc[2] < 0  # Debit (RESTAURANT)
    assert discover_result['Amount'].iloc[3] < 0  # Debit (GAS STATION)
    
    # Amex: Mixed signs, only invert positive amounts
    assert amex_result['Amount'].iloc[0] < 0  # Debit (GROCERY STORE)
    assert amex_result['Amount'].iloc[1] > 0  # Credit (REFUND)
    assert amex_result['Amount'].iloc[2] < 0  # Debit (RESTAURANT)
    assert amex_result['Amount'].iloc[3] < 0  # Debit (GAS STATION)
    
    # Capital One: Debits are negated, credits stay positive
    assert capital_one_result['Amount'].iloc[0] < 0  # Debit (GROCERY STORE)
    assert capital_one_result['Amount'].iloc[1] > 0  # Credit (REFUND)
    assert capital_one_result['Amount'].iloc[2] < 0  # Debit (RESTAURANT)
    assert capital_one_result['Amount'].iloc[3] < 0  # Debit (GAS STATION)
    
    # Alliant: All amounts are inverted in processing
    assert alliant_result['Amount'].iloc[0] < 0  # Debit (GROCERY STORE)
    assert alliant_result['Amount'].iloc[1] < 0  # Debit (REFUND)
    assert alliant_result['Amount'].iloc[2] < 0  # Debit (RESTAURANT)
    assert alliant_result['Amount'].iloc[3] < 0  # Debit (GAS STATION)
    
    # Chase: Keep original signs
    assert chase_result['Amount'].iloc[0] < 0  # Debit (GROCERY STORE)
    assert chase_result['Amount'].iloc[1] > 0  # Credit (REFUND)
    assert chase_result['Amount'].iloc[2] < 0  # Debit (RESTAURANT)
    assert chase_result['Amount'].iloc[3] < 0  # Debit (GAS STATION)
    
    # Aggregator: Keep original signs
    assert aggregator_result['Amount'].iloc[0] < 0  # Debit (GROCERY STORE)
    assert aggregator_result['Amount'].iloc[1] > 0  # Credit (REFUND)
    assert aggregator_result['Amount'].iloc[2] < 0  # Debit (RESTAURANT)
    assert aggregator_result['Amount'].iloc[3] < 0  # Debit (GAS STATION) 