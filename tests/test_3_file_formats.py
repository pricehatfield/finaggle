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
    process_alliant_checking_format,
    process_alliant_visa_format,
    import_csv
)

# Sample data for each format
discover_sample_data = {
    'Trans. Date': ['01/01/2025'],
    'Post Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['40.33'],
    'Category': ['Supermarkets']
}

amex_sample_data = {
    'Transaction Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Post Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Card Member': ['PRICE L HATFIELD'] * 4,
    'Account #': ['-42004'] * 4,
    'Amount': ['123.45', '-123.45', '67.89', '45.00'],  # Positive for debits, negative for credits
    'Category': ['GROCERIES', 'GROCERIES', 'DINING', 'TRANSPORTATION'],
    'source_file': ['amex_2025.csv'] * 4
}

capital_one_sample_data = {
    'Transaction Date': ['2025-01-01'],
    'Posted Date': ['2025-01-02'],
    'Card No.': ['1234'],
    'Description': ['Test Transaction'],
    'Category': ['Shopping'],
    'Debit': ['$50.00'],
    'Credit': ['']
}

alliant_sample_data = {
    'Transaction Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Post Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Amount': ['$123.45', '$123.45', '$67.89', '$45.00'],  # All positive amounts with $ symbol
    'Balance': ['$0.00'] * 4,
    'source_file': ['alliant_2025.csv'] * 4
}

chase_sample_data = {
    'Details': ['DEBIT'],
    'Posting Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['-$95.89'],
    'Type': ['ACH_DEBIT'],
    'Balance': ['$1000.00'],
    'Check or Slip #': ['']
}

aggregator_sample_data = {
    'Transaction Date': ['03/15/2024', '03/15/2024', '03/16/2024', '03/17/2024'],
    'Post Date': ['03/16/2024', '03/16/2024', '03/17/2024', '03/18/2024'],
    'Description': ['GROCERY STORE', 'REFUND', 'RESTAURANT', 'GAS STATION'],
    'Amount': ['-123.45', '123.45', '-67.89', '-45.00'],  # Negative amounts for debits
    'Category': ['GROCERIES', 'GROCERIES', 'DINING', 'TRANSPORTATION'],
    'Tags': ['', '', '', ''],
    'Account': ['Discover'] * 4,
    'source_file': ['empower_2025.csv'] * 4
}

alliant_checking_sample_data = {
    'Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['-$50.00'],
    'Balance': ['$1000.00']
}

alliant_visa_sample_data = {
    'Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['$50.00'],
    'Balance': ['$1000.00'],
    'Post Date': ['01/02/2025']
}

def create_test_df(format_name):
    """Helper function to create test DataFrames with standardized format"""
    if format_name == 'discover':
        return pd.DataFrame(discover_sample_data)
    elif format_name == 'capital_one':
        return pd.DataFrame(capital_one_sample_data)
    elif format_name == 'chase':
        return pd.DataFrame(chase_sample_data)
    elif format_name == 'alliant_checking':
        return pd.DataFrame(alliant_checking_sample_data)
    elif format_name == 'alliant_visa':
        return pd.DataFrame(alliant_visa_sample_data)
    else:
        raise ValueError(f"Unknown format: {format_name}")

@pytest.mark.dependency(depends=["test_2_utils.py"])
class TestFormatValidation:
    """Test suite for format validation"""
    
    def test_invalid_data_types(self):
        """Test handling of invalid data types"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
            df = create_test_df(format_name)
            # Convert amounts to strings
            if format_name == 'discover':
                df['Amount'] = df['Amount'].astype(str)
            elif format_name == 'capital_one':
                df['Debit'] = df['Debit'].astype(str)
                df['Credit'] = df['Credit'].astype(str)
            elif format_name == 'chase':
                df['Amount'] = df['Amount'].astype(str)
            elif format_name == 'alliant_checking':
                df['Amount'] = df['Amount'].astype(str)
            elif format_name == 'alliant_visa':
                df['Amount'] = df['Amount'].astype(str)
            
            # Should not raise any errors
            if format_name == 'discover':
                process_discover_format(df)
            elif format_name == 'capital_one':
                process_capital_one_format(df)
            elif format_name == 'chase':
                process_chase_format(df)
            elif format_name == 'alliant_checking':
                process_alliant_checking_format(df)
            elif format_name == 'alliant_visa':
                process_alliant_visa_format(df)
    
    def test_amount_validation(self):
        """Test amount validation"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
            df = create_test_df(format_name)
            # Test invalid amounts
            if format_name == 'discover':
                df.loc[0, 'Amount'] = 'invalid'
                with pytest.raises(ValueError):
                    process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Debit'] = 'invalid'
                with pytest.raises(ValueError):
                    process_capital_one_format(df)
            elif format_name == 'chase':
                df.loc[0, 'Amount'] = 'invalid'
                with pytest.raises(ValueError):
                    process_chase_format(df)
            elif format_name == 'alliant_checking':
                df.loc[0, 'Amount'] = 'invalid'
                with pytest.raises(ValueError):
                    process_alliant_checking_format(df)
            elif format_name == 'alliant_visa':
                df.loc[0, 'Amount'] = 'invalid'
                with pytest.raises(ValueError):
                    process_alliant_visa_format(df)
    
    def test_date_validation(self):
        """Test date validation"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
            df = create_test_df(format_name)
            # Test invalid dates
            if format_name == 'discover':
                df.loc[0, 'Trans. Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Transaction Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_capital_one_format(df)
            elif format_name == 'chase':
                df.loc[0, 'Posting Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_chase_format(df)
            elif format_name == 'alliant_checking':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_alliant_checking_format(df)
            elif format_name == 'alliant_visa':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_alliant_visa_format(df)
    
    def test_description_validation(self):
        """Test description validation"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
            df = create_test_df(format_name)
            # Test empty descriptions
            if format_name == 'discover':
                df.loc[0, 'Description'] = ''
                with pytest.raises(ValueError):
                    process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Description'] = ''
                with pytest.raises(ValueError):
                    process_capital_one_format(df)
            elif format_name == 'chase':
                df.loc[0, 'Description'] = ''
                with pytest.raises(ValueError):
                    process_chase_format(df)
            elif format_name == 'alliant_checking':
                df.loc[0, 'Description'] = ''
                with pytest.raises(ValueError):
                    process_alliant_checking_format(df)
            elif format_name == 'alliant_visa':
                df.loc[0, 'Description'] = ''
                with pytest.raises(ValueError):
                    process_alliant_visa_format(df)
    
    def test_category_validation(self):
        """Test category validation"""
        for format_name in ['discover', 'capital_one']:
            df = create_test_df(format_name)
            # Test invalid categories
            if format_name == 'discover':
                df.loc[0, 'Category'] = ''
                # Should not raise error for empty category
                process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Category'] = ''
                # Should not raise error for empty category
                process_capital_one_format(df)
    
    def test_date_order_validation(self):
        """Test date order validation"""
        for format_name in ['discover', 'capital_one']:
            df = create_test_df(format_name)
            # Test post date before transaction date
            if format_name == 'discover':
                df.loc[0, 'Trans. Date'] = '01/02/2025'
                df.loc[0, 'Post Date'] = '01/01/2025'
                with pytest.raises(ValueError):
                    process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Transaction Date'] = '2025-01-02'
                df.loc[0, 'Posted Date'] = '2025-01-01'
                with pytest.raises(ValueError):
                    process_capital_one_format(df)
    
    def test_duplicate_validation(self):
        """Test duplicate validation"""
        for format_name in ['discover', 'capital_one', 'chase']:
            df = create_test_df(format_name)
            # Duplicate the row
            df = pd.concat([df, df], ignore_index=True)
            # Should not raise error for duplicates
            if format_name == 'discover':
                process_discover_format(df)
            elif format_name == 'capital_one':
                process_capital_one_format(df)
            elif format_name == 'chase':
                process_chase_format(df)

@pytest.mark.dependency(depends=["test_2_utils.py"])
@pytest.mark.parametrize("format_name,file_pattern,sample_data", [
    ("discover", "discover_*.csv", discover_sample_data),
    ("amex", "amex_*.csv", amex_sample_data),
    ("capital_one", "capital_one_*.csv", capital_one_sample_data),
    ("alliant", "alliant_*.csv", alliant_sample_data),
    ("chase", "chase_*.csv", chase_sample_data),
    ("aggregator", "aggregator_*.csv", aggregator_sample_data),
    ("alliant_checking", "alliant_checking_*.csv", alliant_checking_sample_data),
    ("alliant_visa", "alliant_visa_*.csv", alliant_visa_sample_data)
])
def test_real_data_files(format_name, file_pattern, sample_data, tmp_path):
    """Test reading and validating real data files"""
    # Create test file
    df = pd.DataFrame(sample_data)
    file_path = tmp_path / f"{format_name}_test.csv"
    df.to_csv(file_path, index=False)
    
    # Read and validate
    result = import_csv(file_path)
    assert not result.empty
    assert set(result.columns) == set(sample_data.keys())

@pytest.mark.dependency(depends=["test_2_utils.py"])
def test_data_conversion_consistency():
    """Test consistency of data conversion across formats"""
    for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
        df = create_test_df(format_name)
        if format_name == 'discover':
            result = process_discover_format(df)
        elif format_name == 'capital_one':
            result = process_capital_one_format(df)
        elif format_name == 'chase':
            result = process_chase_format(df)
        elif format_name == 'alliant_checking':
            result = process_alliant_checking_format(df)
        elif format_name == 'alliant_visa':
            result = process_alliant_visa_format(df)
        
        # Verify standardized columns
        assert set(result.columns) == {
            'Transaction Date',
            'Post Date',
            'Description',
            'Amount',
            'Category',
            'source_file'
        }

@pytest.mark.dependency(depends=["test_2_utils.py"])
def test_amount_sign_consistency():
    """Test consistency of amount signs across formats"""
    for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
        df = create_test_df(format_name)
        if format_name == 'discover':
            result = process_discover_format(df)
            assert result['Amount'].iloc[0] < 0  # Discover amounts should be negative
        elif format_name == 'capital_one':
            result = process_capital_one_format(df)
            assert result['Amount'].iloc[0] < 0  # Capital One debits should be negative
        elif format_name == 'chase':
            result = process_chase_format(df)
            assert result['Amount'].iloc[0] < 0  # Chase amounts are already negative
        elif format_name == 'alliant_checking':
            result = process_alliant_checking_format(df)
            assert result['Amount'].iloc[0] < 0  # Alliant Checking debits should be negative
        elif format_name == 'alliant_visa':
            result = process_alliant_visa_format(df)
            assert result['Amount'].iloc[0] < 0  # Alliant Visa amounts should be negative 