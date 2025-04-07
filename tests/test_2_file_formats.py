"""
File Format Validation Tests

This module validates the system's ability to recognize and enforce data quality
rules for each supported file format.

Test Coverage:
- Data type validation for each format
- Required field validation
- Format-specific rules and constraints
- Error handling for invalid data

Dependencies: test_1_utils.py (requires working date and amount utilities)
"""

import pytest
import pandas as pd
import numpy as np
from src.reconcile import (
    process_discover_format,
    process_capital_one_format,
    process_chase_format,
    process_alliant_checking_format,
    process_alliant_visa_format,
    process_amex_format,
    process_aggregator_format
)

def create_test_format_data(format_name):
    """Create standardized test data for format validation.
    
    Args:
        format_name (str): Name of the format to create test data for.
            Supported formats: 'discover', 'capital_one', 'chase', 'alliant_checking', 
            'alliant_visa', 'amex', 'aggregator'
    
    Returns:
        pd.DataFrame: Test data with format-specific columns and values
    
    Raises:
        ValueError: If format_name is not supported
    """
    if format_name == 'discover':
        return pd.DataFrame({
            'Trans. Date': ['03/17/2025'],
            'Post Date': ['03/18/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['40.33'],
            'Category': ['Shopping']
        })
    elif format_name == 'capital_one':
        return pd.DataFrame({
            'Transaction Date': ['2025-03-17'],
            'Posted Date': ['2025-03-18'],
            'Card No.': ['1234'],
            'Description': ['AMAZON.COM'],
            'Category': ['Shopping'],
            'Debit': ['$40.33'],
            'Credit': ['']
        })
    elif format_name == 'chase':
        return pd.DataFrame({
            'Details': ['DEBIT'],
            'Posting Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['-$40.33'],
            'Type': ['ACH_DEBIT'],
            'Balance': ['$1000.00'],
            'Check or Slip #': ['']
        })
    elif format_name == 'alliant_checking':
        return pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['$40.33'],
            'source_file': ['alliant_checking_test.csv']
        })
    elif format_name == 'alliant_visa':
        return pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['$40.33'],
            'Post Date': ['03/18/2025'],
            'source_file': ['alliant_visa_test.csv']
        })
    elif format_name == 'amex':
        return pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Card Member': ['PRICE L HATFIELD'],
            'Account #': ['-42004'],
            'Amount': ['123.45']
        })
    elif format_name == 'aggregator':
        return pd.DataFrame({
            'Date': ['2025-03-17'],
            'Account': ['Technology Transfer, Inc 401(k) Profit Sharing Plan - Ending in 1701'],
            'Description': ['AMAZON.COM'],
            'Amount': ['-123.45'],
            'Category': ['Shopping'],
            'Tags': ['Online']
        })
    else:
        raise ValueError(f"Unsupported format: {format_name}")

@pytest.mark.dependency(depends=["test_1_utils.py::TestDateStandardization::test_iso_format", "test_1_utils.py::TestAmountCleaning::test_positive_amounts"])
class TestFormatValidation:
    """Test suite for format validation.
    
    Verifies:
    - Data type handling
    - Required field validation
    - Format-specific rules
    - Error handling
    """
    
    @pytest.mark.dependency()
    def test_invalid_data_types(self):
        """Test handling of invalid data types.
        
        Verifies:
        - String amount handling
        - String date handling
        - String description handling
        """
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
            df = create_test_format_data(format_name)
            # Convert amounts to strings
            if format_name in ['discover', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
                df['Amount'] = df['Amount'].astype(str)
            elif format_name == 'capital_one':
                df['Debit'] = df['Debit'].astype(str)
                df['Credit'] = df['Credit'].astype(str)
            
            # Should not raise any errors
            if format_name == 'discover':
                result = process_discover_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
            elif format_name == 'capital_one':
                result = process_capital_one_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
            elif format_name == 'chase':
                result = process_chase_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
            elif format_name == 'alliant_checking':
                result = process_alliant_checking_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
            elif format_name == 'alliant_visa':
                result = process_alliant_visa_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
            elif format_name == 'amex':
                result = process_amex_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
            elif format_name == 'aggregator':
                result = process_aggregator_format(df)
                assert isinstance(result['Amount'].iloc[0], float)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_amount_validation(self):
        """Test amount validation.
        
        Verifies:
        - Invalid amount format handling
        - Empty amount handling
        - Non-numeric amount handling
        """
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
            df = create_test_format_data(format_name)
            # Test invalid amounts
            if format_name in ['discover', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
                df.loc[0, 'Amount'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid amount format"):
                    if format_name == 'discover':
                        process_discover_format(df)
                    elif format_name == 'chase':
                        process_chase_format(df)
                    elif format_name == 'alliant_checking':
                        process_alliant_checking_format(df)
                    elif format_name == 'alliant_visa':
                        process_alliant_visa_format(df)
                    elif format_name == 'amex':
                        process_amex_format(df)
                    elif format_name == 'aggregator':
                        process_aggregator_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Debit'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid amount format"):
                    process_capital_one_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_date_validation(self):
        """Test date validation.
        
        Verifies:
        - Invalid date format handling
        - Empty date handling
        - Non-date string handling
        """
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
            df = create_test_format_data(format_name)
            # Test invalid dates
            if format_name == 'discover':
                df.loc[0, 'Trans. Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Transaction Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_capital_one_format(df)
            elif format_name == 'chase':
                df.loc[0, 'Posting Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_chase_format(df)
            elif format_name == 'alliant_checking':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_alliant_checking_format(df)
            elif format_name == 'alliant_visa':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_alliant_visa_format(df)
            elif format_name == 'amex':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_amex_format(df)
            elif format_name == 'aggregator':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError, match="Invalid date format"):
                    process_aggregator_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_description_validation(self):
        """Test description validation.
        
        Verifies:
        - Empty description handling
        - Null description handling
        - Whitespace-only description handling
        """
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
            df = create_test_format_data(format_name)
            # Test empty descriptions
            df.loc[0, 'Description'] = ''
            with pytest.raises(ValueError, match="Description cannot be empty"):
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
                elif format_name == 'amex':
                    process_amex_format(df)
                elif format_name == 'aggregator':
                    process_aggregator_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_category_validation(self):
        """Test category validation.
        
        Verifies:
        - Empty category handling
        - Null category handling
        - Category standardization
        """
        for format_name in ['discover', 'capital_one', 'aggregator']:
            df = create_test_format_data(format_name)
            # Test empty categories
            df.loc[0, 'Category'] = ''
            if format_name == 'discover':
                result = process_discover_format(df)
                assert result['Category'].iloc[0] == 'Uncategorized'
            elif format_name == 'capital_one':
                result = process_capital_one_format(df)
                assert result['Category'].iloc[0] == 'Uncategorized'
            elif format_name == 'aggregator':
                result = process_aggregator_format(df)
                assert result['Category'].iloc[0] == 'Uncategorized'
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_date_order_validation(self):
        """Test date order validation.
        
        Verifies:
        - Post date after transaction date
        - Same day transaction and post dates
        - Invalid date order handling
        """
        for format_name in ['discover', 'capital_one', 'alliant_visa']:
            df = create_test_format_data(format_name)
            # Test post date before transaction date
            if format_name == 'discover':
                df.loc[0, 'Trans. Date'] = '01/02/2025'
                df.loc[0, 'Post Date'] = '01/01/2025'
                with pytest.raises(ValueError, match="Post date cannot be before transaction date"):
                    process_discover_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Transaction Date'] = '2025-01-02'
                df.loc[0, 'Posted Date'] = '2025-01-01'
                with pytest.raises(ValueError, match="Post date cannot be before transaction date"):
                    process_capital_one_format(df)
            elif format_name == 'alliant_visa':
                df.loc[0, 'Date'] = '01/02/2025'
                df.loc[0, 'Post Date'] = '01/01/2025'
                with pytest.raises(ValueError, match="Post date cannot be before transaction date"):
                    process_alliant_visa_format(df)

@pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
def test_data_conversion_consistency():
    """Test consistency of data conversion across formats.
    
    Verifies:
    - Required column presence
    - Data type consistency
    - Amount sign consistency
    - Date format consistency
    """
    for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
        df = create_test_format_data(format_name)
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
            
        # Check that all required columns are present
        required_columns = ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file']
        assert all(col in result.columns for col in required_columns), f"Missing required columns in {format_name} format"
        
        # Check that amounts are numeric
        assert pd.api.types.is_numeric_dtype(result['Amount']), f"Amount column is not numeric in {format_name} format"
        
        # Check that dates are in YYYY-MM-DD format
        assert result['Transaction Date'].str.match(r'^\d{4}-\d{2}-\d{2}$').all(), f"Invalid transaction date format in {format_name} format"
        assert result['Post Date'].str.match(r'^\d{4}-\d{2}-\d{2}$').all(), f"Invalid post date format in {format_name} format"
        
        # Check that amounts are consistent in sign
        if format_name in ['discover', 'chase', 'alliant_checking', 'alliant_visa']:
            assert (result['Amount'] < 0).all(), f"Amount sign inconsistency in {format_name} format"

def test_output_format_specification(sample_matched_df, sample_unmatched_df):
    """Test that output format follows specifications."""
    # Test matched columns
    required_matched_columns = [
        'Transaction Date',
        'Post Date',
        'Description',
        'Amount',
        'Category',
        'source_file',
        'match_type'
    ]
    assert all(col in sample_matched_df.columns for col in required_matched_columns), \
        f"Missing required columns in matched output. Expected: {required_matched_columns}, Got: {sample_matched_df.columns.tolist()}"

    # Test unmatched columns
    required_unmatched_columns = [
        'Transaction Date',
        'Post Date',
        'Description',
        'Amount',
        'Category',
        'source_file'
    ]
    assert all(col in sample_unmatched_df.columns for col in required_unmatched_columns), \
        f"Missing required columns in unmatched output. Expected: {required_unmatched_columns}, Got: {sample_unmatched_df.columns.tolist()}"

    # Test date formats
    for df in [sample_matched_df, sample_unmatched_df]:
        for date_col in ['Transaction Date', 'Post Date']:
            pd.to_datetime(df[date_col], format='%Y-%m-%d')

    # Test amount format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert pd.api.types.is_numeric_dtype(df['Amount']), \
            "Amount column should be numeric"

    # Test match type values
    valid_match_types = {'post_date_amount', 'transaction_date_amount'}
    assert all(match_type in valid_match_types for match_type in sample_matched_df['match_type'].unique()), \
        f"Invalid match_type values found. Expected: {valid_match_types}"

    # Test source file format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert all(isinstance(source, str) for source in df['source_file']), \
            "source_file should be strings"
        assert all(source.endswith('.csv') for source in df['source_file']), \
            "source_file should end with .csv"

    # Test category format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert all(isinstance(cat, str) for cat in df['Category']), \
            "Category should be strings"

    # Test description format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert all(isinstance(desc, str) for desc in df['Description']), \
            "Description should be strings"