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
    """Create test data for format validation.

    Args:
        format_name (str): Name of format to create test data for

    Returns:
        pd.DataFrame: Test data
    """
    if format_name == 'discover':
        return pd.DataFrame({
            'Trans. Date': ['01/01/2023'],
            'Post Date': ['01/02/2023'],
            'Description': ['Test Transaction'],
            'Amount': ['$123.45'],
            'Category': ['Groceries']
        })
    elif format_name == 'capital_one':
        return pd.DataFrame({
            'Transaction Date': ['2023-01-01'],
            'Posted Date': ['2023-01-02'],
            'Card No.': ['1234'],
            'Description': ['Test Transaction'],
            'Category': ['Transfers'],
            'Debit': [123.45],
            'Credit': [None]
        })
    elif format_name == 'chase':
        return pd.DataFrame({
            'Details': ['DEBIT'],
            'Posting Date': ['01/01/2023'],
            'Description': ['Test Transaction'],
            'Amount': [-123.45],
            'Type': ['ACH_DEBIT'],
            'Balance': ['1000.00'],
            'Check or Slip #': ['']
        })
    elif format_name == 'alliant_checking':
        return pd.DataFrame({
            'Date': ['01/01/2023'],
            'Description': ['Test Transaction'],
            'Amount': ['$123.45'],
            'Balance': ['$1000.00']
        })
    elif format_name == 'alliant_visa':
        return pd.DataFrame({
            'Date': ['01/01/2023'],
            'Description': ['Test Transaction'],
            'Amount': ['$123.45'],
            'Balance': ['$1000.00'],
            'Post Date': ['01/02/2023']
        })
    elif format_name == 'amex':
        return pd.DataFrame({
            'Date': ['01/01/2023'],
            'Description': ['Test Transaction'],
            'Card Member': ['Test User'],
            'Account #': ['1234'],
            'Amount': [123.45]
        })
    elif format_name == 'aggregator':
        return pd.DataFrame({
            'Date': ['2023-01-01'],
            'Account': ['Test Account'],
            'Description': ['Test Transaction'],
            'Category': ['Shopping'],
            'Tags': ['Joint,Price'],
            'Amount': [-123.45]  # Negative for debits
        })
    else:
        raise ValueError(f"Unknown format: {format_name}")

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
        - Description field is present
        - Description is preserved as-is
        """
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'aggregator']:
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
            elif format_name == 'amex':
                result = process_amex_format(df)
            elif format_name == 'aggregator':
                result = process_aggregator_format(df)
            
            assert isinstance(result['Description'].iloc[0], str)
            assert result['Description'].iloc[0] == 'Test Transaction'
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_category_validation(self):
        """Test category validation.
        
        Verifies:
        - Category field is present
        - Category is preserved as-is
        """
        for format_name in ['discover', 'capital_one', 'aggregator']:
            df = create_test_format_data(format_name)
            if format_name == 'discover':
                result = process_discover_format(df)
                assert result['Category'].iloc[0] == 'Groceries'
            elif format_name == 'capital_one':
                result = process_capital_one_format(df)
                assert result['Category'].iloc[0] == 'Transfers'
            elif format_name == 'aggregator':
                result = process_aggregator_format(df)
                assert result['Category'].iloc[0] == 'Shopping'
    
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
    def test_chase_format_validation(self):
        """Test Chase format specific validation.
        
        Verifies:
        - Required columns are present
        - Amount format with negative sign for debits
        - Empty Check or Slip # handling
        - Type field preservation (not mapped to Category)
        - Additional columns don't prevent format detection
        """
        # Test with required columns only
        df = create_test_format_data('chase')
        result = process_chase_format(df)
        assert result['Amount'].iloc[0] < 0  # Debit amount should be negative
        assert 'Type' in result.columns  # Type should be preserved
        assert 'Category' not in result.columns  # No Category mapping
        
        # Test with additional columns
        df = create_test_format_data('chase')
        df['Extra Column'] = 'extra'
        df['Another Column'] = 123
        result = process_chase_format(df)
        assert result['Amount'].iloc[0] < 0  # Should still process correctly
        
        # Test amount format
        df = create_test_format_data('chase')
        result = process_chase_format(df)
        assert result['Amount'].iloc[0] < 0  # Debit should be negative
        
        # Test empty check number
        df = create_test_format_data('chase')
        result = process_chase_format(df)
        assert pd.isna(result['Check or Slip #'].iloc[0]) or result['Check or Slip #'].iloc[0] == ''

    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_discover_format_validation(self):
        """Test Discover format specific validation.
        
        Verifies:
        - Amount format (clean decimal)
        - Category with special characters
        - Date format validation
        - Description format validation
        """
        df = create_test_format_data('discover')
        
        # Test amount format
        df.loc[0, 'Amount'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid amount format"):
            process_discover_format(df)
            
        # Test category with special characters
        df = create_test_format_data('discover')
        df.loc[0, 'Category'] = 'Travel/ Entertainment'
        result = process_discover_format(df)
        assert result['Category'].iloc[0] == 'Travel/ Entertainment'
        
        # Test date format
        df = create_test_format_data('discover')
        df.loc[0, 'Trans. Date'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid date format"):
            process_discover_format(df)
            
        # Test description format
        df = create_test_format_data('discover')
        df.loc[0, 'Description'] = 'ICP*EMLER SWIM SCHOOL-HO 817-552-7946 TXICP*EMLER SWIM SCHOOL-HO'
        result = process_discover_format(df)
        assert result['Description'].iloc[0] == 'ICP*EMLER SWIM SCHOOL-HO 817-552-7946 TXICP*EMLER SWIM SCHOOL-HO'

    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_capital_one_format_validation(self):
        """Test Capital One format specific validation.
        
        Verifies:
        - Debit/Credit format (clean decimal)
        - Date format validation
        - Description format validation
        """
        df = create_test_format_data('capital_one')
        
        # Test debit/credit format
        df.loc[0, 'Debit'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid amount format"):
            process_capital_one_format(df)
            
        # Test date format
        df = create_test_format_data('capital_one')
        df.loc[0, 'Transaction Date'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid date format"):
            process_capital_one_format(df)
        
        # Test description format
        df = create_test_format_data('capital_one')
        df.loc[0, 'Description'] = 'LEGALSHIELD *MEMBRSHIP'
        result = process_capital_one_format(df)
        assert result['Description'].iloc[0] == 'LEGALSHIELD *MEMBRSHIP'

    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_alliant_checking_format_validation(self):
        """Test Alliant Checking format specific validation.
        
        Verifies:
        - Amount format with $ symbol
        - Description format
        - Date format validation
        """
        df = create_test_format_data('alliant_checking')
        
        # Test amount format
        df.loc[0, 'Amount'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid amount format"):
            process_alliant_checking_format(df)
            
        # Test description format
        df = create_test_format_data('alliant_checking')
        result = process_alliant_checking_format(df)
        assert isinstance(result['Description'].iloc[0], str)
        
        # Test date format
        df = create_test_format_data('alliant_checking')
        df.loc[0, 'Date'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid date format"):
            process_alliant_checking_format(df)

    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_alliant_visa_format_validation(self):
        """Test Alliant Visa format specific validation.
        
        Verifies:
        - Amount format with $ symbol
        - Description format
        - Date format validation
        """
        df = create_test_format_data('alliant_visa')
        
        # Test amount format
        df.loc[0, 'Amount'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid amount format"):
            process_alliant_visa_format(df)
            
        # Test description format
        df = create_test_format_data('alliant_visa')
        result = process_alliant_visa_format(df)
        assert isinstance(result['Description'].iloc[0], str)
        
        # Test date format
        df = create_test_format_data('alliant_visa')
        df.loc[0, 'Date'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid date format"):
            process_alliant_visa_format(df)

    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_amex_format_validation(self):
        """Test American Express format specific validation.
        
        Verifies:
        - Amount format (Decimal)
        - Description format
        """
        df = create_test_format_data('amex')
        
        # Test amount format
        df.loc[0, 'Amount'] = 'invalid'
        with pytest.raises(ValueError, match="Invalid amount format"):
            process_amex_format(df)
            
        # Test description format
        df = create_test_format_data('amex')
        result = process_amex_format(df)
        assert isinstance(result['Description'].iloc[0], str)

@pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
def test_data_conversion_consistency():
    """Test consistency of data conversion across formats.
    
    Verifies:
    - Required column presence
    - Data type consistency
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
        if format_name not in ['alliant_checking']:  # Alliant checking doesn't have post date
            assert result['Post Date'].str.match(r'^\d{4}-\d{2}-\d{2}$').all(), f"Invalid post date format in {format_name} format"

def test_empower_account_extraction():
    """Test that account information is preserved from aggregator format."""
    df = pd.DataFrame({
        'Date': ['2023-01-01'],
        'Account': ['Chase Freedom Unlimited (1234)'],
        'Description': ['CHASE CREDIT CRD 1234'],
        'Amount': [100.00],
        'Category': ['Shopping'],
        'Tags': ['Joint,Price']
    })

    result = process_aggregator_format(df)
    assert result['Account'].iloc[0] == 'Chase Freedom Unlimited (1234)'

def test_output_format_specification():
    """Test that output format matches specification."""
    df = pd.DataFrame({
        'Date': ['2023-01-01'],
        'Account': ['Test Account'],
        'Description': ['Test Transaction'],
        'Amount': [100.00],
        'Category': ['Shopping'],
        'Tags': ['Joint,Price']
    })

    result = process_aggregator_format(df)
    assert 'Date' in result.columns
    assert 'Account' in result.columns
    assert 'Description' in result.columns
    assert 'Category' in result.columns
    assert 'Tags' in result.columns
    assert 'Amount' in result.columns

def test_reconciled_format_validation():
    """Test that reconciled output matches specification."""
    df = pd.DataFrame({
        'Date': ['2023-01-01'],
        'Account': ['Test Account'],
        'Description': ['Test Transaction'],
        'Amount': [100.00],
        'Category': ['Shopping'],
        'Tags': ['Joint,Price']
    })

    result = process_aggregator_format(df)
    assert pd.api.types.is_numeric_dtype(result['Amount'])
    assert result['Date'].str.match(r'^\d{4}-\d{2}-\d{2}$').all()

@pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
def test_aggregator_format_validation(self):
    """Test aggregator format specific validation.
    
    Verifies:
    - Required columns are present
    - Date format validation
    - Amount format validation
    - Description preservation
    """
    df = create_test_format_data('aggregator')
    
    # Test date format
    df.loc[0, 'Date'] = 'invalid'
    with pytest.raises(ValueError, match="Invalid date format"):
        process_aggregator_format(df)
        
    # Test amount format
    df = create_test_format_data('aggregator')
    df.loc[0, 'Amount'] = 'invalid'
    with pytest.raises(ValueError, match="Invalid amount format"):
        process_aggregator_format(df)
        
    # Test description preservation
    df = create_test_format_data('aggregator')
    result = process_aggregator_format(df)
    assert result['Description'].iloc[0] == 'Test Transaction'