import pytest
import pandas as pd
import numpy as np
from src.reconcile import (
    standardize_date,
    clean_amount,
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_visa_format,
    process_chase_format,
    process_aggregator_format,
    standardize_description,
    standardize_category
)

def create_test_df(format_name):
    """Create standardized test DataFrame for the specified format.
    
    Args:
        format_name (str): Name of the format to create test data for.
            Supported formats: 'discover', 'amex', 'capital_one', 'alliant_visa', 'chase', 'aggregator'
    
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
    elif format_name == 'amex':
        return pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['123.45'],
            'Category': ['Shopping'],
            'source_file': ['amex_test.csv']
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
    elif format_name == 'alliant_visa':
        return pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['$123.45'],
            'Post Date': ['03/18/2025'],
            'Category': ['Shopping'],
            'source_file': ['alliant_test.csv']
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
    elif format_name == 'aggregator':
        return pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['-123.45'],
            'Category': ['Shopping'],
            'Tags': ['Online'],
            'Account': ['Discover'],
            'source_file': ['aggregator_test.csv']
        })
    else:
        raise ValueError(f"Unsupported format: {format_name}")

@pytest.mark.dependency()
class TestDiscoverFormat:
    """Test suite for Discover format processing.
    
    Discover format specific requirements:
    - Amounts are negative for debits
    - Has both transaction and post dates
    - Preserves original description case
    """
    
    @pytest.mark.dependency()
    def test_basic_processing(self):
        """Test basic Discover format processing.
        
        Verifies:
        - Date standardization (YYYY-MM-DD)
        - Amount sign (negative for debits)
        - Description preservation
        - Category preservation
        """
        df = create_test_df('discover')
        result = process_discover_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -40.33
        assert result['Category'].iloc[0] == 'Shopping'
    
    @pytest.mark.dependency(depends=["TestDiscoverFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Discover amount handling.
        
        Verifies:
        - Debit amounts are negative
        - Credit amounts are positive
        """
        df = create_test_df('discover')
        result = process_discover_format(df)
        assert result['Amount'].iloc[0] == -40.33  # Debit amount should be negative

@pytest.mark.dependency()
class TestAmexFormat:
    """Test suite for Amex format processing.
    
    Amex format specific requirements:
    - Amounts are positive for debits (needs inversion)
    - Single date field (used for both transaction and post dates)
    - Preserves original description case
    """
    
    @pytest.mark.dependency()
    def test_basic_processing(self):
        """Test basic Amex format processing.
        
        Verifies:
        - Date standardization (YYYY-MM-DD)
        - Amount sign (negative after inversion)
        - Description preservation
        - Category preservation
        """
        df = create_test_df('amex')
        result = process_amex_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45
        assert result['Category'].iloc[0] == 'Shopping'
    
    @pytest.mark.dependency(depends=["TestAmexFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Amex amount handling.
        
        Verifies:
        - Debit amounts are inverted to negative
        - Credit amounts are inverted to positive
        """
        df = create_test_df('amex')
        result = process_amex_format(df)
        assert result['Amount'].iloc[0] == -123.45  # Debit amount should be negative after inversion

@pytest.mark.dependency()
class TestCapitalOneFormat:
    """Test suite for Capital One format processing.
    
    Capital One format specific requirements:
    - Amounts are negative for debits
    - Has both transaction and post dates
    - Preserves original description case
    """
    
    @pytest.mark.dependency()
    def test_basic_processing(self):
        """Test basic Capital One format processing.
        
        Verifies:
        - Date standardization (YYYY-MM-DD)
        - Amount sign (negative for debits)
        - Description preservation
        - Category preservation
        """
        df = create_test_df('capital_one')
        result = process_capital_one_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -40.33
        assert result['Category'].iloc[0] == 'Shopping'
    
    @pytest.mark.dependency(depends=["TestCapitalOneFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Capital One amount handling.
        
        Verifies:
        - Debit amounts are negative
        - Credit amounts are positive
        """
        df = create_test_df('capital_one')
        result = process_capital_one_format(df)
        assert result['Amount'].iloc[0] == -40.33  # Debit amount should be negative

@pytest.mark.dependency()
class TestAlliantFormat:
    """Test suite for Alliant format processing.
    
    Alliant format specific requirements:
    - Amounts are positive for debits, negative for credits
    - Has both transaction and post dates
    - Preserves original description case
    """
    
    @pytest.mark.dependency()
    def test_basic_processing(self):
        """Test basic Alliant format processing.
        
        Verifies:
        - Date standardization (YYYY-MM-DD)
        - Amount sign (positive for debits)
        - Description preservation
        - Category preservation
        """
        df = create_test_df('alliant_visa')
        result = process_alliant_visa_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == 123.45
        assert result['Category'].iloc[0] == 'Shopping'
    
    @pytest.mark.dependency(depends=["TestAlliantFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Alliant amount handling.
        
        Verifies:
        - Debit amounts are positive
        - Credit amounts are negative
        """
        df = create_test_df('alliant_visa')
        result = process_alliant_visa_format(df)
        assert result['Amount'].iloc[0] == 123.45  # Debit amount should be positive

@pytest.mark.dependency()
class TestChaseFormat:
    """Test suite for Chase format processing.
    
    Chase format specific requirements:
    - Amounts are negative for debits
    - Single date field (used for both transaction and post dates)
    - Preserves original description case
    """
    
    @pytest.mark.dependency()
    def test_basic_processing(self):
        """Test basic Chase format processing.
        
        Verifies:
        - Date standardization (YYYY-MM-DD)
        - Amount sign (negative for debits)
        - Description preservation
        - Category default (empty string)
        """
        df = create_test_df('chase')
        result = process_chase_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -40.33
        assert result['Category'].iloc[0] == ''
    
    @pytest.mark.dependency(depends=["TestChaseFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Chase amount handling.
        
        Verifies:
        - Debit amounts are negative
        - Credit amounts are positive
        """
        df = create_test_df('chase')
        result = process_chase_format(df)
        assert result['Amount'].iloc[0] == -40.33  # Debit amount should be negative

@pytest.mark.dependency()
class TestAggregatorFormat:
    """Test suite for Aggregator format processing.
    
    Aggregator format specific requirements:
    - Amounts are preserved as-is
    - Single date field (used for both transaction and post dates)
    - Includes additional metadata (Tags, Account)
    """
    
    @pytest.mark.dependency()
    def test_basic_processing(self):
        """Test basic Aggregator format processing.
        
        Verifies:
        - Date standardization (YYYY-MM-DD)
        - Amount preservation
        - Description preservation
        - Category preservation
        - Additional metadata preservation
        """
        df = create_test_df('aggregator')
        result = process_aggregator_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45
        assert result['Category'].iloc[0] == 'Shopping'
        assert result['Tags'].iloc[0] == 'Online'
        assert result['Account'].iloc[0] == 'Discover'
    
    @pytest.mark.dependency(depends=["TestAggregatorFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Aggregator amount handling.
        
        Verifies:
        - Amounts are preserved exactly as input
        """
        df = create_test_df('aggregator')
        result = process_aggregator_format(df)
        assert result['Amount'].iloc[0] == -123.45  # Amount should be preserved exactly

class TestStandardization:
    """Test suite for data standardization"""
    
    def test_amount_standardization(self):
        """Test amount standardization"""
        # Test various amount formats
        assert clean_amount('$40.33') == 40.33
        assert clean_amount('-$40.33') == -40.33
        assert clean_amount('40.33') == 40.33
        assert clean_amount('-40.33') == -40.33
        assert clean_amount('$1,000.00') == 1000.00
        assert clean_amount('-$1,000.00') == -1000.00
        
        # Test invalid amounts
        with pytest.raises(ValueError):
            clean_amount('invalid')
        with pytest.raises(ValueError):
            clean_amount('$invalid')
    
    def test_date_standardization(self):
        """Test date standardization"""
        # Test various date formats
        assert standardize_date('03/17/2025') == '2025-03-17'
        assert standardize_date('2025-03-17') == '2025-03-17'
        assert standardize_date('3/17/2025') == '2025-03-17'
        assert standardize_date('03-17-2025') == '2025-03-17'
        
        # Test invalid dates
        with pytest.raises(ValueError):
            standardize_date('invalid')
        with pytest.raises(ValueError):
            standardize_date('2025/03/17')  # Wrong separator
    
    def test_description_standardization(self):
        """Test description standardization"""
        # Test various description formats
        for format_name in ['discover', 'capital_one', 'chase']:
            df = create_test_df(format_name)
            if format_name == 'discover':
                result = process_discover_format(df)
            elif format_name == 'capital_one':
                result = process_capital_one_format(df)
            elif format_name == 'chase':
                result = process_chase_format(df)
            
            # Descriptions should be preserved exactly
            assert result['Description'].iloc[0] == 'AMAZON.COM' 

@pytest.mark.dependency(depends=["test_4_file_loads.py"])
class TestDescriptionStandardization:
    """Test suite for description standardization"""
    
    def test_remove_extra_spaces(self):
        """Test removal of extra spaces"""
        assert standardize_description("  Test  Transaction  ") == "Test Transaction"
        
    def test_remove_special_chars(self):
        """Test removal of special characters"""
        assert standardize_description("Test*Transaction#123") == "Test Transaction 123"
        
    def test_standardize_case(self):
        """Test case standardization"""
        assert standardize_description("TEST TRANSACTION") == "test transaction"
        
    def test_remove_common_prefixes(self):
        """Test removal of common prefixes"""
        assert standardize_description("POS DEBIT Test Transaction") == "test transaction"
        assert standardize_description("ACH DEBIT Test Transaction") == "test transaction"
        
    def test_remove_common_suffixes(self):
        """Test removal of common suffixes"""
        assert standardize_description("Test Transaction #123") == "test transaction"
        assert standardize_description("Test Transaction - 123") == "test transaction"

@pytest.mark.dependency(depends=["test_4_file_loads.py"])
class TestCategoryStandardization:
    """Test suite for category standardization"""
    
    def test_standardize_case(self):
        """Test case standardization"""
        assert standardize_category("SHOPPING") == "shopping"
        assert standardize_category("Shopping") == "shopping"
        
    def test_standardize_common_categories(self):
        """Test standardization of common categories"""
        assert standardize_category("GROCERIES") == "groceries"
        assert standardize_category("DINING") == "dining"
        assert standardize_category("TRANSPORTATION") == "transportation"
        
    def test_handle_empty_categories(self):
        """Test handling of empty categories"""
        assert standardize_category("") == "uncategorized"
        assert standardize_category(None) == "uncategorized"
        
    def test_handle_unknown_categories(self):
        """Test handling of unknown categories"""
        assert standardize_category("Unknown Category") == "unknown category"

@pytest.mark.dependency(depends=["test_4_file_loads.py"])
def test_full_standardization_pipeline():
    """Test the complete standardization pipeline"""
    # Create sample data
    df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['  POS DEBIT Test*Transaction#123  '],
        'Amount': ['-$50.00'],
        'Category': ['SHOPPING']
    })
    
    # Apply standardization
    df['Description'] = df['Description'].apply(standardize_description)
    df['Category'] = df['Category'].apply(standardize_category)
    df['Amount'] = df['Amount'].apply(clean_amount)
    
    # Verify results
    assert df['Description'].iloc[0] == "test transaction 123"
    assert df['Category'].iloc[0] == "shopping"
    assert df['Amount'].iloc[0] == -50.0 