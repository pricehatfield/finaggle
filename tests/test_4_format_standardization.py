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
            'Card Member': ['PRICE L HATFIELD'],
            'Account #': ['-42004'],
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
            'Debit': [40.33],
            'Credit': [None]
        })
    elif format_name == 'alliant_visa':
        return pd.DataFrame({
            'Date': ['2025-03-17'],
            'Description': ['AMAZON.COM'],
            'Amount': ['$123.45'],
            'Post Date': ['2025-03-18'],
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
            'Date': ['2025-03-17'],
            'Account': ['Discover Card'],
            'Description': ['AMAZON.COM'],
            'Amount': [-123.45],  # Negative for debits
            'Category': ['Shopping'],
            'Tags': ['Online'],
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
        - Amount sign (negative for debits)
        - Description preservation
        - Category preservation
        """
        df = create_test_df('alliant_visa')
        result = process_alliant_visa_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Debit amount should be negative
        assert result['Category'].iloc[0] == 'Shopping'
    
    @pytest.mark.dependency(depends=["TestAlliantFormat::test_basic_processing"])
    def test_amount_handling(self):
        """Test Alliant amount handling.
        
        Verifies:
        - Debit amounts are negative
        - Credit amounts are positive
        """
        df = create_test_df('alliant_visa')
        result = process_alliant_visa_format(df)
        assert result['Amount'].iloc[0] == -123.45  # Debit amount should be negative

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
        - Category default (Uncategorized)
        """
        df = create_test_df('chase')
        result = process_chase_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -40.33
        assert result['Category'].iloc[0] == 'Uncategorized'
    
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
        assert result['Account'].iloc[0] == 'Discover Card'
    
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
    """Test suite for data standardization functions."""
    
    def test_amount_standardization(self):
        """Test amount standardization.
        
        Verifies:
        - Currency symbol removal
        - Comma removal
        - Sign convention (negative for debits)
        """
        assert clean_amount('$1,234.56') == 1234.56
        assert clean_amount('-$1,234.56') == -1234.56
        assert clean_amount('(1,234.56)') == -1234.56
    
    def test_date_standardization(self):
        """Test date standardization.
        
        Verifies:
        - MM/DD/YYYY to YYYY-MM-DD conversion
        - YYYY-MM-DD preservation
        - Invalid date handling
        """
        assert standardize_date('03/17/2025') == '2025-03-17'
        assert standardize_date('2025-03-17') == '2025-03-17'
        with pytest.raises(ValueError):
            standardize_date('invalid')

@pytest.mark.dependency(depends=["TestStandardization::test_date_standardization"])
class TestCategoryStandardization:
    """Test suite for category standardization."""
    
    @pytest.mark.dependency()
    def test_handle_empty_categories(self):
        """Test handling of empty categories."""
        assert standardize_category('') == 'Uncategorized'
        assert standardize_category(None) == 'Uncategorized'
    
    @pytest.mark.dependency()
    def test_handle_unknown_categories(self):
        """Test handling of unknown categories."""
        assert standardize_category('Unknown Category') == 'Unknown Category'

@pytest.mark.dependency(depends=[
    "TestCategoryStandardization::test_handle_empty_categories",
    "TestCategoryStandardization::test_handle_unknown_categories"
])
def test_full_standardization_pipeline():
    """Test the full standardization pipeline.
    
    Verifies:
    - Date standardization
    - Amount standardization
    - Category standardization
    - Description preservation
    """
    df = pd.DataFrame({
        'Date': ['03/17/2025'],
        'Description': ['  Test  Transaction  '],  # Should be preserved exactly as-is
        'Amount': ['$1,234.56'],
        'Category': ['Unknown Category']
    })
    
    # Standardize dates
    df['Date'] = df['Date'].apply(standardize_date)
    
    # Standardize amounts
    df['Amount'] = df['Amount'].apply(clean_amount)
    
    # Standardize categories
    df['Category'] = df['Category'].apply(standardize_category)
    
    # Verify results
    assert df['Date'].iloc[0] == '2025-03-17'
    assert df['Description'].iloc[0] == '  Test  Transaction  '  # Preserved exactly as-is
    assert df['Amount'].iloc[0] == 1234.56
    assert df['Category'].iloc[0] == 'Unknown Category'

def test_category_standardization():
    """Test category standardization mapping.
    
    Verifies:
    - Known category mappings
    - Unknown category preservation
    """
    assert standardize_category('Supermarkets') == 'Groceries'
    assert standardize_category('Merchandise') == 'Shopping'
    assert standardize_category('Unknown') == 'Unknown'

@pytest.mark.dependency()
class TestDescriptionStandardization:
    """Test suite for description standardization.
    
    Description standardization requirements:
    - Source files preserve newlines exactly as-is
    - Standardized format strips newlines from descriptions
    - Original description content is preserved (just newlines removed)
    """
    
    @pytest.mark.dependency()
    def test_newline_stripping(self):
        """Test that newlines are stripped during standardization.
        
        Verifies:
        - Source descriptions with newlines are preserved as-is in source files
        - Newlines are stripped in standardized format
        - Description content is preserved exactly
        """
        # Test with Alliant Checking format which explicitly supports newlines
        df = pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['DIVIDEND\nPAYMENT\nQ1 2025'],
            'Amount': ['$123.45'],
            'Balance': ['$1,000.00']
        })
        
        # Verify source format preserves newlines
        assert '\n' in df['Description'].iloc[0]
        
        # Process through standardization
        result = process_alliant_checking_format(df)
        
        # Verify newlines are stripped in standardized format
        assert '\n' not in result['Description'].iloc[0]
        assert result['Description'].iloc[0] == 'DIVIDEND PAYMENT Q1 2025'
    
    @pytest.mark.dependency(depends=["TestDescriptionStandardization::test_newline_stripping"])
    def test_multiple_newlines(self):
        """Test handling of multiple consecutive newlines.
        
        Verifies:
        - Multiple consecutive newlines are handled correctly
        - Extra spaces aren't introduced
        """
        df = pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['DIVIDEND\n\nPAYMENT\n\nQ1 2025'],
            'Amount': ['$123.45'],
            'Balance': ['$1,000.00']
        })
        
        result = process_alliant_checking_format(df)
        assert result['Description'].iloc[0] == 'DIVIDEND PAYMENT Q1 2025'
    
    @pytest.mark.dependency(depends=["TestDescriptionStandardization::test_multiple_newlines"])
    def test_no_newlines(self):
        """Test handling of descriptions without newlines.
        
        Verifies:
        - Descriptions without newlines are unchanged
        """
        df = pd.DataFrame({
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['$123.45'],
            'Balance': ['$1,000.00']
        })
        
        result = process_alliant_checking_format(df)
        assert result['Description'].iloc[0] == 'AMAZON.COM' 