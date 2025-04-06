import pytest
import pandas as pd
from src.reconcile import (
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_format,
    process_chase_format,
    process_aggregator_format
)

class TestDiscoverFormat:
    """Test suite for Discover format processing"""
    
    def test_basic_processing(self):
        """Test basic Discover format processing"""
        input_data = {
            'Trans. Date': ['03/17/2025'],
            'Post Date': ['03/18/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['123.45'],
            'Category': ['Shopping']
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'discover_test.csv'
        
        result = process_discover_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Amount inverted
        assert result['Category'].iloc[0] == 'Shopping'
        
    def test_amount_handling(self):
        """Test Discover amount handling"""
        input_data = {
            'Trans. Date': ['03/17/2025'] * 2,
            'Post Date': ['03/18/2025'] * 2,
            'Description': ['DEBIT', 'CREDIT'],
            'Amount': ['123.45', '-67.89'],
            'Category': ['Test'] * 2
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'discover_test.csv'
        
        result = process_discover_format(df)
        
        assert result['Amount'].iloc[0] == -123.45  # Debit inverted
        assert result['Amount'].iloc[1] == 67.89    # Credit inverted

class TestAmexFormat:
    """Test suite for Amex format processing"""
    
    def test_basic_processing(self):
        """Test basic Amex format processing"""
        input_data = {
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['123.45'],
            'Category': ['Shopping']
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'amex_test.csv'
        
        result = process_amex_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'  # Same as transaction date
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Amount inverted
        assert result['Category'].iloc[0] == 'Shopping'
        
    def test_amount_handling(self):
        """Test Amex amount handling"""
        input_data = {
            'Date': ['03/17/2025'] * 2,
            'Description': ['DEBIT', 'CREDIT'],
            'Amount': ['123.45', '-67.89'],
            'Category': ['Test'] * 2
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'amex_test.csv'
        
        result = process_amex_format(df)
        
        assert result['Amount'].iloc[0] == -123.45  # Debit inverted
        assert result['Amount'].iloc[1] == 67.89    # Credit inverted

class TestCapitalOneFormat:
    """Test suite for Capital One format processing"""
    
    def test_basic_processing(self):
        """Test basic Capital One format processing"""
        input_data = {
            'Transaction Date': ['03/17/2025'],
            'Posted Date': ['03/18/2025'],
            'Description': ['AMAZON.COM'],
            'Debit': ['123.45'],
            'Credit': [''],
            'Category': ['Shopping']
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'capital_one_test.csv'
        
        result = process_capital_one_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Debit negated
        assert result['Category'].iloc[0] == 'Shopping'
        
    def test_amount_handling(self):
        """Test Capital One amount handling"""
        input_data = {
            'Transaction Date': ['03/17/2025'] * 2,
            'Posted Date': ['03/18/2025'] * 2,
            'Description': ['DEBIT', 'CREDIT'],
            'Debit': ['123.45', ''],
            'Credit': ['', '67.89'],
            'Category': ['Test'] * 2
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'capital_one_test.csv'
        
        result = process_capital_one_format(df)
        
        assert result['Amount'].iloc[0] == -123.45  # Debit negated
        assert result['Amount'].iloc[1] == 67.89    # Credit stays positive

class TestAlliantFormat:
    """Test suite for Alliant format processing"""
    
    def test_basic_processing(self):
        """Test basic Alliant format processing"""
        input_data = {
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['$123.45'],
            'Post Date': ['03/18/2025'],
            'Category': ['Shopping']
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'alliant_test.csv'
        
        result = process_alliant_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-18'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Amount inverted
        assert result['Category'].iloc[0] == 'Shopping'
        
    def test_amount_handling(self):
        """Test Alliant amount handling"""
        input_data = {
            'Date': ['03/17/2025'] * 2,
            'Description': ['DEBIT', 'CREDIT'],
            'Amount': ['$123.45', '$67.89'],
            'Post Date': ['03/18/2025'] * 2,
            'Category': ['Test'] * 2
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'alliant_test.csv'
        
        result = process_alliant_format(df)
        
        assert result['Amount'].iloc[0] == -123.45  # Amount inverted
        assert result['Amount'].iloc[1] == -67.89   # Amount inverted

class TestChaseFormat:
    """Test suite for Chase format processing"""
    
    def test_basic_processing(self):
        """Test basic Chase format processing"""
        input_data = {
            'Posting Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['-123.45'],
            'Category': ['Shopping']
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'chase_test.csv'
        
        result = process_chase_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Amount preserved
        assert result['Category'].iloc[0] == 'Shopping'
        
    def test_amount_handling(self):
        """Test Chase amount handling"""
        input_data = {
            'Posting Date': ['03/17/2025'] * 2,
            'Description': ['DEBIT', 'CREDIT'],
            'Amount': ['-123.45', '67.89'],
            'Category': ['Test'] * 2
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'chase_test.csv'
        
        result = process_chase_format(df)
        
        assert result['Amount'].iloc[0] == -123.45  # Amount preserved
        assert result['Amount'].iloc[1] == 67.89    # Amount preserved

class TestAggregatorFormat:
    """Test suite for Aggregator format processing"""
    
    def test_basic_processing(self):
        """Test basic Aggregator format processing"""
        input_data = {
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['-123.45'],
            'Category': ['Shopping'],
            'Tags': ['Online'],
            'Account': ['Discover']
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'aggregator_test.csv'
        
        result = process_aggregator_format(df)
        
        assert result['Transaction Date'].iloc[0] == '2025-03-17'
        assert result['Post Date'].iloc[0] == '2025-03-17'
        assert result['Description'].iloc[0] == 'AMAZON.COM'
        assert result['Amount'].iloc[0] == -123.45  # Amount preserved
        assert result['Category'].iloc[0] == 'Shopping'
        assert result['Tags'].iloc[0] == 'Online'
        assert result['Account'].iloc[0] == 'Discover'
        
    def test_amount_handling(self):
        """Test Aggregator amount handling"""
        input_data = {
            'Date': ['03/17/2025'] * 2,
            'Description': ['DEBIT', 'CREDIT'],
            'Amount': ['-123.45', '67.89'],
            'Category': ['Test'] * 2,
            'Tags': [''] * 2,
            'Account': ['Discover'] * 2
        }
        df = pd.DataFrame(input_data)
        df['source_file'] = 'aggregator_test.csv'
        
        result = process_aggregator_format(df)
        
        assert result['Amount'].iloc[0] == -123.45  # Amount preserved
        assert result['Amount'].iloc[1] == 67.89    # Amount preserved 