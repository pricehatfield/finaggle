import pytest
import pandas as pd
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

class TestDataStandardization:
    """Test suite for data standardization functionality"""
    
    def test_standardized_columns(self):
        """Test that all formats produce the same standardized columns"""
        # Create sample data for each format
        formats = {
            'discover': {
                'Trans. Date': ['03/17/2025'],
                'Post Date': ['03/18/2025'],
                'Description': ['TEST'],
                'Amount': ['123.45'],
                'Category': ['Test']
            },
            'amex': {
                'Date': ['03/17/2025'],
                'Description': ['TEST'],
                'Amount': ['123.45'],
                'Category': ['Test']
            },
            'capital_one': {
                'Transaction Date': ['03/17/2025'],
                'Posted Date': ['03/18/2025'],
                'Description': ['TEST'],
                'Debit': ['123.45'],
                'Credit': [''],
                'Category': ['Test']
            },
            'alliant': {
                'Date': ['03/17/2025'],
                'Description': ['TEST'],
                'Amount': ['$123.45'],
                'Post Date': ['03/18/2025'],
                'Category': ['Test']
            },
            'chase': {
                'Posting Date': ['03/17/2025'],
                'Description': ['TEST'],
                'Amount': ['-123.45'],
                'Category': ['Test']
            },
            'aggregator': {
                'Date': ['03/17/2025'],
                'Description': ['TEST'],
                'Amount': ['-123.45'],
                'Category': ['Test'],
                'Tags': [''],
                'Account': ['Test']
            }
        }
        
        # Process each format
        results = {}
        for format_name, data in formats.items():
            df = pd.DataFrame(data)
            df['source_file'] = f'{format_name}_test.csv'
            
            if format_name == 'discover':
                results[format_name] = process_discover_format(df)
            elif format_name == 'amex':
                results[format_name] = process_amex_format(df)
            elif format_name == 'capital_one':
                results[format_name] = process_capital_one_format(df)
            elif format_name == 'alliant':
                results[format_name] = process_alliant_format(df)
            elif format_name == 'chase':
                results[format_name] = process_chase_format(df)
            elif format_name == 'aggregator':
                results[format_name] = process_aggregator_format(df)
        
        # Verify standardized columns
        required_columns = {
            'Transaction Date',
            'Post Date',
            'Description',
            'Amount',
            'Category',
            'Tags',
            'Account',
            'source_file'
        }
        
        for format_name, result in results.items():
            assert set(result.columns) == required_columns, f"Format {format_name} missing required columns"
            
    def test_amount_sign_standardization(self):
        """Test that amounts are standardized to negative for debits, positive for credits"""
        # Create sample data with debits and credits
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'] * 2,
                'Post Date': ['03/18/2025'] * 2,
                'Description': ['DEBIT', 'CREDIT'],
                'Amount': ['123.45', '-67.89'],
                'Category': ['Test'] * 2
            },
            'amex': {
                'Date': ['03/17/2025'] * 2,
                'Description': ['DEBIT', 'CREDIT'],
                'Amount': ['123.45', '-67.89'],
                'Category': ['Test'] * 2
            },
            'capital_one': {
                'Transaction Date': ['03/17/2025'] * 2,
                'Posted Date': ['03/18/2025'] * 2,
                'Description': ['DEBIT', 'CREDIT'],
                'Debit': ['123.45', ''],
                'Credit': ['', '67.89'],
                'Category': ['Test'] * 2
            },
            'alliant': {
                'Date': ['03/17/2025'] * 2,
                'Description': ['DEBIT', 'CREDIT'],
                'Amount': ['$123.45', '$67.89'],
                'Post Date': ['03/18/2025'] * 2,
                'Category': ['Test'] * 2
            },
            'chase': {
                'Posting Date': ['03/17/2025'] * 2,
                'Description': ['DEBIT', 'CREDIT'],
                'Amount': ['-123.45', '67.89'],
                'Category': ['Test'] * 2
            },
            'aggregator': {
                'Date': ['03/17/2025'] * 2,
                'Description': ['DEBIT', 'CREDIT'],
                'Amount': ['-123.45', '67.89'],
                'Category': ['Test'] * 2,
                'Tags': [''] * 2,
                'Account': ['Test'] * 2
            }
        }
        
        # Process each format
        results = {}
        for format_name, data in test_data.items():
            df = pd.DataFrame(data)
            df['source_file'] = f'{format_name}_test.csv'
            
            if format_name == 'discover':
                results[format_name] = process_discover_format(df)
            elif format_name == 'amex':
                results[format_name] = process_amex_format(df)
            elif format_name == 'capital_one':
                results[format_name] = process_capital_one_format(df)
            elif format_name == 'alliant':
                results[format_name] = process_alliant_format(df)
            elif format_name == 'chase':
                results[format_name] = process_chase_format(df)
            elif format_name == 'aggregator':
                results[format_name] = process_aggregator_format(df)
        
        # Verify amount signs
        for format_name, result in results.items():
            # First row should be a debit (negative)
            assert result['Amount'].iloc[0] < 0, f"Format {format_name} failed to standardize debit amount"
            # Second row should be a credit (positive)
            assert result['Amount'].iloc[1] > 0, f"Format {format_name} failed to standardize credit amount"
            
    def test_date_standardization(self):
        """Test that dates are standardized to YYYY-MM-DD format"""
        # Create sample data with various date formats
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025', '3/17/25', '2025-03-17', '17-03-2025'],
                'Post Date': ['03/18/2025', '3/18/25', '2025-03-18', '18-03-2025'],
                'Description': ['TEST'] * 4,
                'Amount': ['123.45'] * 4,
                'Category': ['Test'] * 4
            }
        }
        
        # Process the data
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        result = process_discover_format(df)
        
        # Verify date standardization
        expected_date = '2025-03-17'
        expected_post_date = '2025-03-18'
        
        for i in range(4):
            assert result['Transaction Date'].iloc[i] == expected_date
            assert result['Post Date'].iloc[i] == expected_post_date
            
    def test_description_standardization(self):
        """Test that descriptions are standardized (uppercase)"""
        # Create sample data with mixed case descriptions
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'] * 3,
                'Post Date': ['03/18/2025'] * 3,
                'Description': ['Test Description', 'TEST DESCRIPTION', 'test description'],
                'Amount': ['123.45'] * 3,
                'Category': ['Test'] * 3
            }
        }
        
        # Process the data
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        result = process_discover_format(df)
        
        # Verify description standardization
        expected_description = 'TEST DESCRIPTION'
        for i in range(3):
            assert result['Description'].iloc[i] == expected_description 