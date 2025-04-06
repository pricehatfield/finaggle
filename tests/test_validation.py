import pytest
import pandas as pd
import numpy as np
from src.reconcile import (
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_format,
    process_chase_format,
    process_aggregator_format
)

class TestDataValidation:
    """Test suite for data validation functionality"""
    
    def test_required_fields(self):
        """Test that required fields are present and non-empty"""
        # Create sample data missing required fields
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'],
                'Post Date': ['03/18/2025'],
                'Description': [''],
                'Amount': ['123.45'],
                'Category': ['Test']
            },
            'amex': {
                'Date': ['03/17/2025'],
                'Description': ['TEST'],
                'Amount': [None],
                'Category': ['Test']
            },
            'capital_one': {
                'Transaction Date': ['03/17/2025'],
                'Posted Date': ['03/18/2025'],
                'Description': ['TEST'],
                'Debit': [''],
                'Credit': [''],
                'Category': ['Test']
            }
        }
        
        # Process each format and verify validation
        for format_name, data in test_data.items():
            df = pd.DataFrame(data)
            df['source_file'] = f'{format_name}_test.csv'
            
            with pytest.raises(ValueError):
                if format_name == 'discover':
                    process_discover_format(df)
                elif format_name == 'amex':
                    process_amex_format(df)
                elif format_name == 'capital_one':
                    process_capital_one_format(df)
                    
    def test_data_types(self):
        """Test that data types are correctly enforced"""
        # Create sample data with incorrect data types
        test_data = {
            'discover': {
                'Trans. Date': [12345],  # Should be string
                'Post Date': ['03/18/2025'],
                'Description': ['TEST'],
                'Amount': ['not a number'],  # Should be numeric
                'Category': ['Test']
            }
        }
        
        # Process the data and verify type validation
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        
        with pytest.raises(ValueError):
            process_discover_format(df)
            
    def test_amount_validation(self):
        """Test amount validation rules"""
        # Create sample data with invalid amounts
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'] * 3,
                'Post Date': ['03/18/2025'] * 3,
                'Description': ['TEST'] * 3,
                'Amount': ['0.00', '-0.00', '0'],  # Zero amounts
                'Category': ['Test'] * 3
            }
        }
        
        # Process the data
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        result = process_discover_format(df)
        
        # Verify amount validation
        for amount in result['Amount']:
            assert amount == 0.0, "Zero amounts should be standardized to 0.0"
            
    def test_date_validation(self):
        """Test date validation rules"""
        # Create sample data with invalid dates
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025', '13/45/2025', '2025-02-30'],  # Invalid dates
                'Post Date': ['03/18/2025'] * 3,
                'Description': ['TEST'] * 3,
                'Amount': ['123.45'] * 3,
                'Category': ['Test'] * 3
            }
        }
        
        # Process the data and verify date validation
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        
        with pytest.raises(ValueError):
            process_discover_format(df)
            
    def test_description_validation(self):
        """Test description validation rules"""
        # Create sample data with invalid descriptions
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'] * 3,
                'Post Date': ['03/18/2025'] * 3,
                'Description': ['', '   ', None],  # Empty descriptions
                'Amount': ['123.45'] * 3,
                'Category': ['Test'] * 3
            }
        }
        
        # Process the data and verify description validation
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        
        with pytest.raises(ValueError):
            process_discover_format(df)
            
    def test_category_validation(self):
        """Test category validation rules"""
        # Create sample data with invalid categories
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'] * 3,
                'Post Date': ['03/18/2025'] * 3,
                'Description': ['TEST'] * 3,
                'Amount': ['123.45'] * 3,
                'Category': ['', None, 'Invalid Category']  # Invalid categories
            }
        }
        
        # Process the data and verify category validation
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        
        with pytest.raises(ValueError):
            process_discover_format(df)
            
    def test_cross_field_validation(self):
        """Test validation rules that involve multiple fields"""
        # Create sample data with cross-field validation issues
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025', '03/18/2025'],  # Transaction date after post date
                'Post Date': ['03/18/2025', '03/17/2025'],
                'Description': ['TEST'] * 2,
                'Amount': ['123.45'] * 2,
                'Category': ['Test'] * 2
            }
        }
        
        # Process the data and verify cross-field validation
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        
        with pytest.raises(ValueError):
            process_discover_format(df)
            
    def test_duplicate_validation(self):
        """Test validation of duplicate transactions"""
        # Create sample data with duplicate transactions
        test_data = {
            'discover': {
                'Trans. Date': ['03/17/2025'] * 2,
                'Post Date': ['03/18/2025'] * 2,
                'Description': ['TEST'] * 2,
                'Amount': ['123.45'] * 2,
                'Category': ['Test'] * 2
            }
        }
        
        # Process the data and verify duplicate validation
        df = pd.DataFrame(test_data['discover'])
        df['source_file'] = 'discover_test.csv'
        
        with pytest.raises(ValueError):
            process_discover_format(df) 