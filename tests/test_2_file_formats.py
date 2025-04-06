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

@pytest.mark.dependency(depends=["test_1_utils.py::TestDateStandardization::test_iso_format", "test_1_utils.py::TestAmountCleaning::test_positive_amounts"])
class TestFormatValidation:
    """Test suite for format validation"""
    
    @pytest.mark.dependency()
    def test_invalid_data_types(self, create_test_df):
        """Test handling of invalid data types"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'empower']:
            df = create_test_df(format_name)
            # Convert amounts to strings
            if format_name in ['discover', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'empower']:
                df['Amount'] = df['Amount'].astype(str)
            elif format_name == 'capital_one':
                df['Debit'] = df['Debit'].astype(str)
                df['Credit'] = df['Credit'].astype(str)
            
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
            elif format_name == 'amex':
                process_amex_format(df)
            elif format_name == 'empower':
                process_aggregator_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_amount_validation(self, create_test_df):
        """Test amount validation"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'empower']:
            df = create_test_df(format_name)
            # Test invalid amounts
            if format_name in ['discover', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'empower']:
                df.loc[0, 'Amount'] = 'invalid'
                with pytest.raises(ValueError):
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
                    elif format_name == 'empower':
                        process_aggregator_format(df)
            elif format_name == 'capital_one':
                df.loc[0, 'Debit'] = 'invalid'
                with pytest.raises(ValueError):
                    process_capital_one_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_date_validation(self, create_test_df):
        """Test date validation"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'empower']:
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
            elif format_name == 'amex':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_amex_format(df)
            elif format_name == 'empower':
                df.loc[0, 'Date'] = 'invalid'
                with pytest.raises(ValueError):
                    process_aggregator_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_description_validation(self, create_test_df):
        """Test description validation"""
        for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa', 'amex', 'empower']:
            df = create_test_df(format_name)
            # Test empty descriptions
            df.loc[0, 'Description'] = ''
            with pytest.raises(ValueError):
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
                elif format_name == 'empower':
                    process_aggregator_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_category_validation(self, create_test_df):
        """Test category validation"""
        for format_name in ['discover', 'capital_one', 'empower']:
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
            elif format_name == 'empower':
                df.loc[0, 'Category'] = ''
                # Should not raise error for empty category
                process_aggregator_format(df)
    
    @pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
    def test_date_order_validation(self, create_test_df):
        """Test date order validation"""
        for format_name in ['discover', 'capital_one', 'alliant_visa']:
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
            elif format_name == 'alliant_visa':
                df.loc[0, 'Date'] = '01/02/2025'
                df.loc[0, 'Post Date'] = '01/01/2025'
                with pytest.raises(ValueError):
                    process_alliant_visa_format(df)

@pytest.mark.dependency(depends=["TestFormatValidation::test_invalid_data_types"])
def test_data_conversion_consistency(create_test_df):
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
            
        # Check that all required columns are present
        assert all(col in result.columns for col in ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'])
        # Check that amounts are numeric
        assert pd.api.types.is_numeric_dtype(result['Amount']) 