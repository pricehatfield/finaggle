import pytest
import pandas as pd
import numpy as np
from src.reconcile import standardize_date, clean_amount

@pytest.mark.dependency(depends=["test_1_conftest.py"])
class TestDateStandardization:
    """Test suite for date standardization functionality"""
    
    def test_iso_format(self):
        """Test ISO format dates (YYYY-MM-DD)"""
        assert standardize_date('2025-03-17') == '2025-03-17'
        assert standardize_date('2025-03-17 12:34:56') == '2025-03-17'
        
    def test_us_format(self):
        """Test US format dates (MM/DD/YYYY)"""
        assert standardize_date('03/17/2025') == '2025-03-17'
        assert standardize_date('3/17/2025') == '2025-03-17'
        
    def test_uk_format(self):
        """Test UK format dates (DD-MM-YYYY)"""
        assert standardize_date('17-03-2025') == '2025-03-17'
        assert standardize_date('17/03/2025') == '2025-03-17'
        
    def test_compact_format(self):
        """Test compact date formats"""
        assert standardize_date('20250317') == '2025-03-17'
        assert standardize_date('03172025') == '2025-03-17'
        
    def test_short_year_format(self):
        """Test dates with 2-digit years"""
        assert standardize_date('03/17/25') == '2025-03-17'
        assert standardize_date('17-03-25') == '2025-03-17'
        
    def test_invalid_dates(self):
        """Test handling of invalid dates"""
        assert standardize_date('invalid') is None
        assert standardize_date('2025-13-01') is None
        assert standardize_date('2025-02-30') is None

@pytest.mark.dependency(depends=["test_1_conftest.py"])
class TestAmountCleaning:
    """Test suite for amount cleaning functionality"""
    
    def test_positive_amounts(self):
        """Test cleaning of positive amounts"""
        assert clean_amount('$50.00') == 50.0
        assert clean_amount('50.00') == 50.0
        assert clean_amount('1,234.56') == 1234.56
        assert clean_amount('50') == 50.0
        
    def test_negative_amounts(self):
        """Test cleaning of negative amounts"""
        assert clean_amount('-$50.00') == -50.0
        assert clean_amount('-50.00') == -50.0
        assert clean_amount('($50.00)') == -50.0
        assert clean_amount('(50.00)') == -50.0
        
    def test_invalid_amounts(self):
        """Test handling of invalid amounts"""
        assert clean_amount('invalid') == 0.0
        assert clean_amount('') == 0.0
        assert clean_amount(None) == 0.0
        
    def test_edge_cases(self):
        """Test edge cases in amount cleaning"""
        assert clean_amount('0.00') == 0.0
        assert clean_amount('0') == 0.0
        assert clean_amount('0.0') == 0.0
        assert clean_amount('00.00') == 0.0 