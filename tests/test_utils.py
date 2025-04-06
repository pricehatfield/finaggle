import pytest
import pandas as pd
import numpy as np
from src.reconcile import standardize_date, clean_amount

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
        assert standardize_date('17-3-2025') == '2025-03-17'
        
    def test_compact_format(self):
        """Test compact format dates (YYYYMMDD)"""
        assert standardize_date('20250317') == '2025-03-17'
        
    def test_short_year_format(self):
        """Test short year format dates (M/D/YY)"""
        assert standardize_date('3/17/25') == '2025-03-17'
        
    def test_invalid_dates(self):
        """Test handling of invalid dates"""
        assert standardize_date(None) is None
        assert standardize_date('') is None
        assert standardize_date(np.nan) is None
        assert standardize_date('invalid') is None

class TestAmountCleaning:
    """Test suite for amount cleaning functionality"""
    
    def test_positive_amounts(self):
        """Test positive amount formats"""
        assert clean_amount('$123.45') == 123.45
        assert clean_amount('123.45') == 123.45
        assert clean_amount('1,234.56') == 1234.56
        assert clean_amount('$1,234.56') == 1234.56
        
    def test_negative_amounts(self):
        """Test negative amount formats"""
        assert clean_amount('-123.45') == -123.45
        assert clean_amount('$-123.45') == -123.45
        assert clean_amount('-1,234.56') == -1234.56
        assert clean_amount('$-1,234.56') == -1234.56
        
    def test_invalid_amounts(self):
        """Test handling of invalid amounts"""
        assert clean_amount('') == 0.0
        assert clean_amount(None) == 0.0
        assert clean_amount(np.nan) == 0.0
        assert clean_amount('invalid') == 0.0
        
    def test_edge_cases(self):
        """Test edge cases in amount cleaning"""
        assert clean_amount('0') == 0.0
        assert clean_amount('0.00') == 0.0
        assert clean_amount('$0.00') == 0.0
        assert clean_amount('-0.00') == 0.0
        assert clean_amount('$-0.00') == 0.0 