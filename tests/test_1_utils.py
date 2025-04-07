import pytest
import pandas as pd
import numpy as np
import os
from src.reconcile import standardize_date, clean_amount
from src.utils import ensure_directory, create_output_directories
import logging

def create_test_date_data():
    """Create standardized test data for date standardization.
    
    Returns:
        dict: Test data with various date formats
    """
    return {
        'iso': '2025-03-17',
        'iso_with_time': '2025-03-17 12:34:56',
        'us': '03/17/2025',
        'us_short': '3/17/2025',
        'compact': '20250317',
        'compact_us': '03172025',
        'short_year': '03/17/25',
        'invalid': 'invalid',
        'invalid_month': '2025-13-01',
        'invalid_day': '2025-02-30'
    }

def create_test_amount_data():
    """Create standardized test data for amount cleaning.
    
    Returns:
        dict: Test data with various amount formats
    """
    return {
        'positive_currency': '$50.00',
        'positive_no_currency': '50.00',
        'positive_with_commas': '1,234.56',
        'positive_integer': '50',
        'negative_currency': '-$50.00',
        'negative_no_currency': '-50.00',
        'negative_parentheses': '($50.00)',
        'negative_parentheses_no_currency': '(50.00)',
        'zero_currency': '$0.00',
        'zero_no_currency': '0.00',
        'zero_integer': '0',
        'zero_padded': '00.00',
        'invalid': 'invalid',
        'empty': '',
        'none': None
    }

@pytest.mark.dependency()
class TestDateStandardization:
    """Test suite for date standardization functionality.
    
    Verifies handling of various date formats and invalid inputs.
    """
    
    @pytest.mark.dependency()
    def test_iso_format(self):
        """Test ISO format dates (YYYY-MM-DD).
        
        Verifies:
        - Basic ISO format parsing
        """
        data = create_test_date_data()
        assert standardize_date(data['iso']) == '2025-03-17'
        assert standardize_date(data['iso_with_time']) == '2025-03-17'  # Should work with time component
        
    @pytest.mark.dependency(depends=["TestDateStandardization::test_iso_format"])
    def test_us_format(self):
        """Test US format dates (MM/DD/YYYY).
        
        Verifies:
        - Standard US format parsing
        - US format with single-digit month/day
        """
        data = create_test_date_data()
        assert standardize_date(data['us']) == '2025-03-17'
        assert standardize_date(data['us_short']) == '2025-03-17'
        
    @pytest.mark.dependency(depends=["TestDateStandardization::test_iso_format"])
    def test_compact_format(self):
        """Test compact date formats.
        
        Verifies:
        - ISO compact format parsing
        - US compact format parsing
        """
        data = create_test_date_data()
        assert standardize_date(data['compact']) == '2025-03-17'
        assert standardize_date(data['compact_us']) == '2025-03-17'
        
    @pytest.mark.dependency(depends=["TestDateStandardization::test_iso_format"])
    def test_short_year_format(self):
        """Test dates with 2-digit years.
        
        Verifies:
        - Short year format parsing
        - Year conversion to 4-digit format
        """
        data = create_test_date_data()
        assert standardize_date(data['short_year']) == '2025-03-17'
        
    @pytest.mark.dependency(depends=["TestDateStandardization::test_iso_format"])
    def test_invalid_dates(self):
        """Test handling of invalid dates.
        
        Verifies:
        - Invalid format returns None
        - Invalid month returns None
        - Invalid day returns None
        """
        data = create_test_date_data()
        assert standardize_date(data['invalid']) is None
        assert standardize_date(data['invalid_month']) is None
        assert standardize_date(data['invalid_day']) is None

@pytest.mark.dependency()
class TestAmountCleaning:
    """Test suite for amount cleaning functionality.
    
    Verifies handling of various amount formats and invalid inputs.
    """
    
    @pytest.mark.dependency()
    def test_positive_amounts(self):
        """Test cleaning of positive amounts.
        
        Verifies:
        - Currency symbol removal
        - Comma removal
        - Decimal point handling
        - Integer conversion
        """
        data = create_test_amount_data()
        assert clean_amount(data['positive_currency']) == 50.0
        assert clean_amount(data['positive_no_currency']) == 50.0
        assert clean_amount(data['positive_with_commas']) == 1234.56
        assert clean_amount(data['positive_integer']) == 50.0
        
    @pytest.mark.dependency(depends=["TestAmountCleaning::test_positive_amounts"])
    def test_negative_amounts(self):
        """Test cleaning of negative amounts.
        
        Verifies:
        - Negative sign handling
        - Parentheses handling
        - Currency symbol removal
        """
        data = create_test_amount_data()
        assert clean_amount(data['negative_currency']) == -50.0
        assert clean_amount(data['negative_no_currency']) == -50.0
        assert clean_amount(data['negative_parentheses']) == -50.0
        assert clean_amount(data['negative_parentheses_no_currency']) == -50.0
        
    @pytest.mark.dependency(depends=["TestAmountCleaning::test_positive_amounts"])
    def test_invalid_amounts(self):
        """Test handling of invalid amounts.
        
        Verifies:
        - Invalid format raises ValueError
        - Empty string raises ValueError
        - None raises ValueError
        """
        data = create_test_amount_data()
        with pytest.raises(ValueError):
            clean_amount(data['invalid'])
        with pytest.raises(ValueError):
            clean_amount('')
        with pytest.raises(ValueError):
            clean_amount(None)
        
    @pytest.mark.dependency(depends=["TestAmountCleaning::test_positive_amounts"])
    def test_edge_cases(self):
        """Test edge cases in amount cleaning.
        
        Verifies:
        - Zero amount handling
        - Padded zero handling
        """
        data = create_test_amount_data()
        assert clean_amount(data['zero_currency']) == 0.0
        assert clean_amount(data['zero_no_currency']) == 0.0
        assert clean_amount(data['zero_integer']) == 0.0
        assert clean_amount(data['zero_padded']) == 0.0

@pytest.mark.dependency()
class TestDirectoryOperations:
    """Test suite for directory operations.
    
    Verifies directory creation and validation functionality.
    """
    
    @pytest.mark.dependency()
    def test_ensure_directory(self, tmp_path):
        """Test directory creation and validation.
        
        Verifies:
        - Archive directory creation
        - Logs directory creation
        - Invalid directory type handling
        """
        # Test creating archive directory
        archive_dir = ensure_directory("archive")
        assert os.path.exists(archive_dir)
        assert os.path.isdir(archive_dir)
        
        # Test creating logs directory
        logs_dir = ensure_directory("logs")
        assert os.path.exists(logs_dir)
        assert os.path.isdir(logs_dir)
        
        # Test invalid directory type
        with pytest.raises(ValueError):
            ensure_directory("invalid")
    
    @pytest.mark.dependency(depends=["TestDirectoryOperations::test_ensure_directory"])
    def test_create_output_directories(self, tmp_path):
        """Test output directory creation.
        
        Verifies:
        - Base output directory creation
        - Reconciled subdirectory creation
        - Unmatched subdirectory creation
        - String path handling
        """
        output_dir = tmp_path / "output"
        create_output_directories(output_dir)
        
        # Check that directories were created
        assert os.path.exists(output_dir)
        assert os.path.exists(output_dir / "reconciled")
        assert os.path.exists(output_dir / "unmatched")
        
        # Test with string path
        str_output_dir = str(tmp_path / "output2")
        create_output_directories(str_output_dir)
        assert os.path.exists(str_output_dir)
        assert os.path.exists(os.path.join(str_output_dir, "reconciled"))
        assert os.path.exists(os.path.join(str_output_dir, "unmatched"))

def test_setup_logging(tmp_path, monkeypatch):
    """Test logging setup"""
    log_file = tmp_path / 'test.log'
    monkeypatch.setenv('LOG_FILE', str(log_file))
    
    # Import after setting environment variable
    from src.utils import setup_logging
    setup_logging()
    
    assert log_file.exists()
    assert logging.getLogger().level == logging.INFO 