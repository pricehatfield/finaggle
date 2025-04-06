import pytest
import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
from src.reconcile import (
    import_csv,
    import_folder,
    setup_logging,
    create_output_directories
)

@pytest.mark.dependency(depends=["test_3_file_formats.py"])
def test_logging_setup(tmp_path):
    """Test logging setup"""
    log_dir = tmp_path / "logs"
    setup_logging(log_dir)
    assert os.path.exists(log_dir)
    assert os.path.exists(log_dir / "reconciliation.log")

@pytest.mark.dependency(depends=["test_3_file_formats.py"])
def test_directory_creation(tmp_path):
    """Test directory creation"""
    output_dir = tmp_path / "output"
    create_output_directories(output_dir)
    assert os.path.exists(output_dir)
    assert os.path.exists(output_dir / "reconciled")
    assert os.path.exists(output_dir / "unmatched")

@pytest.mark.dependency(depends=["test_3_file_formats.py"])
def test_csv_import(tmp_path):
    """Test CSV import functionality"""
    # Create test CSV
    df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['Test Transaction'],
        'Amount': ['-50.00'],
        'Category': ['Shopping']
    })
    file_path = tmp_path / "test.csv"
    df.to_csv(file_path, index=False)
    
    # Import and validate
    result = import_csv(file_path)
    assert not result.empty
    assert set(result.columns) == set(df.columns)

@pytest.mark.dependency(depends=["test_3_file_formats.py"])
def test_folder_import(tmp_path):
    """Test folder import functionality"""
    # Create test files
    formats = ['discover', 'capital_one', 'chase']
    for format_name in formats:
        df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': [f'Test {format_name} Transaction'],
            'Amount': ['-50.00'],
            'Category': ['Shopping']
        })
        file_path = tmp_path / f"{format_name}_test.csv"
        df.to_csv(file_path, index=False)
    
    # Import folder and validate
    result = import_folder(tmp_path)
    assert not result.empty
    assert len(result) == len(formats)
    assert all(format_name in result['source_file'].values for format_name in formats)

class TestDataLoading:
    """Test suite for data loading functionality"""
    
    def test_csv_import(self, tmp_path):
        """Test basic CSV import functionality"""
        # Create a test CSV file
        test_data = {
            'Date': ['2025-03-17', '2025-03-18'],
            'Description': ['Test1', 'Test2'],
            'Amount': ['123.45', '67.89']
        }
        df = pd.DataFrame(test_data)
        file_path = tmp_path / "test.csv"
        df.to_csv(file_path, index=False)
        
        # Test import
        result = import_csv(str(file_path))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['Date', 'Description', 'Amount']
        
    def test_invalid_file(self, tmp_path):
        """Test handling of invalid files"""
        # Non-existent file
        with pytest.raises(FileNotFoundError):
            import_csv(str(tmp_path / "nonexistent.csv"))
            
        # Empty file
        empty_file = tmp_path / "empty.csv"
        empty_file.touch()
        with pytest.raises(pd.errors.EmptyDataError):
            import_csv(str(empty_file))
            
    def test_malformed_csv(self, tmp_path):
        """Test handling of malformed CSV files"""
        # Create malformed CSV
        malformed_file = tmp_path / "malformed.csv"
        with open(malformed_file, 'w') as f:
            f.write("Date,Description,Amount\n")
            f.write("2025-03-17,Test1,123.45\n")
            f.write("2025-03-18,Test2\n")  # Missing amount
            
        # Test that malformed data is handled by filling with NaN
        result = import_csv(str(malformed_file))
        assert len(result) == 2
        assert pd.isna(result.iloc[1]['Amount'])  # Check that missing amount is NaN
            
    def test_directory_handling(self, tmp_path):
        """Test directory creation and handling"""
        # Test directory creation
        test_dir = tmp_path / "test_dir"
        result = ensure_directory(str(test_dir))
        assert os.path.exists(result)
        assert os.path.isdir(result)
        
        # Test existing directory
        result = ensure_directory(str(test_dir))
        assert os.path.exists(result)
        assert os.path.isdir(result)
        
    def test_file_format_detection(self, tmp_path):
        """Test automatic file format detection"""
        # Create test files for different formats
        formats = {
            'discover': {
                'Trans. Date': ['03/17/2025'],
                'Post Date': ['03/18/2025'],
                'Description': ['Test'],
                'Amount': ['123.45']
            },
            'amex': {
                'Date': ['03/17/2025'],
                'Description': ['Test'],
                'Amount': ['123.45']
            },
            'capital_one': {
                'Transaction Date': ['03/17/2025'],
                'Posted Date': ['03/18/2025'],
                'Description': ['Test'],
                'Debit': ['123.45'],
                'Credit': ['']
            }
        }
        
        for format_name, data in formats.items():
            df = pd.DataFrame(data)
            file_path = tmp_path / f"{format_name}_test.csv"
            df.to_csv(file_path, index=False)
            
            result = import_csv(str(file_path))
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            
    def test_data_validation(self, tmp_path):
        """Test basic data validation during import"""
        # Create test data with invalid values
        test_data = {
            'Date': ['2025-03-17', 'invalid_date'],
            'Description': ['Test1', 'Test2'],
            'Amount': ['123.45', 'invalid_amount']
        }
        df = pd.DataFrame(test_data)
        file_path = tmp_path / "validation_test.csv"
        df.to_csv(file_path, index=False)
        
        result = import_csv(str(file_path))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # Note: Actual validation would be handled by format-specific processors 