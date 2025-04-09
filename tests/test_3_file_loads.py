"""
File Loading Tests

This module tests the system's ability to load and process transaction files
from the filesystem.

Test Coverage:
- CSV file import functionality
- File format auto-detection
- Error handling for invalid/malformed files
- Batch processing of multiple files

Dependencies: test_2_file_formats.py (requires working format validation)
"""

import pytest
import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
from src.reconcile import (
    import_csv,
    import_folder
)

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
    # Check that all input columns are present in the result
    assert all(col in result.columns for col in df.columns)
    # Check that source_file is present
    assert 'source_file' in result.columns

@pytest.mark.parametrize("format_name,file_pattern", [
    ("discover", "discover_*.csv"),
    ("amex", "amex_*.csv"),
    ("capital_one", "capital_one_*.csv"),
    ("chase", "chase_*.csv"),
    ("alliant_checking", "alliant_checking_*.csv"),
    ("alliant_visa", "alliant_visa_*.csv"),
    ("empower", "empower_*.csv")
])
def test_file_format_detection(format_name, file_pattern, tmp_path, create_test_df):
    """Test automatic file format detection"""
    # Create test file
    df = create_test_df(format_name)
    file_path = tmp_path / f"{format_name}_test.csv"
    df.to_csv(file_path, index=False)
    
    # Read and validate
    result = import_csv(file_path)
    assert not result.empty
    assert all(col in result.columns for col in ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'])
    assert pd.api.types.is_numeric_dtype(result['Amount'])

def test_folder_import(tmp_path, create_test_df):
    """Test folder import functionality"""
    # Create test files
    formats = ['discover', 'capital_one', 'chase']
    for format_name in formats:
        df = create_test_df(format_name)
        file_path = tmp_path / f"{format_name}_test.csv"
        df.to_csv(file_path, index=False)
        print(f"\nCreated file: {file_path}")
        print(f"Content:\n{df}")
    
    # Import folder and validate
    result = import_folder(tmp_path)
    print("\nImported results:")
    for i, df in enumerate(result):
        print(f"\nResult {i}:")
        print(f"source_file: {df['source_file'].iloc[0]}")
        print(f"DataFrame:\n{df}")
    
    assert len(result) > 0  # Should have at least one DataFrame
    assert all(isinstance(df, pd.DataFrame) for df in result)  # All items should be DataFrames
    assert all(not df.empty for df in result)  # No DataFrames should be empty
    assert len(result) == len(formats)  # Should have one DataFrame per format
    
    # Sort both lists by source_file/format_name to ensure they match
    result_sorted = sorted(result, key=lambda df: df['source_file'].iloc[0].lower())
    formats_sorted = sorted(formats, key=str.lower)
    
    # Check that each DataFrame's source_file contains the format name (case-insensitive)
    for df, format_name in zip(result_sorted, formats_sorted):
        source_file = df['source_file'].iloc[0].lower()
        assert format_name.lower() in source_file, f"Expected {format_name} in {source_file}"

def test_invalid_file_handling(tmp_path):
    """Test handling of invalid files"""
    # Non-existent file
    with pytest.raises(FileNotFoundError):
        import_csv(str(tmp_path / "nonexistent.csv"))
        
    # Empty file
    empty_file = tmp_path / "empty.csv"
    empty_file.touch()
    with pytest.raises(pd.errors.EmptyDataError):
        import_csv(str(empty_file))
        
    # Malformed CSV
    malformed_file = tmp_path / "malformed.csv"
    with open(malformed_file, 'w') as f:
        f.write("Date,Description,Amount\n")
        f.write("2025-03-17,Test1,123.45\n")
        f.write("2025-03-18,Test2\n")  # Missing amount
        
    with pytest.raises(ValueError):
        import_csv(str(malformed_file))

def test_amount_sign_consistency(tmp_path, create_test_df):
    """Test consistency of amount signs across formats"""
    for format_name in ['discover', 'capital_one', 'chase', 'alliant_checking', 'alliant_visa']:
        df = create_test_df(format_name)
        print(f"\n{format_name} original data:")
        print(df)
        print(f"Amount dtype: {df['Amount'].dtype if 'Amount' in df.columns else 'No Amount column'}")
        
        file_path = tmp_path / f"{format_name}_test.csv"
        df.to_csv(file_path, index=False)
        
        # Read the CSV file directly to check what was written
        print(f"\n{format_name} CSV contents:")
        with open(file_path) as f:
            print(f.read())
        
        result = import_csv(file_path)
        print(f"\n{format_name} processed result:")
        print(result)
        print(f"Amount dtype: {result['Amount'].dtype}")
        print(f"Amount values: {result['Amount'].values}")
        
        assert result['Amount'].iloc[0] < 0, f"{format_name} amounts should be negative for debits"

def test_capitalized_file_extensions(tmp_path):
    """Test handling of capitalized file extensions"""
    # Create test CSV with capitalized extension
    df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['Test Transaction'],
        'Amount': ['-50.00'],
        'Category': ['Shopping']
    })
    file_path = tmp_path / "test.CSV"
    df.to_csv(file_path, index=False)
    
    # Import and validate
    result = import_csv(file_path)
    assert not result.empty
    assert all(col in result.columns for col in df.columns)
    assert 'source_file' in result.columns 