import pandas as pd
import pytest
import numpy as np
from datetime import datetime
import os
import logging
import re
from src.reconcile import (
    generate_reconciliation_report,
    import_csv,
    import_folder,
    ensure_directory,
    save_reconciliation_results,
    format_report_summary
)
from src.utils import setup_logging

def create_test_df(name, num_records=3):
    """Helper function to create test DataFrames with standardized format"""
    data = {
        'Transaction Date': [f'2025-03-{i+17}' for i in range(num_records)],
        'Post Date': [f'2025-03-{i+17}' for i in range(num_records)],
        'Description': [f'TEST TRANSACTION {i+1}' for i in range(num_records)],
        'Amount': [-123.45 * (i+1) for i in range(num_records)],
        'Category': ['Shopping'] * num_records,
        'source_file': [f'{name}.csv'] * num_records
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_discover_df():
    """Create a sample Discover DataFrame"""
    return pd.DataFrame({
        'Trans. Date': ['03/17/2025'],
        'Post Date': ['03/18/2025'],
        'Description': ['AMAZON.COM'],
        'Amount': ['40.33'],
        'Category': ['Shopping']
    })

@pytest.fixture
def sample_capital_one_df():
    """Create a sample Capital One DataFrame"""
    return pd.DataFrame({
        'Transaction Date': ['2025-03-17'],
        'Posted Date': ['2025-03-18'],
        'Card No.': ['1234'],
        'Description': ['AMAZON.COM'],
        'Category': ['Shopping'],
        'Debit': ['$40.33'],
        'Credit': ['']
    })

@pytest.fixture
def sample_chase_df():
    """Create a sample Chase DataFrame"""
    return pd.DataFrame({
        'Details': ['DEBIT'],
        'Posting Date': ['03/17/2025'],
        'Description': ['AMAZON.COM'],
        'Amount': ['-$40.33'],
        'Type': ['ACH_DEBIT'],
        'Balance': ['$1000.00'],
        'Check or Slip #': ['']
    })

@pytest.fixture
def sample_aggregator_df():
    """Create a sample aggregator DataFrame"""
    return pd.DataFrame({
        'Transaction Date': ['2025-03-17'],
        'Post Date': ['2025-03-18'],
        'Description': ['AMAZON.COM'],
        'Amount': ['-40.33'],
        'Category': ['Shopping'],
        'source_file': ['aggregator.csv']
    })

def test_ensure_directory(tmp_path, monkeypatch):
    """Test directory creation"""
    test_dir = tmp_path / 'test_dir'
    monkeypatch.setenv('DATA_DIR', str(test_dir))
    
    # Import after setting environment variable
    from src.reconcile import ensure_directory
    
    # Test both directory types
    archive_dir = ensure_directory("archive")
    logs_dir = ensure_directory("logs")
    
    assert os.path.exists(archive_dir)
    assert os.path.isdir(archive_dir)
    assert os.path.exists(logs_dir)
    assert os.path.isdir(logs_dir)

def test_import_csv(tmp_path):
    """Test CSV import"""
    # Create test CSV
    df = create_test_df('test')
    file_path = tmp_path / 'test.csv'
    df.to_csv(file_path, index=False)
    
    # Import and verify
    result = import_csv(file_path)
    assert not result.empty
    assert set(result.columns) == set(df.columns)

def test_import_folder(tmp_path):
    """Test folder import"""
    # Create test CSVs
    for name in ['test1', 'test2']:
        df = create_test_df(name)
        file_path = tmp_path / f'{name}.csv'
        df.to_csv(file_path, index=False)
    
    # Import and verify
    result = import_folder(tmp_path)
    assert len(result) == 2
    assert all(not df.empty for df in result)

class TestReporting:
    """Test suite for reconciliation reporting"""
    
    def test_report_generation(self, tmp_path):
        """Test report generation"""
        # Create sample data
        matches = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'source_file': ['source.csv'],
            'target_file': ['target.csv']
        })
        
        unmatched = pd.DataFrame({
            'Transaction Date': ['2025-01-03'],
            'Post Date': ['2025-01-04'],
            'Description': ['unmatched transaction'],
            'Amount': [-75.00],
            'Category': ['dining'],
            'source_file': ['source.csv']
        })
        
        # Generate report
        report_path = tmp_path / "report.txt"
        generate_reconciliation_report(matches, unmatched, report_path)
        
        # Verify report exists and has content
        assert os.path.exists(report_path)
        with open(report_path, 'r') as f:
            content = f.read()
            assert "Matched Transactions" in content
            assert "Unmatched Transactions" in content
            
    def test_results_saving(self, tmp_path):
        """Test saving reconciliation results"""
        # Create sample data
        matches = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'source_file': ['source.csv'],
            'target_file': ['target.csv']
        })
        
        unmatched = pd.DataFrame({
            'Transaction Date': ['2025-01-03'],
            'Post Date': ['2025-01-04'],
            'Description': ['unmatched transaction'],
            'Amount': [-75.00],
            'Category': ['dining'],
            'source_file': ['source.csv']
        })
        
        # Save results
        output_dir = tmp_path / "output"
        save_reconciliation_results(matches, unmatched, output_dir)
        
        # Verify files exist
        assert os.path.exists(output_dir / "matched.csv")
        assert os.path.exists(output_dir / "unmatched.csv")
        
    def test_report_summary(self):
        """Test report summary formatting"""
        # Create sample data
        matches = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-02'],
            'Post Date': ['2025-01-02', '2025-01-03'],
            'Description': ['test transaction 1', 'test transaction 2'],
            'Amount': [-50.00, -75.00],
            'Category': ['shopping', 'dining'],
            'source_file': ['source.csv', 'source.csv'],
            'target_file': ['target.csv', 'target.csv']
        })
        
        unmatched = pd.DataFrame({
            'Transaction Date': ['2025-01-03'],
            'Post Date': ['2025-01-04'],
            'Description': ['unmatched transaction'],
            'Amount': [-100.00],
            'Category': ['transportation'],
            'source_file': ['source.csv']
        })
        
        # Format summary
        summary = format_report_summary(matches, unmatched)
        
        # Verify summary content
        assert "Total Transactions: 3" in summary
        assert "Matched Transactions: 2" in summary
        assert "Unmatched Transactions: 1" in summary
        assert "Total Amount: $225.00" in summary
        assert "Matched Amount: $125.00" in summary
        assert "Unmatched Amount: $100.00" in summary

def test_report_validation():
    """Test report validation"""
    # Create sample data with missing required columns
    matches = pd.DataFrame({
        'Description': ['test transaction'],  # Missing Transaction Date, Post Date, and Amount
        'Category': ['shopping'],
        'source_file': ['source.csv']
    })
    
    # Should raise ValueError for missing required columns
    with pytest.raises(ValueError):
        generate_reconciliation_report(matches, pd.DataFrame(), "report.txt")

def test_empty_results_handling(tmp_path):
    """Test handling of empty results"""
    # Create empty DataFrames
    matches = pd.DataFrame()
    unmatched = pd.DataFrame()
    
    # Generate report
    report_path = tmp_path / "report.txt"
    generate_reconciliation_report(matches, unmatched, report_path)
    
    # Verify report exists and has appropriate message
    assert os.path.exists(report_path)
    with open(report_path, 'r') as f:
        content = f.read()
        assert "Total Transactions: 0" in content
        assert "No matched transactions found" in content
        assert "No unmatched transactions found" in content

def test_output_format_validation(sample_matched_df, sample_unmatched_df):
    """Test that output format follows specifications."""
    # Test matched columns
    required_matched_columns = [
        'Transaction Date',
        'Post Date',
        'Description',
        'Amount',
        'Category',
        'source_file',
        'match_type'
    ]
    assert all(col in sample_matched_df.columns for col in required_matched_columns), \
        f"Missing required columns in matched output. Expected: {required_matched_columns}, Got: {sample_matched_df.columns.tolist()}"

    # Test unmatched columns
    required_unmatched_columns = [
        'Transaction Date',
        'Post Date',
        'Description',
        'Amount',
        'Category',
        'source_file'
    ]
    assert all(col in sample_unmatched_df.columns for col in required_unmatched_columns), \
        f"Missing required columns in unmatched output. Expected: {required_unmatched_columns}, Got: {sample_unmatched_df.columns.tolist()}"

    # Test date formats
    for df in [sample_matched_df, sample_unmatched_df]:
        for date_col in ['Transaction Date', 'Post Date']:
            pd.to_datetime(df[date_col], format='%Y-%m-%d')

    # Test amount format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert pd.api.types.is_numeric_dtype(df['Amount']), \
            "Amount column should be numeric"

    # Test match type values
    valid_match_types = {'post_date_amount', 'transaction_date_amount'}
    assert all(match_type in valid_match_types for match_type in sample_matched_df['match_type'].unique()), \
        f"Invalid match_type values found. Expected: {valid_match_types}"

    # Test source file format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert all(isinstance(source, str) for source in df['source_file']), \
            "source_file should be strings"
        assert all(source.endswith('.csv') for source in df['source_file']), \
            "source_file should end with .csv"

    # Test category format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert all(isinstance(cat, str) for cat in df['Category']), \
            "Category should be strings"

    # Test description format
    for df in [sample_matched_df, sample_unmatched_df]:
        assert all(isinstance(desc, str) for desc in df['Description']), \
            "Description should be strings"

def test_report_generation_with_matched_and_unmatched(sample_matched_df, sample_unmatched_df, tmp_path):
    """Test report generation with both matched and unmatched transactions."""
    # Generate report
    report_path = tmp_path / "report.txt"
    generate_reconciliation_report(sample_matched_df, sample_unmatched_df, report_path)
    
    # Verify report exists and has content
    assert os.path.exists(report_path)
    with open(report_path, 'r') as f:
        content = f.read()
        assert "Matched Transactions" in content
        assert "Unmatched Transactions" in content
        assert "Total Transactions: 8" in content
        assert "Matched Transactions: 5" in content
        assert "Unmatched Transactions: 3" in content
        assert "Total Amount: $1,615.00" in content
        assert "Matched Amount: $1,745.00" in content
        assert "Unmatched Amount: $130.00" in content

def test_report_generation_empty_data(tmp_path):
    """Test report generation with empty DataFrames."""
    # Create empty DataFrames
    empty_matched = pd.DataFrame(columns=[
        'Transaction Date', 'Post Date', 'Description', 'Amount', 
        'Category', 'source_file', 'match_type'
    ])
    empty_unmatched = pd.DataFrame(columns=[
        'Transaction Date', 'Post Date', 'Description', 'Amount', 
        'Category', 'source_file'
    ])
    
    # Generate report
    report_path = tmp_path / "report.txt"
    generate_reconciliation_report(empty_matched, empty_unmatched, report_path)
    
    # Verify report exists and has appropriate message
    assert os.path.exists(report_path)
    with open(report_path, 'r') as f:
        content = f.read()
        assert "Total Transactions: 0" in content
        assert "No matched transactions found" in content
        assert "No unmatched transactions found" in content

def test_save_reconciliation_results(sample_matched_df, sample_unmatched_df, tmp_path):
    """Test saving reconciliation results to CSV files."""
    # Save results
    output_dir = tmp_path / "output"
    save_reconciliation_results(sample_matched_df, sample_unmatched_df, output_dir)
    
    # Verify files exist
    assert os.path.exists(output_dir / "matched.csv")
    assert os.path.exists(output_dir / "unmatched.csv")
    
    # Verify content
    matched_df = pd.read_csv(output_dir / "matched.csv")
    unmatched_df = pd.read_csv(output_dir / "unmatched.csv")
    
    assert len(matched_df) == len(sample_matched_df)
    assert len(unmatched_df) == len(sample_unmatched_df)
    assert set(matched_df.columns) == set(sample_matched_df.columns)
    assert set(unmatched_df.columns) == set(sample_unmatched_df.columns) 