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

@pytest.fixture
def sample_matched_df():
    """Create a sample DataFrame of matched transactions"""
    return pd.DataFrame({
        'Transaction Date': pd.to_datetime(['2025-03-17', '2025-03-18']),
        'Post Date': pd.to_datetime(['2025-03-18', '2025-03-19']),
        'Description': ['AMAZON.COM', 'WALMART'],
        'Amount': [-40.33, -25.99],
        'Category': ['Shopping', 'Groceries'],
        'source_file': ['discover.csv', 'capital_one.csv'],
        'Date': pd.to_datetime(['2025-03-17', '2025-03-18']),
        'YearMonth': ['2025-03', '2025-03'],
        'Account': ['Matched - discover.csv', 'Matched - capital_one.csv'],
        'Tags': ['', ''],
        'reconciled_key': ['2025-03-17', '2025-03-18'],
        'Matched': [True, True]
    })

@pytest.fixture
def sample_unmatched_df():
    """Create a sample DataFrame of unmatched transactions"""
    return pd.DataFrame({
        'Transaction Date': pd.to_datetime(['2025-03-19', '2025-03-20']),
        'Post Date': pd.to_datetime(['2025-03-20', '2025-03-21']),
        'Description': ['TARGET', 'COSTCO'],
        'Amount': [-75.50, -150.25],
        'Category': ['Shopping', 'Groceries'],
        'source_file': ['chase.csv', 'amex.csv'],
        'Date': pd.to_datetime(['2025-03-19', '2025-03-20']),
        'YearMonth': ['2025-03', '2025-03'],
        'Account': ['Unreconciled - chase.csv', 'Unreconciled - amex.csv'],
        'Tags': ['', ''],
        'reconciled_key': ['2025-03-19', '2025-03-20'],
        'Matched': [False, False]
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
            'Date': ['2025-01-01'],
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'source_file': ['source.csv'],
            'target_file': ['target.csv'],
            'Account': ['Matched - source.csv'],
            'YearMonth': ['2025-01'],
            'Tags': [''],
            'reconciled_key': ['2025-01-01'],
            'Matched': [True]
        })
        
        unmatched = pd.DataFrame({
            'Date': ['2025-01-03'],
            'Transaction Date': ['2025-01-03'],
            'Post Date': ['2025-01-04'],
            'Description': ['unmatched transaction'],
            'Amount': [-75.00],
            'Category': ['dining'],
            'source_file': ['source.csv'],
            'Account': ['Unreconciled - source.csv'],
            'YearMonth': ['2025-01'],
            'Tags': [''],
            'reconciled_key': ['2025-01-03'],
            'Matched': [False]
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

        # Verify all_transactions.csv exists and has correct format
        all_transactions_path = output_dir / "all_transactions.csv"
        assert os.path.exists(all_transactions_path)
        
        # Read and verify contents
        df = pd.read_csv(all_transactions_path)
        assert 'Matched' in df.columns
        assert len(df) == len(matches) + len(unmatched)
        assert df['Matched'].sum() == len(matches)  # Count of True values should equal matches length
        assert (~df['Matched']).sum() == len(unmatched)  # Count of False values should equal unmatched length

    def test_report_summary(self):
        """Test report summary formatting"""
        # Create sample data
        matches = pd.DataFrame({
            'Date': ['2025-01-01', '2025-01-02'],
            'Transaction Date': ['2025-01-01', '2025-01-02'],
            'Post Date': ['2025-01-02', '2025-01-03'],
            'Description': ['test transaction 1', 'test transaction 2'],
            'Amount': [-50.00, -75.00],
            'Category': ['shopping', 'dining'],
            'source_file': ['source.csv', 'source.csv'],
            'target_file': ['target.csv', 'target.csv']
        })
        
        unmatched = pd.DataFrame({
            'Date': ['2025-01-03'],
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

def test_output_format_validation(sample_transactions_df):
    """Test that output format follows specifications."""
    # Test required columns
    required_columns = [
        'Date',
        'YearMonth',
        'Account',
        'Description',
        'Category',
        'Tags',
        'Amount',
        'reconciled_key',
        'Matched'
    ]
    assert all(col in sample_transactions_df.columns for col in required_columns), \
        f"Missing required columns in output. Expected: {required_columns}, Got: {sample_transactions_df.columns.tolist()}"

    # Test date formats
    assert pd.to_datetime(sample_transactions_df['Date']).dt.strftime('%Y-%m-%d').equals(sample_transactions_df['Date']), \
        "Date must be in YYYY-MM-DD format"
    
    # Test YearMonth format
    assert sample_transactions_df['YearMonth'].str.match(r'^\d{4}-\d{2}$').all(), \
        "YearMonth must be in YYYY-MM format"

    # Test amount format
    assert pd.api.types.is_numeric_dtype(sample_transactions_df['Amount']), \
        "Amount column should be numeric"

    # Test Matched format
    assert pd.api.types.is_bool_dtype(sample_transactions_df['Matched']), \
        "Matched should be boolean"

    # Test reconciled_key format
    assert pd.to_datetime(sample_transactions_df['reconciled_key']).dt.strftime('%Y-%m-%d').equals(sample_transactions_df['reconciled_key']), \
        "reconciled_key must be in YYYY-MM-DD format"

    # Test Account format
    assert all(acc.startswith(('Matched - ', 'Unreconciled - ')) for acc in sample_transactions_df['Account']), \
        "Account must start with 'Matched - ' or 'Unreconciled - '"

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
        assert f"Total Transactions: {len(sample_matched_df) + len(sample_unmatched_df)}" in content

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

    # Verify all_transactions.csv exists and has correct format
    all_transactions_path = output_dir / "all_transactions.csv"
    assert os.path.exists(all_transactions_path)
    
    # Read and verify contents
    df = pd.read_csv(all_transactions_path)
    assert 'Matched' in df.columns
    assert len(df) == len(sample_matched_df) + len(sample_unmatched_df)
    assert df['Matched'].sum() == len(sample_matched_df)  # Count of True values should equal matches length
    assert (~df['Matched']).sum() == len(sample_unmatched_df)  # Count of False values should equal unmatched length
    
    # Verify data integrity
    matched_rows = df[df['Matched']]
    unmatched_rows = df[~df['Matched']]
    
    # Check matched transactions
    assert all(matched_rows['Description'].isin(sample_matched_df['Description']))
    assert all(matched_rows['Amount'].isin(sample_matched_df['Amount']))
    
    # Check unmatched transactions
    assert all(unmatched_rows['Description'].isin(sample_unmatched_df['Description']))
    assert all(unmatched_rows['Amount'].isin(sample_unmatched_df['Amount']))

def test_reconciled_output_format(tmp_path):
    """Test that reconciliation results are saved in the correct format"""
    # Create sample matched transactions
    matched_data = {
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [100.00, -50.00],
        'Category': ['Income', 'Expense'],
        'source_file': ['bank1.csv', 'bank2.csv']
    }
    matched_df = pd.DataFrame(matched_data)

    # Create sample unmatched transactions
    unmatched_data = {
        'Transaction Date': ['2024-01-03', '2024-01-04'],
        'Description': ['Test Transaction 3', 'Test Transaction 4'],
        'Amount': [75.00, -25.00],
        'Category': ['Income', 'Expense'],
        'source_file': ['bank3.csv', 'bank4.csv']
    }
    unmatched_df = pd.DataFrame(unmatched_data)

    # Test Excel output
    excel_path = tmp_path / "reconciliation_results.xlsx"
    save_reconciliation_results(matched_df, unmatched_df, excel_path)

    # Verify Excel output
    with pd.ExcelFile(excel_path) as excel:
        # Check sheet names
        assert set(excel.sheet_names) == {'All Transactions'}
        
        # Read and verify contents
        df = pd.read_excel(excel_path, sheet_name='All Transactions')
        assert 'Matched' in df.columns
        assert len(df) == len(matched_df) + len(unmatched_df)
        assert df['Matched'].sum() == len(matched_df)  # Count of True values should equal matches length
        assert (~df['Matched']).sum() == len(unmatched_df)  # Count of False values should equal unmatched length
        
        # Verify data integrity
        matched_rows = df[df['Matched']]
        unmatched_rows = df[~df['Matched']]
        
        # Check matched transactions
        assert all(matched_rows['Description'].isin(matched_df['Description']))
        assert all(matched_rows['Amount'].isin(matched_df['Amount']))
        
        # Check unmatched transactions
        assert all(unmatched_rows['Description'].isin(unmatched_df['Description']))
        assert all(unmatched_rows['Amount'].isin(unmatched_df['Amount'])) 