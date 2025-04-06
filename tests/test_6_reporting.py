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

def test_setup_logging(tmp_path, monkeypatch):
    """Test logging setup"""
    log_file = tmp_path / 'test.log'
    monkeypatch.setenv('LOG_FILE', str(log_file))
    
    # Import after setting environment variable
    from src.reconcile import setup_logging
    setup_logging()
    
    assert log_file.exists()
    assert logging.getLogger().level == logging.INFO

def test_ensure_directory(tmp_path, monkeypatch):
    """Test directory creation"""
    test_dir = tmp_path / 'test_dir'
    monkeypatch.setenv('DATA_DIR', str(test_dir))
    
    # Import after setting environment variable
    from src.reconcile import ensure_directory
    ensure_directory()
    
    assert test_dir.exists()
    assert test_dir.is_dir()

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

@pytest.mark.dependency(depends=["test_6_reconciliation.py"])
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
        assert "Total Matched Transactions: 2" in summary
        assert "Total Unmatched Transactions: 1" in summary
        assert "Total Amount Matched: -125.00" in summary
        assert "Total Amount Unmatched: -100.00" in summary

@pytest.mark.dependency(depends=["test_6_reconciliation.py"])
def test_report_validation():
    """Test report validation"""
    # Create sample data with invalid values
    matches = pd.DataFrame({
        'Transaction Date': ['invalid_date'],
        'Post Date': ['2025-01-02'],
        'Description': ['test transaction'],
        'Amount': ['invalid_amount'],
        'Category': ['shopping'],
        'source_file': ['source.csv'],
        'target_file': ['target.csv']
    })
    
    # Should raise ValueError for invalid data
    with pytest.raises(ValueError):
        generate_reconciliation_report(matches, pd.DataFrame(), "report.txt")

@pytest.mark.dependency(depends=["test_6_reconciliation.py"])
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
        assert "No transactions to report" in content 