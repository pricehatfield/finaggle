import pandas as pd
import pytest
import numpy as np
from datetime import datetime
import os
import logging
import re
from src.reconcile import (
    reconcile_transactions,
    import_csv,
    import_folder,
    ensure_directory,
    process_discover_format,
    process_capital_one_format,
    process_chase_format,
    find_matches,
    calculate_discrepancies
)

def create_test_df(name, num_records=1, with_dates=False):
    """Helper function to create test DataFrames with standardized format"""
    if with_dates:
        dates = [f'2025-03-{i+17}' for i in range(num_records)]
        post_dates = [f'2025-03-{i+18}' for i in range(num_records)]
    else:
        dates = ['2025-03-17'] * num_records
        post_dates = ['2025-03-18'] * num_records
    
    data = {
        'Transaction Date': dates,
        'Post Date': post_dates,
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

@pytest.mark.dependency(depends=["test_5_format_standardization.py"])
class TestReconciliation:
    """Test suite for transaction reconciliation"""
    
    def test_basic_matching(self):
        """Test basic transaction matching"""
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        matches, unmatched = reconcile_transactions(source_df, target_df)
        assert len(matches) == 1
        assert len(unmatched) == 0
        
    def test_multiple_matches(self):
        """Test multiple transaction matches"""
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-02'],
            'Post Date': ['2025-01-02', '2025-01-03'],
            'Description': ['test transaction 1', 'test transaction 2'],
            'Amount': [-50.00, -75.00],
            'Category': ['shopping', 'dining']
        })
        
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-02'],
            'Post Date': ['2025-01-02', '2025-01-03'],
            'Description': ['test transaction 1', 'test transaction 2'],
            'Amount': [-50.00, -75.00],
            'Category': ['shopping', 'dining']
        })
        
        matches, unmatched = reconcile_transactions(source_df, target_df)
        assert len(matches) == 2
        assert len(unmatched) == 0
        
    def test_unmatched_transactions(self):
        """Test handling of unmatched transactions"""
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-03'],
            'Post Date': ['2025-01-04'],
            'Description': ['different transaction'],
            'Amount': [-75.00],
            'Category': ['dining']
        })
        
        matches, unmatched = reconcile_transactions(source_df, target_df)
        assert len(matches) == 0
        assert len(unmatched) == 2
        
    def test_duplicate_handling(self):
        """Test handling of duplicate transactions"""
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-01'],
            'Post Date': ['2025-01-02', '2025-01-02'],
            'Description': ['test transaction', 'test transaction'],
            'Amount': [-50.00, -50.00],
            'Category': ['shopping', 'shopping']
        })
        
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        matches, unmatched = reconcile_transactions(source_df, target_df)
        assert len(matches) == 1
        assert len(unmatched) == 1
        
    def test_date_matching(self):
        """Test date-based matching"""
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-02'],  # Different date
            'Post Date': ['2025-01-03'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        matches, unmatched = reconcile_transactions(source_df, target_df)
        assert len(matches) == 0
        assert len(unmatched) == 2
        
    def test_amount_matching(self):
        """Test amount-based matching"""
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping']
        })
        
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-75.00],  # Different amount
            'Category': ['shopping']
        })
        
        matches, unmatched = reconcile_transactions(source_df, target_df)
        assert len(matches) == 0
        assert len(unmatched) == 2

@pytest.mark.dependency(depends=["test_5_format_standardization.py"])
def test_find_matches():
    """Test the find_matches function"""
    source_df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['test transaction'],
        'Amount': [-50.00],
        'Category': ['shopping']
    })
    
    target_df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['test transaction'],
        'Amount': [-50.00],
        'Category': ['shopping']
    })
    
    matches = find_matches(source_df, target_df)
    assert len(matches) == 1
    assert matches.iloc[0]['source_index'] == 0
    assert matches.iloc[0]['target_index'] == 0

@pytest.mark.dependency(depends=["test_5_format_standardization.py"])
def test_calculate_discrepancies():
    """Test the calculate_discrepancies function"""
    source_df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['test transaction'],
        'Amount': [-50.00],
        'Category': ['shopping']
    })
    
    target_df = pd.DataFrame({
        'Transaction Date': ['2025-01-01'],
        'Post Date': ['2025-01-02'],
        'Description': ['test transaction'],
        'Amount': [-75.00],  # Different amount
        'Category': ['shopping']
    })
    
    discrepancies = calculate_discrepancies(source_df, target_df)
    assert len(discrepancies) == 1
    assert discrepancies.iloc[0]['amount_difference'] == 25.00 