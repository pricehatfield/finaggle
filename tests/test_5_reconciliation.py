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
    process_chase_format
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
        'reconciled_key': ['P:2025-03-17_40.33', 'P:2025-03-18_25.99'],
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
        'reconciled_key': ['U:2025-03-19_75.50', 'U:2025-03-20_150.25'],
        'Matched': [False, False]
    })

def test_setup_logging(tmp_path, monkeypatch):
    """Test logging setup"""
    log_file = tmp_path / 'test.log'
    monkeypatch.setenv('LOG_FILE', str(log_file))
    
    # Import after setting environment variable
    from src.reconcile import setup_logging
    
    # Reset logging configuration
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    setup_logging()
    assert log_file.exists()
    assert logging.getLogger().level == logging.INFO

def test_ensure_directory(tmp_path, monkeypatch):
    """Test directory creation"""
    test_dir = tmp_path / 'test_dir'
    monkeypatch.setenv('DATA_DIR', str(test_dir))
    
    # Import after setting environment variable
    from src.reconcile import ensure_directory
    
    # Test creating archive directory
    archive_dir = ensure_directory('archive')
    assert os.path.exists(archive_dir)
    assert os.path.isdir(archive_dir)
    
    # Test creating logs directory
    logs_dir = ensure_directory('logs')
    assert os.path.exists(logs_dir)
    assert os.path.isdir(logs_dir)
    
    # Test invalid directory type
    with pytest.raises(ValueError):
        ensure_directory('invalid')

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
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(df, pd.DataFrame) for df in result)
    assert all(not df.empty for df in result)
    
    # Verify source files
    source_files = {df['source_file'].iloc[0] for df in result}
    assert source_files == {'test1', 'test2'}

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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
        assert len(matches) == 0
        assert len(unmatched) == 2

    def test_reconciled_output_format(self, sample_matched_df, sample_unmatched_df):
        """Test the format of reconciled output"""
        # Test matched transactions format
        assert not sample_matched_df.empty
        required_columns = [
            'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category',
            'source_file', 'Date', 'YearMonth', 'Account', 'Tags', 'reconciled_key', 'Matched'
        ]
        assert all(col in sample_matched_df.columns for col in required_columns)
        
        # Test unmatched transactions format
        assert not sample_unmatched_df.empty
        assert all(col in sample_unmatched_df.columns for col in required_columns)
        
        # Test data types
        assert pd.api.types.is_datetime64_any_dtype(sample_matched_df['Transaction Date'])
        assert pd.api.types.is_datetime64_any_dtype(sample_matched_df['Post Date'])
        assert pd.api.types.is_datetime64_any_dtype(sample_matched_df['Date'])
        assert pd.api.types.is_float_dtype(sample_matched_df['Amount'])
        assert pd.api.types.is_string_dtype(sample_matched_df['Description'])
        assert pd.api.types.is_string_dtype(sample_matched_df['Category'])
        assert pd.api.types.is_string_dtype(sample_matched_df['source_file'])
        assert pd.api.types.is_string_dtype(sample_matched_df['Account'])
        assert pd.api.types.is_string_dtype(sample_matched_df['Tags'])
        assert pd.api.types.is_string_dtype(sample_matched_df['reconciled_key'])
        assert pd.api.types.is_bool_dtype(sample_matched_df['Matched'])

    def test_reconciled_key_format(self):
        """Test that reconciled keys are in the correct format"""
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
        
        # Verify matched transaction key format
        assert matches['reconciled_key'].iloc[0] == 'P:2025-01-01_50.00'
        
        # Verify unmatched transaction key format
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
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
        assert unmatched['reconciled_key'].iloc[0] == 'U:2025-01-01_50.00'

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
    
    matches, unmatched = reconcile_transactions(source_df, [target_df])
    assert len(matches) == 0  # Should not match due to different amounts
    assert len(unmatched) == 2  # Both transactions should be unmatched 

def create_test_aggregator_data():
    """Create test data for aggregator format."""
    return pd.DataFrame({
        'Transaction Date': ['2025-03-17', '2025-03-18', '2025-03-19'],
        'Post Date': ['2025-03-17', '2025-03-18', '2025-03-19'],
        'Description': ['AMAZON.COM', 'NETFLIX.COM', 'WALMART'],
        'Amount': [-40.33, -13.99, -50.00],
        'Category': ['Shopping', 'Entertainment', 'Shopping'],
        'Tags': ['Online', 'Subscription', 'Groceries'],
        'source_file': ['aggregator', 'aggregator', 'aggregator']
    })

def create_test_detail_data():
    """Create test data for detail format."""
    return pd.DataFrame({
        'Transaction Date': ['2025-03-17', '2025-03-18', '2025-03-19'],
        'Post Date': ['2025-03-17', '2025-03-18', '2025-03-19'],
        'Description': ['AMAZON.COM', 'NETFLIX.COM', 'WALMART'],
        'Amount': [-40.33, -13.99, -50.00],
        'Category': ['Shopping', 'Entertainment', 'Shopping'],
        'source_file': ['discover', 'discover', 'discover']
    })

def test_tag_preservation():
    """Test that tags from aggregator are preserved in reconciliation output.
    
    Verifies:
    - Tags from aggregator are preserved in matched records
    - Tags from aggregator are preserved in unmatched aggregator records
    - Empty tags for unmatched detail records
    """
    # Create test data
    aggregator_df = create_test_aggregator_data()
    detail_df = create_test_detail_data()
    
    # Run reconciliation
    matches_df, unmatched_df = reconcile_transactions(aggregator_df, [detail_df])
    
    # Check matched records
    assert not matches_df.empty
    assert 'Tags' in matches_df.columns
    assert matches_df['Tags'].tolist() == ['Online', 'Subscription', 'Groceries']
    
    # Check unmatched records (should be empty in this case since all records match)
    assert unmatched_df.empty

    # Test with mismatched data to verify unmatched behavior
    detail_df_modified = detail_df.copy()
    detail_df_modified['Amount'] = [-41.33, -14.99, -51.00]  # Change amounts to force unmatched
    
    matches_df, unmatched_df = reconcile_transactions(aggregator_df, [detail_df_modified])
    
    # Check unmatched aggregator records
    aggregator_unmatched = unmatched_df[unmatched_df['source_file'] == 'aggregator']
    assert not aggregator_unmatched.empty
    assert aggregator_unmatched['Tags'].tolist() == ['Online', 'Subscription', 'Groceries']
    
    # Check unmatched detail records
    detail_unmatched = unmatched_df[unmatched_df['source_file'] == 'discover']
    assert not detail_unmatched.empty
    assert all(tag == '' for tag in detail_unmatched['Tags']) 