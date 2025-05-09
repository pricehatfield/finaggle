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
        aggregator_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'Account': ['Test Account']
        })
        
        detail_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'source_file': ['test_target.csv']
        })
        
        # Use aggregator as first argument, detail as second argument
        matches, unmatched = reconcile_transactions(aggregator_df, [detail_df])
        assert len(matches) == 1
        assert len(unmatched) == 0
    
    def test_multiple_matches(self):
        """Test multiple transaction matches"""
        aggregator_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-02'],
            'Post Date': ['2025-01-02', '2025-01-03'],
            'Description': ['test transaction 1', 'test transaction 2'],
            'Amount': [-50.00, -75.00],
            'Category': ['shopping', 'dining'],
            'Account': ['Test Account 1', 'Test Account 2']
        })
        
        detail_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-02'],
            'Post Date': ['2025-01-02', '2025-01-03'],
            'Description': ['test transaction 1', 'test transaction 2'],
            'Amount': [-50.00, -75.00],
            'Category': ['shopping', 'dining'],
            'source_file': ['test_target.csv', 'test_target.csv']
        })
        
        # Use aggregator as first argument, detail as second argument
        matches, unmatched = reconcile_transactions(aggregator_df, [detail_df])
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
        aggregator_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01', '2025-01-01'],
            'Post Date': ['2025-01-02', '2025-01-02'],
            'Description': ['test transaction', 'test transaction'],
            'Amount': [-50.00, -50.00],
            'Category': ['shopping', 'shopping'],
            'Account': ['Test Account', 'Test Account']
        })
        
        detail_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'source_file': ['test_target.csv']
        })
        
        # Use aggregator as first argument, detail as second argument
        matches, unmatched = reconcile_transactions(aggregator_df, [detail_df])
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
        aggregator_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'Account': ['Test Account']
        })
        
        detail_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'source_file': ['test_target.csv']
        })
        
        # Use aggregator as first argument, detail as second argument
        matches, unmatched = reconcile_transactions(aggregator_df, [detail_df])
        
        # Verify matched transaction key format - uses Post Date when available
        assert not matches.empty, "No matches found"
        assert matches['reconciled_key'].iloc[0] == 'P:2025-01-02_50.00'
        
        # Create new test data for unmatched scenario
        source_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-50.00],
            'Category': ['shopping'],
            'Account': ['Test Account']
        })
        
        # Different amount to ensure no match
        target_df = pd.DataFrame({
            'Transaction Date': ['2025-01-01'],
            'Post Date': ['2025-01-02'],
            'Description': ['test transaction'],
            'Amount': [-75.00],  # Different amount 
            'Category': ['shopping'],
            'source_file': ['test_target.csv']
        })
        
        matches, unmatched = reconcile_transactions(source_df, [target_df])
        
        # Verify unmatched transaction key format
        assert not unmatched.empty, "No unmatched records found"
        source_unmatched = unmatched[unmatched['Account'].str.contains('Test Account')]
        assert not source_unmatched.empty, "No source unmatched records found"
        assert source_unmatched['reconciled_key'].iloc[0].startswith('U:'), f"Expected key to start with U: but got {source_unmatched['reconciled_key'].iloc[0]}"

    def test_tag_preservation(self):
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
        assert not matches_df.empty, "No matches found between aggregator and detail records"
        assert 'Tags' in matches_df.columns
        assert len(matches_df['Tags'].tolist()) == 3, f"Expected 3 tags but got {len(matches_df['Tags'].tolist())}"
        assert set(matches_df['Tags'].tolist()) == set(['Online', 'Subscription', 'Groceries']), f"Tags don't match expected values"
        
        # Check unmatched records (should be empty in this case since all records match)
        assert unmatched_df.empty, "Expected no unmatched records"

        # Test with mismatched data to verify unmatched behavior
        detail_df_modified = detail_df.copy()
        # Change amounts to force unmatched
        detail_df_modified['Amount'] = [-41.33, -14.99, -51.00]  
        
        # Run reconciliation with modified data
        matches_df, unmatched_df = reconcile_transactions(aggregator_df, [detail_df_modified])
        
        # Verify no matches
        assert matches_df.empty, "Expected no matches due to different amounts"
        
        # Check unmatched records
        assert not unmatched_df.empty, "Expected unmatched records"
        
        # Check unmatched aggregator records
        aggregator_unmatched = unmatched_df[unmatched_df['Account'].str.contains('Aggregator')]
        assert not aggregator_unmatched.empty, "No unmatched aggregator records found"
        
        # Check tags preserved in unmatched aggregator records
        assert set(aggregator_unmatched['Tags'].tolist()) == set(['Online', 'Subscription', 'Groceries']), "Tags not preserved in unmatched aggregator records"
        
        # Check unmatched detail records
        detail_unmatched = unmatched_df[unmatched_df['Account'].str.contains('discover')]
        assert not detail_unmatched.empty, "No unmatched detail records found"
        
        # Check empty tags in unmatched detail records
        assert all(tag == '' for tag in detail_unmatched['Tags']), "Expected empty tags in unmatched detail records"

    def test_aggregator_field_precedence(self):
        """Test that aggregator fields take precedence over detail fields for matched transactions.
        
        This test verifies:
        - For matched transactions, all fields available in the aggregator record take precedence
        - Detail record fields are only used when the corresponding aggregator field is null/empty
        - This applies to: Date, Account, Description, Category, Amount fields
        - Tags are exclusively sourced from the aggregator
        """
        # Create test data with different values in aggregator vs detail
        aggregator_df = pd.DataFrame({
            'Transaction Date': ['2025-03-17'],  # Both aggregator and detail use the same date
            'Description': ['AMAZON AGGREGATOR DESC'],  # Different from detail
            'Amount': [-40.33],
            'Category': ['Aggregator Category'],  # Different from detail
            'Account': ['Aggregator Account'],    # Different from detail
            'Tags': ['Aggregator Tag'],           # Only in aggregator
            'source_file': ['aggregator']
        })
        
        detail_df = pd.DataFrame({
            'Transaction Date': ['2025-03-17'],  # Both aggregator and detail use the same date
            'Post Date': ['2025-03-17'],         # Same as Transaction Date for testing
            'Description': ['AMAZON DETAIL DESC'],  # Different from aggregator
            'Amount': [-40.33],                     # Same as aggregator for matching
            'Category': ['Detail Category'],        # Different from aggregator
            'source_file': ['detail']
        })
        
        # Print dataframes for debugging
        print("\nAggregator DataFrame:")
        print(aggregator_df)
        print("\nDetail DataFrame:")
        print(detail_df)
        
        # Run reconciliation - we need to use P: keys for matching
        agg_key = f"P:{aggregator_df['Transaction Date'].iloc[0]}_{abs(aggregator_df['Amount'].iloc[0]):.2f}"
        detail_key = f"P:{detail_df['Post Date'].iloc[0]}_{abs(detail_df['Amount'].iloc[0]):.2f}"
        print(f"\nAggregator key: {agg_key}")
        print(f"Detail key: {detail_key}")
        
        matches_df, unmatched_df = reconcile_transactions(aggregator_df, [detail_df])
        
        print("\nMatched DataFrame:")
        print(matches_df if not matches_df.empty else "No matches found!")
        print("\nUnmatched DataFrame:")
        print(unmatched_df)
        
        # Verify matched transaction uses aggregator's fields for all available fields
        assert not matches_df.empty, "No matches found between aggregator and detail records"
        assert matches_df['Description'].iloc[0] == 'AMAZON AGGREGATOR DESC', f"Expected 'AMAZON AGGREGATOR DESC' but got {matches_df['Description'].iloc[0]}"
        assert matches_df['Category'].iloc[0] == 'Aggregator Category', f"Expected 'Aggregator Category' but got {matches_df['Category'].iloc[0]}"
        assert matches_df['Amount'].iloc[0] == -40.33, f"Expected -40.33 but got {matches_df['Amount'].iloc[0]}"
        assert matches_df['Account'].iloc[0] == 'Aggregator Account', f"Expected 'Aggregator Account' but got {matches_df['Account'].iloc[0]}"
        assert matches_df['Tags'].iloc[0] == 'Aggregator Tag', f"Expected 'Aggregator Tag' but got {matches_df['Tags'].iloc[0]}"
        
        # Test with null fields in aggregator
        aggregator_df_null = aggregator_df.copy()
        aggregator_df_null['Category'] = None  # Null category in aggregator
        aggregator_df_null['Description'] = None  # Null description in aggregator
        
        matches_df, unmatched_df = reconcile_transactions(aggregator_df_null, [detail_df])
        
        # Verify detail fields are used when aggregator fields are null
        assert not matches_df.empty, "No matches found with null aggregator field"
        assert matches_df['Description'].iloc[0] == 'AMAZON DETAIL DESC', f"Expected 'AMAZON DETAIL DESC' but got {matches_df['Description'].iloc[0]}"
        assert matches_df['Category'].iloc[0] == 'Detail Category', f"Expected 'Detail Category' but got {matches_df['Category'].iloc[0]}"

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
        'Post Date': ['2025-03-17', '2025-03-18', '2025-03-19'],  # Same as Transaction Date
        'Description': ['AMAZON.COM', 'NETFLIX.COM', 'WALMART'],
        'Amount': [-40.33, -13.99, -50.00],
        'Category': ['Shopping', 'Entertainment', 'Shopping'],
        'Tags': ['Online', 'Subscription', 'Groceries'],
        'Account': ['Aggregator Account', 'Aggregator Account', 'Aggregator Account'],
        'source_file': ['aggregator', 'aggregator', 'aggregator']
    })

def create_test_detail_data():
    """Create test data for detail format."""
    return pd.DataFrame({
        'Transaction Date': ['2025-03-17', '2025-03-18', '2025-03-19'],
        'Post Date': ['2025-03-17', '2025-03-18', '2025-03-19'],  # Same as corresponding aggregator Post Date
        'Description': ['AMAZON.COM', 'NETFLIX.COM', 'WALMART'],
        'Amount': [-40.33, -13.99, -50.00],  # Exact match to aggregator amounts
        'Category': ['Shopping', 'Entertainment', 'Shopping'],
        'source_file': ['discover', 'discover', 'discover']
    }) 