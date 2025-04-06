import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
import re
from src.reconcile import (
    standardize_date,
    clean_amount,
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_format,
    process_chase_format,
    process_aggregator_format,
    reconcile_transactions,
    import_csv,
    import_folder,
    ensure_directory,
    setup_logging
)

# Fixtures for test data
@pytest.fixture
def sample_date_strings():
    return {
        'standard': '2024-03-15',
        'us_format': '03/15/2024',
        'with_time': '2024-03-15 14:30:00',
        'invalid': 'not-a-date'
    }

@pytest.fixture
def sample_amounts():
    return {
        'simple': '123.45',
        'with_currency': '$123.45',
        'with_commas': '1,234.56',
        'negative': '-123.45',
        'invalid': 'not-a-number'
    }

@pytest.fixture
def sample_df1():
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [-123.45, -456.78],
        'Category': ['', ''],
        'source_file': ['test1.csv', 'test1.csv']
    })

@pytest.fixture
def sample_df2():
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [-123.45, -456.78],
        'Category': ['', ''],
        'source_file': ['test2.csv', 'test2.csv']
    })

@pytest.fixture
def sample_df3():
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [-123.45, 456.78],
        'Category': ['', ''],
        'source_file': ['test3.csv', 'test3.csv']
    })

@pytest.fixture
def sample_aggregator_df():
    """Sample aggregator transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['2024-01-02', '2024-01-03'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Amount': [-50.00, -25.00],
        'Category': ['Shopping', 'Groceries'],
        'Tags': ['', ''],
        'Account': ['Discover', 'Discover'],
        'source_file': ['aggregator.csv', 'aggregator.csv']
    })

@pytest.fixture
def sample_discover_df():
    """Sample Discover credit card transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['2024-01-02', '2024-01-03'],
        'Description': ['AMAZON.COM', 'WALMART'],
        'Amount': ['$50.00', '$25.00'],
        'Category': ['Shopping', 'Groceries'],
        'source_file': ['discover.csv', 'discover.csv']
    })

@pytest.fixture
def sample_amex_df():
    """Sample American Express transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['2024-01-01', '2024-01-02'],
        'Description': ['UBER', 'NETFLIX'],
        'Amount': ['$30.00', '$15.99'],
        'Category': ['Transportation', 'Entertainment'],
        'source_file': ['amex.csv', 'amex.csv']
    })

@pytest.fixture
def sample_capital_one_df():
    """Sample Capital One transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['2024-01-02', '2024-01-03'],
        'Description': ['TARGET', 'COSTCO'],
        'Amount': [-50.00, -100.00],
        'Category': ['Shopping', 'Groceries'],
        'source_file': ['capital_one.csv', 'capital_one.csv']
    })

@pytest.fixture
def sample_alliant_df():
    """Sample Alliant credit union transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['2024-01-02', '2024-01-03'],
        'Description': ['TEST_MERCHANT_1 123-456-7890 ST', 'TEST_MERCHANT_2 987-654-3210 ST'],
        'Amount': ['$42.80', '$7.57'],
        'Category': ['', ''],
        'source_file': ['alliant.csv', 'alliant.csv']
    })

@pytest.fixture
def sample_chase_df():
    """Sample Chase transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['2024-01-02', '2024-01-03'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [-95.89, -428.68],
        'Category': ['', ''],
        'source_file': ['chase.csv', 'chase.csv']
    })

@pytest.fixture
def sample_transaction_types_df():
    """Test data for different transaction types."""
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-15', '2024-03-15', '2024-03-15', '2024-03-15', '2024-03-15'],
        'Post Date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-16', '2024-03-15', '2024-03-15'],
        'Description': [
            'AMAZON.COM*ABC123',  # Regular purchase
            'RETURN AMAZON.COM*ABC123',  # Return
            'PENDING AMAZON.COM*ABC123',  # Pending transaction
            'HOLD AMAZON.COM*ABC123',  # Authorization hold
            'NETFLIX.COM*SUBSCRIPTION',  # Recurring payment
            'TRANSFER TO SAVINGS'  # Transfer
        ],
        'Amount': [-123.45, 123.45, -123.45, -123.45, -15.99, -500.00],
        'Category': ['Shopping', 'Shopping', 'Shopping', 'Shopping', 'Entertainment', 'Transfer'],
        'source_file': ['transactions.csv'] * 6
    })

@pytest.fixture
def sample_duplicate_transactions_df():
    """Test data for duplicate transactions."""
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-15'],
        'Post Date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-15'],
        'Description': [
            'AMAZON.COM*ABC123',
            'AMAZON.COM*ABC123',  # Exact duplicate
            'AMAZON.COM*ABC123',  # Different date
            'AMAZON.COM*ABC123'   # Different amount
        ],
        'Amount': [-123.45, -123.45, -123.45, -124.45],
        'Category': ['Shopping', 'Shopping', 'Shopping', 'Shopping'],
        'source_file': ['duplicates.csv'] * 4
    })

@pytest.fixture
def sample_duplicate_aggregator_df():
    """Test data for duplicate transactions in aggregator
    Matches with sample_duplicate_transactions_df to test duplicate handling
    """
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-15'],
        'Post Date': ['2024-03-15', '2024-03-15'],
        'Description': ['AMAZON.COM*ABC123', 'AMAZON.COM*ABC123'],
        'Amount': [-123.45, -123.45],
        'Category': ['Shopping', 'Shopping'],
        'Tags': ['', ''],
        'Account': ['Test Account', 'Test Account'],
        'source_file': ['aggregator_dupes.csv', 'aggregator_dupes.csv']
    })

# Basic utility function tests
def test_standardize_date(sample_date_strings):
    """Test date standardization to YYYY-MM-DD format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for standardized date format
    - All dates must be in YYYY-MM-DD format
    - Various input formats must be properly converted
    """
    assert standardize_date(sample_date_strings['standard']) == '2024-03-15'
    assert standardize_date(sample_date_strings['us_format']) == '2024-03-15'
    assert standardize_date(sample_date_strings['with_time']) == '2024-03-15'
    assert standardize_date(sample_date_strings['invalid']) is None

def test_clean_amount(sample_amounts):
    """Test amount standardization to decimal format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for standardized amount format
    - All amounts must be decimal with 2 decimal places
    - Various input formats must be properly converted
    """
    assert clean_amount(sample_amounts['simple']) == 123.45
    assert clean_amount(sample_amounts['with_currency']) == 123.45
    assert clean_amount(sample_amounts['with_commas']) == 1234.56
    assert clean_amount(sample_amounts['negative']) == -123.45
    assert clean_amount(sample_amounts['invalid']) == 0.0

# Infrastructure tests
def test_setup_logging(tmp_path, monkeypatch):
    """Test logging setup
    INTERNAL CHECK: Verifies development infrastructure for logging
    - Log directory creation
    - Log file naming
    - Log rotation
    """
    log_dir = tmp_path / "logs"
    monkeypatch.setattr('reconcile.__file__', str(tmp_path / "reconcile.py"))
    setup_logging(logging.DEBUG)
    assert os.path.exists(log_dir)
    assert any(file.endswith('.log') for file in os.listdir(log_dir))

def test_ensure_directory(tmp_path, monkeypatch):
    """Test directory creation
    INTERNAL CHECK: Verifies development infrastructure for directory management
    - Directory creation
    - Directory permissions
    - Error handling
    """
    monkeypatch.setattr('reconcile.__file__', str(tmp_path / "reconcile.py"))
    archive_dir = ensure_directory("archive")
    assert os.path.exists(archive_dir)
    assert os.path.isdir(archive_dir)
    
    log_dir = ensure_directory("logs")
    assert os.path.exists(log_dir)
    assert os.path.isdir(log_dir)

# Individual format processing tests
def test_process_discover_format(sample_discover_df):
    """Test processing of Discover format."""
    result = process_discover_format(sample_discover_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert result['Transaction Date'].iloc[0] == '2024-01-01'
    assert result['Post Date'].iloc[0] == '2024-01-02'
    assert result['Amount'].iloc[0] == -50.00  # Should be negative for debits
    assert result['source_file'].iloc[0] == 'discover'

def test_process_amex_format(sample_amex_df):
    """Test processing of American Express format."""
    result = process_amex_format(sample_amex_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert result['Transaction Date'].iloc[0] == '2024-01-01'
    assert result['Post Date'].iloc[0] == '2024-01-01'  # AMEX only provides one date
    assert result['Amount'].iloc[0] == -30.00  # Should be negative for debits
    assert result['source_file'].iloc[0] == 'amex'

def test_process_capital_one_format(sample_capital_one_df):
    """Test processing of Capital One format."""
    result = process_capital_one_format(sample_capital_one_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert result['Transaction Date'].iloc[0] == '2024-01-01'
    assert result['Post Date'].iloc[0] == '2024-01-02'
    assert result['Amount'].iloc[0] == -50.00  # Should be negative for debits
    assert result['source_file'].iloc[0] == 'capital_one'

def test_process_alliant_format(sample_alliant_df):
    """Test processing Alliant format."""
    result = process_alliant_format(sample_alliant_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert result['Post Date'].iloc[0] == '2024-01-02'
    assert result['Transaction Date'].iloc[0] == '2024-01-01'
    assert result['Amount'].iloc[0] == -42.80  # Amount is inverted and cleaned
    assert result['Description'].iloc[0] == 'TEST_MERCHANT_1 123-456-7890 ST'
    assert result['source_file'].iloc[0] == 'alliant'

def test_process_chase_format(sample_chase_df):
    """Test processing Chase format."""
    result = process_chase_format(sample_chase_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert result['Post Date'].iloc[0] == '2024-01-02'
    assert result['Transaction Date'].iloc[0] == '2024-01-01'
    assert result['Amount'].iloc[0] == -95.89  # Chase amounts are already negative for debits
    assert result['source_file'].iloc[0] == 'chase'

def test_process_aggregator_format(sample_aggregator_df):
    """Test processing aggregator format."""
    result = process_aggregator_format(sample_aggregator_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert result['Post Date'].iloc[0] == '2024-01-02'
    assert result['Transaction Date'].iloc[0] == '2024-01-01'
    assert result['Amount'].iloc[0] == -50.00
    assert result['Category'].iloc[0] == 'Shopping'
    assert result['source_file'].iloc[0] == 'aggregator'

# Basic reconciliation tests
def test_reconcile_transactions_basic_matching(sample_aggregator_df, sample_discover_df):
    """Test basic transaction matching."""
    result = reconcile_transactions(sample_aggregator_df, [process_discover_format(sample_discover_df)])
    assert len(result) == 2
    assert result['Matched'].iloc[0] == True
    assert result['reconciled_key'].iloc[0].startswith('P:')  # Should match on Post Date first

def test_reconcile_transactions_date_priority(sample_aggregator_df, sample_discover_df):
    """Test that Post Date matches take priority over Transaction Date."""
    agg = sample_aggregator_df.copy()
    agg.loc[0, 'Post Date'] = '2024-01-03'  # Change Post Date to force Transaction Date match
    result = reconcile_transactions(agg, [process_discover_format(sample_discover_df)])
    assert len(result) == 2
    assert result['Matched'].iloc[0] == True
    assert result['reconciled_key'].iloc[0].startswith('T:')  # Should fall back to Transaction Date match

def test_reconcile_transactions_duplicates(sample_aggregator_df, sample_discover_df):
    """Test handling of duplicate transactions."""
    # Create duplicate transaction in detail records
    detail_dup = sample_discover_df.copy()
    detail_dup = pd.concat([detail_dup, detail_dup.iloc[[0]]])
    result = reconcile_transactions(sample_aggregator_df, [process_discover_format(detail_dup)])
    assert len(result) == 3  # Original 2 + 1 duplicate
    matched_count = result['Matched'].sum()
    assert matched_count == 2  # Should only match the original transactions

# File import tests
def test_import_csv(tmp_path):
    """Test importing CSV files with different formats."""
    # Test Discover format
    discover_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test'],
        'Amount': ['123.45'],
        'Category': ['']
    })
    discover_path = tmp_path / "discover_card.csv"
    discover_df.to_csv(discover_path, index=False)
    discover_result = import_csv(str(discover_path))
    assert discover_result is not None
    assert set(discover_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    
    # Test Chase format
    chase_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test Transaction'],
        'Amount': [-123.45],
        'Category': ['']
    })
    chase_path = tmp_path / "chase_card.csv"
    chase_df.to_csv(chase_path, index=False)
    chase_result = import_csv(str(chase_path))
    assert chase_result is not None
    assert set(chase_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    assert chase_result['Amount'].iloc[0] == -123.45  # Keep original sign from Chase
    
    # Test Amex format
    amex_df = pd.DataFrame({
        'Date': ['2024-03-15'],
        'Description': ['Test'],
        'Amount': ['123.45'],
        'Category': ['']
    })
    amex_path = tmp_path / "amex_card.csv"
    amex_df.to_csv(amex_path, index=False)
    amex_result = import_csv(str(amex_path))
    assert amex_result is not None
    assert set(amex_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    
    # Test Capital One format
    capital_one_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Posted Date': ['2024-03-15'],
        'Description': ['Test'],
        'Debit': ['123.45'],
        'Credit': [''],
        'Category': ['']
    })
    capital_one_path = tmp_path / "capital_one_card.csv"
    capital_one_df.to_csv(capital_one_path, index=False)
    capital_one_result = import_csv(str(capital_one_path))
    assert capital_one_result is not None
    assert set(capital_one_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}
    
    # Test Alliant format
    alliant_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test'],
        'Amount': ['$42.80'],
        'Category': ['']
    })
    alliant_path = tmp_path / "alliant_card.csv"
    alliant_df.to_csv(alliant_path, index=False)
    alliant_result = import_csv(str(alliant_path))
    assert alliant_result is not None
    assert set(alliant_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'source_file'}

def test_import_folder(tmp_path):
    """Test importing multiple CSV files with different institution formats
    BUSINESS REQUIREMENT: Verifies the spec's requirement for folder import functionality
    - Support for multiple files
    - Proper format detection per file
    - Correct combination of records
    """
    # Create test folder with multiple institution formats
    discover_df = pd.DataFrame({
        'Trans. Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test1'],
        'Amount': ['123.45'],
        'Category': ['']
    })
    discover_df.to_csv(tmp_path / "discover_card.csv", index=False)
    
    amex_df = pd.DataFrame({
        'Date': ['2024-03-16'],
        'Description': ['Test2'],
        'Card Member': ['JOHN DOE'],
        'Account #': ['1234'],
        'Amount': ['456.78']
    })
    amex_df.to_csv(tmp_path / "amex_card.csv", index=False)
    
    result = import_folder(str(tmp_path))
    assert result is not None
    assert len(result) == 2  # Should combine both CSV files
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    assert all(source in result['source_file'].unique() for source in ['discover_card.csv', 'amex_card.csv'])

# Complex reconciliation tests
def test_reconcile_transactions_date_matching(sample_alliant_df, sample_aggregator_df):
    """Test that reconciliation properly handles Post Date and Transaction Date matching
    BUSINESS REQUIREMENT: Verifies the spec's requirement for matching on both Post Date and Transaction Date
    - Post Date matches use P: prefix
    - Transaction Date matches use T: prefix
    - Both types of matches are valid
    """
    # Create test data where aggregator date matches Post Date for one record
    # and Transaction Date for another
    detail_records = process_alliant_format(sample_alliant_df)

    # Modify aggregator dates and amounts to match both Post Date and Transaction Date
    agg_df = sample_aggregator_df.copy()
    agg_df.loc[0, 'Transaction Date'] = '2025-01-02'  # Should match Post Date
    agg_df.loc[0, 'Amount'] = -42.80  # Match first detail record amount
    agg_df.loc[1, 'Transaction Date'] = '2025-01-01'  # Should match Transaction Date
    agg_df.loc[1, 'Amount'] = -7.57  # Match second detail record amount

    result = reconcile_transactions(detail_records, agg_df)

    # Verify matches
    assert result['Matched'].sum() == 2, "Should have two matches"
    assert 'reconciled_key' in result.columns
    assert 'Category' in result.columns
    assert result['Matched'].dtype == bool
    
    # Verify the matches were made correctly and have the right reconciled_key format
    post_date_match = result[result['Transaction Date'] == '2025-01-02']
    trans_date_match = result[result['Transaction Date'] == '2025-01-01']
    assert not post_date_match.empty, "Should have a Post Date match"
    assert not trans_date_match.empty, "Should have a Transaction Date match"
    
    # Verify reconciled_key format for Post Date match
    assert post_date_match['reconciled_key'].iloc[0] == f"P:2025-01-02_-42.80", "Post Date match should have P: prefix and underscore separator"
    
    # Verify reconciled_key format for Transaction Date match
    assert trans_date_match['reconciled_key'].iloc[0] == f"T:2025-01-01_-7.57", "Transaction Date match should have T: prefix and underscore separator"
    
    # Verify the original dates are preserved in the detail records
    assert detail_records['Post Date'].iloc[0] == '2025-01-02'
    assert detail_records['Transaction Date'].iloc[0] == '2025-01-01'
    
    # Verify the matches were made using the correct date fields
    post_date_match_detail = detail_records[detail_records['Post Date'] == '2025-01-02']
    trans_date_match_detail = detail_records[detail_records['Transaction Date'] == '2025-01-01']
    assert not post_date_match_detail.empty, "Should have a record with matching Post Date"
    assert not trans_date_match_detail.empty, "Should have a record with matching Transaction Date"
    
    # Verify the matches were made using the correct date fields in the result
    post_date_match_result = result[result['Transaction Date'] == '2025-01-02']
    trans_date_match_result = result[result['Transaction Date'] == '2025-01-01']
    assert not post_date_match_result.empty, "Should have a record with matching Post Date in result"
    assert not trans_date_match_result.empty, "Should have a record with matching Transaction Date in result"

def test_reconcile_transactions_chase_format():
    """Test reconciliation with Chase format records
    BUSINESS REQUIREMENT: Verifies handling of Chase-specific format as specified in the spec
    - Proper processing of Chase date formats
    - Correct handling of Chase amount formats
    - Proper matching with aggregator records
    """
    # Create Chase record with anonymized data
    chase_df = pd.DataFrame({
        'Transaction Date': ['2024-01-01', '2024-01-02'],
        'Post Date': ['03/12/2025', '03/10/2025'],
        'Description': ['TEST_MERCHANT_3           ACH_DEBIT  TEST_REFERENCE     WEB ID: TEST123'],
        'Amount': [-95.89, -428.68],
        'Type': ['ACH_DEBIT', 'ACH_DEBIT'],
        'Balance': [3990.63, 4086.52],
        'Check or Slip #': ['', '']
    })
    
    # Create matching aggregator record
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2025-03-12'],
        'Post Date': ['2025-03-12'],
        'Description': ['Test Subscription Service'],
        'Amount': [-95.89],
        'Category': ['Subscription'],
        'Tags': ['Test'],
        'Account': ['Test Account']
    })
    
    # Process Chase record
    processed_chase = process_chase_format(chase_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_chase])
    
    # Verify the transaction was matched
    assert len(result) == 1, "Should have exactly one record"
    assert result['Matched'].iloc[0], "Transaction should be marked as matched"
    assert result['reconciled_key'].iloc[0].startswith(('P:', 'T:')), "Should have P: or T: prefix"
    assert result['Amount'].iloc[0] == -95.89, "Amount should match"
    assert result['Transaction Date'].iloc[0] == '2025-03-12', "Date should match"

# Data structure and edge case tests
def test_dataframe_structure_consistency(sample_discover_df, sample_amex_df, sample_capital_one_df, sample_alliant_df, sample_aggregator_df, tmp_path):
    """Regression test to ensure consistent DataFrame structures across all processing functions
    BUSINESS REQUIREMENT: Verifies the spec's requirements for data structure consistency
    - Required columns for detail records (Transaction Date, Post Date, Description, Amount, source_file)
    - Required columns for aggregator records (Date, Description, Amount, Category, Tags, Account)
    - Required columns for reconciled output (Date, YearMonth, Account, Description, Category, Tags, Amount, reconciled_key, Matched)
    - Data type consistency for critical columns
    - No missing values in critical columns
    """
    
    # Define expected column structures
    DETAIL_COLUMNS = ['Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file']
    AGGREGATOR_COLUMNS = ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account']
    FINAL_COLUMNS = ['Transaction Date', 'Post Date', 'Description', 'Amount', 'Category', 'Tags', 'Account', 'reconciled_key', 'Matched', 'YearMonth']
    
    # Test individual processing functions
    discover_processed = process_discover_format(sample_discover_df)
    amex_processed = process_amex_format(sample_amex_df)
    capital_one_processed = process_capital_one_format(sample_capital_one_df)
    alliant_processed = process_alliant_format(sample_alliant_df)
    
    # Verify all processing functions output the same structure
    assert list(discover_processed.columns) == DETAIL_COLUMNS, "Discover format processing changed column structure"
    assert list(amex_processed.columns) == DETAIL_COLUMNS, "Amex format processing changed column structure"
    assert list(capital_one_processed.columns) == DETAIL_COLUMNS, "Capital One format processing changed column structure"
    assert list(alliant_processed.columns) == DETAIL_COLUMNS, "Alliant format processing changed column structure"
    
    # Test import_csv with different formats
    # Discover format
    discover_df = pd.DataFrame({
        'Trans. Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test'],
        'Amount': ['123.45'],
        'Category': ['']
    })
    discover_path = tmp_path / "discover_card.csv"
    discover_df.to_csv(discover_path, index=False)
    discover_result = import_csv(str(discover_path))
    
    # Amex format
    amex_df = pd.DataFrame({
        'Date': ['2024-03-15'],
        'Description': ['Test'],
        'Card Member': ['JOHN DOE'],
        'Account #': ['1234'],
        'Amount': ['123.45']
    })
    amex_path = tmp_path / "amex_card.csv"
    amex_df.to_csv(amex_path, index=False)
    amex_result = import_csv(str(amex_path))
    
    # Capital One format
    capital_one_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Posted Date': ['2024-03-15'],
        'Card No.': ['1234'],
        'Description': ['Test'],
        'Category': [''],
        'Debit': ['123.45'],
        'Credit': [np.nan]
    })
    capital_one_path = tmp_path / "capital_one_card.csv"
    capital_one_df.to_csv(capital_one_path, index=False)
    capital_one_result = import_csv(str(capital_one_path))
    
    # Alliant format
    alliant_df = pd.DataFrame({
        'Date': ['01/01/2025'],
        'Description': ['Test'],
        'Amount': ['$42.80 '],
        'Balance': ['$0.00 '],
        'Post Date': ['01/02/2025']
    })
    alliant_path = tmp_path / "alliant_card.csv"
    alliant_df.to_csv(alliant_path, index=False)
    alliant_result = import_csv(str(alliant_path))
    
    # Verify import_csv maintains structure
    assert list(discover_result.columns) == DETAIL_COLUMNS, "Discover CSV import changed column structure"
    assert list(amex_result.columns) == DETAIL_COLUMNS, "Amex CSV import changed column structure"
    assert list(capital_one_result.columns) == DETAIL_COLUMNS, "Capital One CSV import changed column structure"
    assert list(alliant_result.columns) == DETAIL_COLUMNS, "Alliant CSV import changed column structure"
    
    # Test import_folder with multiple formats
    result_combined = import_folder(str(tmp_path))
    assert list(result_combined.columns) == DETAIL_COLUMNS, "Folder import changed column structure"
    
    # Verify data types are consistent
    assert result_combined['Transaction Date'].dtype == 'object', "Transaction Date column type changed"
    assert result_combined['Post Date'].dtype == 'object', "Post Date column type changed"
    assert result_combined['Description'].dtype == 'object', "Description column type changed"
    assert result_combined['Amount'].dtype == 'float64', "Amount column type changed"
    
    # Test final reconciliation structure
    reconciled = reconcile_transactions(result_combined, sample_aggregator_df)
    assert list(reconciled.columns) == FINAL_COLUMNS, "Reconciliation changed final column structure"
    
    # Verify critical columns maintain their data types
    assert reconciled['Transaction Date'].dtype == 'object', "Transaction Date column type changed in reconciliation"
    assert reconciled['Amount'].dtype == 'float64', "Amount column type changed in reconciliation"
    assert reconciled['reconciled_key'].dtype == 'object', "Reconciliation key type is incorrect"
    assert reconciled['Matched'].dtype == bool, "Matched column should be boolean"
    
    # Verify no NaN values in critical columns
    assert not reconciled['Transaction Date'].isna().any(), "Found records with missing dates"
    assert not reconciled['Amount'].isna().any(), "Found records with missing amounts"
    assert not reconciled['reconciled_key'].isna().any(), "Found NaN values in reconciliation key"
    assert not reconciled['Matched'].isna().any(), "Found NaN values in Matched column"
    
    # Verify amount values are properly formatted (no currency symbols, commas, etc.)
    assert all(isinstance(x, (int, float)) for x in reconciled['Amount']), "Found non-numeric values in Amount column"

def test_date_handling_edge_cases():
    """Test date handling for various edge cases
    INTERNAL CHECK: Verifies robustness of date handling implementation
    - Various date format inputs
    - Invalid date handling
    - Null/empty date handling
    """
    # Test with various date formats
    test_dates = pd.DataFrame({
        'Transaction Date': [
            '2024-03-15',           # Standard ISO format
            '03/15/2024',           # US format
            '15-03-2024',           # UK format
            '2024-03-15 14:30:00',  # With time
            '20240315',             # Compact format
            '3/15/24',              # Short year
            None,                   # None value
            '',                     # Empty string
            'invalid'               # Invalid date
        ],
        'Post Date': [
            '2024-03-15',           # Standard ISO format
            '03/15/2024',           # US format
            '15-03-2024',           # UK format
            '2024-03-15 14:30:00',  # With time
            '20240315',             # Compact format
            '3/15/24',              # Short year
            None,                   # None value
            '',                     # Empty string
            'invalid'               # Invalid date
        ],
        'Description': ['Test'] * 9,
        'Amount': ['100'] * 9,
        'Category': [''] * 9
    })
    
    # Process through Discover format
    result = process_discover_format(test_dates.copy())  # Use copy to avoid SettingWithCopyWarning
    
    # Check that the DataFrame has the expected columns
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file', 'Category'}
    
    # Check date standardization
    assert result['Transaction Date'].iloc[0] == '2024-03-15'  # Standard ISO format
    assert result['Transaction Date'].iloc[1] == '2024-03-15'  # US format
    assert result['Transaction Date'].iloc[2] == '2024-03-15'  # UK format
    assert result['Transaction Date'].iloc[3] == '2024-03-15'  # With time
    assert result['Transaction Date'].iloc[4] == '2024-03-15'  # Compact format
    assert result['Transaction Date'].iloc[5] == '2024-03-15'  # Short year
    assert pd.isna(result['Transaction Date'].iloc[6])  # None value
    assert pd.isna(result['Transaction Date'].iloc[7])  # Empty string
    assert pd.isna(result['Transaction Date'].iloc[8])  # Invalid date
    
    # Verify date handling for valid dates
    valid_trans_dates = result['Transaction Date'].iloc[0:6]  # First 6 dates should be valid
    valid_post_dates = result['Post Date'].iloc[0:6]  # First 6 dates should be valid
    for date in valid_trans_dates:
        assert pd.notna(date), "Valid Transaction Date was converted to NaN"
        assert isinstance(date, str), "Transaction Date should be string"
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', date), f"Invalid Transaction Date format: {date}"
    for date in valid_post_dates:
        assert pd.notna(date), "Valid Post Date was converted to NaN"
        assert isinstance(date, str), "Post Date should be string"
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', date), f"Invalid Post Date format: {date}"
    
    # Verify handling of invalid dates
    invalid_trans_dates = result['Transaction Date'].iloc[6:]  # Last 3 dates should be invalid
    invalid_post_dates = result['Post Date'].iloc[6:]  # Last 3 dates should be invalid
    for date in invalid_trans_dates:
        assert date is None, f"Invalid Transaction Date should be None, got {date}"
    for date in invalid_post_dates:
        assert date is None, f"Invalid Post Date should be None, got {date}"
    
    # Test YearMonth column in reconciliation
    result['source_file'] = 'test.csv'
    reconciled = reconcile_transactions(result, pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Description': ['Test'],
        'Amount': ['-100'],
        'Category': ['Test'],
        'Tags': [''],
        'Account': ['Test']
    }))
    
    # Verify YearMonth column
    assert 'YearMonth' in reconciled.columns, "YearMonth column missing from reconciled output"
    assert reconciled['YearMonth'].iloc[0] == '2024-03', "YearMonth format incorrect"
    assert reconciled['YearMonth'].dtype == 'object', "YearMonth should be string type"

def test_source_file_tracking(tmp_path):
    """Test that source files are properly tracked through the reconciliation process
    BUSINESS REQUIREMENT: Verifies the spec's requirements for source file tracking
    - Source file identification in imported data
    - Source tracking in unmatched records (Unreconciled - [source_file] format)
    - Source preservation in matched records
    - Proper handling of source files across the entire reconciliation process
    """
    # Create test files with different institution formats
    discover_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test1'],
        'Amount': ['123.45'],  # Will become -123.45 after processing
        'Category': ['']
    })
    amex_df = pd.DataFrame({
        'Date': ['2024-03-16'],
        'Description': ['Test2'],
        'Card Member': ['JOHN DOE'],
        'Account #': ['1234'],
        'Amount': ['456.78']  # Will become -456.78 after processing
    })
    capital_one_df = pd.DataFrame({
        'Transaction Date': ['2024-03-17'],
        'Posted Date': ['2024-03-17'],
        'Card No.': ['1234'],
        'Description': ['Test3'],
        'Category': [''],
        'Debit': ['789.01'],  # Will become -789.01 after processing
        'Credit': [np.nan]
    })
    
    # Save files with institution names
    discover_path = tmp_path / "discover_card.csv"
    amex_path = tmp_path / "amex_card.csv"
    capital_one_path = tmp_path / "capital_one_card.csv"
    
    discover_df.to_csv(discover_path, index=False)
    amex_df.to_csv(amex_path, index=False)
    capital_one_df.to_csv(capital_one_path, index=False)
    
    # Create aggregator file with some matching and some unmatched transactions
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-18'],  # One match, one unmatched
        'Post Date': ['2024-03-15', '2024-03-18'],
        'Description': ['Agg1', 'Agg2'],
        'Amount': ['-123.45', '-999.99'],
        'Category': ['Cat1', 'Cat2'],
        'Tags': ['Tag1', 'Tag2'],
        'Account': ['Account1', 'Account2']
    })
    
    # Import and reconcile
    details_df = import_folder(str(tmp_path))
    assert details_df is not None, "Failed to import detail files"
    
    # Verify source files are tracked in imported data
    assert 'source_file' in details_df.columns, "Source file tracking column missing"
    
    reconciled = reconcile_transactions(details_df, aggregator_df)
    
    # Verify source file tracking for unreconciled entries
    unreconciled_entries = reconciled[reconciled['Account'].str.startswith('Unreconciled -')]
    assert not unreconciled_entries.empty, "No unreconciled entries found"
    
    # Test that unmatched detail records show their source file
    unreconciled_accounts = unreconciled_entries['Account'].tolist()
    # The discover transaction should be matched, so we don't expect it in unreconciled
    assert any('amex_card.csv' in acc for acc in unreconciled_accounts), "Missing source file for amex transactions"
    assert any('capital_one_card.csv' in acc for acc in unreconciled_accounts), "Missing source file for capital one transactions"
    
    # Test that matched records keep their original account
    matched_entries = reconciled[reconciled['Matched']]
    assert not matched_entries.empty, "No matched entries found"
    assert all(acc in ['Account1', 'Account2'] for acc in matched_entries['Account']), "Matched entries should keep original account"
    
    # Verify the discover transaction is matched
    discover_transaction = details_df[details_df['source_file'] == 'discover_card.csv']
    assert not discover_transaction.empty, "Discover transaction not found in detail records"
    discover_amount = discover_transaction['Amount'].iloc[0]  # Should be -123.45 after processing
    discover_date = discover_transaction['Post Date'].iloc[0]  # Use Post Date since it matches
    matched_discover = reconciled[
        (reconciled['Matched']) & 
        (reconciled['Amount'] == discover_amount) & 
        (reconciled['Transaction Date'] == discover_date)
    ]
    assert not matched_discover.empty, "Discover transaction not found in matched records"
    assert matched_discover['Amount'].iloc[0] == -123.45, "Discover transaction amount mismatch"  # Verify exact amount
    assert matched_discover['reconciled_key'].iloc[0] == f"P:{discover_date}|{discover_amount}", "Matched discover transaction should have P: prefix"
    
    # Verify unmatched records have U: prefix in reconciled_key
    for _, row in unreconciled_entries.iterrows():
        assert row['reconciled_key'].startswith('U:'), f"Unmatched record should have U: prefix, got {row['reconciled_key']}"
        key_parts = row['reconciled_key'].split('|')
        assert len(key_parts) == 2, "Reconciled key should have date and amount parts"
        date_part = key_parts[0].replace('U:', '')
        amount_part = float(key_parts[1])
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', date_part), f"Invalid date format in reconciled_key: {date_part}"
        assert isinstance(amount_part, float), f"Invalid amount in reconciled_key: {amount_part}"
    
    # Verify no records have empty dates or amounts
    assert not reconciled['Transaction Date'].isna().any(), "Found records with missing dates"
    assert not reconciled['Amount'].isna().any(), "Found records with missing amounts"
    assert all(isinstance(date, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', date) for date in reconciled['Transaction Date'] if pd.notna(date)), "Found dates in incorrect format"

def test_reconcile_single_discover_format():
    """Test reconciliation with only Discover format records
    BUSINESS REQUIREMENT: Verifies handling of Discover format in isolation
    - Proper processing of Discover records
    - Correct matching with aggregator records
    - Proper handling of unmatched records
    """
    # Create Discover record
    discover_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test Transaction'],
        'Amount': ['123.45'],
        'Category': ['']
    })
    
    # Create matching aggregator record
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test Transaction'],
        'Amount': ['-123.45'],
        'Category': ['Test'],
        'Tags': [''],
        'Account': ['Test Account']
    })
    
    # Process Discover record
    processed_discover = process_discover_format(discover_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_discover])
    
    # Verify the transaction was matched
    assert len(result) == 1, "Should have exactly one record"
    assert result['Matched'].iloc[0], "Transaction should be marked as matched"
    assert result['reconciled_key'].iloc[0].startswith(('P:', 'T:')), "Should have P: or T: prefix"
    assert result['Amount'].iloc[0] == -123.45, "Amount should match"
    assert result['Transaction Date'].iloc[0] == '2024-03-15', "Date should match"

def test_reconcile_single_amex_format():
    """Test reconciliation with only Amex format records
    BUSINESS REQUIREMENT: Verifies handling of Amex format in isolation
    - Proper processing of Amex records
    - Correct matching with aggregator records
    - Proper handling of unmatched records
    """
    # Create Amex record
    amex_df = pd.DataFrame({
        'Date': ['2024-03-15'],
        'Description': ['Test Transaction'],
        'Card Member': ['JOHN DOE'],
        'Account #': ['1234'],
        'Amount': ['123.45']
    })
    
    # Create matching aggregator record
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test Transaction'],
        'Amount': ['-123.45'],
        'Category': ['Test'],
        'Tags': [''],
        'Account': ['Test Account']
    })
    
    # Process Amex record
    processed_amex = process_amex_format(amex_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_amex])
    
    # Verify the transaction was matched
    assert len(result) == 1, "Should have exactly one record"
    assert result['Matched'].iloc[0], "Transaction should be marked as matched"
    assert result['reconciled_key'].iloc[0].startswith(('P:', 'T:')), "Should have P: or T: prefix"
    assert result['Amount'].iloc[0] == -123.45, "Amount should match"
    assert result['Transaction Date'].iloc[0] == '2024-03-15', "Date should match"

def test_reconcile_single_capital_one_format():
    """Test reconciliation with only Capital One format records
    BUSINESS REQUIREMENT: Verifies handling of Capital One format in isolation
    - Proper processing of Capital One records
    - Correct matching with aggregator records
    - Proper handling of unmatched records
    """
    # Create Capital One record
    capital_one_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Posted Date': ['2024-03-15'],
        'Card No.': ['1234'],
        'Description': ['Test Transaction'],
        'Category': [''],
        'Debit': ['123.45'],
        'Credit': [np.nan]
    })
    
    # Create matching aggregator record
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test Transaction'],
        'Amount': ['-123.45'],
        'Category': ['Test'],
        'Tags': [''],
        'Account': ['Test Account']
    })
    
    # Process Capital One record
    processed_capital_one = process_capital_one_format(capital_one_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_capital_one])
    
    # Verify the transaction was matched
    assert len(result) == 1, "Should have exactly one record"
    assert result['Matched'].iloc[0], "Transaction should be marked as matched"
    assert result['reconciled_key'].iloc[0].startswith(('P:', 'T:')), "Should have P: or T: prefix"
    assert result['Amount'].iloc[0] == -123.45, "Amount should match"
    assert result['Transaction Date'].iloc[0] == '2024-03-15', "Date should match"

def test_reconcile_discover_with_aggregator():
    """Test reconciliation between Discover and Aggregator formats
    BUSINESS REQUIREMENT: Verifies handling of Discover-Aggregator reconciliation
    - Proper matching of transactions
    - Correct handling of unmatched records
    - Proper source file tracking
    """
    # Create Discover records
    discover_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['123.45', '456.78'],
        'Category': ['', '']
    })
    
    # Create matching aggregator records
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-17'],  # One match, one unmatched
        'Post Date': ['2024-03-15', '2024-03-17'],
        'Description': ['Test Transaction 1', 'Test Transaction 3'],
        'Amount': ['-123.45', '-789.01'],
        'Category': ['Test1', 'Test3'],
        'Tags': ['', ''],
        'Account': ['Test Account', 'Test Account']
    })
    
    # Process Discover records
    processed_discover = process_discover_format(discover_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_discover])
    
    # Verify matches and unmatched records
    assert len(result) == 3, "Should have three records (one match, two unmatched)"
    assert result['Matched'].sum() == 1, "Should have one matched record"
    assert len(result[result['reconciled_key'].str.startswith('U:')]) == 2, "Should have two unmatched records"
    assert result[result['Matched']]['Amount'].iloc[0] == -123.45, "Matched amount should be correct"

def test_reconcile_amex_with_aggregator():
    """Test reconciliation between Amex and Aggregator formats
    BUSINESS REQUIREMENT: Verifies handling of Amex-Aggregator reconciliation
    - Proper matching of transactions
    - Correct handling of unmatched records
    - Proper source file tracking
    """
    # Create Amex records
    amex_df = pd.DataFrame({
        'Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Card Member': ['JOHN DOE', 'JOHN DOE'],
        'Account #': ['1234', '1234'],
        'Amount': ['123.45', '456.78']
    })
    
    # Create matching aggregator records
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-17'],  # One match, one unmatched
        'Post Date': ['2024-03-15', '2024-03-17'],
        'Description': ['Test Transaction 1', 'Test Transaction 3'],
        'Amount': ['-123.45', '-789.01'],
        'Category': ['Test1', 'Test3'],
        'Tags': ['', ''],
        'Account': ['Test Account', 'Test Account']
    })
    
    # Process Amex records
    processed_amex = process_amex_format(amex_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_amex])
    
    # Verify matches and unmatched records
    assert len(result) == 3, "Should have three records (one match, two unmatched)"
    assert result['Matched'].sum() == 1, "Should have one matched record"
    assert len(result[result['reconciled_key'].str.startswith('U:')]) == 2, "Should have two unmatched records"
    assert result[result['Matched']]['Amount'].iloc[0] == -123.45, "Matched amount should be correct"

def test_reconcile_capital_one_with_aggregator():
    """Test reconciliation between Capital One and Aggregator formats
    BUSINESS REQUIREMENT: Verifies handling of Capital One-Aggregator reconciliation
    - Proper matching of transactions
    - Correct handling of unmatched records
    - Proper source file tracking
    """
    # Create Capital One records
    capital_one_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Posted Date': ['2024-03-15', '2024-03-16'],
        'Card No.': ['1234', '1234'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Category': ['', ''],
        'Debit': ['123.45', '456.78'],
        'Credit': [np.nan, np.nan]
    })
    
    # Create matching aggregator records
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-17'],  # One match, one unmatched
        'Post Date': ['2024-03-15', '2024-03-17'],
        'Description': ['Test Transaction 1', 'Test Transaction 3'],
        'Amount': ['-123.45', '-789.01'],
        'Category': ['Test1', 'Test3'],
        'Tags': ['', ''],
        'Account': ['Test Account', 'Test Account']
    })
    
    # Process Capital One records
    processed_capital_one = process_capital_one_format(capital_one_df)
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, [processed_capital_one])
    
    # Verify matches and unmatched records
    assert len(result) == 3, "Should have three records (one match, two unmatched)"
    assert result['Matched'].sum() == 1, "Should have one matched record"
    assert len(result[result['reconciled_key'].str.startswith('U:')]) == 2, "Should have two unmatched records"
    assert result[result['Matched']]['Amount'].iloc[0] == -123.45, "Matched amount should be correct"

def test_reconcile_all_formats_with_aggregator():
    """Test reconciliation with all detail formats and aggregator
    BUSINESS REQUIREMENT: Verifies handling of complete reconciliation process
    - Proper processing of all format types
    - Correct matching across all formats
    - Proper handling of unmatched records
    - Source file tracking for all formats
    """
    # Create records for each format
    discover_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15'],
        'Post Date': ['2024-03-15'],
        'Description': ['Test Transaction 1'],
        'Amount': ['123.45'],
        'Category': ['']
    })
    
    amex_df = pd.DataFrame({
        'Date': ['2024-03-16'],
        'Description': ['Test Transaction 2'],
        'Card Member': ['JOHN DOE'],
        'Account #': ['1234'],
        'Amount': ['456.78']
    })
    
    capital_one_df = pd.DataFrame({
        'Transaction Date': ['2024-03-17'],
        'Posted Date': ['2024-03-17'],
        'Card No.': ['1234'],
        'Description': ['Test Transaction 3'],
        'Category': [''],
        'Debit': ['789.01'],
        'Credit': [np.nan]
    })
    
    alliant_df = pd.DataFrame({
        'Date': ['2024-03-18'],
        'Description': ['Test Transaction 4'],
        'Amount': ['$42.80'],
        'Balance': ['$0.00'],
        'Post Date': ['2024-03-18']
    })
    
    chase_df = pd.DataFrame({
        'Transaction Date': ['2024-03-19'],
        'Post Date': ['03/19/2024'],
        'Description': ['Test Transaction 5'],
        'Amount': [-95.89],
        'Type': ['ACH_DEBIT'],
        'Balance': [3990.63],
        'Check or Slip #': ['']
    })
    
    # Create matching aggregator records
    aggregator_df = pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16', '2024-03-17', '2024-03-18'],
        'Post Date': ['2024-03-15', '2024-03-16', '2024-03-17', '2024-03-18'],
        'Description': ['Test Transaction 1', 'Test Transaction 2', 'Test Transaction 3', 'Test Transaction 5'],
        'Amount': ['-123.45', '-456.78', '-789.01', '-95.89'],
        'Category': ['Test1', 'Test2', 'Test3', 'Test5'],
        'Tags': [''] * 4,
        'Account': ['Test Account'] * 4
    })
    
    # Process all detail records
    processed_discover = process_discover_format(discover_df)
    processed_amex = process_amex_format(amex_df)
    processed_capital_one = process_capital_one_format(capital_one_df)
    processed_alliant = process_alliant_format(alliant_df)
    processed_chase = process_chase_format(chase_df)
    
    # Combine all detail records
    detail_records = [processed_discover, processed_amex, processed_capital_one, 
                     processed_alliant, processed_chase]
    
    # Perform reconciliation
    result = reconcile_transactions(aggregator_df, detail_records)
    
    # Verify matches and unmatched records
    assert len(result) == 5, "Should have five records (four matches, one unmatched)"
    assert result['Matched'].sum() == 4, "Should have four matched records"
    assert len(result[result['reconciled_key'].str.startswith('U:')]) == 1, "Should have one unmatched record"
    
    # Verify each format was matched correctly
    matched_amounts = result[result['Matched']]['Amount'].tolist()
    assert -123.45 in matched_amounts, "Discover transaction not matched"
    assert -456.78 in matched_amounts, "Amex transaction not matched"
    assert -789.01 in matched_amounts, "Capital One transaction not matched"
    assert -95.89 in matched_amounts, "Alliant transaction not matched"
    
    # Verify source file tracking
    assert all(result[result['Matched']]['Account'] == 'Test Account'), "Matched records should keep original account"
    assert result[~result['Matched']]['Account'].iloc[0].startswith('Unreconciled -'), "Unmatched record should show source"

def test_process_different_transaction_types(sample_transaction_types_df):
    """Test processing of different transaction types
    
    Requirements:
    - Must handle various transaction types correctly
    - Must maintain original transaction information
    - Must standardize dates and amounts
    """
    result = process_discover_format(sample_transaction_types_df)
    
    # Check basic structure
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == len(sample_transaction_types_df)
    
    # Check column presence
    required_columns = {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file', 'Category'}
    assert set(result.columns) >= required_columns
    
    # Check date standardization
    assert all(pd.to_datetime(result['Transaction Date']).dt.strftime('%Y-%m-%d') == result['Transaction Date'])
    assert all(pd.to_datetime(result['Post Date']).dt.strftime('%Y-%m-%d') == result['Post Date'])
    
    # Check amount standardization
    assert all(isinstance(amt, float) for amt in result['Amount'])
    
    # Check description preservation
    original_descriptions = set(sample_transaction_types_df['Description'])
    result_descriptions = set(result['Description'])
    assert original_descriptions == result_descriptions
    
    # Check category preservation
    original_categories = set(sample_transaction_types_df['Category'])
    result_categories = set(result['Category'])
    assert original_categories == result_categories

def test_duplicate_handling(sample_duplicate_transactions_df, sample_duplicate_aggregator_df):
    """Test handling of duplicate transactions
    
    Requirements:
    - Must match each transaction only once
    - Must handle similar but not identical transactions correctly
    - Must maintain correct matching counts
    """
    # Process detail records
    detail_df = process_discover_format(sample_duplicate_transactions_df)
    
    # Process aggregator records
    agg_df = process_aggregator_format(sample_duplicate_aggregator_df)
    
    # Reconcile transactions
    result = reconcile_transactions(detail_df, agg_df)
    
    # Check matching results
    assert len(result['matched']) == 1  # Should match only once
    assert len(result['unmatched_detail']) == 3  # Should have 3 unmatched (2 duplicates + 1 different amount)
    assert len(result['unmatched_agg']) == 1  # Should have 1 unmatched aggregator record
    
    # Verify the matched transaction
    matched = result['matched']
    assert len(matched) == 1
    assert matched.iloc[0]['Amount_detail'] == 123.45
    assert matched.iloc[0]['Amount_agg'] == -123.45
    
    # Verify unmatched transactions
    unmatched = result['unmatched_detail']
    assert len(unmatched[unmatched['Amount'] == 123.45]) == 2  # Two duplicates
    assert len(unmatched[unmatched['Amount'] == 124.45]) == 1  # One different amount

# ... rest of the existing tests ... 