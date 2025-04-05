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
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['$123.45', '-$456.78']
    })

@pytest.fixture
def sample_df2():
    return pd.DataFrame({
        'Date': ['03/15/2024', '03/16/2024'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['123.45', '-456.78']
    })

@pytest.fixture
def sample_df3():
    return pd.DataFrame({
        'Posted Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Debit': ['123.45', np.nan],
        'Credit': [np.nan, '456.78']
    })

@pytest.fixture
def sample_aggregator_df():
    return pd.DataFrame({
        'Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Agg Transaction 1', 'Agg Transaction 2'],
        'Amount': ['-123.45', '456.78'],
        'Category': ['Category1', 'Category2'],
        'Tags': ['Tag1', 'Tag2'],
        'Account': ['Account1', 'Account2']
    })

@pytest.fixture
def sample_discover_df():
    return pd.DataFrame({
        'Trans. Date': ['2024-03-15', '2024-03-16'],
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['$123.45', '-$456.78'],
        'Category': ['', '']
    })

@pytest.fixture
def sample_amex_df():
    return pd.DataFrame({
        'Date': ['03/15/2024', '03/16/2024'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Card Member': ['JOHN DOE', 'JOHN DOE'],
        'Account #': ['1234', '1234'],
        'Amount': ['123.45', '-456.78']
    })

@pytest.fixture
def sample_capital_one_df():
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Posted Date': ['2024-03-15', '2024-03-16'],
        'Card No.': ['1234', '1234'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Category': ['', ''],
        'Debit': ['123.45', np.nan],
        'Credit': [np.nan, '456.78']
    })

@pytest.fixture
def sample_chase_df():
    return pd.DataFrame({
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['-123.45', '456.78'],
        'Type': ['Sale', 'Payment'],
        'Balance': ['1000.00', '876.55']
    })

@pytest.fixture
def sample_alliant_df():
    return pd.DataFrame({
        'Date': ['01/01/2025', '01/01/2025'],
        'Description': ['TEST_MERCHANT_1 123-456-7890 ST', 'TEST_MERCHANT_2 987-654-3210 ST'],
        'Amount': ['$42.80 ', '$7.57 '],
        'Balance': ['$0.00 ', '$0.00 '],
        'Post Date': ['01/02/2025', '01/02/2025']
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
    """Test processing Discover format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for Discover format processing
    - Correct column mapping
    - Proper date standardization
    - Amount sign handling
    - Source file tracking
    """
    result = process_discover_format(sample_discover_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file', 'Category'}
    assert result['Post Date'].iloc[0] == '2024-03-15'
    assert result['Transaction Date'].iloc[0] == '2024-03-15'
    assert result['Amount'].iloc[0] == -123.45
    assert result['source_file'].iloc[0] == 'discover'

def test_process_amex_format(sample_amex_df):
    """Test processing Amex format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for Amex format processing
    - Correct column mapping
    - Proper date standardization
    - Amount sign handling
    - Source file tracking
    """
    result = process_amex_format(sample_amex_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    assert result['Transaction Date'].iloc[0] == '2024-03-15'
    assert result['Post Date'].iloc[0] == '2024-03-15'
    assert result['Amount'].iloc[0] == -123.45
    assert result['source_file'].iloc[0] == 'amex'

def test_process_capital_one_format(sample_capital_one_df):
    """Test processing Capital One format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for Capital One format processing
    - Correct column mapping
    - Proper date standardization
    - Amount sign handling
    - Source file tracking
    """
    result = process_capital_one_format(sample_capital_one_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file', 'Category'}
    assert result['Post Date'].iloc[0] == '2024-03-15'
    assert result['Transaction Date'].iloc[0] == '2024-03-15'
    assert result['Amount'].iloc[0] == -123.45
    assert result['source_file'].iloc[0] == 'capital_one'

def test_process_alliant_format(sample_alliant_df):
    """Test processing Alliant format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for Alliant format processing
    - Correct column mapping
    - Proper date standardization
    - Amount sign handling
    - Source file tracking
    """
    result = process_alliant_format(sample_alliant_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    assert result['Post Date'].iloc[0] == '2025-01-02'  # Uses Post Date
    assert result['Transaction Date'].iloc[0] == '2025-01-01'  # Uses Date as Transaction Date
    assert result['Amount'].iloc[0] == -42.80  # Amount is inverted and cleaned
    assert result['Description'].iloc[0] == 'TEST_MERCHANT_1 123-456-7890 ST'
    assert result['source_file'].iloc[0] == 'alliant'

def test_process_chase_format(sample_chase_df):
    """Test processing Chase format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for Chase format processing
    - Correct column mapping
    - Proper date standardization
    - Amount sign handling
    - Source file tracking
    """
    # Create test data matching actual Chase format
    test_df = pd.DataFrame({
        'Details': ['DEBIT', 'CREDIT'],
        'Posting Date': ['03/15/2024', '03/16/2024'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [-123.45, 456.78],
        'Type': ['ACH_DEBIT', 'ACH_CREDIT'],
        'Balance': [1000.00, 876.55],
        'Check or Slip #': ['', '']
    })
    
    result = process_chase_format(test_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    assert result['Post Date'].iloc[0] == '2024-03-15'
    assert result['Transaction Date'].iloc[0] == '2024-03-15'
    assert result['Amount'].iloc[0] == -123.45  # Keep original sign from Chase
    assert result['Amount'].iloc[1] == 456.78  # Keep original sign from Chase
    assert result['source_file'].iloc[0] == 'chase'

def test_process_aggregator_format(sample_aggregator_df):
    """Test processing aggregator format
    BUSINESS REQUIREMENT: Verifies the spec's requirement for aggregator format processing
    - Correct column mapping
    - Proper date standardization
    - Amount sign handling
    - Source file tracking
    """
    result = process_aggregator_format(sample_aggregator_df)
    assert set(result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file', 'Category', 'Tags'}
    assert result['Post Date'].iloc[0] == '2024-03-15'
    assert result['Transaction Date'].iloc[0] == '2024-03-15'
    assert result['Amount'].iloc[0] == -123.45
    assert result['Category'].iloc[0] == 'Category1'
    assert result['Tags'].iloc[0] == 'Tag1'
    assert result['source_file'].iloc[0] == 'aggregator'

# Basic reconciliation tests
def test_reconcile_transactions_basic_matching():
    """Test basic reconciliation between detail and aggregator records
    BUSINESS REQUIREMENT: Verifies the fundamental matching logic specified in the spec
    - Correct handling of date and amount matches
    - Proper reconciled_key format with P: or T: prefix
    - Underscore separator in reconciled_key
    """
    # Create test data with clear matches
    detail_df = pd.DataFrame({
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Transaction Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Test 1', 'Test 2'],
        'Amount': [-100.00, -200.00]
    })
    
    aggregator_df = pd.DataFrame({
        'Date': ['2024-03-15', '2024-03-16'],
        'Description': ['Agg 1', 'Agg 2'],
        'Amount': [-100.00, -200.00],
        'Category': ['Cat1', 'Cat2'],
        'Tags': ['', ''],
        'Account': ['Acc1', 'Acc1']
    })
    
    result = reconcile_transactions(aggregator_df, [detail_df])
    
    # Verify basic structure
    assert set(result.columns) == {'Date', 'YearMonth', 'Account', 'Description', 'Category', 'Tags', 'Amount', 'reconciled_key', 'Matched'}
    
    # Verify matches
    matches = result[result['reconciled_key'].str.startswith(('P:', 'T:'))]
    assert len(matches) == 2, "Should have two matches"
    assert all(matches['Matched']), "All matches should be marked as matched"
    
    # Verify reconciled_key format
    for _, row in matches.iterrows():
        assert re.match(r'[PT]:\d{4}-\d{2}-\d{2}_-?\d+\.\d{2}', row['reconciled_key']), "Key should match format [PT]:YYYY-MM-DD_AMOUNT"

def test_reconcile_transactions_date_priority():
    """Test that reconciliation follows date matching priority (Post Date first, then Transaction Date)
    BUSINESS REQUIREMENT: Verifies the spec's requirement for Post Date priority over Transaction Date
    - Post Date matches take precedence
    - Transaction Date matches are used as fallback
    """
    # Create test data where Post Date and Transaction Date differ
    detail_df = pd.DataFrame({
        'Post Date': ['2024-03-15', '2024-03-16'],
        'Transaction Date': ['2024-03-14', '2024-03-15'],
        'Description': ['Test 1', 'Test 2'],
        'Amount': [-100.00, -200.00]
    })
    
    aggregator_df = pd.DataFrame({
        'Date': ['2024-03-15', '2024-03-15'],  # Both match Post Date of first record
        'Description': ['Agg 1', 'Agg 2'],
        'Amount': [-100.00, -200.00],
        'Category': ['Cat1', 'Cat2'],
        'Tags': ['', ''],
        'Account': ['Acc1', 'Acc1']
    })
    
    result = reconcile_transactions(aggregator_df, [detail_df])
    
    # Verify Post Date match takes priority
    post_date_match = result[result['Date'] == '2024-03-15']
    assert post_date_match['reconciled_key'].iloc[0].startswith('P:'), "Post Date match should have P: prefix"
    
    # Verify Transaction Date match is used as fallback
    trans_date_match = result[result['Date'] == '2024-03-15']
    assert trans_date_match['reconciled_key'].iloc[1].startswith('T:'), "Transaction Date match should have T: prefix"

def test_reconcile_transactions_duplicates():
    """Test handling of duplicate transactions
    BUSINESS REQUIREMENT: Verifies the spec's requirements for handling duplicate records
    - Only one match should be made when multiple detail records match the same aggregator record
    - Unmatched duplicates should be marked with D: prefix
    - Proper handling of duplicate records in reconciliation process
    """
    # Create test data with duplicates
    detail_df = pd.DataFrame({
        'Post Date': ['2024-03-15', '2024-03-15'],
        'Transaction Date': ['2024-03-15', '2024-03-15'],
        'Description': ['Test 1', 'Test 1'],
        'Amount': [-100.00, -100.00]
    })
    
    aggregator_df = pd.DataFrame({
        'Date': ['2024-03-15'],
        'Description': ['Agg 1'],
        'Amount': [-100.00],
        'Category': ['Cat1'],
        'Tags': [''],
        'Account': ['Acc1']
    })
    
    result = reconcile_transactions(aggregator_df, [detail_df])
    
    # Verify only one match
    matches = result[result['reconciled_key'].str.startswith(('P:', 'T:'))]
    assert len(matches) == 1, "Should only match one duplicate"
    
    # Verify other duplicate is unmatched
    unmatched = result[result['reconciled_key'].str.startswith('D:')]
    assert len(unmatched) == 1, "Should have one unmatched duplicate"

# File import tests
def test_import_csv(tmp_path):
    """Test importing CSV files with different formats
    BUSINESS REQUIREMENT: Verifies the spec's requirement for CSV import functionality
    - Support for all required formats (Discover, Chase, Amex, Capital One, Alliant)
    - Proper format detection
    - Correct processing of each format
    """
    # Test Discover format
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
    assert discover_result is not None
    assert set(discover_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    
    # Test Chase format
    chase_df = pd.DataFrame({
        'Details': ['DEBIT'],
        'Posting Date': ['03/15/2024'],
        'Description': ['Test Transaction'],
        'Amount': [-123.45],
        'Type': ['ACH_DEBIT'],
        'Balance': [1000.00],
        'Check or Slip #': ['']
    })
    chase_path = tmp_path / "chase_card.csv"
    chase_df.to_csv(chase_path, index=False)
    chase_result = import_csv(str(chase_path))
    assert chase_result is not None
    assert set(chase_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    assert chase_result['Amount'].iloc[0] == -123.45  # Keep original sign from Chase
    
    # Test Amex format
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
    assert amex_result is not None
    assert set(amex_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    
    # Test Capital One format
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
    assert capital_one_result is not None
    assert set(capital_one_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}
    
    # Test Alliant format
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
    assert alliant_result is not None
    assert set(alliant_result.columns) == {'Transaction Date', 'Post Date', 'Description', 'Amount', 'source_file'}

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
    agg_df.loc[0, 'Date'] = '2025-01-02'  # Should match Post Date
    agg_df.loc[0, 'Amount'] = -42.80  # Match first detail record amount
    agg_df.loc[1, 'Date'] = '2025-01-01'  # Should match Transaction Date
    agg_df.loc[1, 'Amount'] = -7.57  # Match second detail record amount

    result = reconcile_transactions(detail_records, agg_df)

    # Verify matches
    assert result['Matched'].sum() == 2, "Should have two matches"
    assert 'reconciled_key' in result.columns
    assert 'Category' in result.columns
    assert result['Matched'].dtype == bool
    
    # Verify the matches were made correctly and have the right reconciled_key format
    post_date_match = result[result['Date'] == '2025-01-02']
    trans_date_match = result[result['Date'] == '2025-01-01']
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
    post_date_match_result = result[result['Date'] == '2025-01-02']
    trans_date_match_result = result[result['Date'] == '2025-01-01']
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
        'Details': ['DEBIT'],
        'Posting Date': ['03/12/2025'],
        'Description': ['TEST_MERCHANT_3           ACH_DEBIT  TEST_REFERENCE     WEB ID: TEST123'],
        'Amount': [-95.89],
        'Type': ['ACH_DEBIT'],
        'Balance': [3990.63],
        'Check or Slip #': ['']
    })
    
    # Create matching aggregator record
    aggregator_df = pd.DataFrame({
        'Date': ['2025-03-12'],
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
    assert result['Date'].iloc[0] == '2025-03-12', "Date should match"

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
    AGGREGATOR_COLUMNS = ['Date', 'Description', 'Amount', 'Category', 'Tags', 'Account']
    FINAL_COLUMNS = ['Date', 'YearMonth', 'Account', 'Description', 'Category', 'Tags', 'Amount', 'reconciled_key', 'Matched']
    
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
    assert reconciled['Date'].dtype == 'object', "Date column type changed in reconciliation"
    assert reconciled['Amount'].dtype == 'float64', "Amount column type changed in reconciliation"
    assert reconciled['reconciled_key'].dtype == 'object', "Reconciliation key type is incorrect"
    assert reconciled['Matched'].dtype == bool, "Matched column should be boolean"
    
    # Verify no NaN values in critical columns
    assert not reconciled['Date'].isna().any(), "Found records with missing dates"
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
        'Trans. Date': [
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
        'Date': ['2024-03-15'],
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
        'Trans. Date': ['2024-03-15'],
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
        'Date': ['2024-03-15', '2024-03-18'],  # One matching, one unmatched
        'Description': ['Agg1', 'Agg2'],
        'Amount': ['-123.45', '-999.99'],  # Negative for debits (money out)
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
        (reconciled['Date'] == discover_date)
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
    assert not reconciled['Date'].isna().any(), "Found records with missing dates"
    assert not reconciled['Amount'].isna().any(), "Found records with missing amounts"
    assert all(isinstance(date, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', date) for date in reconciled['Date'] if pd.notna(date)), "Found dates in incorrect format" 