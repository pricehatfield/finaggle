import pytest
import pandas as pd

@pytest.fixture
def sample_capital_one_df():
    """Sample Capital One transaction data matching the documented format."""
    return pd.DataFrame({
        'Transaction Date': ['2025-01-01', '2025-01-02'],
        'Posted Date': ['2025-01-02', '2025-01-03'],
        'Card No.': ['1234', '1234'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Category': ['Shopping', 'Income'],
        'Debit': ['$50.00', ''],
        'Credit': ['', '$25.00']
    })

@pytest.fixture
def sample_chase_df():
    """Sample Chase transaction data matching the documented format."""
    return pd.DataFrame({
        'Details': ['DEBIT', 'CREDIT'],
        'Posting Date': ['01/01/2025', '01/02/2025'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['-$95.89', '$50.00'],
        'Type': ['ACH_DEBIT', 'ACH_CREDIT'],
        'Balance': ['$1000.00', '$1050.00'],
        'Check or Slip #': ['', '']
    })

@pytest.fixture
def sample_discover_df():
    """Sample Discover transaction data matching the documented format."""
    return pd.DataFrame({
        'Trans. Date': ['01/01/2025', '01/02/2025'],
        'Post Date': ['01/01/2025', '01/02/2025'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': ['40.33', '42.66'],
        'Category': ['Supermarkets', 'Merchandise']
    })

@pytest.fixture
def sample_standardized_df():
    """Sample standardized transaction data after processing."""
    return pd.DataFrame({
        'Transaction Date': ['2025-01-01', '2025-01-02'],
        'Post Date': ['2025-01-02', '2025-01-03'],
        'Description': ['Test Transaction 1', 'Test Transaction 2'],
        'Amount': [-50.00, 25.00],
        'Category': ['Shopping', 'Income'],
        'source_file': ['capital_one', 'chase']
    }) 