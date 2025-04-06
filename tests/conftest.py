import pytest
import pandas as pd

# Sample data for each format
discover_sample_data = {
    'Trans. Date': ['01/01/2025'],
    'Post Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['-40.33'],  # Negative for debits
    'Category': ['Supermarkets']
}

amex_sample_data = {
    'Date': ['03/13/2025', '02/24/2025'],
    'Description': ['AT&T UVERSE PAYMENT 8002882020 TX', 'MOBILE PAYMENT - THANK YOU'],
    'Card Member': ['PRICE L HATFIELD', 'PRICE L HATFIELD'],
    'Account #': ['-42004', '-42004'],
    'Amount': ['126.12', '-250.91'],  # Positive for debits, negative for credits
    'Category': ['Telephone', 'Payment']  # Added Category column
}

capital_one_sample_data = {
    'Transaction Date': ['2025-01-01'],
    'Posted Date': ['2025-01-02'],
    'Card No.': ['1234'],
    'Description': ['Test Transaction'],
    'Category': ['Shopping'],
    'Debit': ['50.00'],  # Positive for debits
    'Credit': ['']  # Empty for debit transactions
}

chase_sample_data = {
    'Details': ['DEBIT'],
    'Posting Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['-95.89'],  # Negative for debits
    'Type': ['ACH_DEBIT'],
    'Balance': ['1000.00'],
    'Check or Slip #': ['']
}

alliant_checking_sample_data = {
    'Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['-50.00'],  # Negative for debits
    'Balance': ['1000.00'],
    'Category': ['Shopping']
}

alliant_visa_sample_data = {
    'Date': ['01/01/2025'],
    'Description': ['Test Transaction'],
    'Amount': ['50.00'],  # Positive for debits
    'Balance': ['1000.00'],
    'Post Date': ['01/02/2025'],
    'Category': ['Shopping']
}

empower_sample_data = {
    'Date': ['2025-03-17', '2025-03-13'],
    'Account': ['Technology Transfer, Inc 401(k) Profit Sharing Plan - Ending in 1701', 'Hilton Honors Surpass Card - Ending in 2004'],
    'Description': ['Putnam Retirement Advantage Trst 2040 X', 'At&t Uverse Payment Xxxxxx2020 Tx'],
    'Category': ['Retirement Contributions', 'Telephone'],
    'Tags': ['', 'Joint'],
    'Amount': ['0', '-126.12']  # Negative for debits, positive for credits
}

@pytest.fixture
def create_test_df():
    """Helper fixture to create test DataFrames with standardized format"""
    def _create_df(format_name):
        sample_data = {
            'discover': discover_sample_data,
            'capital_one': capital_one_sample_data,
            'chase': chase_sample_data,
            'alliant_checking': alliant_checking_sample_data,
            'alliant_visa': alliant_visa_sample_data,
            'amex': amex_sample_data,
            'empower': empower_sample_data
        }
        if format_name not in sample_data:
            raise ValueError(f"Unknown format: {format_name}")
        return pd.DataFrame(sample_data[format_name])
    return _create_df

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