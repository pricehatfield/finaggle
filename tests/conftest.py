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
    'Description': [
        'Discover Card - Ending in 0877',
        'AMAZON.COM'
    ],
    'Category': ['Shopping', 'Shopping'],
    'Tags': ['Online', 'Online'],
    'Amount': ['-123.45', '-45.67']  # Negative for debits
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

@pytest.fixture
def sample_transactions_df():
    """Sample transactions DataFrame for testing reconciliation scenarios.
    
    Contains a mix of matched and unmatched transactions with various match conditions:
    - Post date and amount matches
    - Transaction date and amount matches
    - Unmatched transactions
    - Different account types
    """
    return pd.DataFrame({
        'Date': [
            '2025-01-01',  # Matched - post date
            '2025-01-03',  # Matched - transaction date
            '2025-01-05',  # Matched - post date
            '2025-01-07',  # Matched - transaction date
            '2025-01-09',  # Matched - post date
            '2025-01-11',  # Unmatched
            '2025-01-13',  # Unmatched
            '2025-01-15'   # Unmatched
        ],
        'YearMonth': ['2025-01'] * 8,
        'Account': [
            'Matched - alliant_checking_2025.csv',
            'Matched - alliant_visa_2025.csv',
            'Matched - amex_2025.csv',
            'Matched - alliant_checking_2025.csv',
            'Matched - alliant_visa_2025.csv',
            'Unreconciled - alliant_checking_2025.csv',
            'Unreconciled - alliant_visa_2025.csv',
            'Unreconciled - amex_2025.csv'
        ],
        'Description': [
            'Grocery Store',
            'Gas Station',
            'Restaurant',
            'Salary',
            'Online Shopping',
            'Unknown Transaction',
            'Pending Charge',
            'Disputed Transaction'
        ],
        'Amount': [-50.00, -30.00, -75.00, 2000.00, -100.00, -25.00, -45.00, -60.00],
        'Category': [
            'Groceries',
            'Transportation',
            'Dining',
            'Income',
            'Shopping',
            'Uncategorized',
            'Uncategorized',
            'Uncategorized'
        ],
        'Tags': [''] * 8,
        'reconciled_key': [
            '2025-01-01',
            '2025-01-03',
            '2025-01-05',
            '2025-01-07',
            '2025-01-09',
            '2025-01-11',
            '2025-01-13',
            '2025-01-15'
        ],
        'Matched': [True] * 5 + [False] * 3
    })

@pytest.fixture
def sample_matched_df():
    """Sample matched transactions DataFrame."""
    return pd.DataFrame({
        'Date': ['2025-01-01', '2025-01-03', '2025-01-05', '2025-01-07', '2025-01-09'],
        'YearMonth': ['2025-01'] * 5,
        'Account': [
            'Matched - alliant_checking_2025.csv',
            'Matched - alliant_visa_2025.csv',
            'Matched - amex_2025.csv',
            'Matched - alliant_checking_2025.csv',
            'Matched - alliant_visa_2025.csv'
        ],
        'Description': [
            'Grocery Store',
            'Gas Station',
            'Restaurant',
            'Salary',
            'Online Shopping'
        ],
        'Amount': [-50.00, -30.00, -75.00, 2000.00, -100.00],
        'Category': [
            'Groceries',
            'Transportation',
            'Dining',
            'Income',
            'Shopping'
        ],
        'Tags': [''] * 5,
        'reconciled_key': [
            '2025-01-01',
            '2025-01-03',
            '2025-01-05',
            '2025-01-07',
            '2025-01-09'
        ],
        'Matched': [True] * 5,
        'Transaction Date': ['2025-01-01', '2025-01-03', '2025-01-05', '2025-01-07', '2025-01-09'],
        'Post Date': ['2025-01-02', '2025-01-04', '2025-01-06', '2025-01-08', '2025-01-10'],
        'source_file': [
            'alliant_checking_2025.csv',
            'alliant_visa_2025.csv',
            'amex_2025.csv',
            'alliant_checking_2025.csv',
            'alliant_visa_2025.csv'
        ]
    })

@pytest.fixture
def sample_unmatched_df():
    """Sample unmatched transactions DataFrame."""
    return pd.DataFrame({
        'Date': ['2025-01-11', '2025-01-13', '2025-01-15'],
        'YearMonth': ['2025-01'] * 3,
        'Account': [
            'Unreconciled - alliant_checking_2025.csv',
            'Unreconciled - alliant_visa_2025.csv',
            'Unreconciled - amex_2025.csv'
        ],
        'Description': [
            'Unknown Transaction',
            'Pending Charge',
            'Disputed Transaction'
        ],
        'Amount': [-25.00, -45.00, -60.00],
        'Category': ['Uncategorized'] * 3,
        'Tags': [''] * 3,
        'reconciled_key': [
            '2025-01-11',
            '2025-01-13',
            '2025-01-15'
        ],
        'Matched': [False] * 3,
        'Transaction Date': ['2025-01-11', '2025-01-13', '2025-01-15'],
        'Post Date': ['2025-01-12', '2025-01-14', '2025-01-16'],
        'source_file': [
            'alliant_checking_2025.csv',
            'alliant_visa_2025.csv',
            'amex_2025.csv'
        ]
    }) 