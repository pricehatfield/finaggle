# Transaction Reconciliation Tool

This tool reconciles transaction records from various financial institutions (detail records) against a consolidated record (aggregator record).

## Project Structure
```
local_reconcile/
├── src/
│   ├── reconcile.py
│   └── logs/
├── tests/
│   ├── test_functions.py
│   ├── test_file_formats.py
│   └── test_reconcile.py
├── data/              # Data files directory
├── setup.py
├── requirements.txt
└── README.md
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd local_reconcile
```

2. Create and activate a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the package in development mode:
```bash
pip install -e .
```

4. Install development dependencies:
```bash
pip install -r requirements.txt
```

## Overview

The reconciliation process follows these steps:
1. Read and validate input files
2. Standardize detail records to a common format
3. Standardize aggregator records to a common format
4. Match transactions between detail and aggregator records
5. Generate a reconciliation report showing matched and unmatched transactions

## Data Flow

### Detail Records Processing
1. Each detail record file is processed based on its institution format
2. Records are standardized to include:
   - `transaction_date`: Standardized transaction date
   - `post_date`: Standardized posting date
   - `description`: Transaction description
   - `amount`: Standardized amount (negative for debits, positive for credits)
   - `category`: Transaction category (if available)
   - `source_file`: Original file name

### Aggregator Record Processing
1. Aggregator records are standardized to include:
   - `transaction_date`: Standardized transaction date
   - `post_date`: Same as transaction_date (since aggregator records don't distinguish)
   - `description`: Transaction description
   - `amount`: Standardized amount (negative for debits, positive for credits)
   - `category`: Transaction category
   - `tags`: Transaction tags
   - `account`: Account name/identifier
   - `source_file`: "aggregator"

### Reconciliation Process
1. Create unique keys for each transaction:
   - Detail records: `D:YYYY-MM-DD_AMOUNT`
   - Aggregator records: `A:YYYY-MM-DD_AMOUNT`
   - Example: `D:2024-03-15_-123.45`

2. Match transactions based on:
   - Primary: Post date matches (marked with `P:` prefix)
   - Secondary: Transaction date matches (marked with `T:` prefix)
   - Amount matches (including sign)

3. Mark transactions as:
   - `P:YYYY-MM-DD_AMOUNT` for Post Date matched transactions
   - `T:YYYY-MM-DD_AMOUNT` for Transaction Date matched transactions
   - `U:YYYY-MM-DD_AMOUNT` for unmatched aggregator records
   - `D:YYYY-MM-DD_AMOUNT` for unmatched detail records

### Reconciled Key Format

The `reconciled_key` is a unique identifier for each transaction with the following format:
```
{PREFIX}:{DATE}_{AMOUNT}
```

Where:
- `PREFIX` is one of:
  - `P`: Post Date matched transaction
  - `T`: Transaction Date matched transaction
  - `U`: Unmatched aggregator record
  - `D`: Unmatched detail record
  - `A`: Aggregator record (before matching)
- `DATE` is the transaction date in YYYY-MM-DD format
- `AMOUNT` is the standardized amount with sign
  - Negative for debits (money out)
  - Positive for credits (money in)
  - No currency symbols or commas
  - Decimal point for cents

Examples:
- `P:2024-03-15_-123.45` - Post Date matched transaction
- `T:2024-03-16_-456.78` - Transaction Date matched transaction
- `D:2024-03-17_-789.01` - Unmatched detail record
- `U:2024-03-18_234.56` - Unmatched aggregator record
- `A:2024-03-19_-345.67` - Aggregator record before matching

## Data Formats and Processing Stages

### Input DataFrames
Each input file is read into a pandas DataFrame with institution-specific columns:

#### Detail Records (Example - Discover)
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'post_date': ['2024-03-16', '2024-03-16', '2024-03-17', '2024-03-18'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': [123.45, 123.45, 67.89, 45.00],  # Note: Discover shows positive amounts for debits
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['discover_card.csv', 'discover_card.csv', 'discover_card.csv', 'discover_card.csv']
}
```

#### Detail Records (Example - Amex)
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': [-123.45, 123.45, -67.89, 45.00],  # Note: Amex uses mixed signs for debits
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['amex_card.csv', 'amex_card.csv', 'amex_card.csv', 'amex_card.csv']
}
```

#### Detail Records (Example - Capital One)
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'post_date': ['2024-03-16', '2024-03-16', '2024-03-17', '2024-03-18'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'debit': [123.45, 123.45, 67.89, 45.00],  # Note: Capital One uses separate Debit/Credit columns
    'credit': [None, None, None, None],
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['capital_one.csv', 'capital_one.csv', 'capital_one.csv', 'capital_one.csv']
}
```

#### Detail Records (Example - Alliant)
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': ['$123.45', '$123.45', '$67.89', '$45.00'],  # Note: Alliant uses $ symbol and positive amounts
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['alliant.csv', 'alliant.csv', 'alliant.csv', 'alliant.csv']
}
```

#### Detail Records (Example - Chase)
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'post_date': ['2024-03-16', '2024-03-16', '2024-03-17', '2024-03-18'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': [-123.45, -123.45, -67.89, -45.00],  # Note: Chase shows negative amounts for debits
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['chase.csv', 'chase.csv', 'chase.csv', 'chase.csv']
}
```

#### Aggregator Record (Example - Empower)
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': [-123.45, -123.45, -67.89, -45.00],  # Note: Aggregator shows negative amounts for debits
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'tags': ['', '', '', ''],
    'account': ['Discover', 'Discover', 'Discover', 'Discover'],
    'source_file': ['empower.csv', 'empower.csv', 'empower.csv', 'empower.csv']
}
```

### Standardized Format
After processing, all records are standardized to:
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'post_date': ['2024-03-16', '2024-03-16', '2024-03-17', '2024-03-18'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': [-123.45, -123.45, -67.89, -45.00],  # All amounts standardized to negative for debits
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'tags': ['', '', '', ''],
    'account': ['Discover', 'Discover', 'Discover', 'Discover'],
    'source_file': ['discover_card.csv', 'discover_card.csv', 'discover_card.csv', 'discover_card.csv']
}
```

### Amount Sign Handling by Format
Each format requires specific processing to standardize amounts:

1. **Discover Format**
   - Raw data: Positive amounts for debits
   - Processing: Invert all amounts (multiply by -1)

2. **Amex Format**
   - Raw data: Mixed signs (some positive, some negative for debits)
   - Processing: Invert all amounts (multiply by -1)

3. **Capital One Format**
   - Raw data: Separate Debit and Credit columns
   - Processing: Combine columns, negate debits, keep credits positive

4. **Alliant Format**
   - Raw data: Positive amounts with $ symbol
   - Processing: Remove $ symbol, invert all amounts (multiply by -1)

5. **Chase Format**
   - Raw data: Negative amounts for debits
   - Processing: Keep original signs

6. **Aggregator Format (Empower)**
   - Raw data: Negative amounts for debits
   - Processing: Keep original signs

### Intermediate DataFrames

#### Standardized Detail Records
After processing each institution's format, all detail records are standardized to:
```python
{
    'Transaction Date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'Post Date': ['2024-03-16', '2024-03-16', '2024-03-17', '2024-03-18'],
    'Description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'Amount': [-123.45, -123.45, -67.89, -45.00],
    'Category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['discover_card.csv', 'discover_card.csv', 'discover_card.csv', 'discover_card.csv']
}
```

#### Standardized Aggregator Records
Aggregator records are standardized to:
```python
{
    'Transaction Date': ['2024-03-15', '2024-03-16', '2024-03-19'],
    'Post Date': ['2024-03-15', '2024-03-16', '2024-03-19'],
    'Description': ['Grocery Store', 'Restaurant', 'Online Purchase'],
    'Amount': [-123.45, -67.89, -99.99],
    'Category': ['Groceries', 'Dining', 'Shopping'],
    'Tags': ['', '', ''],
    'Account': ['Discover', 'Discover', 'Discover'],
    'source_file': ['aggregator', 'aggregator', 'aggregator']
}
```

### Matching Process

1. **Key Generation**
   - Detail records: `D:YYYY-MM-DD_AMOUNT`
   - Aggregator records: `A:YYYY-MM-DD_AMOUNT`
   - Example: `D:2024-03-15_-123.45`

2. **Matching Rules**
   - Primary match: Post date matches
   - Secondary match: Transaction date matches
   - Amount matches (including sign)
   - Position/Order: Transactions are matched in order of appearance within their respective files

3. **Match Types**
   - **Exact Match**: 
     - Post date matches exactly
     - Transaction date matches exactly
     - Amount matches exactly (including sign)
     - Position in file is considered for tie-breaking
   - **Partial Match**: 
     - Post date matches but transaction date or amount differs
     - Transaction date matches but post date or amount differs
     - Position in file is considered for tie-breaking
   - **No Match**: 
     - Post dates don't match
     - And transaction dates don't match
     - Or amounts don't match
     - Or position constraints not met

4. **Match Resolution**
   - If multiple potential matches exist:
     1. First try to match on post date and amount, considering position
     2. If no match, try to match on transaction date and amount, considering position
     3. If still no match, mark as unmatched
   - Position Rules:
     - Matches must maintain relative order within their respective files
     - A transaction can only match with records that appear after its last matched transaction
     - This prevents "cross-matching" of transactions that would violate the natural order
   - Unmatched transactions are preserved in the output
   - Matched transactions are marked with the source of the match

### Example: Full Merge Process with Position

#### Input Data

**Detail Records (Discover)**
```python
{
    'transaction_date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17'],
    'post_date': ['2024-03-16', '2024-03-16', '2024-03-17', '2024-03-18'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station'],
    'amount': [123.45, 123.45, 67.89, 45.00],  # Note: Discover shows positive amounts for debits
    'category': ['Food', 'Food', 'Food', 'Transportation'],
    'source_file': ['discover_card.csv', 'discover_card.csv', 'discover_card.csv', 'discover_card.csv']
}
```

**Aggregator Records**
```python
{
    'transaction_date': ['2024-03-16', '2024-03-16', '2024-03-19'],
    'post_date': ['2024-03-16', '2024-03-16', '2024-03-19'],
    'description': ['Grocery Store', 'Restaurant', 'Online Purchase'],
    'amount': [-123.45, -67.89, -99.99],
    'category': ['Groceries', 'Dining', 'Shopping'],
    'tags': ['', '', ''],
    'account': ['Discover', 'Discover', 'Discover'],
    'source_file': ['aggregator', 'aggregator', 'aggregator']
}
```

#### Matching Process with Position

1. **First Detail Transaction (Grocery Store, -123.45)**:
   ✓ Detail Post Date (2024-03-16) matches Aggregator Transaction Date (2024-03-16)
   ✓ Amount matches (-123.45)
   ✓ Unmatched records available: Yes
   = Result: P:2024-03-15_-123.45 (matched)
   - Category from aggregator ('Groceries') is used
   - Note: Transaction Date not evaluated since Post Date matched

2. **Second Detail Transaction (Grocery Store, -123.45)**:
   ✓ Detail Post Date (2024-03-16) matches Aggregator Transaction Date (2024-03-16)
   ✓ Amount matches (-123.45)
   ✗ Unmatched records available: No (aggregator record already matched)
   = Result: D:2024-03-15_-123.45 (unmatched detail)
   - Category from detail record ('Food') is used
   - Note: Transaction Date not evaluated since Post Date matched

3. **Third Detail Transaction (Restaurant, -67.89)**:
   ✗ Detail Post Date (2024-03-17) doesn't match Aggregator Transaction Date (2024-03-16)
   ✓ Detail Transaction Date (2024-03-16) matches Aggregator Transaction Date (2024-03-16)
   ✓ Amount matches (-67.89)
   ✓ Unmatched records available: Yes
   = Result: P:2024-03-16_-67.89 (matched)
   - Category from aggregator ('Dining') is used

4. **Fourth Detail Transaction (Gas Station, -45.00)**:
   ✗ Detail Post Date (2024-03-18) doesn't match Aggregator Transaction Date (2024-03-16)
   ✗ Detail Transaction Date (2024-03-17) doesn't match Aggregator Transaction Date (2024-03-16)
   ✓ Amount matches (-45.00)
   ✗ Unmatched records available: No (no matching dates)
   = Result: D:2024-03-17_-45.00 (unmatched detail)
   - Category from detail record ('Transportation') is used

5. **Fifth Transaction (Online Purchase, -99.99)**:
   ✗ Detail Post Date (2024-03-19) doesn't match any remaining Detail Post Date
   ✗ Detail Transaction Date (2024-03-19) doesn't match any remaining Detail Transaction Date
   ✓ Amount matches (-99.99)
   ✗ Unmatched records available: No (no matching dates)
   = Result: U:2024-03-19_-99.99 (unmatched aggregator)
   - Category from aggregator ('Shopping') is used
   - Note: This is an unreconciled aggregator record

#### Final Output
```python
{
    'date': ['2024-03-15', '2024-03-15', '2024-03-16', '2024-03-17', '2024-03-19'],
    'year_month': ['2024-03', '2024-03', '2024-03', '2024-03', '2024-03'],
    'account': ['Discover', 'Discover', 'Discover', 'Discover', 'Discover'],
    'description': ['Grocery Store', 'Grocery Store', 'Restaurant', 'Gas Station', 'Online Purchase'],
    'category': ['Groceries', 'Food', 'Dining', 'Transportation', 'Shopping'],
    'tags': ['', '', '', '', ''],
    'amount': [-123.45, -123.45, -67.89, -45.00, -99.99],
    'reconciled_key': ['P:2024-03-15_-123.45', 'D:2024-03-15_-123.45', 'P:2024-03-16_-67.89', 'D:2024-03-17_-45.00', 'U:2024-03-19_-99.99'],
    'matched': [True, False, True, False, False]
}
```

Key Points:
- Categories from aggregator take precedence for matched transactions
- All transactions are preserved in the output, whether matched or not
- Each transaction can only be matched once
- We don't match duplicate detail transactions even if they match an aggregator record
- We don't enforce chronological ordering in the matching process

## Input File Formats

### Aggregator Record (Required)
A CSV file containing the consolidated transaction records with the following columns:
- `Date`: Transaction date (YYYY-MM-DD format)
- `Description`: Transaction description
- `Amount`: Transaction amount (negative for debits, positive for credits)
- `Category`: Transaction category
- `Tags`: Optional transaction tags
- `Account`: Account name/identifier

### Detail Records (Required)
CSV files from various financial institutions. The tool supports the following formats:

#### Discover Card
Required columns:
- `Trans. Date`: Transaction date
- `Post Date`: Posting date
- `Description`: Transaction description
- `Amount`: Transaction amount
- `Category`: Transaction category (optional)

#### American Express
Required columns:
- `Date`: Transaction date
- `Description`: Transaction description
- `Card Member`: Cardholder name
- `Account #`: Account number
- `Amount`: Transaction amount

#### Capital One
Required columns:
- `Transaction Date`: Transaction date
- `Posted Date`: Posting date
- `Card No.`: Card number
- `Description`: Transaction description
- `Category`: Transaction category (optional)
- `Debit`: Debit amount
- `Credit`: Credit amount

#### Alliant Credit Union
Required columns:
- `Date`: Transaction date
- `Description`: Transaction description
- `Amount`: Transaction amount
- `Balance`: Account balance
- `Post Date`: Posting date

#### Chase
Required columns:
- `Details`: Transaction details
- `Posting Date`: Posting date
- `Description`: Transaction description
- `Amount`: Transaction amount
- `Type`: Transaction type
- `Balance`: Account balance

## Data Standardization Rules

### Date Handling
- All dates are standardized to YYYY-MM-DD format
- Supports various input formats:
  - ISO format (YYYY-MM-DD)
  - US format (MM/DD/YYYY)
  - UK format (DD-MM-YYYY)
  - Compact format (YYYYMMDD)
  - Short year format (M/D/YY)
- Invalid dates are converted to NULL
- Missing dates are converted to NULL

### Amount Handling
- All amounts are standardized to decimal numbers
- Sign convention:
  - Negative for debits (money out)
  - Positive for credits (money in)
- Currency symbols and commas are removed
- Invalid amounts default to 0.0
- Missing amounts default to 0.0

### Description Handling
- Leading/trailing whitespace is removed
- Multiple spaces are collapsed to single space
- Special characters are preserved
- Empty descriptions are converted to "NO DESCRIPTION"

### Category Handling
- Leading/trailing whitespace is removed
- Empty categories are converted to NULL
- Categories are preserved from source if available

## Output Format

The reconciliation report is generated as an Excel file with the following columns:
- `date`: Transaction date
- `year_month`: Year and month of transaction (YYYY-MM)
- `account`: Account name
- `description`: Transaction description
- `category`: Transaction category (from aggregator if matched, from detail if unmatched)
- `tags`: Transaction tags (from aggregator if available)
- `amount`: Transaction amount
- `reconciled_key`: Unique key for the transaction (see Reconciled Key Format section)
- `matched`: Boolean indicating if transaction was matched

## Usage

```bash
python reconcile.py aggregator.csv details_folder/ --log-level DEBUG
```

Arguments:
- `aggregator.csv`: Path to the aggregator record file
- `details_folder/`: Path to folder containing detail record files
- `--log-level`: Optional logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Output Files

The tool generates:
1. Reconciliation report (Excel file) in the `archive` directory
2. Log file in the `logs` directory

## Error Handling

- Invalid dates are converted to NULL
- Invalid amounts default to 0.0
- Missing required columns cause the file to be skipped
- Empty files are logged but not processed
- Duplicate transactions are handled based on the first occurrence
- Missing files are logged but don't stop processing
- File permission errors are logged but don't stop processing

## Features

- Supports multiple financial institution formats
- Handles different date formats and amount formats
- Comprehensive matching strategy using compound keys
- Generates detailed reconciliation reports in Excel format
- Comprehensive logging for debugging and auditing
- Graceful error handling for various edge cases

## Development

### Running Tests
```bash
python -m pytest test_reconcile.py -v
```

### Project Structure
```
local_reconcile/
├── reconcile.py          # Main reconciliation logic
├── test_reconcile.py     # Test suite
├── requirements.txt      # Project dependencies
├── .gitignore           # Git ignore rules
├── README.md            # Project documentation
├── logs/                # Log files (gitignored)
└── archive/             # Output files (gitignored)
```

## License

[Add your license information here] 