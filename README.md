# Local Reconcile

A Python package for reconciling financial transactions across multiple sources.

## Supported File Formats

### Discover
- **File Pattern**: `discover_*.csv`
- **Columns**:
  - `Trans. Date`: MM/DD/YYYY
  - `Post Date`: MM/DD/YYYY
  - `Description`: String
  - `Amount`: Decimal (positive for credits, negative for debits)
  - `Category`: String

### Capital One
- **File Pattern**: `capital_one_*.csv`
- **Columns**:
  - `Transaction Date`: YYYY-MM-DD
  - `Posted Date`: YYYY-MM-DD
  - `Card No.`: String
  - `Description`: String
  - `Category`: String
  - `Debit`: Decimal (positive)
  - `Credit`: Decimal (positive)

### Chase
- **File Pattern**: `chase_*.csv`
- **Columns**:
  - `Details`: String
  - `Posting Date`: MM/DD/YYYY
  - `Description`: String
  - `Amount`: Decimal (positive for credits, negative for debits)
  - `Type`: String
  - `Balance`: Decimal
  - `Check or Slip #`: String

### Alliant Checking
- **File Pattern**: `alliant_checking_*.csv`
- **Columns**:
  - `Date`: MM/DD/YYYY
  - `Description`: String
  - `Amount`: String (with $ symbol, positive for credits, negative for debits)
  - `Balance`: Decimal

### Alliant Visa
- **File Pattern**: `alliant_visa_*.csv`
- **Columns**:
  - `Date`: MM/DD/YYYY
  - `Description`: String
  - `Amount`: String (with $ symbol, positive for credits, negative for debits)
  - `Balance`: Decimal
  - `Post Date`: MM/DD/YYYY

### American Express
- **File Pattern**: `amex_*.csv`
- **Columns**:
  - `Date`: MM/DD/YYYY
  - `Description`: String
  - `Card Member`: String
  - `Account #`: Integer
  - `Amount`: Decimal (positive for charges, negative for payments/credits)

### Aggregator (Empower)
- **File Pattern**: `empower_*.csv`
- **Columns**:
  - `Date`: YYYY-MM-DD
  - `Account`: String
  - `Description`: String
  - `Category`: String
  - `Tags`: String
  - `Amount`: Decimal (negative for debits, positive for credits)

## Test Structure

The test suite is organized into seven sequentially numbered files that follow a progressive testing path:

1. `conftest.py`: Base fixtures and configuration
2. `test_1_utils.py`: Utility functions (date standardization, amount cleaning)
3. `test_2_file_formats.py`: File format validation and processing
4. `test_3_file_loads.py`: File and folder import functionality
5. `test_4_format_standardization.py`: Data standardization (descriptions, categories)
6. `test_5_reconciliation.py`: Transaction matching and reconciliation
7. `test_6_reporting.py`: Report generation and output

Note: The numbering is for human readability and organization. Test execution order is enforced using pytest dependency markers.

## Installation

```bash
pip install -e .
```

## Usage

```python
from src.reconcile import reconcile_transactions

# Import transactions from multiple sources
source_df = import_folder("path/to/source/files")
target_df = import_folder("path/to/target/files")

# Reconcile transactions
matches, unmatched = reconcile_transactions(source_df, target_df)

# Generate report
generate_reconciliation_report(matches, unmatched, "report.txt")
```

## Output Format

The reconciliation process generates two output files in the `output` directory:

### Matched Transactions (matched.csv)
- **Columns**:
  - `Transaction Date`: YYYY-MM-DD
  - `Post Date`: YYYY-MM-DD
  - `Description`: String
  - `Amount`: Decimal (negative for debits)
  - `Category`: String
  - `source_file`: String
  - `match_type`: String (describes matching strategy used)
    - `post_date_amount`: Matched on post date and amount
    - `transaction_date_amount`: Matched on transaction date and amount

### Unmatched Transactions (unmatched.csv)
- **Columns**:
  - `Transaction Date`: YYYY-MM-DD
  - `Post Date`: YYYY-MM-DD
  - `Description`: String
  - `Amount`: Decimal (negative for debits)
  - `Category`: String
  - `source_file`: String

### Potential Issues
1. **Date Handling**: 
   - The current format uses YYYY-MM-DD for all dates, which may differ from the input formats
   - Post dates are required for matching, which may cause issues with formats that don't provide them

2. **Amount Sign Convention**:
   - All amounts are stored as negative for debits, which may differ from some input formats
   - This convention is enforced during processing but may need adjustment for certain use cases

3. **Category Standardization**:
   - Categories are preserved from the source files
   - No standardization is performed, which may lead to inconsistent categorization

4. **Source Tracking**:
   - The `source_file` column helps track where transactions originated
   - However, it may not be clear which specific account or card the transaction came from

5. **Match Type Limitation**:
   - Primary matching uses post date and amount (`post_date_amount` match type)
   - Secondary matching uses transaction date and amount (`transaction_date_amount` match type)
   - No support for description-based matching or fuzzy matching

## Development

### Running Tests

```bash
pytest
```

### Test Structure

Tests are organized in numbered files for human readability and use pytest dependency markers to enforce execution order:

1. `conftest.py`: Test fixtures and shared resources
2. `test_1_utils.py`: Utility function tests (date formatting, amount cleaning)
3. `test_2_file_formats.py`: File format validation (column names, data types)
4. `test_3_file_loads.py`: File reading and parsing tests
5. `test_4_format_standardization.py`: Format conversion and standardization
6. `test_5_reconciliation.py`: Transaction matching and reconciliation
7. `test_6_reporting.py`: Report generation and formatting

Each test file builds on the previous ones, with dependency markers ensuring proper execution order. The numbered filenames make it easy to navigate the test suite and understand the progression of tests.

### Code Style

This project uses black for code formatting:

```bash
black .
``` 