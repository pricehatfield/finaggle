# Transaction Reconciliation System

A Python-based system for reconciling financial transactions across multiple sources and formats.

## Overview

This system standardizes transaction data from various financial institutions into a common format for reconciliation purposes. It supports multiple input formats and provides a standardized output format that makes it easy to match transactions across different sources.

## Supported Formats

The system currently supports the following input formats:

1. **Discover Format**
   - Trans. Date: Transaction date
   - Post Date: Posting date
   - Description: Transaction description
   - Amount: Transaction amount (positive for debits)
   - Category: Transaction category

2. **American Express Format**
   - Date: Transaction date
   - Description: Transaction description
   - Card Member: Card member name
   - Account #: Account number
   - Amount: Transaction amount (positive for debits)

3. **Capital One Format**
   - Transaction Date: Transaction date
   - Posted Date: Posting date
   - Card No.: Card number
   - Description: Transaction description
   - Category: Transaction category
   - Debit: Debit amount
   - Credit: Credit amount

4. **Alliant Format**
   - Date: Transaction date
   - Description: Transaction description
   - Amount: Transaction amount (with $ prefix)
   - Balance: Account balance
   - Post Date: Posting date

5. **Chase Format**
   - Details: Transaction type
   - Posting Date: Posting date
   - Description: Transaction description
   - Amount: Transaction amount (negative for debits)
   - Type: Transaction type
   - Balance: Account balance
   - Check or Slip #: Check number

6. **Aggregator Format**
   - Date: Transaction date
   - Description: Transaction description
   - Amount: Transaction amount (negative for debits)
   - Category: Transaction category
   - Tags: Additional tags
   - Account: Account name

## Standardized Output Format

All input formats are converted to a common output format with the following columns:

- **Transaction Date**: Date of the transaction (YYYY-MM-DD)
- **Post Date**: Date the transaction posted (YYYY-MM-DD)
- **Description**: Cleaned transaction description
- **Amount**: Standardized amount (negative for debits, positive for credits)
- **Category**: Transaction category
- **source_file**: Origin of the transaction

## Reconciliation Process

The system uses a sophisticated matching algorithm that:

1. Standardizes all input formats to the common output format
2. Creates reconciliation keys for matching:
   - P: prefix for Post Date matches (from aggregator)
   - T: prefix for Transaction Date matches (from detail records)
   - UA: prefix for unmatched aggregator records
   - UD: prefix for unmatched detail records
3. Matches transactions based on both Post Date and Transaction Date
4. Preserves metadata from the aggregator for matched records
5. Includes both unmatched aggregator and detail records in the output

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface

```bash
python src/reconcile.py <aggregator_file> <details_folder> [--log-level LEVEL]
```

Arguments:
- `aggregator_file`: Path to the aggregator CSV file
- `details_folder`: Path to folder containing detail CSV files
- `--log-level`: Optional logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

### Python API

```python
from reconcile import reconcile_transactions, import_csv, import_folder

# Import data
aggregator_df = import_csv('path/to/aggregator.csv')
detail_records = import_folder('path/to/details/folder')

# Reconcile transactions
result = reconcile_transactions(aggregator_df, detail_records)
```

## Output

The system generates a reconciliation report with the following information:

1. Matched transactions (using aggregator metadata)
2. Unmatched transactions from both aggregator and detail sources
3. Reconciliation keys for tracking matches
4. YearMonth grouping for analysis
5. Source file tracking

## Development

### Running Tests

```bash
python -m pytest tests/ -v --html=logs/report.html
```

### Adding New Formats

To add support for a new format:

1. Create a new processing function in `src/reconcile.py`
2. Add format detection in `import_csv()`
3. Add test cases in `tests/test_reconcile.py`

## License

This project is licensed under the MIT License - see the LICENSE file for details. 