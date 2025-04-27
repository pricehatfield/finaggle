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

### Aggregator (aggregator)
- **File Pattern**: `aggregator_*.csv`
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

## User Guide

### Getting Started

1. **Prepare Your Files**
   - The default configuration expects files in these locations:
     - Bank statements: `data/2025/details/`
     - Aggregator exports: `data/2025/`
   - If you want to use different locations, you can specify them with command-line arguments
   - Download your bank statements and save them with the correct names:
     - Discover: `discover_2025_03.csv`
     - Capital One: `capital_one_2025_03.csv`
     - Chase: `chase_2025_03.csv`
     - Alliant Checking: `alliant_checking_2025_03.csv`
     - Alliant Visa: `alliant_visa_2025_03.csv`
     - American Express: `amex_2025_03.csv`
   - Download your aggregator export and save it as `aggregator_2025_03.csv`

2. **Basic Usage**
   ```bash
   # Run with default paths (data/2025/details and data/2025)
   python -m src.reconcile
   
   # Run with custom paths
   python -m src.reconcile --statements statements/2025/03 --aggregator aggregator/2025/03
   
   # Run with debug logging
   python -m src.reconcile --debug
   
   # Run with specific log level
   python -m src.reconcile --log-level warning
   ```

3. **Understanding the Results**
   After running the reconciliation, you'll find:
   - `output/all_transactions.csv`: Contains all transactions with their reconciliation status
   - `reconciliation_report.txt`: Shows a summary of the reconciliation including:
     - Total number of transactions
     - Number of matched transactions
     - List of unmatched transactions with details
     - Common issues found during reconciliation

### Common Use Cases

1. **Using Default Paths**
   ```bash
   # Just run the reconciliation with default paths
   python -m src.reconcile
   ```

2. **Custom Paths**
   ```bash
   # For March 2025 with custom paths
   python -m src.reconcile --statements statements/2025/03 --aggregator aggregator/2025/03
   
   # For Q1 2025 with custom paths
   python -m src.reconcile --statements statements/2025/Q1 --aggregator aggregator/2025/Q1
   ```

### Handling Unmatched Transactions

1. **Review the Report**
   - Check `reconciliation_report.txt` for unmatched transactions
   - Look for patterns in the unmatched transactions
   - Common issues include:
     - Date mismatches (transaction vs. post date)
     - Amount rounding differences
     - Description variations

2. **Investigate Common Issues**
   - Date format mismatches
   - Amount sign conventions
   - Description standardization
   - Category mapping

### Best Practices

1. **File Management**
   - Keep your source files organized by date
   - Maintain a consistent file naming convention
   - Archive processed files after reconciliation

2. **Data Quality**
   - Verify column headers match the expected format
   - Check for missing or malformed data
   - Ensure consistent date formats
   - Validate amount signs and decimal places

3. **Regular Reconciliation**
   - Perform reconciliations monthly
   - Keep track of recurring unmatched transactions
   - Document any systematic issues

### Troubleshooting

1. **Common Errors**
   - "Invalid file format": Check column headers and file naming
   - "Missing required columns": Verify all required columns are present
   - "Invalid date format": Ensure dates follow the expected format
   - "Amount conversion error": Check for non-numeric characters in amount fields

2. **Debugging Tips**
   - Use the `--debug` flag for detailed logging
   - Check the generated report for details
   - Verify file contents match the expected format
   - Look for patterns in unmatched transactions

## Output Format

The reconciliation process generates a single output file in the `output` directory:

### All Transactions (all_transactions.csv)
- **Columns**:
  - `Date`: YYYY-MM-DD
  - `YearMonth`: YYYY-MM
  - `Account`: String (source of transaction)
  - `Description`: String
  - `Category`: String
  - `Tags`: String (preserved from aggregator file)
  - `Amount`: Decimal (negative for debits)
  - `reconciled_key`: String (date-based key for reconciliation)
  - `Matched`: Boolean (True for matched transactions)

### Tag Handling
- Tags from the aggregator file are preserved in the output
- Tags are maintained for both matched and unmatched transactions from the aggregator
- Transactions from other sources have empty tags

### Matching Strategy
1. Primary matching: Post date and amount
2. Secondary matching: Transaction date and amount
3. Unmatched transactions are preserved with their original data

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
   - The `Account` column helps track where transactions originated
   - Format: "Matched - {source_file}" or "Unreconciled - {source_file}"

5. **Match Type Limitation**:
   - Primary matching uses post date and amount
   - Secondary matching uses transaction date and amount
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