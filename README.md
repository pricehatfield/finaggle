# Local Reconcile

A Python package for reconciling financial transactions across multiple sources.

## Data Formats

### Source Detail Formats

#### Discover Format
- **Columns**:
  - `Trans. Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
  - `Post Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
  - `Description`: String (may contain special characters)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Category`: String (may contain special characters)
- **Notes**:
  - Both transaction and post dates are provided
  - Amount sign convention: negative for debits, positive for credits
  - Amounts may include $ symbol and commas (e.g., "$1,234.56")
  - Requires explicit decimal conversion

#### Capital One Format
- **Columns**:
  - `Transaction Date`: String (YYYY-MM-DD, Python datetime format "%Y-%m-%d")
  - `Posted Date`: String (YYYY-MM-DD, Python datetime format "%Y-%m-%d")
  - `Card No.`: String (may contain spaces or special characters)
  - `Description`: String (may contain special characters)
  - `Category`: String (may contain special characters)
  - `Debit`: String (requires cleaning: remove $, commas, convert to Decimal, null for credits)
  - `Credit`: String (requires cleaning: remove $, commas, convert to Decimal, null for debits)
- **Notes**:
  - Uses separate columns for debits and credits
  - For each transaction, exactly one of Debit or Credit will be populated
  - The other column will be null/empty
  - Both transaction and post dates are provided
  - Amounts may include $ symbol and commas
  - Null/empty values must be preserved as null (not converted to Decimal(0))

#### Chase Format
- **Columns**:
  - `Details`: String (strictly "DEBIT" or "CREDIT")
  - `Posting Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y", represents post date)
  - `Description`: String (may contain special characters and newlines)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Type`: String (preserve as-is)
  - `Balance`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Check or Slip #`: String (often empty, may contain special characters)
- **Notes**:
  - Uses transaction type in `Details` column
  - `Type` provides more specific transaction classification
  - Only posting date is provided
  - Amounts may include $ symbol and commas
  - Descriptions may contain newlines and special characters

#### Alliant Checking Format
- **Columns**:
  - `Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y", represents transaction date)
  - `Description`: String (may contain newlines and special characters)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Balance`: String (requires cleaning: remove $, commas, convert to Decimal)
- **Notes**:
  - Only transaction date is provided
  - Amount sign convention: negative for debits, positive for credits
  - Amounts include $ symbol and may include commas
  - Descriptions may contain newlines (e.g., dividend descriptions)

#### Alliant Visa Format
- **Columns**:
  - `Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
  - `Description`: String (may contain special characters)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Balance`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Post Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
- **Notes**:
  - Both transaction and post dates are provided
  - Amount sign convention: negative for debits, positive for credits
  - Amounts include $ symbol and may include commas
  - Requires explicit decimal conversion with proper rounding

#### American Express Format
- **Columns**:
  - `Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y", represents transaction date)
  - `Description`: String (may contain special characters)
  - `Card Member`: String (may contain spaces and special characters)
  - `Account #`: String (preserve as-is)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
- **Notes**:
  - Only transaction date is provided
  - Amount sign convention: positive for charges, negative for credits
  - Amounts may include $ symbol and commas

### Standardized Detail Format
All source detail records are converted to this intermediate format before matching:
- `Transaction Date`: YYYY-MM-DD
- `Post Date`: YYYY-MM-DD (null if not provided)
- `Description`: String (preserved unchanged)
- `Amount`: Decimal (negative for debits, positive for credits)
- `Category`: String (preserved from source)
- `source_file`: String (identifies the source of the transaction)
- `reconciled_key`: String (see Reconciliation Key Format section)

Notes:
- Account information is preserved from the aggregator format but not from detail formats
- Post dates are required for matching but may be null for formats that don't provide them
- Amount sign convention is standardized to negative for debits
- Category and Description are preserved as-is from source files

### Aggregator Format
- **Columns**:
  - `Date`: String (YYYY-MM-DD, Python datetime format "%Y-%m-%d")
  - `Account`: String (contains account name and number)
  - `Description`: String (may contain special characters)
  - `Category`: String (may contain special characters)
  - `Tags`: String (optional, comma-separated values, may contain spaces)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `reconciled_key`: String (see Reconciliation Key Format section)
  - `Matched`: String (strictly "True" or "False", preserved as text for human readability)
- **Notes**:
  - Tags can contain multiple values (e.g., "Joint,Price")
  - Amounts may include $ symbol and commas
  - Matched field is preserved as text for human readability

### Output Format
The reconciliation process generates a single output file in the `output` directory:

#### All Transactions (all_transactions.csv)
- **Columns**:
  - `Date`: YYYY-MM-DD
  - `YearMonth`: YYYY-MM
  - `Account`: String (source of transaction)
  - `Description`: String
  - `Category`: String
  - `Tags`: String (preserved from aggregator file)
  - `Amount`: Decimal (negative for debits)
  - `reconciled_key`: String (see Reconciliation Key Format section)
  - `Matched`: Boolean (True for matched transactions)

#### Tag Handling
- Tags from the aggregator file are preserved in the output
- Tags are maintained for both matched and unmatched transactions from the aggregator
- Transactions from other sources have empty tags

## Reconciliation Key Format

The system uses a standardized reconciliation key format to uniquely identify and match transactions:

```
{prefix}:{date}_{amount}
```

Where:
- `prefix`: Indicates the match type and date source
  - `P:`: Post Date match (from aggregator)
  - `T:`: Transaction Date match (from detail records)
  - `U:`: Unmatched record
- `date`: The date used for matching (YYYY-MM-DD format)
  - For `P:` keys: Uses the Post Date
  - For `T:` keys: Uses the Transaction Date
  - For `U:` keys: Uses the Transaction Date
- `amount`: The absolute value of the transaction amount (formatted to 2 decimal places)

### Key Generation Logic

1. **For Detail Records with Both Dates**:
   - First attempt: Generate key using Post Date (`P:{post_date}_{amount}`)
   - If no match found: Generate key using Transaction Date (`T:{trans_date}_{amount}`)
   - If still no match: Mark as unmatched (`U:{trans_date}_{amount}`)

2. **For Detail Records with Single Date**:
   - Generate key using the available date with appropriate prefix based on date type:
     - `P:` for post dates
     - `T:` for transaction dates
   - If no match found: Mark as unmatched (`U:{date}_{amount}`)

3. **For Aggregator Records**:
   - Generate key using Post Date (`P:{date}_{amount}`)
   - If no match found: Mark as unmatched (`U:{date}_{amount}`)

### Matching Priority

1. Primary matching: Post Date and amount
   - Uses `P:` prefix
   - Attempts to match using the transaction's post date
   - Most reliable match as post dates are more consistent

2. Secondary matching: Transaction Date and amount
   - Uses `T:` prefix
   - Attempts to match using the transaction date
   - Used when post date matching fails
   - Less reliable due to potential date variations

3. Unmatched records
   - Uses `U:` prefix
   - Preserves original transaction date
   - Used for manual review and reconciliation

Example keys:
- `P:2024-03-15_123.45`: A transaction matched using Post Date
- `T:2024-03-14_123.45`: A transaction matched using Transaction Date
- `U:2024-03-15_123.45`: An unmatched transaction

## Installation

```bash
pip install -e .
```

## Usage

### Basic Usage
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

### Understanding the Results
After running the reconciliation, you'll find:
- `output/all_transactions.csv`: Contains all transactions with their reconciliation status
- `reconciliation_report.txt`: Shows a summary of the reconciliation including:
  - Total number of transactions
  - Number of matched transactions
  - List of unmatched transactions with details
  - Common issues found during reconciliation

## Troubleshooting

### Common Errors
- "Invalid file format": Check column headers and file naming
- "Missing required columns": Verify all required columns are present
- "Invalid date format": Ensure dates follow the expected format
- "Amount conversion error": Check for non-numeric characters in amount fields

### Debugging Tips
- Use the `--debug` flag for detailed logging
- Check the generated report for details
- Verify file contents match the expected format
- Look for patterns in unmatched transactions

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