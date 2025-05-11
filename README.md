# Local Reconcile

A Python package for reconciling financial transactions across multiple sources.

## Data Formats

### Important Note on Description Handling
- **Descriptions are NEVER modified**: All transaction descriptions are preserved exactly as they appear in the source files. No cleaning, standardization, or modification is performed on description fields.
- **Purpose**: This ensures that the original transaction details are maintained for accurate reconciliation and audit purposes.
- **Implementation**: The system treats description fields as opaque strings, preserving all special characters, spacing, and formatting exactly as they appear in the source files.

### Important Note on CSV Quoting
- **CSV files in `data/` may use mixed quoting**: Not all fields are necessarily quoted, and quoting is typically only used for fields containing commas or special characters (e.g., Description fields).
- **Import logic**: Always use pandas' default quoting behavior when reading CSVs, unless the file is known to be fully quoted. Forcing `quoting=csv.QUOTE_ALL` can cause column misalignment and data errors.
- **If you encounter import errors**: First check the data file for quoting style, then update the import logic or README as needed to match real-world data.
- **Output files**: All fields in output CSV files should be properly quote-encapsulated (using `csv.QUOTE_NONNUMERIC` or equivalent) to ensure consistent parsing across different tools and systems.

### Source Detail Formats

#### Discover Format
- **Columns**:
  - `Trans. Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
  - `Post Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
  - `Description`: String (preserved exactly as-is)
  - `Amount`: String (requires cleaning: remove $, commas, convert to Decimal)
  - `Category`: String (preserved exactly as-is)
- **Notes**:
  - Both transaction and post dates are provided
  - Amount sign convention: negative for debits, positive for credits
  - Amounts may include $ symbol and commas (e.g., "$1,234.56")
  - Requires explicit decimal conversion
  - Credits are represented as positive amounts (e.g., "INTERNET PAYMENT - THANK YOU")

#### Capital One Format
- **Columns**:
  - `Transaction Date`: String (YYYY-MM-DD, Python datetime format "%Y-%m-%d")
  - `Posted Date`: String (YYYY-MM-DD, Python datetime format "%Y-%m-%d")
  - `Card No.`: String (preserved exactly as-is)
  - `Description`: String (preserved exactly as-is)
  - `Category`: String (preserved exactly as-is)
  - `Debit`: Decimal (null for credits)
  - `Credit`: Decimal (null for debits)
- **Notes**:
  - Uses separate columns for debits and credits
  - For each transaction, exactly one of Debit or Credit will be populated
  - The other column will be null/empty
  - Both transaction and post dates are provided
  - Amounts are already in decimal format
  - Null/empty values must be preserved as null (not converted to Decimal(0))

#### Chase Format
- **Columns**:
  - `Details`: String (preserved exactly as-is)
  - `Posting Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y", represents post date)
  - `Description`: String (preserved exactly as-is, contains transaction IDs, reference numbers, and special characters)
  - `Amount`: Decimal (already in decimal format, negative for debits, positive for credits)
  - `Type`: String (preserved exactly as-is, represents transaction classification such as ACH_DEBIT, ACH_CREDIT, etc.)
  - `Balance`: Decimal (already in decimal format)
  - `Check or Slip #`: String (may be empty)
- **Notes**:
  - Uses transaction type in `Details` column
  - `Type` provides specific transaction classification (e.g., ACH_DEBIT, LOAN_PMT) and is not a vendor/expenditure category
  - Only posting date is provided
  - Amounts are already in decimal format (no $ or commas)
  - No Category field is present in this format

#### Alliant Checking Format
- **Columns**:
  - `Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y", represents transaction date)
  - `Description`: String (preserved exactly as-is, may contain newlines)
  - `Amount`: String (includes $ symbol, positive for credits, negative for debits [TBC])
  - `Balance`: String (includes $ symbol and commas for thousands)
- **Notes**:
  - Only transaction date is provided
  - Amount sign convention: positive for credits, negative for debits [TBC]
  - Amounts include $ symbol
  - Balance includes $ symbol and commas
  - Description may contain newlines (e.g., in dividend entries)

#### Alliant Visa Format
- **Columns**:
  - `Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
  - `Description`: String (preserved exactly as-is)
  - `Amount`: String (includes $ symbol, negative for debits, positive for credits, uses parentheses for credits)
  - `Balance`: String (includes $ symbol)
  - `Post Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y")
- **Notes**:
  - Both transaction and post dates are provided
  - Amount sign convention: negative for debits, positive for credits
  - Amounts include $ symbol
  - Credits are shown in parentheses (e.g., "($764.86)")

#### American Express Format
- **Columns**:
  - `Date`: String (MM/DD/YYYY, Python datetime format "%m/%d/%Y", represents transaction date)
  - `Description`: String (preserved exactly as-is)
  - `Card Member`: String (contains full name)
  - `Account #`: String (contains account identifier, prefixed with minus sign)
  - `Amount`: Decimal (negative for credits, positive for charges)
- **Notes**:
  - Only transaction date is provided
  - Amount sign convention: positive for charges, negative for credits
  - Amounts are in decimal format (no $ or commas)
  - Account numbers are prefixed with minus sign (e.g., "-42004")

### Standardized Detail Format
All source detail records are converted to this intermediate format before matching:
- `Transaction Date`: YYYY-MM-DD
- `Post Date`: YYYY-MM-DD (null if not provided)
- `Description`: String (preserved from source with newlines stripped)
- `Amount`: Decimal (negative for debits, positive for credits)
- `Category`: String (preserved from source)
- `source_file`: String (identifies the source of the transaction)
- `reconciled_key`: String (see Reconciliation Key Format section)

Notes:
- Account information is preserved from the aggregator format but not from detail formats
- Post dates are required for matching but may be null for formats that don't provide them
- Amount sign convention is standardized to negative for debits
- Category is preserved exactly as-is from source files
- Description is preserved from source with newlines stripped to ensure consistent matching
- Source files are read and preserved exactly as-is, with no modifications

### Aggregator Format
- **Columns**:
  - `Date`: String (YYYY-MM-DD, Python datetime format "%Y-%m-%d")
  - `Account`: String (contains card type and last 4 digits, e.g., "Cashback Visa Signature - Ending in 1967")
  - `Description`: String (preserved exactly as-is)
  - `Category`: String (preserved exactly as-is)
  - `Tags`: String (optional, comma-separated values, may contain spaces, may be empty)
  - `Amount`: Decimal (negative for debits, positive for credits)
  - `reconciled_key`: String (see Reconciliation Key Format section)
  - `Matched`: String (strictly "True" or "False", for human readability)
- **Notes**:
  - Tags can contain multiple values (e.g., "Joint,Price")
  - Tags field may be empty
  - Amounts are in decimal format
  - Account field includes card type and last 4 digits

### Output Format
The reconciliation process generates a single output file in the `output` directory:

#### All Transactions (all_transactions.csv)
- **Columns**:
  - `Date`: YYYY-MM-DD (for matched transactions, from aggregator; otherwise from source record)
  - `YearMonth`: YYYY-MM (derived from Date field)
  - `Account`: String (for matched transactions, from aggregator; otherwise from source record)
  - `Description`: String (for matched transactions, from aggregator; otherwise from source record)
  - `Category`: String (for matched transactions, from aggregator; otherwise from source record)
  - `Tags`: String (from aggregator file; empty for unmatched detail records)
  - `Amount`: Decimal (negative for debits; for matched transactions, from aggregator; otherwise from source record)
  - `reconciled_key`: String (see Reconciliation Key Format section)
  - `Matched`: String (strictly "True" or "False", for human readability)

#### Field Sourcing for Matched Transactions
For transactions that are successfully matched between aggregator and detail records:
- All fields available in the aggregator record take precedence
- Detail record fields are only used when the corresponding aggregator field is null/empty
- This applies to: Date, Account, Description, Category, Amount fields
- Tags are exclusively sourced from the aggregator (empty for unmatched detail records)

For unmatched transactions, all available fields from the original source are preserved.

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