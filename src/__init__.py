"""
Local Reconcile - A tool for reconciling bank transactions across different formats.

This package provides functionality to:
- Read transaction data from various bank formats (Capital One, Chase, Discover)
- Standardize transaction data into a common format
- Compare and reconcile transactions across accounts
- Generate reconciliation reports

The standardized format includes:
- Transaction Date: Date of the transaction (YYYY-MM-DD)
- Post Date: Date the transaction posted (YYYY-MM-DD)
- Description: Transaction description
- Amount: Numeric amount (negative for debits)
- Category: Transaction category
- source_file: Source of the transaction data
"""

from .reconcile import (
    read_transactions,
    standardize_date,
    clean_amount,
    process_capital_one_format,
    process_chase_format,
    process_discover_format,
    reconcile_transactions,
    generate_reconciliation_report
)

__all__ = [
    'read_transactions',
    'standardize_date',
    'clean_amount',
    'process_capital_one_format',
    'process_chase_format',
    'process_discover_format',
    'reconcile_transactions',
    'generate_reconciliation_report'
] 