import pytest
import pandas as pd
from src.reconcile import (
    process_discover_format,
    process_amex_format,
    process_capital_one_format,
    process_alliant_format,
    process_chase_format,
    process_aggregator_format,
    reconcile_transactions
)

class TestReconciliation:
    """Test suite for reconciliation functionality"""
    
    def test_basic_matching(self):
        """Test basic transaction matching"""
        # Create matching detail and aggregator records
        detail_data = {
            'Trans. Date': ['03/17/2025'],
            'Post Date': ['03/18/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['123.45'],
            'Category': ['Shopping']
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025'],
            'Description': ['AMAZON.COM'],
            'Amount': ['-123.45'],
            'Category': ['Shopping'],
            'Tags': [''],
            'Account': ['Discover']
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Verify matching
        assert len(result['matched']) == 1
        assert len(result['unmatched_detail']) == 0
        assert len(result['unmatched_aggregator']) == 0
        
    def test_amount_matching(self):
        """Test transaction matching by amount"""
        # Create records with same date but different amounts
        detail_data = {
            'Trans. Date': ['03/17/2025'] * 2,
            'Post Date': ['03/18/2025'] * 2,
            'Description': ['AMAZON.COM'] * 2,
            'Amount': ['123.45', '67.89'],
            'Category': ['Shopping'] * 2
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025'] * 2,
            'Description': ['AMAZON.COM'] * 2,
            'Amount': ['-123.45', '-67.89'],
            'Category': ['Shopping'] * 2,
            'Tags': [''] * 2,
            'Account': ['Discover'] * 2
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Verify matching
        assert len(result['matched']) == 2
        assert len(result['unmatched_detail']) == 0
        assert len(result['unmatched_aggregator']) == 0
        
    def test_date_matching(self):
        """Test transaction matching by date"""
        # Create records with same amount but different dates
        detail_data = {
            'Trans. Date': ['03/17/2025', '03/18/2025'],
            'Post Date': ['03/18/2025', '03/19/2025'],
            'Description': ['AMAZON.COM'] * 2,
            'Amount': ['123.45'] * 2,
            'Category': ['Shopping'] * 2
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025', '03/18/2025'],
            'Description': ['AMAZON.COM'] * 2,
            'Amount': ['-123.45'] * 2,
            'Category': ['Shopping'] * 2,
            'Tags': [''] * 2,
            'Account': ['Discover'] * 2
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Verify matching
        assert len(result['matched']) == 2
        assert len(result['unmatched_detail']) == 0
        assert len(result['unmatched_aggregator']) == 0
        
    def test_description_matching(self):
        """Test transaction matching by description"""
        # Create records with same amount and date but different descriptions
        detail_data = {
            'Trans. Date': ['03/17/2025'] * 2,
            'Post Date': ['03/18/2025'] * 2,
            'Description': ['AMAZON.COM', 'AMAZON MARKETPLACE'],
            'Amount': ['123.45'] * 2,
            'Category': ['Shopping'] * 2
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025'] * 2,
            'Description': ['AMAZON.COM', 'AMAZON MARKETPLACE'],
            'Amount': ['-123.45'] * 2,
            'Category': ['Shopping'] * 2,
            'Tags': [''] * 2,
            'Account': ['Discover'] * 2
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Verify matching
        assert len(result['matched']) == 2
        assert len(result['unmatched_detail']) == 0
        assert len(result['unmatched_aggregator']) == 0
        
    def test_unmatched_transactions(self):
        """Test handling of unmatched transactions"""
        # Create records with some unmatched transactions
        detail_data = {
            'Trans. Date': ['03/17/2025', '03/18/2025'],
            'Post Date': ['03/18/2025', '03/19/2025'],
            'Description': ['AMAZON.COM', 'WALMART'],
            'Amount': ['123.45', '67.89'],
            'Category': ['Shopping'] * 2
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025', '03/19/2025'],
            'Description': ['AMAZON.COM', 'TARGET'],
            'Amount': ['-123.45', '-89.01'],
            'Category': ['Shopping'] * 2,
            'Tags': [''] * 2,
            'Account': ['Discover'] * 2
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Verify matching
        assert len(result['matched']) == 1  # Only AMAZON.COM matches
        assert len(result['unmatched_detail']) == 1  # WALMART unmatched
        assert len(result['unmatched_aggregator']) == 1  # TARGET unmatched
        
    def test_partial_matches(self):
        """Test handling of partial matches"""
        # Create records with partial matches
        detail_data = {
            'Trans. Date': ['03/17/2025', '03/18/2025'],
            'Post Date': ['03/18/2025', '03/19/2025'],
            'Description': ['AMAZON.COM', 'AMAZON.COM'],
            'Amount': ['123.45', '123.45'],
            'Category': ['Shopping'] * 2
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025', '03/18/2025'],
            'Description': ['AMAZON.COM', 'AMAZON.COM'],
            'Amount': ['-123.45', '-123.45'],
            'Category': ['Shopping'] * 2,
            'Tags': [''] * 2,
            'Account': ['Discover'] * 2
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Verify matching
        assert len(result['matched']) == 2  # Both should match
        assert len(result['unmatched_detail']) == 0
        assert len(result['unmatched_aggregator']) == 0 