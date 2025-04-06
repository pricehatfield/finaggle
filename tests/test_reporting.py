import pytest
import pandas as pd
from src.reconcile import (
    process_discover_format,
    process_aggregator_format,
    reconcile_transactions,
    generate_reconciliation_report
)

class TestReporting:
    """Test suite for reporting functionality"""
    
    def test_summary_statistics(self):
        """Test generation of summary statistics"""
        # Create sample data with matched and unmatched transactions
        detail_data = {
            'Trans. Date': ['03/17/2025', '03/18/2025', '03/19/2025'],
            'Post Date': ['03/18/2025', '03/19/2025', '03/20/2025'],
            'Description': ['AMAZON.COM', 'WALMART', 'TARGET'],
            'Amount': ['123.45', '67.89', '89.01'],
            'Category': ['Shopping'] * 3
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025', '03/19/2025', '03/20/2025'],
            'Description': ['AMAZON.COM', 'TARGET', 'COSTCO'],
            'Amount': ['-123.45', '-89.01', '-45.67'],
            'Category': ['Shopping'] * 3,
            'Tags': [''] * 3,
            'Account': ['Discover'] * 3
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Generate report
        report = generate_reconciliation_report(result)
        
        # Verify summary statistics
        assert report['total_transactions'] == 6
        assert report['matched_transactions'] == 2
        assert report['unmatched_detail'] == 1
        assert report['unmatched_aggregator'] == 1
        assert report['match_rate'] == pytest.approx(0.333, rel=1e-3)
        
    def test_unmatched_transactions_report(self):
        """Test reporting of unmatched transactions"""
        # Create sample data with unmatched transactions
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
        
        # Generate report
        report = generate_reconciliation_report(result)
        
        # Verify unmatched transactions report
        assert len(report['unmatched_detail_list']) == 1
        assert len(report['unmatched_aggregator_list']) == 1
        assert report['unmatched_detail_list'][0]['Description'] == 'WALMART'
        assert report['unmatched_aggregator_list'][0]['Description'] == 'TARGET'
        
    def test_matched_transactions_report(self):
        """Test reporting of matched transactions"""
        # Create sample data with matched transactions
        detail_data = {
            'Trans. Date': ['03/17/2025', '03/18/2025'],
            'Post Date': ['03/18/2025', '03/19/2025'],
            'Description': ['AMAZON.COM', 'TARGET'],
            'Amount': ['123.45', '89.01'],
            'Category': ['Shopping'] * 2
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': ['03/17/2025', '03/18/2025'],
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
        
        # Generate report
        report = generate_reconciliation_report(result)
        
        # Verify matched transactions report
        assert len(report['matched_list']) == 2
        assert report['matched_list'][0]['Description'] == 'AMAZON.COM'
        assert report['matched_list'][1]['Description'] == 'TARGET'
        
    def test_report_formatting(self):
        """Test report formatting and structure"""
        # Create sample data
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
        
        # Generate report
        report = generate_reconciliation_report(result)
        
        # Verify report structure
        assert 'summary' in report
        assert 'matched_list' in report
        assert 'unmatched_detail_list' in report
        assert 'unmatched_aggregator_list' in report
        assert 'timestamp' in report
        
        # Verify summary format
        summary = report['summary']
        assert 'total_transactions' in summary
        assert 'matched_transactions' in summary
        assert 'unmatched_detail' in summary
        assert 'unmatched_aggregator' in summary
        assert 'match_rate' in summary
        
    def test_empty_report(self):
        """Test report generation with no transactions"""
        # Create empty data
        detail_data = {
            'Trans. Date': [],
            'Post Date': [],
            'Description': [],
            'Amount': [],
            'Category': []
        }
        detail_df = pd.DataFrame(detail_data)
        detail_df['source_file'] = 'discover_test.csv'
        detail_records = process_discover_format(detail_df)
        
        aggregator_data = {
            'Date': [],
            'Description': [],
            'Amount': [],
            'Category': [],
            'Tags': [],
            'Account': []
        }
        aggregator_df = pd.DataFrame(aggregator_data)
        aggregator_df['source_file'] = 'aggregator_test.csv'
        aggregator_records = process_aggregator_format(aggregator_df)
        
        # Reconcile transactions
        result = reconcile_transactions(detail_records, aggregator_records)
        
        # Generate report
        report = generate_reconciliation_report(result)
        
        # Verify empty report
        assert report['summary']['total_transactions'] == 0
        assert report['summary']['matched_transactions'] == 0
        assert report['summary']['unmatched_detail'] == 0
        assert report['summary']['unmatched_aggregator'] == 0
        assert report['summary']['match_rate'] == 0.0
        assert len(report['matched_list']) == 0
        assert len(report['unmatched_detail_list']) == 0
        assert len(report['unmatched_aggregator_list']) == 0 