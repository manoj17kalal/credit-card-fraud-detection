#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration Tests for Dashboard Functionality

This module contains integration tests for the Streamlit dashboard functionality,
focusing on data loading, filtering, and visualization components.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the dashboard module
with patch('streamlit.sidebar'):
    with patch('streamlit.title'):
        with patch('streamlit.session_state', {}):
            from dashboard.streamlit_app import (
                connect_to_database,
                load_transactions,
                load_fraud_by_country,
                load_fraud_by_category,
                load_fraud_by_hour,
                filter_data_by_time,
                filter_data_by_merchant,
                filter_data_by_country,
                filter_data_by_amount,
                display_metrics,
                display_map,
                display_fraud_trends,
                display_fraud_by_category,
                display_fraud_by_country
            )


class TestDashboardIntegration(unittest.TestCase):
    """Integration tests for the dashboard functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock database connection
        self.mock_db = MagicMock()
        
        # Create sample transaction data
        self.sample_transactions = pd.DataFrame({
            'transaction_id': [f'T{i:03d}' for i in range(1, 101)],
            'timestamp': [datetime.now() - timedelta(hours=i % 24) for i in range(1, 101)],
            'card_number': [f'{i:04d}****{i:04d}' for i in range(1, 101)],
            'amount': np.random.uniform(10, 10000, 100),
            'merchant_id': [f'M{i:03d}' for i in range(1, 21)] * 5,
            'merchant_name': [f'Merchant {chr(65 + i % 20)}' for i in range(1, 101)],
            'merchant_category': ['Electronics', 'Dining', 'Travel', 'Retail', 'Entertainment'] * 20,
            'country': ['USA', 'Canada', 'UK', 'Mexico', 'Brazil', 'France', 'Germany', 'Japan', 'Australia', 'China'] * 10,
            'city': [f'City {i}' for i in range(1, 101)],
            'latitude': np.random.uniform(20, 60, 100),
            'longitude': np.random.uniform(-150, 150, 100),
            'is_fraud': [i % 10 == 0 for i in range(1, 101)]  # 10% fraud rate
        })
        
        # Create sample fraud by country data
        self.sample_fraud_by_country = pd.DataFrame({
            'country': ['USA', 'Canada', 'UK', 'Mexico', 'Brazil', 'France', 'Germany', 'Japan', 'Australia', 'China'],
            'fraud_count': [5, 3, 2, 1, 1, 1, 1, 1, 1, 1],
            'total_amount': [25000, 15000, 10000, 5000, 5000, 5000, 5000, 5000, 5000, 5000],
            'latitude': [37.0902, 56.1304, 51.5074, 19.4326, -15.7801, 46.2276, 51.1657, 36.2048, -25.2744, 35.8617],
            'longitude': [-95.7129, -106.3468, -0.1278, -99.1332, -47.9292, 2.2137, 10.4515, 138.2529, 133.7751, 104.1954]
        })
        
        # Create sample fraud by category data
        self.sample_fraud_by_category = pd.DataFrame({
            'merchant_category': ['Electronics', 'Travel', 'Dining', 'Retail', 'Entertainment'],
            'fraud_count': [5, 3, 2, 1, 1],
            'total_amount': [25000, 15000, 10000, 5000, 5000]
        })
        
        # Create sample fraud by hour data
        self.sample_fraud_by_hour = pd.DataFrame({
            'hour': list(range(24)),
            'fraud_count': [1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        })
    
    @patch('dashboard.streamlit_app.psycopg2.connect')
    def test_connect_to_database_integration(self, mock_connect):
        """Test database connection function"""
        # Set up the mock
        mock_connect.return_value = self.mock_db
        
        # Call the function
        with patch.dict('os.environ', {
            'DB_HOST': 'test_host',
            'DB_NAME': 'test_db',
            'DB_USER': 'test_user',
            'DB_PASSWORD': 'test_password',
            'DB_PORT': '5432'
        }):
            conn = connect_to_database()
        
        # Verify the connection
        self.assertEqual(conn, self.mock_db)
        mock_connect.assert_called_once_with(
            host='test_host',
            database='test_db',
            user='test_user',
            password='test_password',
            port='5432'
        )
    
    def test_load_transactions_integration(self):
        """Test loading transactions from the database"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.transaction_id, row.timestamp, row.card_number, row.amount,
             row.merchant_id, row.merchant_name, row.merchant_category,
             row.country, row.city, row.latitude, row.longitude, row.is_fraud)
            for _, row in self.sample_transactions.iterrows()
        ]
        mock_cursor.description = [
            ('transaction_id',), ('timestamp',), ('card_number',), ('amount',),
            ('merchant_id',), ('merchant_name',), ('merchant_category',),
            ('country',), ('city',), ('latitude',), ('longitude',), ('is_fraud',)
        ]
        
        # Call the function
        result = load_transactions(self.mock_db)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('FROM', query)
        self.assertIn('ORDER BY timestamp DESC', query)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 100)  # All transactions
        self.assertEqual(sum(result['is_fraud']), 10)  # 10 fraud transactions
    
    def test_load_fraud_by_country_integration(self):
        """Test loading fraud by country data from the database"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.country, row.fraud_count, row.total_amount, row.latitude, row.longitude)
            for _, row in self.sample_fraud_by_country.iterrows()
        ]
        mock_cursor.description = [
            ('country',), ('fraud_count',), ('total_amount',), ('latitude',), ('longitude',)
        ]
        
        # Call the function
        result = load_fraud_by_country(self.mock_db)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('GROUP BY country', query)
        self.assertIn('ORDER BY fraud_count DESC', query)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 10)  # 10 countries
        self.assertEqual(result['fraud_count'].sum(), 17)  # Total fraud count
    
    def test_load_fraud_by_category_integration(self):
        """Test loading fraud by category data from the database"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.merchant_category, row.fraud_count, row.total_amount)
            for _, row in self.sample_fraud_by_category.iterrows()
        ]
        mock_cursor.description = [
            ('merchant_category',), ('fraud_count',), ('total_amount',)
        ]
        
        # Call the function
        result = load_fraud_by_category(self.mock_db)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('GROUP BY merchant_category', query)
        self.assertIn('ORDER BY fraud_count DESC', query)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)  # 5 categories
        self.assertEqual(result['fraud_count'].sum(), 12)  # Total fraud count
    
    def test_load_fraud_by_hour_integration(self):
        """Test loading fraud by hour data from the database"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.hour, row.fraud_count)
            for _, row in self.sample_fraud_by_hour.iterrows()
        ]
        mock_cursor.description = [
            ('hour',), ('fraud_count',)
        ]
        
        # Call the function
        result = load_fraud_by_hour(self.mock_db)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('EXTRACT(HOUR FROM timestamp)', query)
        self.assertIn('GROUP BY hour', query)
        self.assertIn('ORDER BY hour', query)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 24)  # 24 hours
        self.assertEqual(result['fraud_count'].sum(), 18)  # Total fraud count
    
    def test_filter_data_by_time_integration(self):
        """Test filtering data by time range"""
        # Define time range
        start_time = datetime.now() - timedelta(hours=12)
        end_time = datetime.now()
        
        # Call the function
        result = filter_data_by_time(self.sample_transactions, start_time, end_time)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(len(result) > 0)  # Should have some data
        self.assertTrue(len(result) < 100)  # Should be filtered
        
        # Check that all timestamps are within the range
        self.assertTrue(all(result['timestamp'] >= start_time))
        self.assertTrue(all(result['timestamp'] <= end_time))
    
    def test_filter_data_by_merchant_integration(self):
        """Test filtering data by merchant"""
        # Define merchant filter
        merchant = 'Merchant A'
        
        # Call the function
        result = filter_data_by_merchant(self.sample_transactions, merchant)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(len(result) > 0)  # Should have some data
        self.assertTrue(len(result) < 100)  # Should be filtered
        
        # Check that all rows have the selected merchant
        self.assertTrue(all(result['merchant_name'] == merchant))
    
    def test_filter_data_by_country_integration(self):
        """Test filtering data by country"""
        # Define country filter
        country = 'USA'
        
        # Call the function
        result = filter_data_by_country(self.sample_transactions, country)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(len(result) > 0)  # Should have some data
        self.assertTrue(len(result) < 100)  # Should be filtered
        
        # Check that all rows have the selected country
        self.assertTrue(all(result['country'] == country))
    
    def test_filter_data_by_amount_integration(self):
        """Test filtering data by amount range"""
        # Define amount range
        min_amount = 1000
        max_amount = 5000
        
        # Call the function
        result = filter_data_by_amount(self.sample_transactions, min_amount, max_amount)
        
        # Verify the result
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(len(result) > 0)  # Should have some data
        self.assertTrue(len(result) < 100)  # Should be filtered
        
        # Check that all amounts are within the range
        self.assertTrue(all(result['amount'] >= min_amount))
        self.assertTrue(all(result['amount'] <= max_amount))
    
    @patch('streamlit.metric')
    def test_display_metrics_integration(self, mock_metric):
        """Test displaying metrics"""
        # Call the function
        display_metrics(self.sample_transactions)
        
        # Verify that metrics were displayed
        self.assertEqual(mock_metric.call_count, 4)  # 4 metrics
        
        # Check the metric values
        metric_names = [call.args[0] for call in mock_metric.call_args_list]
        self.assertIn('Total Transactions', metric_names)
        self.assertIn('Fraud Transactions', metric_names)
        self.assertIn('Fraud Rate', metric_names)
        self.assertIn('Total Fraud Amount', metric_names)
    
    @patch('streamlit.pydeck_chart')
    def test_display_map_integration(self, mock_pydeck_chart):
        """Test displaying the map"""
        # Call the function
        display_map(self.sample_fraud_by_country)
        
        # Verify that the map was displayed
        mock_pydeck_chart.assert_called_once()
    
    @patch('streamlit.line_chart')
    def test_display_fraud_trends_integration(self, mock_line_chart):
        """Test displaying fraud trends"""
        # Call the function
        display_fraud_trends(self.sample_fraud_by_hour)
        
        # Verify that the chart was displayed
        mock_line_chart.assert_called_once()
    
    @patch('streamlit.bar_chart')
    def test_display_fraud_by_category_integration(self, mock_bar_chart):
        """Test displaying fraud by category"""
        # Call the function
        display_fraud_by_category(self.sample_fraud_by_category)
        
        # Verify that the chart was displayed
        mock_bar_chart.assert_called_once()
    
    @patch('streamlit.bar_chart')
    def test_display_fraud_by_country_integration(self, mock_bar_chart):
        """Test displaying fraud by country"""
        # Call the function
        display_fraud_by_country(self.sample_fraud_by_country)
        
        # Verify that the chart was displayed
        mock_bar_chart.assert_called_once()
    
    @patch('dashboard.streamlit_app.connect_to_database')
    @patch('dashboard.streamlit_app.load_transactions')
    @patch('dashboard.streamlit_app.load_fraud_by_country')
    @patch('dashboard.streamlit_app.load_fraud_by_category')
    @patch('dashboard.streamlit_app.load_fraud_by_hour')
    def test_data_loading_integration(self, mock_load_hour, mock_load_category, 
                                     mock_load_country, mock_load_transactions, 
                                     mock_connect):
        """Test the integration of data loading functions"""
        # Set up the mocks
        mock_connect.return_value = self.mock_db
        mock_load_transactions.return_value = self.sample_transactions
        mock_load_country.return_value = self.sample_fraud_by_country
        mock_load_category.return_value = self.sample_fraud_by_category
        mock_load_hour.return_value = self.sample_fraud_by_hour
        
        # Call the functions in sequence as they would be called in the app
        conn = connect_to_database()
        transactions = load_transactions(conn)
        fraud_by_country = load_fraud_by_country(conn)
        fraud_by_category = load_fraud_by_category(conn)
        fraud_by_hour = load_fraud_by_hour(conn)
        
        # Verify the results
        self.assertEqual(conn, self.mock_db)
        self.assertEqual(transactions.equals(self.sample_transactions), True)
        self.assertEqual(fraud_by_country.equals(self.sample_fraud_by_country), True)
        self.assertEqual(fraud_by_category.equals(self.sample_fraud_by_category), True)
        self.assertEqual(fraud_by_hour.equals(self.sample_fraud_by_hour), True)
    
    def test_filtering_chain_integration(self):
        """Test chaining multiple filters together"""
        # Define filter parameters
        start_time = datetime.now() - timedelta(hours=12)
        end_time = datetime.now()
        country = 'USA'
        min_amount = 1000
        max_amount = 5000
        
        # Apply filters in sequence
        filtered_data = filter_data_by_time(self.sample_transactions, start_time, end_time)
        filtered_data = filter_data_by_country(filtered_data, country)
        filtered_data = filter_data_by_amount(filtered_data, min_amount, max_amount)
        
        # Verify the result
        self.assertIsInstance(filtered_data, pd.DataFrame)
        
        # Check that all filters were applied correctly
        if not filtered_data.empty:
            self.assertTrue(all(filtered_data['timestamp'] >= start_time))
            self.assertTrue(all(filtered_data['timestamp'] <= end_time))
            self.assertTrue(all(filtered_data['country'] == country))
            self.assertTrue(all(filtered_data['amount'] >= min_amount))
            self.assertTrue(all(filtered_data['amount'] <= max_amount))


# Main test runner
if __name__ == '__main__':
    unittest.main()