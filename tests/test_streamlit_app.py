#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Streamlit Dashboard

This module contains unit tests for the Streamlit dashboard application
that displays credit card fraud detection results.
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

# Import the dashboard components
# Note: We'll patch streamlit since it can't be directly tested
with patch('streamlit.sidebar'):
    with patch('streamlit.title'):
        from dashboard.streamlit_app import (
            get_db_connection,
            load_data,
            load_fraud_data,
            load_fraud_by_country,
            load_fraud_by_merchant_category,
            load_fraud_hourly,
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


class TestDashboardFunctions(unittest.TestCase):
    """Test cases for the Streamlit dashboard functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock database connection
        self.mock_db = MagicMock()
        
        # Create sample transaction data
        self.sample_transactions = pd.DataFrame({
            'transaction_id': ['T001', 'T002', 'T003', 'T004', 'T005'],
            'timestamp': [
                datetime.now() - timedelta(hours=1),
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=3),
                datetime.now() - timedelta(hours=4),
                datetime.now() - timedelta(hours=5)
            ],
            'card_number': ['1234****5678'] * 5,
            'amount': [100.0, 200.0, 5000.0, 150.0, 6000.0],
            'merchant_id': ['M001', 'M002', 'M003', 'M001', 'M002'],
            'merchant_name': ['Merchant A', 'Merchant B', 'Merchant C', 'Merchant A', 'Merchant B'],
            'merchant_category': ['Electronics', 'Dining', 'Travel', 'Electronics', 'Dining'],
            'country': ['USA', 'Canada', 'UK', 'USA', 'Mexico'],
            'city': ['New York', 'Toronto', 'London', 'Los Angeles', 'Mexico City'],
            'latitude': [40.7128, 43.6532, 51.5074, 34.0522, 19.4326],
            'longitude': [-74.0060, -79.3832, -0.1278, -118.2437, -99.1332],
            'is_fraud': [False, False, True, False, True]
        })
        
        # Create sample fraud data by country
        self.sample_fraud_by_country = pd.DataFrame({
            'country': ['UK', 'Mexico', 'Brazil'],
            'fraud_count': [1, 1, 0],
            'total_amount': [5000.0, 6000.0, 0.0]
        })
        
        # Create sample fraud data by merchant category
        self.sample_fraud_by_category = pd.DataFrame({
            'merchant_category': ['Travel', 'Dining', 'Electronics'],
            'fraud_count': [1, 1, 0],
            'total_amount': [5000.0, 6000.0, 0.0]
        })
        
        # Create sample fraud data by hour
        self.sample_fraud_hourly = pd.DataFrame({
            'hour': list(range(24)),
            'fraud_count': [0] * 24,
            'total_amount': [0.0] * 24
        })
        # Set some values for testing
        self.sample_fraud_hourly.loc[3, 'fraud_count'] = 1
        self.sample_fraud_hourly.loc[3, 'total_amount'] = 5000.0
        self.sample_fraud_hourly.loc[5, 'fraud_count'] = 1
        self.sample_fraud_hourly.loc[5, 'total_amount'] = 6000.0
    
    @patch('dashboard.streamlit_app.psycopg2.connect')
    def test_get_db_connection(self, mock_connect):
        """Test database connection function"""
        # Set up the mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Call the function with test parameters
        conn = get_db_connection(
            host='test-host',
            database='test-db',
            user='test-user',
            password='test-pass'
        )
        
        # Verify the connection was established with the right parameters
        mock_connect.assert_called_once_with(
            host='test-host',
            database='test-db',
            user='test-user',
            password='test-pass'
        )
        self.assertEqual(conn, mock_conn)
    
    def test_load_data(self):
        """Test loading transaction data"""
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
        result = load_data(self.mock_db, limit=10)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('FROM transactions', query)
        self.assertIn('LIMIT 10', query)
        
        # Verify the result
        self.assertEqual(len(result), 5)
        self.assertListEqual(list(result.columns), [
            'transaction_id', 'timestamp', 'card_number', 'amount',
            'merchant_id', 'merchant_name', 'merchant_category',
            'country', 'city', 'latitude', 'longitude', 'is_fraud'
        ])
    
    def test_load_fraud_data(self):
        """Test loading fraud data"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Filter the sample data for fraud transactions
        fraud_data = self.sample_transactions[self.sample_transactions['is_fraud']]
        
        # Configure the mock to return our sample fraud data
        mock_cursor.fetchall.return_value = [
            (row.transaction_id, row.timestamp, row.card_number, row.amount,
             row.merchant_id, row.merchant_name, row.merchant_category,
             row.country, row.city, row.latitude, row.longitude, row.is_fraud)
            for _, row in fraud_data.iterrows()
        ]
        mock_cursor.description = [
            ('transaction_id',), ('timestamp',), ('card_number',), ('amount',),
            ('merchant_id',), ('merchant_name',), ('merchant_category',),
            ('country',), ('city',), ('latitude',), ('longitude',), ('is_fraud',)
        ]
        
        # Call the function
        result = load_fraud_data(self.mock_db, limit=10)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('FROM fraudulent_transactions', query)
        self.assertIn('LIMIT 10', query)
        
        # Verify the result
        self.assertEqual(len(result), 2)  # We have 2 fraud transactions in our sample
        self.assertTrue(all(result['is_fraud']))  # All should be fraud
    
    def test_load_fraud_by_country(self):
        """Test loading fraud data by country"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.country, row.fraud_count, row.total_amount)
            for _, row in self.sample_fraud_by_country.iterrows()
        ]
        mock_cursor.description = [
            ('country',), ('fraud_count',), ('total_amount',)
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
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result.columns), ['country', 'fraud_count', 'total_amount'])
    
    def test_load_fraud_by_merchant_category(self):
        """Test loading fraud data by merchant category"""
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
        result = load_fraud_by_merchant_category(self.mock_db)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('GROUP BY merchant_category', query)
        self.assertIn('ORDER BY fraud_count DESC', query)
        
        # Verify the result
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result.columns), ['merchant_category', 'fraud_count', 'total_amount'])
    
    def test_load_fraud_hourly(self):
        """Test loading hourly fraud data"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.hour, row.fraud_count, row.total_amount)
            for _, row in self.sample_fraud_hourly.iterrows()
        ]
        mock_cursor.description = [
            ('hour',), ('fraud_count',), ('total_amount',)
        ]
        
        # Call the function
        result = load_fraud_hourly(self.mock_db)
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('EXTRACT(HOUR FROM timestamp)', query)
        self.assertIn('GROUP BY hour', query)
        self.assertIn('ORDER BY hour', query)
        
        # Verify the result
        self.assertEqual(len(result), 24)  # 24 hours in a day
        self.assertListEqual(list(result.columns), ['hour', 'fraud_count', 'total_amount'])
    
    def test_filter_data_by_time(self):
        """Test filtering data by time range"""
        # Define a time range
        start_time = datetime.now() - timedelta(hours=4)
        end_time = datetime.now()
        
        # Call the function
        result = filter_data_by_time(self.sample_transactions, start_time, end_time)
        
        # Verify the result
        self.assertEqual(len(result), 4)  # 4 transactions within the last 4 hours
        self.assertTrue(all(result['timestamp'] >= start_time))
        self.assertTrue(all(result['timestamp'] <= end_time))
    
    def test_filter_data_by_merchant(self):
        """Test filtering data by merchant"""
        # Call the function with a specific merchant
        result = filter_data_by_merchant(self.sample_transactions, 'Merchant A')
        
        # Verify the result
        self.assertEqual(len(result), 2)  # 2 transactions from Merchant A
        self.assertTrue(all(result['merchant_name'] == 'Merchant A'))
        
        # Test with 'All' merchants
        result = filter_data_by_merchant(self.sample_transactions, 'All')
        self.assertEqual(len(result), 5)  # All transactions
    
    def test_filter_data_by_country(self):
        """Test filtering data by country"""
        # Call the function with a specific country
        result = filter_data_by_country(self.sample_transactions, 'USA')
        
        # Verify the result
        self.assertEqual(len(result), 2)  # 2 transactions from USA
        self.assertTrue(all(result['country'] == 'USA'))
        
        # Test with 'All' countries
        result = filter_data_by_country(self.sample_transactions, 'All')
        self.assertEqual(len(result), 5)  # All transactions
    
    def test_filter_data_by_amount(self):
        """Test filtering data by amount range"""
        # Call the function with a specific amount range
        result = filter_data_by_amount(self.sample_transactions, 0, 1000)
        
        # Verify the result
        self.assertEqual(len(result), 3)  # 3 transactions with amount <= 1000
        self.assertTrue(all(result['amount'] >= 0))
        self.assertTrue(all(result['amount'] <= 1000))
    
    @patch('dashboard.streamlit_app.st')
    def test_display_metrics(self, mock_st):
        """Test displaying metrics"""
        # Call the function with our sample data
        display_metrics(self.sample_transactions)
        
        # Verify that streamlit's metric function was called with the right values
        self.assertEqual(mock_st.metric.call_count, 4)  # 4 metrics
        
        # Check the metric values
        # Total Transactions
        mock_st.metric.assert_any_call('Total Transactions', 5)
        
        # Fraud Transactions
        mock_st.metric.assert_any_call('Fraud Transactions', 2)
        
        # Fraud Rate
        mock_st.metric.assert_any_call('Fraud Rate', '40.0%')
        
        # Total Amount
        mock_st.metric.assert_any_call('Total Amount', '$11,450.00')
    
    @patch('dashboard.streamlit_app.st')
    @patch('dashboard.streamlit_app.px')
    def test_display_map(self, mock_px, mock_st):
        """Test displaying the map"""
        # Set up the mock
        mock_fig = MagicMock()
        mock_px.scatter_mapbox.return_value = mock_fig
        
        # Call the function with our sample data
        display_map(self.sample_transactions)
        
        # Verify that plotly express was called with the right parameters
        mock_px.scatter_mapbox.assert_called_once()
        args, kwargs = mock_px.scatter_mapbox.call_args
        
        # Check that the data was passed correctly
        self.assertEqual(kwargs['lat'], 'latitude')
        self.assertEqual(kwargs['lon'], 'longitude')
        self.assertEqual(kwargs['color'], 'is_fraud')
        self.assertEqual(kwargs['hover_name'], 'merchant_name')
        
        # Verify that streamlit's plotly_chart was called
        mock_st.plotly_chart.assert_called_once_with(mock_fig, use_container_width=True)
    
    @patch('dashboard.streamlit_app.st')
    @patch('dashboard.streamlit_app.px')
    def test_display_fraud_trends(self, mock_px, mock_st):
        """Test displaying fraud trends"""
        # Set up the mock
        mock_fig = MagicMock()
        mock_px.bar.return_value = mock_fig
        
        # Call the function with our sample data
        display_fraud_trends(self.sample_fraud_hourly)
        
        # Verify that plotly express was called with the right parameters
        mock_px.bar.assert_called_once()
        args, kwargs = mock_px.bar.call_args
        
        # Check that the data was passed correctly
        self.assertEqual(kwargs['x'], 'hour')
        self.assertEqual(kwargs['y'], 'fraud_count')
        self.assertEqual(kwargs['title'], 'Fraud Transactions by Hour')
        
        # Verify that streamlit's plotly_chart was called
        mock_st.plotly_chart.assert_called_once_with(mock_fig, use_container_width=True)
    
    @patch('dashboard.streamlit_app.st')
    @patch('dashboard.streamlit_app.px')
    def test_display_fraud_by_category(self, mock_px, mock_st):
        """Test displaying fraud by category"""
        # Set up the mock
        mock_fig = MagicMock()
        mock_px.bar.return_value = mock_fig
        
        # Call the function with our sample data
        display_fraud_by_category(self.sample_fraud_by_category)
        
        # Verify that plotly express was called with the right parameters
        mock_px.bar.assert_called_once()
        args, kwargs = mock_px.bar.call_args
        
        # Check that the data was passed correctly
        self.assertEqual(kwargs['x'], 'merchant_category')
        self.assertEqual(kwargs['y'], 'fraud_count')
        self.assertEqual(kwargs['title'], 'Fraud Transactions by Merchant Category')
        
        # Verify that streamlit's plotly_chart was called
        mock_st.plotly_chart.assert_called_once_with(mock_fig, use_container_width=True)
    
    @patch('dashboard.streamlit_app.st')
    @patch('dashboard.streamlit_app.px')
    def test_display_fraud_by_country(self, mock_px, mock_st):
        """Test displaying fraud by country"""
        # Set up the mock
        mock_fig = MagicMock()
        mock_px.bar.return_value = mock_fig
        
        # Call the function with our sample data
        display_fraud_by_country(self.sample_fraud_by_country)
        
        # Verify that plotly express was called with the right parameters
        mock_px.bar.assert_called_once()
        args, kwargs = mock_px.bar.call_args
        
        # Check that the data was passed correctly
        self.assertEqual(kwargs['x'], 'country')
        self.assertEqual(kwargs['y'], 'fraud_count')
        self.assertEqual(kwargs['title'], 'Fraud Transactions by Country')
        
        # Verify that streamlit's plotly_chart was called
        mock_st.plotly_chart.assert_called_once_with(mock_fig, use_container_width=True)


# Main test runner
if __name__ == '__main__':
    unittest.main()