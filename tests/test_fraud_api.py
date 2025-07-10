#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Fraud API

This module contains unit tests for the FastAPI endpoints in fraud_api.py
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from fastapi.testclient import TestClient

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the FastAPI app
from api.fraud_api import app, get_db_connection


class TestFraudAPI(unittest.TestCase):
    """Test cases for the Fraud API endpoints"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a test client
        self.client = TestClient(app)
        
        # Mock the API key
        self.env_patcher = patch.dict('os.environ', {
            'API_KEY': 'test_api_key'
        })
        self.env_patcher.start()
        
        # Create sample dataframes for mock returns
        # Recent frauds
        self.recent_frauds = pd.DataFrame({
            'transaction_id': [f'T00{i}' for i in range(1, 11)],
            'timestamp': [datetime.now() - timedelta(hours=i) for i in range(10)],
            'card_number': [f'1234****{i}{i}{i}{i}' for i in range(1, 11)],
            'amount': [5000.0 - i * 100 for i in range(10)],
            'merchant_name': [f'Merchant {i}' for i in range(1, 11)],
            'merchant_category': ['Electronics', 'Travel', 'Retail', 'Dining', 'Entertainment'] * 2,
            'country': ['USA', 'Canada', 'UK', 'Germany', 'France'] * 2,
            'city': [f'City {i}' for i in range(1, 11)],
            'fraud_type': ['High Amount', 'Unusual Location', 'Rapid Transactions', 'Duplicate Transaction', 'Late Night Spending'] * 2,
            'fraud_score': [0.9 - i * 0.05 for i in range(10)]
        })
        
        # Fraud stats
        self.fraud_stats = pd.DataFrame({
            'total_frauds': [58],
            'total_amount': [87000.0],
            'avg_amount': [1500.0],
            'max_amount': [5000.0],
            'min_amount': [100.0],
            'avg_fraud_score': [0.75],
            'affected_cards': [30],
            'affected_categories': [5]
        })
        
        # Frauds by country
        self.frauds_by_country = pd.DataFrame({
            'country': ['USA', 'Canada', 'UK', 'Germany', 'France'],
            'fraud_count': [25, 15, 10, 5, 3],
            'total_amount': [37500.0, 22500.0, 15000.0, 7500.0, 4500.0],
            'avg_fraud_score': [0.75, 0.8, 0.85, 0.7, 0.65]
        })
        
        # Frauds by category
        self.frauds_by_category = pd.DataFrame({
            'merchant_category': ['Electronics', 'Travel', 'Retail', 'Dining', 'Entertainment'],
            'fraud_count': [20, 15, 10, 5, 3],
            'total_amount': [30000.0, 22500.0, 15000.0, 7500.0, 4500.0],
            'avg_fraud_score': [0.8, 0.75, 0.7, 0.65, 0.6]
        })
        
        # Mock the database connection
        self.mock_db = MagicMock()
        self.db_patcher = patch('api.fraud_api.get_db_connection', return_value=self.mock_db)
        self.mock_get_db = self.db_patcher.start()
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
        self.db_patcher.stop()
    
    def test_health_check(self):
        """Test the health check endpoint"""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "healthy", "message": "API is operational"})
    
    def test_recent_frauds_with_valid_api_key(self):
        """Test the recent frauds endpoint with a valid API key"""
        # Set up the mock to return our sample dataframe
        self.mock_db.execute_query.return_value = self.recent_frauds
        
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/frauds/recent",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 10)  # Should return 10 frauds
        self.assertIn('transaction_id', data[0])  # Check for expected fields
        self.assertIn('amount', data[0])
        self.assertIn('fraud_type', data[0])
        
        # Verify the mock was called with the correct query
        self.mock_db.execute_query.assert_called_once()
        self.assertIn('SELECT', self.mock_db.execute_query.call_args[0][0].upper())
        self.assertIn('FRAUDULENT_TRANSACTIONS', self.mock_db.execute_query.call_args[0][0].upper())
    
    def test_recent_frauds_with_invalid_api_key(self):
        """Test the recent frauds endpoint with an invalid API key"""
        response = self.client.get(
            "/api/v1/frauds/recent",
            headers={"X-API-Key": "invalid_key"}
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "Invalid API key"})
    
    def test_recent_frauds_without_api_key(self):
        """Test the recent frauds endpoint without an API key"""
        response = self.client.get("/api/v1/frauds/recent")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "API key required"})
    
    def test_fraud_stats_with_valid_api_key(self):
        """Test the fraud stats endpoint with a valid API key"""
        # Set up the mock to return our sample dataframe
        self.mock_db.execute_query.return_value = self.fraud_stats
        
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/frauds/stats",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('total_frauds', data)
        self.assertIn('total_amount', data)
        self.assertIn('avg_fraud_score', data)
        self.assertEqual(data['total_frauds'], 58)
        self.assertEqual(data['total_amount'], 87000.0)
        
        # Verify the mock was called with the correct query
        self.mock_db.execute_query.assert_called_once()
        self.assertIn('SELECT', self.mock_db.execute_query.call_args[0][0].upper())
        self.assertIn('COUNT', self.mock_db.execute_query.call_args[0][0].upper())
    
    def test_frauds_by_country_with_valid_api_key(self):
        """Test the frauds by country endpoint with a valid API key"""
        # Set up the mock to return our sample dataframe
        self.mock_db.execute_query.return_value = self.frauds_by_country
        
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/frauds/by-country",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 5)  # Should return 5 countries
        self.assertIn('country', data[0])
        self.assertIn('fraud_count', data[0])
        self.assertIn('total_amount', data[0])
        
        # Verify the mock was called with the correct query
        self.mock_db.execute_query.assert_called_once()
        self.assertIn('GROUP BY', self.mock_db.execute_query.call_args[0][0].upper())
        self.assertIn('COUNTRY', self.mock_db.execute_query.call_args[0][0].upper())
    
    def test_frauds_by_category_with_valid_api_key(self):
        """Test the frauds by category endpoint with a valid API key"""
        # Set up the mock to return our sample dataframe
        self.mock_db.execute_query.return_value = self.frauds_by_category
        
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/frauds/by-category",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 5)  # Should return 5 categories
        self.assertIn('merchant_category', data[0])
        self.assertIn('fraud_count', data[0])
        self.assertIn('total_amount', data[0])
        
        # Verify the mock was called with the correct query
        self.mock_db.execute_query.assert_called_once()
        self.assertIn('GROUP BY', self.mock_db.execute_query.call_args[0][0].upper())
        self.assertIn('MERCHANT_CATEGORY', self.mock_db.execute_query.call_args[0][0].upper())
    
    def test_get_db_connection(self):
        """Test the get_db_connection function"""
        # Mock the PostgreSQL connection
        with patch('api.fraud_api.psycopg2.connect') as mock_connect:
            # Set up the mock to return a connection object
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            # Mock environment variables
            with patch.dict('os.environ', {
                'DB_HOST': 'test_host',
                'DB_PORT': '5432',
                'DB_NAME': 'test_db',
                'DB_USER': 'test_user',
                'DB_PASSWORD': 'test_password'
            }):
                # Call the function
                connection = get_db_connection()
                
                # Verify the connection was created with the correct parameters
                mock_connect.assert_called_once_with(
                    host='test_host',
                    port='5432',
                    dbname='test_db',
                    user='test_user',
                    password='test_password'
                )
                
                # Verify the connection was returned
                self.assertEqual(connection, mock_connection)


if __name__ == '__main__':
    unittest.main()