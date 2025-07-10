#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Data Export Functionality

This module contains unit tests for the DataExporter class that handles
exporting fraud data to various formats.
"""

import os
import sys
import json
import unittest
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the DataExporter class
from utils.data_export import DataExporter


class TestDataExporter(unittest.TestCase):
    """Test cases for the DataExporter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary directory for exports
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a mock database handler
        self.mock_db = MagicMock()
        
        # Create sample dataframes for mock returns
        # Transactions data
        self.transactions_df = pd.DataFrame({
            'transaction_id': ['T001', 'T002', 'T003'],
            'timestamp': [datetime.now() - timedelta(hours=i) for i in range(3)],
            'card_number': ['1234****5678', '5678****1234', '9876****5432'],
            'amount': [1000.0, 5000.0, 2500.0],
            'merchant_name': ['Online Store', 'Electronics Shop', 'Travel Agency'],
            'merchant_category': ['Retail', 'Electronics', 'Travel'],
            'country': ['USA', 'Canada', 'UK'],
            'city': ['New York', 'Toronto', 'London'],
            'latitude': [40.7128, 43.6532, 51.5074],
            'longitude': [-74.0060, -79.3832, -0.1278],
            'fraud_type': ['High Amount', 'Unusual Location', 'Rapid Transactions'],
            'fraud_score': [0.7, 0.8, 0.9]
        })
        
        # Daily summary data
        self.daily_df = pd.DataFrame({
            'date': [datetime.now().date() - timedelta(days=i) for i in range(3)],
            'fraud_count': [10, 15, 8],
            'total_amount': [15000.0, 22500.0, 12000.0],
            'avg_amount': [1500.0, 1500.0, 1500.0],
            'avg_fraud_score': [0.75, 0.8, 0.7]
        })
        
        # Category summary data
        self.category_df = pd.DataFrame({
            'merchant_category': ['Electronics', 'Travel', 'Retail'],
            'fraud_count': [20, 15, 10],
            'total_amount': [30000.0, 22500.0, 15000.0],
            'avg_fraud_score': [0.8, 0.75, 0.7]
        })
        
        # Country summary data
        self.country_df = pd.DataFrame({
            'country': ['USA', 'Canada', 'UK'],
            'fraud_count': [25, 15, 10],
            'total_amount': [37500.0, 22500.0, 15000.0],
            'avg_fraud_score': [0.75, 0.8, 0.85]
        })
        
        # Patch the EXPORT_DIR in the data_export module
        self.patcher = patch('utils.data_export.EXPORT_DIR', self.temp_dir)
        self.patcher.start()
        
        # Create a DataExporter instance with the mock database
        self.exporter = DataExporter(self.mock_db)
    
    def tearDown(self):
        """Clean up after tests"""
        # Stop the patcher
        self.patcher.stop()
        
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_get_fraud_data(self):
        """Test the _get_fraud_data method"""
        # Set up the mock to return our sample dataframe
        self.mock_db.execute_query.return_value = self.transactions_df
        
        # Call the method
        start_date = datetime.now().date() - timedelta(days=7)
        end_date = datetime.now().date()
        result = self.exporter._get_fraud_data(start_date, end_date)
        
        # Verify the result
        pd.testing.assert_frame_equal(result, self.transactions_df)
        
        # Verify the mock was called correctly
        self.mock_db.execute_query.assert_called_once()
        args, kwargs = self.mock_db.execute_query.call_args
        self.assertIn('SELECT', args[0])  # Query should contain SELECT
        self.assertEqual(len(args[1]), 2)  # Should have two parameters (start and end date)
    
    def test_get_fraud_summary(self):
        """Test the _get_fraud_summary method"""
        # Set up the mock to return our sample dataframes for different queries
        self.mock_db.execute_query.side_effect = [
            self.daily_df,
            self.category_df,
            self.country_df
        ]
        
        # Call the method
        start_date = datetime.now().date() - timedelta(days=7)
        end_date = datetime.now().date()
        result = self.exporter._get_fraud_summary(start_date, end_date)
        
        # Verify the result
        self.assertIsInstance(result, dict)
        self.assertIn('daily', result)
        self.assertIn('category', result)
        self.assertIn('country', result)
        pd.testing.assert_frame_equal(result['daily'], self.daily_df)
        pd.testing.assert_frame_equal(result['category'], self.category_df)
        pd.testing.assert_frame_equal(result['country'], self.country_df)
        
        # Verify the mock was called correctly
        self.assertEqual(self.mock_db.execute_query.call_count, 3)
    
    def test_export_to_csv(self):
        """Test the export_to_csv method"""
        # Set up the mocks
        with patch.object(self.exporter, '_get_fraud_data', return_value=self.transactions_df) as mock_get_data, \
             patch.object(self.exporter, '_get_fraud_summary', return_value={
                 'daily': self.daily_df,
                 'category': self.category_df,
                 'country': self.country_df
             }) as mock_get_summary:
            
            # Call the method
            start_date = datetime.now().date() - timedelta(days=7)
            end_date = datetime.now().date()
            result = self.exporter.export_to_csv(start_date, end_date)
            
            # Verify the result
            self.assertIsInstance(result, dict)
            self.assertIn('transactions', result)
            self.assertIn('daily_summary', result)
            self.assertIn('category_summary', result)
            self.assertIn('country_summary', result)
            
            # Check that files were created
            for file_path in result.values():
                self.assertTrue(os.path.exists(file_path))
                self.assertTrue(file_path.endswith('.csv'))
            
            # Verify the mocks were called correctly
            mock_get_data.assert_called_once_with(start_date, end_date)
            mock_get_summary.assert_called_once_with(start_date, end_date)
    
    def test_export_to_excel(self):
        """Test the export_to_excel method"""
        # Set up the mocks
        with patch.object(self.exporter, '_get_fraud_data', return_value=self.transactions_df) as mock_get_data, \
             patch.object(self.exporter, '_get_fraud_summary', return_value={
                 'daily': self.daily_df,
                 'category': self.category_df,
                 'country': self.country_df
             }) as mock_get_summary:
            
            # Call the method
            start_date = datetime.now().date() - timedelta(days=7)
            end_date = datetime.now().date()
            result = self.exporter.export_to_excel(start_date, end_date)
            
            # Verify the result
            self.assertTrue(os.path.exists(result))
            self.assertTrue(result.endswith('.xlsx'))
            
            # Verify the mocks were called correctly
            mock_get_data.assert_called_once_with(start_date, end_date)
            mock_get_summary.assert_called_once_with(start_date, end_date)
    
    def test_export_to_json(self):
        """Test the export_to_json method"""
        # Set up the mocks
        with patch.object(self.exporter, '_get_fraud_data', return_value=self.transactions_df) as mock_get_data, \
             patch.object(self.exporter, '_get_fraud_summary', return_value={
                 'daily': self.daily_df,
                 'category': self.category_df,
                 'country': self.country_df
             }) as mock_get_summary:
            
            # Call the method
            start_date = datetime.now().date() - timedelta(days=7)
            end_date = datetime.now().date()
            result = self.exporter.export_to_json(start_date, end_date)
            
            # Verify the result
            self.assertTrue(os.path.exists(result))
            self.assertTrue(result.endswith('.json'))
            
            # Check that the JSON file contains valid JSON
            with open(result, 'r') as f:
                data = json.load(f)
                self.assertIsInstance(data, dict)
                self.assertIn('metadata', data)
                self.assertIn('transactions', data)
                self.assertIn('summary', data)
                self.assertIn('daily', data['summary'])
                self.assertIn('category', data['summary'])
                self.assertIn('country', data['summary'])
            
            # Verify the mocks were called correctly
            mock_get_data.assert_called_once_with(start_date, end_date)
            mock_get_summary.assert_called_once_with(start_date, end_date)


if __name__ == '__main__':
    unittest.main()