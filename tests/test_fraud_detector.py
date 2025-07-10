#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Fraud Detection Rules

This module contains unit tests for the fraud detection rules implemented in
the FraudDetector class.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the FraudDetector class
from processing.real_time_processor import FraudDetector


class TestFraudDetector(unittest.TestCase):
    """Test cases for the FraudDetector class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock database handler
        self.mock_db = MagicMock()
        
        # Create a FraudDetector instance with the mock database
        self.detector = FraudDetector(self.mock_db)
        
        # Sample transaction data
        self.transaction = {
            'transaction_id': '123456789',
            'timestamp': datetime.now(),
            'card_number': '1234567890123456',
            'amount': 100.0,
            'merchant_id': 'MERCH001',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Retail',
            'country': 'USA',
            'city': 'New York',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'user_id': 'USER001'
        }
    
    def test_high_amount_rule(self):
        """Test the high amount rule"""
        # Test with amount below threshold
        transaction = self.transaction.copy()
        transaction['amount'] = 4999.99
        result = self.detector.check_high_amount(transaction)
        self.assertFalse(result)
        
        # Test with amount at threshold
        transaction['amount'] = 5000.0
        result = self.detector.check_high_amount(transaction)
        self.assertTrue(result)
        
        # Test with amount above threshold
        transaction['amount'] = 5000.01
        result = self.detector.check_high_amount(transaction)
        self.assertTrue(result)
    
    def test_rapid_transactions_rule(self):
        """Test the rapid transactions rule"""
        # Set up mock for recent transactions
        card_number = self.transaction['card_number']
        timestamp = self.transaction['timestamp']
        
        # Test with fewer than threshold transactions
        self.mock_db.get_recent_transactions.return_value = [
            {'timestamp': timestamp - timedelta(seconds=10)},
            {'timestamp': timestamp - timedelta(seconds=20)}
        ]
        result = self.detector.check_rapid_transactions(self.transaction)
        self.assertFalse(result)
        
        # Test with exactly threshold transactions
        self.mock_db.get_recent_transactions.return_value = [
            {'timestamp': timestamp - timedelta(seconds=10)},
            {'timestamp': timestamp - timedelta(seconds=20)},
            {'timestamp': timestamp - timedelta(seconds=25)}
        ]
        result = self.detector.check_rapid_transactions(self.transaction)
        self.assertTrue(result)
        
        # Test with more than threshold transactions
        self.mock_db.get_recent_transactions.return_value = [
            {'timestamp': timestamp - timedelta(seconds=5)},
            {'timestamp': timestamp - timedelta(seconds=10)},
            {'timestamp': timestamp - timedelta(seconds=15)},
            {'timestamp': timestamp - timedelta(seconds=20)}
        ]
        result = self.detector.check_rapid_transactions(self.transaction)
        self.assertTrue(result)
        
        # Verify the mock was called correctly
        self.mock_db.get_recent_transactions.assert_called_with(
            card_number, timestamp - timedelta(seconds=30)
        )
    
    def test_unusual_location_rule(self):
        """Test the unusual location rule"""
        # Set up mock for recent transactions
        card_number = self.transaction['card_number']
        timestamp = self.transaction['timestamp']
        
        # Test with no previous transactions (should not flag)
        self.mock_db.get_card_countries.return_value = []
        result = self.detector.check_unusual_location(self.transaction)
        self.assertFalse(result)
        
        # Test with same country (should not flag)
        self.mock_db.get_card_countries.return_value = ['USA', 'USA', 'USA']
        result = self.detector.check_unusual_location(self.transaction)
        self.assertFalse(result)
        
        # Test with different country (should flag)
        self.mock_db.get_card_countries.return_value = ['Canada', 'Canada', 'Canada']
        result = self.detector.check_unusual_location(self.transaction)
        self.assertTrue(result)
        
        # Test with mixed countries but current is common
        self.mock_db.get_card_countries.return_value = ['USA', 'USA', 'Canada']
        result = self.detector.check_unusual_location(self.transaction)
        self.assertFalse(result)
        
        # Test with mixed countries and current is uncommon
        self.transaction['country'] = 'Brazil'
        self.mock_db.get_card_countries.return_value = ['USA', 'USA', 'Canada']
        result = self.detector.check_unusual_location(self.transaction)
        self.assertTrue(result)
    
    def test_duplicate_transaction_rule(self):
        """Test the duplicate transaction rule"""
        # Set up mock for recent transactions
        card_number = self.transaction['card_number']
        merchant_id = self.transaction['merchant_id']
        amount = self.transaction['amount']
        timestamp = self.transaction['timestamp']
        
        # Test with no duplicates
        self.mock_db.get_similar_transactions.return_value = []
        result = self.detector.check_duplicate_transaction(self.transaction)
        self.assertFalse(result)
        
        # Test with a duplicate
        self.mock_db.get_similar_transactions.return_value = [
            {'transaction_id': '987654321'}
        ]
        result = self.detector.check_duplicate_transaction(self.transaction)
        self.assertTrue(result)
        
        # Verify the mock was called correctly
        self.mock_db.get_similar_transactions.assert_called_with(
            card_number, merchant_id, amount, timestamp - timedelta(minutes=5)
        )
    
    def test_late_night_spending_rule(self):
        """Test the late night spending rule"""
        # Test with transaction during the day
        transaction = self.transaction.copy()
        transaction['timestamp'] = datetime(2023, 1, 1, 14, 0, 0)  # 2 PM
        result = self.detector.check_late_night_spending(transaction)
        self.assertFalse(result)
        
        # Test with transaction late at night
        transaction['timestamp'] = datetime(2023, 1, 1, 1, 0, 0)  # 1 AM
        result = self.detector.check_late_night_spending(transaction)
        self.assertTrue(result)
        
        # Test boundary conditions
        transaction['timestamp'] = datetime(2023, 1, 1, 0, 0, 0)  # 12 AM
        result = self.detector.check_late_night_spending(transaction)
        self.assertTrue(result)
        
        transaction['timestamp'] = datetime(2023, 1, 1, 5, 0, 0)  # 5 AM
        result = self.detector.check_late_night_spending(transaction)
        self.assertTrue(result)
        
        transaction['timestamp'] = datetime(2023, 1, 1, 6, 0, 0)  # 6 AM
        result = self.detector.check_late_night_spending(transaction)
        self.assertFalse(result)
    
    def test_detect_fraud(self):
        """Test the main detect_fraud method"""
        # Mock the individual rule methods
        with patch.object(self.detector, 'check_high_amount', return_value=False) as mock_high_amount, \
             patch.object(self.detector, 'check_rapid_transactions', return_value=False) as mock_rapid, \
             patch.object(self.detector, 'check_unusual_location', return_value=False) as mock_location, \
             patch.object(self.detector, 'check_duplicate_transaction', return_value=False) as mock_duplicate, \
             patch.object(self.detector, 'check_late_night_spending', return_value=False) as mock_late_night:
            
            # Test with no rules triggered
            result = self.detector.detect_fraud(self.transaction)
            self.assertEqual(result, {'is_fraud': False, 'fraud_type': None, 'fraud_score': 0.0})
            
            # Test with one rule triggered
            mock_high_amount.return_value = True
            result = self.detector.detect_fraud(self.transaction)
            self.assertEqual(result, {'is_fraud': True, 'fraud_type': 'High Amount', 'fraud_score': 0.7})
            
            # Test with multiple rules triggered
            mock_rapid.return_value = True
            result = self.detector.detect_fraud(self.transaction)
            self.assertEqual(result, {'is_fraud': True, 'fraud_type': 'High Amount, Rapid Transactions', 'fraud_score': 0.85})
            
            # Test with all rules triggered
            mock_location.return_value = True
            mock_duplicate.return_value = True
            mock_late_night.return_value = True
            result = self.detector.detect_fraud(self.transaction)
            self.assertEqual(
                result, 
                {'is_fraud': True, 'fraud_type': 'High Amount, Rapid Transactions, Unusual Location, Duplicate Transaction, Late Night Spending', 'fraud_score': 0.99}
            )


if __name__ == '__main__':
    unittest.main()