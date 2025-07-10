#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Real-Time Transaction Processor

This module contains unit tests for the TransactionProcessor class and related
components that handle real-time fraud detection.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from datetime import datetime, timedelta
import json
import queue

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the classes to test
from processing.real_time_processor import (
    FraudDetector, 
    DatabaseHandler, 
    TransactionProcessor
)


class TestFraudDetector(unittest.TestCase):
    """Test cases for the FraudDetector class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.detector = FraudDetector()
        
        # Create a sample transaction
        self.transaction = {
            'transaction_id': 'T12345',
            'timestamp': datetime.now(),
            'card_number': '1234****5678',
            'amount': 1000.0,
            'merchant_id': 'M001',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Electronics',
            'country': 'USA',
            'city': 'New York',
            'latitude': 40.7128,
            'longitude': -74.0060
        }
        
        # Create a transaction history for the card
        self.card_history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(days=1),
                'amount': 500.0,
                'country': 'USA',
                'city': 'New York'
            }
        ]
    
    def test_check_high_amount(self):
        """Test the high amount fraud rule"""
        # Test with amount below threshold
        self.transaction['amount'] = 4999.0
        result = self.detector._check_high_amount(self.transaction)
        self.assertFalse(result)
        
        # Test with amount at threshold
        self.transaction['amount'] = 5000.0
        result = self.detector._check_high_amount(self.transaction)
        self.assertTrue(result)
        
        # Test with amount above threshold
        self.transaction['amount'] = 5001.0
        result = self.detector._check_high_amount(self.transaction)
        self.assertTrue(result)
    
    def test_check_rapid_transactions(self):
        """Test the rapid transactions fraud rule"""
        # Test with no recent transactions
        result = self.detector._check_rapid_transactions(self.transaction, [])
        self.assertFalse(result)
        
        # Test with one recent transaction
        recent_history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(seconds=10),
                'amount': 500.0,
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_rapid_transactions(self.transaction, recent_history)
        self.assertFalse(result)
        
        # Test with three recent transactions (should trigger fraud)
        recent_history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(seconds=10),
                'amount': 500.0,
                'country': 'USA',
                'city': 'New York'
            },
            {
                'transaction_id': 'T12343',
                'timestamp': datetime.now() - timedelta(seconds=20),
                'amount': 600.0,
                'country': 'USA',
                'city': 'New York'
            },
            {
                'transaction_id': 'T12342',
                'timestamp': datetime.now() - timedelta(seconds=25),
                'amount': 700.0,
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_rapid_transactions(self.transaction, recent_history)
        self.assertTrue(result)
        
        # Test with three transactions but over a longer period (should not trigger)
        recent_history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(seconds=10),
                'amount': 500.0,
                'country': 'USA',
                'city': 'New York'
            },
            {
                'transaction_id': 'T12343',
                'timestamp': datetime.now() - timedelta(seconds=20),
                'amount': 600.0,
                'country': 'USA',
                'city': 'New York'
            },
            {
                'transaction_id': 'T12342',
                'timestamp': datetime.now() - timedelta(seconds=40),  # Over 30 seconds
                'amount': 700.0,
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_rapid_transactions(self.transaction, recent_history)
        self.assertFalse(result)
    
    def test_check_unusual_location(self):
        """Test the unusual location fraud rule"""
        # Test with same country (should not trigger)
        self.transaction['country'] = 'USA'
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(hours=1),
                'amount': 500.0,
                'country': 'USA',
                'city': 'Los Angeles'
            }
        ]
        result = self.detector._check_unusual_location(self.transaction, history)
        self.assertFalse(result)
        
        # Test with different country (should trigger)
        self.transaction['country'] = 'Canada'
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(hours=1),
                'amount': 500.0,
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_unusual_location(self.transaction, history)
        self.assertTrue(result)
        
        # Test with no history (should not trigger)
        result = self.detector._check_unusual_location(self.transaction, [])
        self.assertFalse(result)
    
    def test_check_duplicate_transaction(self):
        """Test the duplicate transaction fraud rule"""
        # Test with no similar transactions
        self.transaction['amount'] = 1000.0
        self.transaction['merchant_id'] = 'M001'
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(minutes=5),
                'amount': 500.0,
                'merchant_id': 'M002',
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_duplicate_transaction(self.transaction, history)
        self.assertFalse(result)
        
        # Test with similar amount but different merchant
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(minutes=5),
                'amount': 1000.0,
                'merchant_id': 'M002',
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_duplicate_transaction(self.transaction, history)
        self.assertFalse(result)
        
        # Test with similar merchant but different amount
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(minutes=5),
                'amount': 500.0,
                'merchant_id': 'M001',
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_duplicate_transaction(self.transaction, history)
        self.assertFalse(result)
        
        # Test with similar amount and merchant (should trigger)
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(minutes=5),
                'amount': 1000.0,
                'merchant_id': 'M001',
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_duplicate_transaction(self.transaction, history)
        self.assertTrue(result)
        
        # Test with similar transaction but outside time window
        history = [
            {
                'transaction_id': 'T12344',
                'timestamp': datetime.now() - timedelta(minutes=16),  # Over 15 minutes
                'amount': 1000.0,
                'merchant_id': 'M001',
                'country': 'USA',
                'city': 'New York'
            }
        ]
        result = self.detector._check_duplicate_transaction(self.transaction, history)
        self.assertFalse(result)
    
    def test_check_late_night_spending(self):
        """Test the late night spending fraud rule"""
        # Test during daytime (should not trigger)
        daytime = datetime(2023, 1, 1, 14, 0, 0)  # 2 PM
        self.transaction['timestamp'] = daytime
        result = self.detector._check_late_night_spending(self.transaction)
        self.assertFalse(result)
        
        # Test during late night with small amount (should not trigger)
        late_night = datetime(2023, 1, 1, 1, 0, 0)  # 1 AM
        self.transaction['timestamp'] = late_night
        self.transaction['amount'] = 100.0
        result = self.detector._check_late_night_spending(self.transaction)
        self.assertFalse(result)
        
        # Test during late night with large amount (should trigger)
        self.transaction['timestamp'] = late_night
        self.transaction['amount'] = 1000.0
        result = self.detector._check_late_night_spending(self.transaction)
        self.assertTrue(result)
    
    def test_detect_fraud(self):
        """Test the main detect_fraud method"""
        # Mock the individual check methods
        with patch.object(self.detector, '_check_high_amount', return_value=False) as mock_high_amount, \
             patch.object(self.detector, '_check_rapid_transactions', return_value=False) as mock_rapid, \
             patch.object(self.detector, '_check_unusual_location', return_value=False) as mock_location, \
             patch.object(self.detector, '_check_duplicate_transaction', return_value=False) as mock_duplicate, \
             patch.object(self.detector, '_check_late_night_spending', return_value=False) as mock_late_night:
            
            # Test with no fraud triggers
            result = self.detector.detect_fraud(self.transaction, self.card_history)
            self.assertFalse(result['is_fraud'])
            self.assertEqual(result['fraud_type'], '')
            self.assertEqual(result['fraud_score'], 0.0)
            
            # Test with high amount trigger
            mock_high_amount.return_value = True
            result = self.detector.detect_fraud(self.transaction, self.card_history)
            self.assertTrue(result['is_fraud'])
            self.assertEqual(result['fraud_type'], 'High Amount')
            self.assertGreater(result['fraud_score'], 0.0)
            
            # Test with multiple triggers
            mock_rapid.return_value = True
            mock_location.return_value = True
            result = self.detector.detect_fraud(self.transaction, self.card_history)
            self.assertTrue(result['is_fraud'])
            self.assertIn('High Amount', result['fraud_type'])
            self.assertIn('Rapid Transactions', result['fraud_type'])
            self.assertIn('Unusual Location', result['fraud_type'])
            self.assertGreater(result['fraud_score'], 0.5)  # Score should be higher with multiple triggers


class TestDatabaseHandler(unittest.TestCase):
    """Test cases for the DatabaseHandler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Mock the database connection
        self.mock_db = MagicMock()
        self.handler = DatabaseHandler(self.mock_db)
        
        # Create a sample transaction
        self.transaction = {
            'transaction_id': 'T12345',
            'timestamp': datetime.now(),
            'card_number': '1234****5678',
            'amount': 1000.0,
            'merchant_id': 'M001',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Electronics',
            'country': 'USA',
            'city': 'New York',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'is_fraud': False,
            'fraud_type': '',
            'fraud_score': 0.0
        }
    
    def test_get_card_history(self):
        """Test retrieving card transaction history"""
        # Set up the mock to return a list of transactions
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ('T12344', datetime.now() - timedelta(days=1), 500.0, 'USA', 'New York', 'M002')
        ]
        
        # Call the method
        result = self.handler.get_card_history('1234****5678')
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['transaction_id'], 'T12344')
        self.assertEqual(result[0]['amount'], 500.0)
        self.assertEqual(result[0]['country'], 'USA')
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('FROM transactions', query)
        self.assertIn('WHERE card_number', query)
        self.assertIn('ORDER BY timestamp DESC', query)
    
    def test_store_transaction(self):
        """Test storing a transaction"""
        # Call the method
        self.handler.store_transaction(self.transaction)
        
        # Verify the transaction was added to the batch
        self.assertEqual(len(self.handler.transaction_batch), 1)
        self.assertEqual(self.handler.transaction_batch[0], self.transaction)
    
    def test_store_fraud_transaction(self):
        """Test storing a fraudulent transaction"""
        # Make the transaction fraudulent
        self.transaction['is_fraud'] = True
        self.transaction['fraud_type'] = 'High Amount'
        self.transaction['fraud_score'] = 0.8
        
        # Call the method
        self.handler.store_transaction(self.transaction)
        
        # Verify the transaction was added to both batches
        self.assertEqual(len(self.handler.transaction_batch), 1)
        self.assertEqual(self.handler.transaction_batch[0], self.transaction)
        self.assertEqual(len(self.handler.fraud_batch), 1)
        self.assertEqual(self.handler.fraud_batch[0], self.transaction)
    
    def test_flush_batches(self):
        """Test flushing transaction batches to the database"""
        # Add some transactions to the batches
        self.handler.transaction_batch = [self.transaction]
        fraud_transaction = self.transaction.copy()
        fraud_transaction['is_fraud'] = True
        fraud_transaction['fraud_type'] = 'High Amount'
        fraud_transaction['fraud_score'] = 0.8
        self.handler.fraud_batch = [fraud_transaction]
        
        # Call the method
        self.handler.flush_batches()
        
        # Verify the database methods were called
        self.mock_db.cursor.return_value.__enter__.return_value.executemany.assert_called()
        self.assertEqual(self.mock_db.cursor.return_value.__enter__.return_value.executemany.call_count, 2)
        
        # Verify the batches were cleared
        self.assertEqual(len(self.handler.transaction_batch), 0)
        self.assertEqual(len(self.handler.fraud_batch), 0)


class TestTransactionProcessor(unittest.TestCase):
    """Test cases for the TransactionProcessor class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Mock the components
        self.mock_detector = MagicMock()
        self.mock_db_handler = MagicMock()
        self.mock_alerter = MagicMock()
        
        # Create a processor with mocked components
        self.processor = TransactionProcessor(
            detector=self.mock_detector,
            db_handler=self.mock_db_handler,
            alerter=self.mock_alerter
        )
        
        # Create a sample transaction
        self.transaction = {
            'transaction_id': 'T12345',
            'timestamp': datetime.now(),
            'card_number': '1234****5678',
            'amount': 1000.0,
            'merchant_id': 'M001',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Electronics',
            'country': 'USA',
            'city': 'New York',
            'latitude': 40.7128,
            'longitude': -74.0060
        }
    
    def test_process_transaction_not_fraud(self):
        """Test processing a non-fraudulent transaction"""
        # Set up the mocks
        self.mock_db_handler.get_card_history.return_value = []
        self.mock_detector.detect_fraud.return_value = {
            'is_fraud': False,
            'fraud_type': '',
            'fraud_score': 0.0
        }
        
        # Call the method
        self.processor.process_transaction(self.transaction)
        
        # Verify the methods were called
        self.mock_db_handler.get_card_history.assert_called_once_with(self.transaction['card_number'])
        self.mock_detector.detect_fraud.assert_called_once()
        self.mock_db_handler.store_transaction.assert_called_once()
        self.mock_alerter.send_alert.assert_not_called()  # No alert for non-fraud
    
    def test_process_transaction_fraud(self):
        """Test processing a fraudulent transaction"""
        # Set up the mocks
        self.mock_db_handler.get_card_history.return_value = []
        self.mock_detector.detect_fraud.return_value = {
            'is_fraud': True,
            'fraud_type': 'High Amount',
            'fraud_score': 0.8
        }
        
        # Call the method
        self.processor.process_transaction(self.transaction)
        
        # Verify the methods were called
        self.mock_db_handler.get_card_history.assert_called_once_with(self.transaction['card_number'])
        self.mock_detector.detect_fraud.assert_called_once()
        self.mock_db_handler.store_transaction.assert_called_once()
        self.mock_alerter.send_alert.assert_called_once()  # Alert should be sent
    
    def test_start_processing_kafka(self):
        """Test starting the processor with Kafka"""
        # Mock the Kafka consumer
        mock_consumer = MagicMock()
        
        # Set up the mock to return a message
        mock_message = MagicMock()
        mock_message.value = json.dumps(self.transaction).encode('utf-8')
        mock_consumer.__iter__.return_value = [mock_message]
        
        # Patch the KafkaConsumer class
        with patch('processing.real_time_processor.KafkaConsumer', return_value=mock_consumer):
            # Call the method with a flag to run only once
            self.processor.start_processing(use_kafka=True, run_once=True)
            
            # Verify the processor processed the transaction
            self.mock_db_handler.get_card_history.assert_called_once()
            self.mock_detector.detect_fraud.assert_called_once()
            self.mock_db_handler.store_transaction.assert_called_once()
    
    def test_start_processing_queue(self):
        """Test starting the processor with a queue"""
        # Create a queue with a transaction
        test_queue = queue.Queue()
        test_queue.put(self.transaction)
        
        # Call the method with the queue and a flag to run only once
        self.processor.start_processing(transaction_queue=test_queue, run_once=True)
        
        # Verify the processor processed the transaction
        self.mock_db_handler.get_card_history.assert_called_once()
        self.mock_detector.detect_fraud.assert_called_once()
        self.mock_db_handler.store_transaction.assert_called_once()


if __name__ == '__main__':
    unittest.main()