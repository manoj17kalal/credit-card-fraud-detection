#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Transaction Simulator

This module contains unit tests for the TransactionGenerator class that handles
simulating credit card transactions.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from datetime import datetime
import queue

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the TransactionGenerator class
from data_generator.simulate_transactions import TransactionGenerator


class TestTransactionGenerator(unittest.TestCase):
    """Test cases for the TransactionGenerator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock database connection
        self.mock_db = MagicMock()
        
        # Create a TransactionGenerator instance with mocked components
        with patch('data_generator.simulate_transactions.KafkaProducer') as mock_producer_class:
            self.mock_producer = MagicMock()
            mock_producer_class.return_value = self.mock_producer
            self.generator = TransactionGenerator(
                db_connection=self.mock_db,
                use_kafka=False,
                fraud_probability=0.1
            )
    
    def test_initialization(self):
        """Test TransactionGenerator initialization"""
        # Verify the generator was initialized correctly
        self.assertEqual(self.generator.fraud_probability, 0.1)
        self.assertFalse(self.generator.use_kafka)
        self.assertIsNone(self.generator.kafka_producer)
        self.assertIsNotNone(self.generator.faker)
        self.assertIsNotNone(self.generator.merchant_data)
        self.assertIsNotNone(self.generator.user_cards)
    
    def test_initialization_with_kafka(self):
        """Test TransactionGenerator initialization with Kafka"""
        # Create a generator with Kafka enabled
        with patch('data_generator.simulate_transactions.KafkaProducer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            generator = TransactionGenerator(
                db_connection=self.mock_db,
                use_kafka=True,
                kafka_topic='transactions',
                kafka_bootstrap_servers='localhost:9092'
            )
            
            # Verify the Kafka producer was initialized
            self.assertTrue(generator.use_kafka)
            self.assertEqual(generator.kafka_topic, 'transactions')
            mock_producer_class.assert_called_once_with(
                bootstrap_servers='localhost:9092',
                value_serializer=generator._serialize_transaction
            )
    
    def test_load_merchant_data(self):
        """Test loading merchant data"""
        # Set up the mock to return merchant data
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ('M001', 'Test Merchant 1', 'Electronics', 'USA', 'New York', 40.7128, -74.0060),
            ('M002', 'Test Merchant 2', 'Dining', 'Canada', 'Toronto', 43.6532, -79.3832)
        ]
        
        # Call the method
        result = self.generator._load_merchant_data()
        
        # Verify the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['merchant_id'], 'M001')
        self.assertEqual(result[0]['merchant_name'], 'Test Merchant 1')
        self.assertEqual(result[0]['merchant_category'], 'Electronics')
        self.assertEqual(result[0]['country'], 'USA')
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('FROM merchants', query)
    
    def test_load_user_cards(self):
        """Test loading user card data"""
        # Set up the mock to return user card data
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ('1234****5678', 'John Doe', 'USA'),
            ('5678****1234', 'Jane Smith', 'Canada')
        ]
        
        # Call the method
        result = self.generator._load_user_cards()
        
        # Verify the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['card_number'], '1234****5678')
        self.assertEqual(result[0]['cardholder_name'], 'John Doe')
        self.assertEqual(result[0]['country'], 'USA')
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('FROM user_cards', query)
    
    def test_generate_merchant_data(self):
        """Test generating merchant data"""
        # Call the method
        result = self.generator._generate_merchant_data(100)
        
        # Verify the result
        self.assertEqual(len(result), 100)
        self.assertIn('merchant_id', result[0])
        self.assertIn('merchant_name', result[0])
        self.assertIn('merchant_category', result[0])
        self.assertIn('country', result[0])
        self.assertIn('city', result[0])
        self.assertIn('latitude', result[0])
        self.assertIn('longitude', result[0])
    
    def test_generate_user_cards(self):
        """Test generating user card data"""
        # Call the method
        result = self.generator._generate_user_cards(50)
        
        # Verify the result
        self.assertEqual(len(result), 50)
        self.assertIn('card_number', result[0])
        self.assertIn('cardholder_name', result[0])
        self.assertIn('country', result[0])
    
    def test_serialize_transaction(self):
        """Test serializing a transaction for Kafka"""
        # Create a sample transaction
        transaction = {
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
        
        # Call the method
        result = self.generator._serialize_transaction(transaction)
        
        # Verify the result is a byte string
        self.assertIsInstance(result, bytes)
        
        # Deserialize and verify the content
        import json
        deserialized = json.loads(result.decode('utf-8'))
        self.assertEqual(deserialized['transaction_id'], 'T12345')
        self.assertEqual(deserialized['card_number'], '1234****5678')
        self.assertEqual(deserialized['amount'], 1000.0)
    
    def test_generate_normal_transaction(self):
        """Test generating a normal transaction"""
        # Set up the generator with some data
        self.generator.merchant_data = [
            {
                'merchant_id': 'M001',
                'merchant_name': 'Test Merchant',
                'merchant_category': 'Electronics',
                'country': 'USA',
                'city': 'New York',
                'latitude': 40.7128,
                'longitude': -74.0060
            }
        ]
        self.generator.user_cards = [
            {
                'card_number': '1234****5678',
                'cardholder_name': 'John Doe',
                'country': 'USA'
            }
        ]
        
        # Call the method
        result = self.generator._generate_normal_transaction()
        
        # Verify the result
        self.assertIn('transaction_id', result)
        self.assertIn('timestamp', result)
        self.assertIn('card_number', result)
        self.assertIn('amount', result)
        self.assertIn('merchant_id', result)
        self.assertIn('merchant_name', result)
        self.assertIn('merchant_category', result)
        self.assertIn('country', result)
        self.assertIn('city', result)
        self.assertIn('latitude', result)
        self.assertIn('longitude', result)
        
        # Verify the card and merchant data was used
        self.assertEqual(result['card_number'], '1234****5678')
        self.assertEqual(result['merchant_id'], 'M001')
        self.assertEqual(result['merchant_name'], 'Test Merchant')
    
    def test_generate_fraudulent_transaction(self):
        """Test generating a fraudulent transaction"""
        # Set up the generator with some data
        self.generator.merchant_data = [
            {
                'merchant_id': 'M001',
                'merchant_name': 'Test Merchant',
                'merchant_category': 'Electronics',
                'country': 'USA',
                'city': 'New York',
                'latitude': 40.7128,
                'longitude': -74.0060
            }
        ]
        self.generator.user_cards = [
            {
                'card_number': '1234****5678',
                'cardholder_name': 'John Doe',
                'country': 'USA'
            }
        ]
        
        # Test each fraud pattern
        fraud_patterns = ['high_amount', 'foreign_country', 'rapid_transactions', 'midnight_spending']
        
        for pattern in fraud_patterns:
            # Call the method
            result = self.generator._generate_fraudulent_transaction(pattern)
            
            # Verify the result
            self.assertIn('transaction_id', result)
            self.assertIn('timestamp', result)
            self.assertIn('card_number', result)
            self.assertIn('amount', result)
            self.assertIn('merchant_id', result)
            self.assertIn('merchant_name', result)
            self.assertIn('merchant_category', result)
            self.assertIn('country', result)
            self.assertIn('city', result)
            self.assertIn('latitude', result)
            self.assertIn('longitude', result)
            
            # Verify the fraud pattern was applied
            if pattern == 'high_amount':
                self.assertGreaterEqual(result['amount'], 5000.0)
            elif pattern == 'foreign_country':
                self.assertNotEqual(result['country'], 'USA')  # Different from card's country
            elif pattern == 'midnight_spending':
                hour = result['timestamp'].hour
                self.assertTrue(0 <= hour <= 4)  # Between midnight and 4 AM
    
    def test_generate_transaction(self):
        """Test generating a transaction (normal or fraudulent)"""
        # Mock the specific generation methods
        with patch.object(self.generator, '_generate_normal_transaction') as mock_normal, \
             patch.object(self.generator, '_generate_fraudulent_transaction') as mock_fraud, \
             patch('random.random') as mock_random:
            
            # Set up the mocks to return sample transactions
            normal_transaction = {'transaction_id': 'T12345', 'is_fraud': False}
            fraud_transaction = {'transaction_id': 'T67890', 'is_fraud': True}
            mock_normal.return_value = normal_transaction
            mock_fraud.return_value = fraud_transaction
            
            # Test generating a normal transaction (random > fraud_probability)
            mock_random.return_value = 0.2  # Greater than 0.1 fraud_probability
            result = self.generator.generate_transaction()
            self.assertEqual(result, normal_transaction)
            mock_normal.assert_called_once()
            mock_fraud.assert_not_called()
            
            # Reset the mocks
            mock_normal.reset_mock()
            mock_fraud.reset_mock()
            
            # Test generating a fraudulent transaction (random < fraud_probability)
            mock_random.return_value = 0.05  # Less than 0.1 fraud_probability
            result = self.generator.generate_transaction()
            self.assertEqual(result, fraud_transaction)
            mock_normal.assert_not_called()
            mock_fraud.assert_called_once()
    
    def test_send_to_kafka(self):
        """Test sending a transaction to Kafka"""
        # Create a generator with Kafka enabled
        with patch('data_generator.simulate_transactions.KafkaProducer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            generator = TransactionGenerator(
                db_connection=self.mock_db,
                use_kafka=True,
                kafka_topic='transactions',
                kafka_bootstrap_servers='localhost:9092'
            )
            
            # Create a sample transaction
            transaction = {'transaction_id': 'T12345'}
            
            # Call the method
            generator.send_transaction(transaction)
            
            # Verify the producer's send method was called
            mock_producer.send.assert_called_once_with('transactions', transaction)
    
    def test_send_to_queue(self):
        """Test sending a transaction to a queue"""
        # Create a queue
        test_queue = queue.Queue()
        
        # Call the method
        transaction = {'transaction_id': 'T12345'}
        self.generator.send_transaction(transaction, transaction_queue=test_queue)
        
        # Verify the transaction was added to the queue
        self.assertEqual(test_queue.qsize(), 1)
        self.assertEqual(test_queue.get(), transaction)
    
    def test_start_simulation_with_queue(self):
        """Test starting the simulation with a queue"""
        # Create a queue
        test_queue = queue.Queue()
        
        # Mock the generate_transaction and send_transaction methods
        with patch.object(self.generator, 'generate_transaction') as mock_generate, \
             patch.object(self.generator, 'send_transaction') as mock_send, \
             patch('time.sleep') as mock_sleep:
            
            # Set up the mock to return a transaction
            transaction = {'transaction_id': 'T12345'}
            mock_generate.return_value = transaction
            
            # Call the method with a flag to run only once
            self.generator.start_simulation(transaction_queue=test_queue, run_once=True)
            
            # Verify the methods were called
            mock_generate.assert_called_once()
            mock_send.assert_called_once_with(transaction, transaction_queue=test_queue)
            mock_sleep.assert_not_called()  # Should not sleep when run_once is True
    
    def test_start_simulation_with_kafka(self):
        """Test starting the simulation with Kafka"""
        # Create a generator with Kafka enabled
        with patch('data_generator.simulate_transactions.KafkaProducer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            generator = TransactionGenerator(
                db_connection=self.mock_db,
                use_kafka=True,
                kafka_topic='transactions',
                kafka_bootstrap_servers='localhost:9092'
            )
            
            # Mock the generate_transaction and send_transaction methods
            with patch.object(generator, 'generate_transaction') as mock_generate, \
                 patch.object(generator, 'send_transaction') as mock_send, \
                 patch('time.sleep') as mock_sleep:
                
                # Set up the mock to return a transaction
                transaction = {'transaction_id': 'T12345'}
                mock_generate.return_value = transaction
                
                # Call the method with a flag to run only once
                generator.start_simulation(run_once=True)
                
                # Verify the methods were called
                mock_generate.assert_called_once()
                mock_send.assert_called_once_with(transaction, transaction_queue=None)
                mock_sleep.assert_not_called()  # Should not sleep when run_once is True


if __name__ == '__main__':
    unittest.main()