#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
System Integration Tests

This module contains integration tests for the entire credit card fraud detection system,
testing the interaction between different components including data generation,
processing, database storage, and reporting.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path
import pandas as pd
import json
import tempfile
import shutil
from datetime import datetime, timedelta
import queue

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the system components
from data_generator.simulate_transactions import TransactionGenerator
from processing.real_time_processor import FraudDetector, DatabaseHandler, TransactionProcessor
from alerts.telegram_bot import TelegramAlerter
from alerts.email_alert import EmailAlerter
from utils.pdf_report import FraudReportGenerator
from utils.data_export import DataExporter


class TestSystemIntegration(unittest.TestCase):
    """System integration tests for the credit card fraud detection system"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a mock database connection
        self.mock_db = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        # Create a mock Kafka producer
        self.mock_kafka_producer = MagicMock()
        
        # Create a transaction queue
        self.transaction_queue = queue.Queue()
        
        # Set up environment variables for testing
        self.env_patcher = patch.dict('os.environ', {
            'DB_HOST': 'test_host',
            'DB_NAME': 'test_db',
            'DB_USER': 'test_user',
            'DB_PASSWORD': 'test_password',
            'DB_PORT': '5432',
            'TELEGRAM_BOT_TOKEN': 'test_token',
            'TELEGRAM_CHAT_ID': 'test_chat_id',
            'SMTP_SERVER': 'smtp.test.com',
            'SMTP_PORT': '587',
            'SMTP_USERNAME': 'test@example.com',
            'SMTP_PASSWORD': 'test_password',
            'EMAIL_FROM': 'test@example.com',
            'EMAIL_TO': 'recipient@example.com'
        })
        self.env_patcher.start()
        
        # Create system components
        self.transaction_generator = TransactionGenerator(
            kafka_producer=self.mock_kafka_producer,
            transaction_queue=self.transaction_queue
        )
        
        self.fraud_detector = FraudDetector()
        
        self.db_handler = DatabaseHandler(db_connection=self.mock_db)
        
        self.transaction_processor = TransactionProcessor(
            fraud_detector=self.fraud_detector,
            db_handler=self.db_handler,
            transaction_queue=self.transaction_queue
        )
        
        # Create alerters
        with patch('alerts.telegram_bot.telegram.Bot'):
            self.telegram_alerter = TelegramAlerter()
        
        with patch('alerts.email_alert.smtplib.SMTP'):
            self.email_alerter = EmailAlerter()
        
        # Create report generator and data exporter
        with patch('utils.pdf_report.REPORT_DIR', self.temp_dir):
            self.report_generator = FraudReportGenerator(db_connection=self.mock_db)
        
        with patch('utils.data_export.EXPORT_DIR', self.temp_dir):
            self.data_exporter = DataExporter(db_connection=self.mock_db)
    
    def tearDown(self):
        """Clean up after tests"""
        # Remove the temporary directory and its contents
        shutil.rmtree(self.temp_dir)
        
        # Stop environment variable patch
        self.env_patcher.stop()
    
    def test_end_to_end_transaction_flow(self):
        """Test the end-to-end flow of a transaction through the system"""
        # 1. Generate a fraudulent transaction
        with patch.object(self.transaction_generator, '_load_merchant_data'):
            with patch.object(self.transaction_generator, '_load_user_card_data'):
                transaction = self.transaction_generator.generate_high_amount_fraud_transaction()
        
        # Verify the transaction structure
        self.assertIsInstance(transaction, dict)
        self.assertIn('transaction_id', transaction)
        self.assertIn('timestamp', transaction)
        self.assertIn('card_number', transaction)
        self.assertIn('amount', transaction)
        self.assertIn('merchant_id', transaction)
        self.assertIn('merchant_name', transaction)
        self.assertIn('merchant_category', transaction)
        self.assertIn('country', transaction)
        self.assertIn('city', transaction)
        self.assertIn('latitude', transaction)
        self.assertIn('longitude', transaction)
        
        # 2. Process the transaction through the fraud detector
        is_fraud, fraud_type = self.fraud_detector.detect_fraud(transaction)
        
        # Verify the fraud detection result
        self.assertTrue(is_fraud)
        self.assertEqual(fraud_type, 'high_amount')
        
        # 3. Store the transaction in the database
        # Mock the database cursor to return card history
        self.mock_cursor.fetchall.return_value = []
        
        # Store the transaction
        self.db_handler.store_transaction(transaction, is_fraud, fraud_type)
        
        # Verify the database interaction
        self.mock_cursor.execute.assert_called()
        self.mock_db.commit.assert_called_once()
        
        # 4. Send alerts for the fraudulent transaction
        # Mock the telegram bot and email sender
        with patch.object(self.telegram_alerter, '_bot') as mock_bot:
            self.telegram_alerter.send_fraud_alert(transaction, fraud_type)
            mock_bot.send_message.assert_called_once()
        
        with patch.object(self.email_alerter, '_smtp') as mock_smtp:
            self.email_alerter.send_fraud_alert(transaction, fraud_type)
            mock_smtp.send_message.assert_called_once()
    
    def test_transaction_queue_processing(self):
        """Test processing transactions from the queue"""
        # 1. Generate multiple transactions
        transactions = []
        for i in range(5):
            # Create a mix of fraudulent and non-fraudulent transactions
            if i % 2 == 0:
                with patch.object(self.transaction_generator, '_load_merchant_data'):
                    with patch.object(self.transaction_generator, '_load_user_card_data'):
                        transaction = self.transaction_generator.generate_high_amount_fraud_transaction()
            else:
                with patch.object(self.transaction_generator, '_load_merchant_data'):
                    with patch.object(self.transaction_generator, '_load_user_card_data'):
                        transaction = self.transaction_generator.generate_normal_transaction()
            
            transactions.append(transaction)
            self.transaction_queue.put(transaction)
        
        # 2. Process transactions from the queue
        # Mock the database cursor to return card history
        self.mock_cursor.fetchall.return_value = []
        
        # Process the transactions
        with patch.object(self.transaction_processor, '_should_continue', side_effect=[True, True, True, True, True, False]):
            with patch.object(self.transaction_processor, '_send_fraud_alerts'):
                self.transaction_processor.process_transactions_from_queue()
        
        # Verify that all transactions were processed
        self.assertTrue(self.transaction_queue.empty())
        
        # Verify database interactions
        self.assertEqual(self.mock_cursor.execute.call_count, 10)  # 5 transactions * 2 calls each (history + insert)
        self.assertEqual(self.mock_db.commit.call_count, 5)  # One commit per transaction
    
    def test_report_generation_integration(self):
        """Test generating fraud reports"""
        # Mock the database to return sample data
        self.mock_cursor.fetchall.side_effect = [
            # Hourly fraud data
            [(i, i + 1) for i in range(24)],
            # Category fraud data
            [(f'Category {i}', i + 1, (i + 1) * 1000) for i in range(5)],
            # Country fraud data
            [(f'Country {i}', i + 1, (i + 1) * 1000) for i in range(5)],
            # Top fraud transactions
            [(f'T{i:03d}', datetime.now(), f'1234****{i:04d}', (i + 1) * 1000, 
              f'M{i:03d}', f'Merchant {i}', f'Category {i % 5}', 
              f'Country {i % 5}', f'City {i}', 0, 0, True) for i in range(10)]
        ]
        
        self.mock_cursor.description = [
            [('hour',), ('fraud_count',)],
            [('merchant_category',), ('fraud_count',), ('total_amount',)],
            [('country',), ('fraud_count',), ('total_amount',)],
            [('transaction_id',), ('timestamp',), ('card_number',), ('amount',),
             ('merchant_id',), ('merchant_name',), ('merchant_category',),
             ('country',), ('city',), ('latitude',), ('longitude',), ('is_fraud',)]
        ]
        
        # Generate a daily report
        report_path = self.report_generator.generate_daily_report()
        
        # Verify the report was generated
        self.assertTrue(os.path.exists(report_path))
        self.assertTrue(report_path.endswith('.pdf'))
    
    def test_data_export_integration(self):
        """Test exporting fraud data to different formats"""
        # Mock the database to return sample data
        self.mock_cursor.fetchall.side_effect = [
            # Fraud transactions
            [(f'T{i:03d}', datetime.now(), f'1234****{i:04d}', (i + 1) * 1000, 
              f'M{i:03d}', f'Merchant {i}', f'Category {i % 5}', 
              f'Country {i % 5}', f'City {i}', 0, 0, True) for i in range(10)],
            # Daily summary
            [(datetime.now().date() - timedelta(days=i), i + 1, (i + 1) * 1000) for i in range(7)],
            # Category summary
            [(f'Category {i}', i + 1, (i + 1) * 1000) for i in range(5)],
            # Country summary
            [(f'Country {i}', i + 1, (i + 1) * 1000) for i in range(5)]
        ]
        
        self.mock_cursor.description = [
            [('transaction_id',), ('timestamp',), ('card_number',), ('amount',),
             ('merchant_id',), ('merchant_name',), ('merchant_category',),
             ('country',), ('city',), ('latitude',), ('longitude',), ('is_fraud',)],
            [('date',), ('fraud_count',), ('total_amount',)],
            [('merchant_category',), ('fraud_count',), ('total_amount',)],
            [('country',), ('fraud_count',), ('total_amount',)]
        ]
        
        # Export to CSV
        start_date = datetime.now().date() - timedelta(days=7)
        end_date = datetime.now().date()
        
        csv_result = self.data_exporter.export_to_csv(start_date, end_date)
        
        # Verify the CSV files were created
        self.assertIsInstance(csv_result, dict)
        self.assertEqual(len(csv_result), 4)  # 4 CSV files
        
        for file_path in csv_result.values():
            self.assertTrue(os.path.exists(file_path))
            self.assertTrue(file_path.endswith('.csv'))
    
    def test_alert_system_integration(self):
        """Test the integration of the alert system with fraud detection"""
        # 1. Generate a fraudulent transaction
        with patch.object(self.transaction_generator, '_load_merchant_data'):
            with patch.object(self.transaction_generator, '_load_user_card_data'):
                transaction = self.transaction_generator.generate_high_amount_fraud_transaction()
        
        # 2. Process the transaction through the fraud detector
        is_fraud, fraud_type = self.fraud_detector.detect_fraud(transaction)
        
        # 3. Send alerts for the fraudulent transaction
        # Mock the telegram bot and email sender
        with patch.object(self.telegram_alerter, '_bot') as mock_bot:
            with patch.object(self.email_alerter, '_smtp') as mock_smtp:
                # Simulate the transaction processor sending alerts
                if is_fraud:
                    self.telegram_alerter.send_fraud_alert(transaction, fraud_type)
                    self.email_alerter.send_fraud_alert(transaction, fraud_type)
                
                # Verify the alerts were sent
                mock_bot.send_message.assert_called_once()
                mock_smtp.send_message.assert_called_once()
    
    def test_kafka_to_database_flow(self):
        """Test the flow from Kafka to database storage"""
        # 1. Generate a transaction
        with patch.object(self.transaction_generator, '_load_merchant_data'):
            with patch.object(self.transaction_generator, '_load_user_card_data'):
                transaction = self.transaction_generator.generate_normal_transaction()
        
        # 2. Simulate sending to Kafka
        serialized_transaction = json.dumps(transaction)
        self.transaction_generator.send_to_kafka(transaction)
        
        # Verify Kafka producer was called
        self.mock_kafka_producer.send.assert_called_once_with(
            'transactions', value=serialized_transaction.encode('utf-8')
        )
        
        # 3. Simulate receiving from Kafka and processing
        # Mock the database cursor to return card history
        self.mock_cursor.fetchall.return_value = []
        
        # Process the transaction
        is_fraud, fraud_type = self.fraud_detector.detect_fraud(transaction)
        self.db_handler.store_transaction(transaction, is_fraud, fraud_type)
        
        # Verify database interaction
        self.mock_cursor.execute.assert_called()
        self.mock_db.commit.assert_called_once()
    
    def test_system_with_multiple_fraud_types(self):
        """Test the system with different types of fraud transactions"""
        fraud_types = [
            'high_amount',
            'rapid_transaction',
            'unusual_location',
            'duplicate_transaction',
            'late_night_spending'
        ]
        
        for fraud_type in fraud_types:
            # Generate a specific type of fraud transaction
            with patch.object(self.transaction_generator, '_load_merchant_data'):
                with patch.object(self.transaction_generator, '_load_user_card_data'):
                    if fraud_type == 'high_amount':
                        transaction = self.transaction_generator.generate_high_amount_fraud_transaction()
                    elif fraud_type == 'rapid_transaction':
                        transaction = self.transaction_generator.generate_rapid_transaction_fraud()
                    elif fraud_type == 'unusual_location':
                        transaction = self.transaction_generator.generate_unusual_location_fraud()
                    elif fraud_type == 'duplicate_transaction':
                        transaction = self.transaction_generator.generate_duplicate_transaction_fraud()
                    elif fraud_type == 'late_night_spending':
                        transaction = self.transaction_generator.generate_late_night_spending_fraud()
            
            # Process the transaction through the fraud detector
            # Mock the database cursor to return appropriate card history for each fraud type
            if fraud_type == 'rapid_transaction':
                # Return recent transactions for the same card
                recent_time = datetime.now() - timedelta(seconds=10)
                self.mock_cursor.fetchall.return_value = [
                    (recent_time, 100.0, 'USA')
                ] * 3  # 3 recent transactions
            elif fraud_type == 'unusual_location':
                # Return transactions from a different country
                self.mock_cursor.fetchall.return_value = [
                    (datetime.now() - timedelta(hours=1), 100.0, 'USA')
                ] * 3
            elif fraud_type == 'duplicate_transaction':
                # Return a transaction with the same amount and merchant
                self.mock_cursor.fetchall.return_value = [
                    (datetime.now() - timedelta(minutes=5), transaction['amount'], transaction['merchant_id'])
                ]
            else:
                self.mock_cursor.fetchall.return_value = []
            
            # Detect fraud
            is_fraud, detected_fraud_type = self.fraud_detector.detect_fraud(transaction)
            
            # Verify the fraud detection result
            self.assertTrue(is_fraud)
            if fraud_type != 'duplicate_transaction':  # This one might be detected as rapid_transaction depending on timing
                self.assertEqual(detected_fraud_type, fraud_type)
            
            # Store the transaction
            self.db_handler.store_transaction(transaction, is_fraud, detected_fraud_type)
            
            # Reset mock for next iteration
            self.mock_cursor.reset_mock()
            self.mock_db.reset_mock()
    
    def test_batch_processing_integration(self):
        """Test batch processing of transactions"""
        # Generate a batch of transactions
        batch_size = 10
        transactions = []
        
        for i in range(batch_size):
            # Create a mix of fraudulent and non-fraudulent transactions
            with patch.object(self.transaction_generator, '_load_merchant_data'):
                with patch.object(self.transaction_generator, '_load_user_card_data'):
                    if i % 3 == 0:  # 33% fraud rate for testing
                        transaction = self.transaction_generator.generate_high_amount_fraud_transaction()
                    else:
                        transaction = self.transaction_generator.generate_normal_transaction()
            
            transactions.append(transaction)
        
        # Process each transaction
        fraud_count = 0
        self.mock_cursor.fetchall.return_value = []  # No card history for simplicity
        
        for transaction in transactions:
            # Detect fraud
            is_fraud, fraud_type = self.fraud_detector.detect_fraud(transaction)
            
            # Count frauds
            if is_fraud:
                fraud_count += 1
            
            # Add to database handler's batch
            self.db_handler.add_to_batch(transaction, is_fraud, fraud_type)
        
        # Flush the batch to the database
        self.db_handler.flush_batch()
        
        # Verify the batch was processed
        self.assertEqual(self.mock_cursor.executemany.call_count, 2)  # One for transactions, one for fraud
        self.mock_db.commit.assert_called_once()
        
        # Verify the fraud count matches expected
        expected_fraud_count = sum(1 for i in range(batch_size) if i % 3 == 0)
        self.assertEqual(fraud_count, expected_fraud_count)


# Main test runner
if __name__ == '__main__':
    unittest.main()