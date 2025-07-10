#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Telegram Bot Alerter

This module contains unit tests for the TelegramAlerter class that handles
sending fraud alerts via Telegram.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the TelegramAlerter class
from alerts.telegram_bot import TelegramAlerter


class TestTelegramAlerter(unittest.TestCase):
    """Test cases for the TelegramAlerter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a sample transaction dictionary
        self.transaction = {
            'transaction_id': 'T12345',
            'timestamp': datetime(2023, 1, 1, 12, 30, 0),
            'card_number': '1234****5678',
            'amount': 5000.0,
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Electronics',
            'country': 'USA',
            'city': 'New York',
            'fraud_type': 'High Amount',
            'fraud_score': 0.85
        }
        
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'TELEGRAM_BOT_TOKEN': 'test_token',
            'TELEGRAM_CHAT_ID': 'test_chat_id'
        })
        self.env_patcher.start()
        
        # Create a TelegramAlerter instance with a mocked bot
        with patch('alerts.telegram_bot.Bot') as mock_bot_class:
            self.mock_bot = MagicMock()
            mock_bot_class.return_value = self.mock_bot
            self.alerter = TelegramAlerter()
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test TelegramAlerter initialization"""
        # Test that the bot was initialized with the correct token
        with patch('alerts.telegram_bot.Bot') as mock_bot_class:
            alerter = TelegramAlerter()
            mock_bot_class.assert_called_once_with(token='test_token')
    
    def test_initialization_no_env_vars(self):
        """Test TelegramAlerter initialization with missing environment variables"""
        # Stop the current environment patch
        self.env_patcher.stop()
        
        # Create a new patch with empty environment
        with patch.dict('os.environ', {}, clear=True):
            # Test that initialization raises an exception
            with self.assertRaises(ValueError):
                TelegramAlerter()
        
        # Restart the environment patch for other tests
        self.env_patcher.start()
    
    def test_format_fraud_message(self):
        """Test formatting of fraud message"""
        # Call the method
        message = self.alerter._format_fraud_message(self.transaction)
        
        # Verify the message contains all the important information
        self.assertIn('🚨 FRAUD ALERT 🚨', message)
        self.assertIn('Transaction ID: T12345', message)
        self.assertIn('Card: 1234****5678', message)
        self.assertIn('Amount: $5,000.00', message)
        self.assertIn('Merchant: Test Merchant (Electronics)', message)
        self.assertIn('Location: New York, USA', message)
        self.assertIn('Fraud Type: High Amount', message)
        self.assertIn('Fraud Score: 85%', message)
    
    def test_send_alert(self):
        """Test sending an alert"""
        # Call the method
        self.alerter.send_alert(self.transaction)
        
        # Verify the bot's send_message method was called with the correct parameters
        self.mock_bot.send_message.assert_called_once()
        args, kwargs = self.mock_bot.send_message.call_args
        self.assertEqual(kwargs['chat_id'], 'test_chat_id')
        self.assertIn('🚨 FRAUD ALERT 🚨', kwargs['text'])
        self.assertEqual(kwargs['parse_mode'], 'Markdown')
    
    def test_send_alert_with_exception(self):
        """Test sending an alert when an exception occurs"""
        # Make the bot's send_message method raise an exception
        self.mock_bot.send_message.side_effect = Exception('Test exception')
        
        # Call the method and verify it handles the exception gracefully
        with patch('alerts.telegram_bot.logger.error') as mock_logger:
            self.alerter.send_alert(self.transaction)
            mock_logger.assert_called_once()
            self.assertIn('Failed to send Telegram alert', mock_logger.call_args[0][0])
    
    def test_send_test_alert(self):
        """Test sending a test alert"""
        # Call the method
        self.alerter.send_test_alert()
        
        # Verify the bot's send_message method was called with the correct parameters
        self.mock_bot.send_message.assert_called_once()
        args, kwargs = self.mock_bot.send_message.call_args
        self.assertEqual(kwargs['chat_id'], 'test_chat_id')
        self.assertIn('Test Alert', kwargs['text'])
        self.assertEqual(kwargs['parse_mode'], 'Markdown')


if __name__ == '__main__':
    unittest.main()