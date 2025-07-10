#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Email Alert System

This module contains unit tests for the EmailAlerter class that handles
sending fraud alerts via email.
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

# Import the EmailAlerter class
from alerts.email_alert import EmailAlerter


class TestEmailAlerter(unittest.TestCase):
    """Test cases for the EmailAlerter class"""
    
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
            'SMTP_SERVER': 'smtp.example.com',
            'SMTP_PORT': '587',
            'SMTP_USERNAME': 'test@example.com',
            'SMTP_PASSWORD': 'test_password',
            'ALERT_EMAIL_FROM': 'alerts@example.com',
            'ALERT_EMAIL_TO': 'recipient@example.com'
        })
        self.env_patcher.start()
        
        # Create an EmailAlerter instance with mocked SMTP
        with patch('alerts.email_alert.smtplib.SMTP') as mock_smtp_class:
            self.mock_smtp = MagicMock()
            mock_smtp_class.return_value = self.mock_smtp
            self.alerter = EmailAlerter()
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test EmailAlerter initialization"""
        # Test that the SMTP settings were correctly initialized
        self.assertEqual(self.alerter.smtp_server, 'smtp.example.com')
        self.assertEqual(self.alerter.smtp_port, 587)
        self.assertEqual(self.alerter.username, 'test@example.com')
        self.assertEqual(self.alerter.password, 'test_password')
        self.assertEqual(self.alerter.from_email, 'alerts@example.com')
        self.assertEqual(self.alerter.to_email, 'recipient@example.com')
    
    def test_initialization_no_env_vars(self):
        """Test EmailAlerter initialization with missing environment variables"""
        # Stop the current environment patch
        self.env_patcher.stop()
        
        # Create a new patch with empty environment
        with patch.dict('os.environ', {}, clear=True):
            # Test that initialization raises an exception
            with self.assertRaises(ValueError):
                EmailAlerter()
        
        # Restart the environment patch for other tests
        self.env_patcher.start()
    
    def test_format_text_message(self):
        """Test formatting of plain text message"""
        # Call the method
        message = self.alerter._format_text_message(self.transaction)
        
        # Verify the message contains all the important information
        self.assertIn('FRAUD ALERT', message)
        self.assertIn('Transaction ID: T12345', message)
        self.assertIn('Card: 1234****5678', message)
        self.assertIn('Amount: $5,000.00', message)
        self.assertIn('Merchant: Test Merchant (Electronics)', message)
        self.assertIn('Location: New York, USA', message)
        self.assertIn('Fraud Type: High Amount', message)
        self.assertIn('Fraud Score: 85%', message)
    
    def test_format_html_message(self):
        """Test formatting of HTML message"""
        # Call the method
        message = self.alerter._format_html_message(self.transaction)
        
        # Verify the message contains all the important information and HTML tags
        self.assertIn('<html>', message)
        self.assertIn('<body>', message)
        self.assertIn('<h1>FRAUD ALERT</h1>', message)
        self.assertIn('<strong>Transaction ID:</strong> T12345', message)
        self.assertIn('<strong>Card:</strong> 1234****5678', message)
        self.assertIn('<strong>Amount:</strong> $5,000.00', message)
        self.assertIn('<strong>Merchant:</strong> Test Merchant (Electronics)', message)
        self.assertIn('<strong>Location:</strong> New York, USA', message)
        self.assertIn('<strong>Fraud Type:</strong> High Amount', message)
        self.assertIn('<strong>Fraud Score:</strong> 85%', message)
        self.assertIn('</body>', message)
        self.assertIn('</html>', message)
    
    @patch('alerts.email_alert.MIMEMultipart')
    @patch('alerts.email_alert.MIMEText')
    def test_send_alert(self, mock_mime_text, mock_mime_multipart):
        """Test sending an alert"""
        # Set up the mocks
        mock_message = MagicMock()
        mock_mime_multipart.return_value = mock_message
        
        # Call the method
        self.alerter.send_alert(self.transaction)
        
        # Verify the SMTP methods were called with the correct parameters
        self.mock_smtp.starttls.assert_called_once()
        self.mock_smtp.login.assert_called_once_with('test@example.com', 'test_password')
        self.mock_smtp.send_message.assert_called_once_with(mock_message)
        self.mock_smtp.quit.assert_called_once()
        
        # Verify the message was set up correctly
        mock_message.__setitem__.assert_any_call('From', 'alerts@example.com')
        mock_message.__setitem__.assert_any_call('To', 'recipient@example.com')
        self.assertIn('FRAUD ALERT', mock_message.__setitem__.call_args_list[2][0][1])
    
    def test_send_alert_with_exception(self):
        """Test sending an alert when an exception occurs"""
        # Make the SMTP's send_message method raise an exception
        self.mock_smtp.send_message.side_effect = Exception('Test exception')
        
        # Call the method and verify it handles the exception gracefully
        with patch('alerts.email_alert.logger.error') as mock_logger:
            self.alerter.send_alert(self.transaction)
            mock_logger.assert_called_once()
            self.assertIn('Failed to send email alert', mock_logger.call_args[0][0])
    
    @patch('alerts.email_alert.MIMEMultipart')
    @patch('alerts.email_alert.MIMEText')
    def test_send_test_alert(self, mock_mime_text, mock_mime_multipart):
        """Test sending a test alert"""
        # Set up the mocks
        mock_message = MagicMock()
        mock_mime_multipart.return_value = mock_message
        
        # Call the method
        self.alerter.send_test_alert()
        
        # Verify the SMTP methods were called
        self.mock_smtp.send_message.assert_called_once_with(mock_message)
        
        # Verify the message was set up correctly
        mock_message.__setitem__.assert_any_call('From', 'alerts@example.com')
        mock_message.__setitem__.assert_any_call('To', 'recipient@example.com')
        self.assertIn('Test Alert', mock_message.__setitem__.call_args_list[2][0][1])


if __name__ == '__main__':
    unittest.main()