#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Email Alert System for Credit Card Fraud Detection

This module provides functionality to send fraud alerts via email using SMTP.
It requires email configuration to be set in the environment variables.
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional, Union
from datetime import datetime

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
EMAIL_FROM = os.getenv('EMAIL_FROM', SMTP_USERNAME)
EMAIL_TO = os.getenv('EMAIL_TO')
USE_EMAIL = os.getenv('USE_EMAIL', 'true').lower() == 'true'  # Default to true


class EmailAlerter:
    """Sends fraud alerts via email"""
    
    def __init__(self):
        self.smtp_server = SMTP_SERVER
        self.smtp_port = SMTP_PORT
        self.username = SMTP_USERNAME
        self.password = SMTP_PASSWORD
        self.email_from = EMAIL_FROM
        self.email_to = EMAIL_TO
    
    def is_available(self) -> bool:
        """Check if email alerting is available"""
        return all([USE_EMAIL, self.smtp_server, self.smtp_port, 
                   self.username, self.password, self.email_from, self.email_to])
    
    def format_fraud_message_html(self, transaction: Dict) -> str:
        """Format a fraud alert message in HTML"""
        # Parse the timestamp
        try:
            timestamp = datetime.fromisoformat(transaction['timestamp'])
            time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            time_str = str(transaction['timestamp'])
        
        # Format the card number (show only last 4 digits)
        card_number = transaction['card_number']
        
        # Format the fraud types
        fraud_types_html = ""
        if isinstance(transaction.get('fraud_types'), list):
            fraud_types_html = "<ul>"
            for fraud_type in transaction['fraud_types']:
                fraud_types_html += f"<li>{fraud_type}</li>"
            fraud_types_html += "</ul>"
        else:
            fraud_type = transaction.get('fraud_type', 'Unknown')
            fraud_types_html = f"<ul><li>{fraud_type}</li></ul>"
        
        # Build the HTML message
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #f44336; color: white; padding: 10px; text-align: center; }}
                .content {{ padding: 20px; border: 1px solid #ddd; }}
                .footer {{ font-size: 12px; text-align: center; margin-top: 20px; color: #777; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f2f2f2; }}
                .alert {{ color: #f44336; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚨 FRAUD ALERT 🚨</h1>
                </div>
                <div class="content">
                    <p>A potentially fraudulent transaction has been detected on your credit card:</p>
                    
                    <table>
                        <tr>
                            <th>Transaction ID:</th>
                            <td>{transaction['transaction_id']}</td>
                        </tr>
                        <tr>
                            <th>Time:</th>
                            <td>{time_str}</td>
                        </tr>
                        <tr>
                            <th>Card Number:</th>
                            <td>{card_number}</td>
                        </tr>
                        <tr>
                            <th>Amount:</th>
                            <td>${transaction['amount']:.2f}</td>
                        </tr>
                        <tr>
                            <th>Merchant:</th>
                            <td>{transaction['merchant_name']}</td>
                        </tr>
                        <tr>
                            <th>Category:</th>
                            <td>{transaction['merchant_category']}</td>
                        </tr>
                        <tr>
                            <th>Location:</th>
                            <td>{transaction['city']}, {transaction['country']}</td>
                        </tr>
                    </table>
                    
                    <h3 class="alert">Fraud Detection Reasons:</h3>
                    {fraud_types_html}
        """
        
        if 'fraud_score' in transaction:
            html += f"""
                    <p><strong>Fraud Score:</strong> {transaction['fraud_score']:.2f}</p>
            """
        
        html += f"""
                    <p><strong>⚠️ Please check your account for unauthorized transactions.</strong></p>
                    <p>If you did not make this transaction, please contact your bank immediately.</p>
                </div>
                <div class="footer">
                    <p>This is an automated alert from your Credit Card Fraud Detection System.</p>
                    <p>Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def format_fraud_message_text(self, transaction: Dict) -> str:
        """Format a fraud alert message in plain text"""
        # Parse the timestamp
        try:
            timestamp = datetime.fromisoformat(transaction['timestamp'])
            time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            time_str = str(transaction['timestamp'])
        
        # Format the card number (show only last 4 digits)
        card_number = transaction['card_number']
        
        # Format the fraud types
        if isinstance(transaction.get('fraud_types'), list):
            fraud_types = "\n- " + "\n- ".join(transaction['fraud_types'])
        else:
            fraud_type = transaction.get('fraud_type', 'Unknown')
            fraud_types = f"\n- {fraud_type}"
        
        # Build the text message
        message = f"FRAUD ALERT\n\n"
        message += f"A potentially fraudulent transaction has been detected on your credit card:\n\n"
        message += f"Transaction ID: {transaction['transaction_id']}\n"
        message += f"Time: {time_str}\n"
        message += f"Card: {card_number}\n"
        message += f"Amount: ${transaction['amount']:.2f}\n"
        message += f"Merchant: {transaction['merchant_name']}\n"
        message += f"Category: {transaction['merchant_category']}\n"
        message += f"Location: {transaction['city']}, {transaction['country']}\n\n"
        message += f"Fraud Detection Reasons:{fraud_types}\n"
        
        if 'fraud_score' in transaction:
            message += f"\nFraud Score: {transaction['fraud_score']:.2f}\n"
        
        message += "\n⚠️ Please check your account for unauthorized transactions.\n"
        message += "If you did not make this transaction, please contact your bank immediately.\n\n"
        message += "This is an automated alert from your Credit Card Fraud Detection System.\n"
        message += "Please do not reply to this email.\n"
        
        return message
    
    def send_fraud_alert(self, transaction: Dict) -> bool:
        """Send a fraud alert via email"""
        if not self.is_available():
            logger.warning("Email alerting is not available")
            return False
        
        try:
            # Create the email message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"🚨 FRAUD ALERT: Transaction ${transaction['amount']:.2f} at {transaction['merchant_name']}"
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            
            # Attach plain text and HTML versions
            text_part = MIMEText(self.format_fraud_message_text(transaction), 'plain')
            html_part = MIMEText(self.format_fraud_message_html(transaction), 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Connect to the SMTP server and send the email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()  # Secure the connection
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"Sent fraud alert for transaction {transaction['transaction_id']} via email")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False


# Singleton instance
_email_alerter = None


def get_email_alerter() -> EmailAlerter:
    """Get the singleton EmailAlerter instance"""
    global _email_alerter
    if _email_alerter is None:
        _email_alerter = EmailAlerter()
    return _email_alerter


def send_fraud_alert(transaction: Dict) -> bool:
    """Send a fraud alert for a transaction"""
    alerter = get_email_alerter()
    return alerter.send_fraud_alert(transaction)


if __name__ == "__main__":
    # Test the Email alerter
    if not USE_EMAIL:
        print("Email alerting is disabled or not available")
        print("Please set USE_EMAIL=true and provide SMTP configuration")
        exit(1)
    
    # Create a test transaction
    test_transaction = {
        'transaction_id': 'test-123456',
        'timestamp': datetime.now().isoformat(),
        'card_number': '************1234',
        'amount': 9999.99,
        'merchant_id': 'test-merchant',
        'merchant_name': 'Test Merchant',
        'merchant_category': 'Test',
        'country': 'Test Country',
        'city': 'Test City',
        'latitude': 0.0,
        'longitude': 0.0,
        'fraud_types': [
            'High amount: $9999.99',
            'Unusual location: Home Country -> Test Country'
        ],
        'fraud_score': 0.95
    }
    
    # Send a test alert
    alerter = get_email_alerter()
    if alerter.is_available():
        success = alerter.send_fraud_alert(test_transaction)
        if success:
            print("Test alert sent successfully!")
        else:
            print("Failed to send test alert")
    else:
        print("Email alerter is not available")