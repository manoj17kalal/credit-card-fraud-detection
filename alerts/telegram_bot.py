#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Telegram Bot for Credit Card Fraud Alerts

This module provides functionality to send fraud alerts via Telegram.
It requires a Telegram Bot Token and Chat ID to be set in the environment variables.
"""

import os
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime

from dotenv import load_dotenv

# Optional Telegram support
try:
    import telegram
    from telegram.ext import Updater
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')  # Updated to match .env file
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
USE_TELEGRAM = os.getenv('USE_TELEGRAM', 'true').lower() == 'true' and TELEGRAM_AVAILABLE  # Default to true


class TelegramAlerter:
    """Sends fraud alerts via Telegram"""
    
    def __init__(self):
        self.bot = None
        self.chat_id = TELEGRAM_CHAT_ID
        
        if USE_TELEGRAM and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.bot = telegram.Bot(token=TELEGRAM_TOKEN)
                logger.info("Telegram bot initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Telegram bot: {e}")
                self.bot = None
        elif USE_TELEGRAM:
            logger.warning("Telegram alerting enabled but token or chat ID is missing")
    
    def is_available(self) -> bool:
        """Check if Telegram alerting is available"""
        return self.bot is not None and self.chat_id is not None
    
    def format_fraud_message(self, transaction: Dict) -> str:
        """Format a fraud alert message for Telegram"""
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
        
        # Build the message
        message = f"🚨 *FRAUD ALERT* 🚨\n\n"
        message += f"*Transaction ID:* `{transaction['transaction_id']}`\n"
        message += f"*Time:* {time_str}\n"
        message += f"*Card:* {card_number}\n"
        message += f"*Amount:* ${transaction['amount']:.2f}\n"
        message += f"*Merchant:* {transaction['merchant_name']}\n"
        message += f"*Location:* {transaction['city']}, {transaction['country']}\n"
        message += f"\n*Fraud Detected:*{fraud_types}\n"
        
        if 'fraud_score' in transaction:
            message += f"\n*Fraud Score:* {transaction['fraud_score']:.2f}\n"
        
        message += "\n⚠️ Please check your account for unauthorized transactions."
        
        return message
    
    def send_fraud_alert(self, transaction: Dict) -> bool:
        """Send a fraud alert via Telegram"""
        if not self.is_available():
            logger.warning("Telegram alerting is not available")
            return False
        
        try:
            # Format the message
            message = self.format_fraud_message(transaction)
            
            # Send the message
            self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=telegram.ParseMode.MARKDOWN
            )
            
            logger.info(f"Sent fraud alert for transaction {transaction['transaction_id']} via Telegram")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")
            return False


# Singleton instance
_telegram_alerter = None


def get_telegram_alerter() -> TelegramAlerter:
    """Get the singleton TelegramAlerter instance"""
    global _telegram_alerter
    if _telegram_alerter is None:
        _telegram_alerter = TelegramAlerter()
    return _telegram_alerter


def send_fraud_alert(transaction: Dict) -> bool:
    """Send a fraud alert for a transaction"""
    alerter = get_telegram_alerter()
    return alerter.send_fraud_alert(transaction)


if __name__ == "__main__":
    # Test the Telegram alerter
    if not USE_TELEGRAM:
        print("Telegram alerting is disabled or not available")
        print("Please set USE_TELEGRAM=true and provide TELEGRAM_TOKEN and TELEGRAM_CHAT_ID")
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
    alerter = get_telegram_alerter()
    if alerter.is_available():
        success = alerter.send_fraud_alert(test_transaction)
        if success:
            print("Test alert sent successfully!")
        else:
            print("Failed to send test alert")
    else:
        print("Telegram alerter is not available")