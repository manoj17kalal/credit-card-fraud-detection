#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Alert Manager for Credit Card Fraud Detection

This module integrates different alert channels (Telegram, Email) and provides
a unified interface for sending fraud alerts.
"""

import os
import logging
from typing import Dict, List, Optional, Union

from dotenv import load_dotenv

# Import alert modules
from alerts.telegram_bot import send_fraud_alert as send_telegram_alert
from alerts.email_alert import send_fraud_alert as send_email_alert

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def send_fraud_alert(transaction: Dict) -> Dict[str, bool]:
    """
    Send fraud alerts through all available channels
    
    Args:
        transaction: The fraudulent transaction data
        
    Returns:
        Dictionary with status of each alert channel
    """
    results = {}
    
    # Try to send Telegram alert
    try:
        telegram_result = send_telegram_alert(transaction)
        results['telegram'] = telegram_result
        if telegram_result:
            logger.info(f"Telegram alert sent for transaction {transaction['transaction_id']}")
        else:
            logger.warning(f"Failed to send Telegram alert for transaction {transaction['transaction_id']}")
    except Exception as e:
        logger.error(f"Error sending Telegram alert: {e}")
        results['telegram'] = False
    
    # Try to send Email alert
    try:
        email_result = send_email_alert(transaction)
        results['email'] = email_result
        if email_result:
            logger.info(f"Email alert sent for transaction {transaction['transaction_id']}")
        else:
            logger.warning(f"Failed to send Email alert for transaction {transaction['transaction_id']}")
    except Exception as e:
        logger.error(f"Error sending Email alert: {e}")
        results['email'] = False
    
    return results


if __name__ == "__main__":
    # Test the alert manager
    from datetime import datetime
    
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
    
    # Send test alerts
    results = send_fraud_alert(test_transaction)
    print(f"Alert results: {results}")