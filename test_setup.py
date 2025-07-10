#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test Setup Script for Credit Card Fraud Detection System

This script tests the basic components of the system:
1. Database connection
2. Alert systems (Telegram and Email)
3. Transaction processing
"""

import os
import sys
import logging
import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def test_database_connection():
    """Test connection to PostgreSQL database"""
    import psycopg2
    
    logger.info("Testing database connection...")
    
    try:
        # Get database connection parameters from environment variables
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'creditcard')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'postgres')
        
        # Connect to the database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        
        # Test the connection
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        cur.close()
        conn.close()
        
        logger.info(f"Successfully connected to PostgreSQL: {version[0]}")
        return True
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return False


def test_telegram_alerts():
    """Test Telegram alert functionality"""
    logger.info("Testing Telegram alerts...")
    
    try:
        from alerts.telegram_bot import get_telegram_alerter
        
        # Create a test transaction
        test_transaction = {
            'transaction_id': 'test-telegram-123',
            'timestamp': datetime.datetime.now().isoformat(),
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
                'Test alert from setup script'
            ],
            'fraud_score': 0.95
        }
        
        # Get the Telegram alerter and send a test alert
        alerter = get_telegram_alerter()
        if alerter.is_available():
            success = alerter.send_fraud_alert(test_transaction)
            if success:
                logger.info("Telegram test alert sent successfully!")
                return True
            else:
                logger.error("Failed to send Telegram test alert")
                return False
        else:
            logger.warning("Telegram alerter is not available")
            return False
            
    except Exception as e:
        logger.error(f"Error testing Telegram alerts: {e}")
        return False


def test_email_alerts():
    """Test Email alert functionality"""
    logger.info("Testing Email alerts...")
    
    try:
        from alerts.email_alert import get_email_alerter
        
        # Create a test transaction
        test_transaction = {
            'transaction_id': 'test-email-123',
            'timestamp': datetime.datetime.now().isoformat(),
            'card_number': '************5678',
            'amount': 8888.88,
            'merchant_id': 'test-merchant',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Test',
            'country': 'Test Country',
            'city': 'Test City',
            'latitude': 0.0,
            'longitude': 0.0,
            'fraud_types': [
                'High amount: $8888.88',
                'Test alert from setup script'
            ],
            'fraud_score': 0.90
        }
        
        # Get the Email alerter and send a test alert
        alerter = get_email_alerter()
        if alerter.is_available():
            success = alerter.send_fraud_alert(test_transaction)
            if success:
                logger.info("Email test alert sent successfully!")
                return True
            else:
                logger.error("Failed to send Email test alert")
                return False
        else:
            logger.warning("Email alerter is not available")
            return False
            
    except Exception as e:
        logger.error(f"Error testing Email alerts: {e}")
        return False


def test_transaction_processing():
    """Test transaction processing with a simulated fraudulent transaction"""
    logger.info("Testing transaction processing...")
    
    try:
        from processing.real_time_processor import FraudDetector
        
        # Create a test transaction
        test_transaction = {
            'transaction_id': 'test-processing-123',
            'timestamp': datetime.datetime.now().isoformat(),
            'card_number': '************9012',
            'amount': 7777.77,
            'merchant_id': 'test-merchant',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Test',
            'country': 'Test Country',
            'city': 'Test City',
            'latitude': 0.0,
            'longitude': 0.0
        }
        
        # Create a fraud detector and process the transaction
        detector = FraudDetector()
        result = detector.detect_fraud(test_transaction)
        
        # Check if fraud was detected (should be, due to high amount)
        if result['is_fraudulent']:
            logger.info(f"Fraud detection successful: {result['fraud_types']}")
            return True
        else:
            logger.warning("Fraud detection failed for test transaction")
            return False
            
    except Exception as e:
        logger.error(f"Error testing transaction processing: {e}")
        return False


def test_alert_manager():
    """Test the integrated alert manager"""
    logger.info("Testing alert manager...")
    
    try:
        from alerts.alert_manager import send_fraud_alert
        
        # Create a test transaction
        test_transaction = {
            'transaction_id': 'test-manager-123',
            'timestamp': datetime.datetime.now().isoformat(),
            'card_number': '************3456',
            'amount': 6666.66,
            'merchant_id': 'test-merchant',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Test',
            'country': 'Test Country',
            'city': 'Test City',
            'latitude': 0.0,
            'longitude': 0.0,
            'fraud_types': [
                'High amount: $6666.66',
                'Test alert from alert manager'
            ],
            'fraud_score': 0.85
        }
        
        # Send alerts through the alert manager
        results = send_fraud_alert(test_transaction)
        logger.info(f"Alert manager results: {results}")
        
        # Check if at least one alert channel was successful
        if results.get('telegram', False) or results.get('email', False):
            logger.info("Alert manager test successful!")
            return True
        else:
            logger.warning("Alert manager test failed - no alerts were sent")
            return False
            
    except Exception as e:
        logger.error(f"Error testing alert manager: {e}")
        return False


def run_all_tests():
    """Run all tests and report results"""
    results = {}
    
    # Test database connection
    results['database'] = test_database_connection()
    
    # Test alert systems
    results['telegram'] = test_telegram_alerts()
    results['email'] = test_email_alerts()
    results['alert_manager'] = test_alert_manager()
    
    # Test transaction processing
    results['transaction_processing'] = test_transaction_processing()
    
    # Print summary
    logger.info("\n===== TEST RESULTS =====")
    for test_name, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        logger.info(f"{test_name.upper()}: {status}")
    
    # Overall result
    if all(results.values()):
        logger.info("\n🎉 All tests passed! The system is ready to use.")
    else:
        logger.warning("\n⚠️ Some tests failed. Please check the logs for details.")


if __name__ == "__main__":
    logger.info("Starting Credit Card Fraud Detection System setup tests...")
    run_all_tests()