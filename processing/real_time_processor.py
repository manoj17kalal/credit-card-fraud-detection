#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Real-Time Transaction Processor

This script processes incoming credit card transactions in real-time,
applies fraud detection rules, and stores results in the database.

It can work with either Kafka or a direct queue from the transaction generator.
"""

import os
import time
import json
import logging
import threading
import datetime
from typing import Dict, List, Optional, Union, Any
from queue import Queue, Empty
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv

# Import the transaction queue from the generator
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_generator.simulate_transactions import get_transaction_queue
from db.sqlite_handler import SQLiteHandler

# Alert functionality disabled as per user request
ALERTS_AVAILABLE = False
logging.info("Alert functionality disabled as per user request. Results will only be displayed on the dashboard.")


# Optional Kafka support
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'credit-card-transactions')
KAFKA_GROUP = os.getenv('KAFKA_GROUP', 'fraud-detector')
USE_KAFKA = os.getenv('USE_KAFKA', 'false').lower() == 'true' and KAFKA_AVAILABLE
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'creditcard')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
PROCESSING_INTERVAL = float(os.getenv('PROCESSING_INTERVAL', '1.0'))  # seconds

# Fraud detection thresholds
HIGH_AMOUNT_THRESHOLD = float(os.getenv('HIGH_AMOUNT_THRESHOLD', '5000.0'))
TRANSACTION_TIME_WINDOW = int(os.getenv('TRANSACTION_TIME_WINDOW', '30'))  # seconds
MAX_TRANSACTIONS_IN_WINDOW = int(os.getenv('MAX_TRANSACTIONS_IN_WINDOW', '3'))
LATE_NIGHT_START_HOUR = int(os.getenv('LATE_NIGHT_START_HOUR', '0'))  # 12 AM
LATE_NIGHT_END_HOUR = int(os.getenv('LATE_NIGHT_END_HOUR', '5'))  # 5 AM


class FraudDetector:
    """Detects fraudulent transactions based on predefined rules"""
    
    def __init__(self):
        # Store recent transactions for each card
        self.card_transactions = defaultdict(list)
        # Store card's last known location
        self.card_locations = {}
    
    def _clean_old_transactions(self, card_number: str, current_time: datetime.datetime) -> None:
        """Remove transactions older than the time window"""
        cutoff_time = current_time - datetime.timedelta(seconds=TRANSACTION_TIME_WINDOW)
        self.card_transactions[card_number] = [
            t for t in self.card_transactions[card_number]
            if datetime.datetime.fromisoformat(t['timestamp']) >= cutoff_time
        ]
    
    def check_high_amount(self, transaction: Dict) -> Optional[str]:
        """Check if transaction amount is unusually high"""
        if transaction['amount'] >= HIGH_AMOUNT_THRESHOLD:
            return f"High amount: ${transaction['amount']:.2f}"
        return None
    
    def check_rapid_transactions(self, transaction: Dict) -> Optional[str]:
        """Check if there are too many transactions in a short time window"""
        card_number = transaction['card_number']
        current_time = datetime.datetime.fromisoformat(transaction['timestamp'])
        
        # Clean old transactions first
        self._clean_old_transactions(card_number, current_time)
        
        # Add current transaction to the list
        self.card_transactions[card_number].append(transaction)
        
        # Check if there are too many transactions in the window
        if len(self.card_transactions[card_number]) > MAX_TRANSACTIONS_IN_WINDOW:
            return f"Too many transactions: {len(self.card_transactions[card_number])} in {TRANSACTION_TIME_WINDOW} seconds"
        
        return None
    
    def check_unusual_location(self, transaction: Dict) -> Optional[str]:
        """Check if transaction is from an unusual location"""
        card_number = transaction['card_number']
        current_country = transaction['country']
        
        if card_number in self.card_locations:
            last_country = self.card_locations[card_number]
            if last_country != current_country:
                # Update location and return fraud alert
                self.card_locations[card_number] = current_country
                return f"Unusual location: {last_country} -> {current_country}"
        
        # Store the location for future reference
        self.card_locations[card_number] = current_country
        return None
    
    def check_duplicate_transaction(self, transaction: Dict) -> Optional[str]:
        """Check for duplicate transactions (same amount, merchant within time window)"""
        card_number = transaction['card_number']
        current_time = datetime.datetime.fromisoformat(transaction['timestamp'])
        
        # Clean old transactions first
        self._clean_old_transactions(card_number, current_time)
        
        # Check for duplicates before adding current transaction
        for past_tx in self.card_transactions[card_number]:
            if (past_tx['merchant_id'] == transaction['merchant_id'] and
                past_tx['amount'] == transaction['amount']):
                return f"Duplicate transaction: ${transaction['amount']:.2f} at {transaction['merchant_name']}"
        
        return None
    
    def check_late_night_spending(self, transaction: Dict) -> Optional[str]:
        """Check for unusual late night spending"""
        current_time = datetime.datetime.fromisoformat(transaction['timestamp'])
        hour = current_time.hour
        
        if LATE_NIGHT_START_HOUR <= hour < LATE_NIGHT_END_HOUR and transaction['amount'] > 100:
            return f"Late night spending: ${transaction['amount']:.2f} at {hour:02d}:{current_time.minute:02d}"
        
        return None
    
    def detect_fraud(self, transaction: Dict) -> Dict:
        """Apply all fraud detection rules to a transaction"""
        fraud_types = []
        fraud_score = 0.0
        
        # Apply each fraud detection rule
        high_amount = self.check_high_amount(transaction)
        if high_amount:
            fraud_types.append(high_amount)
            fraud_score += 0.7
        
        rapid_tx = self.check_rapid_transactions(transaction)
        if rapid_tx:
            fraud_types.append(rapid_tx)
            fraud_score += 0.5
        
        unusual_loc = self.check_unusual_location(transaction)
        if unusual_loc:
            fraud_types.append(unusual_loc)
            fraud_score += 0.8
        
        duplicate_tx = self.check_duplicate_transaction(transaction)
        if duplicate_tx:
            fraud_types.append(duplicate_tx)
            fraud_score += 0.9
        
        late_night = self.check_late_night_spending(transaction)
        if late_night:
            fraud_types.append(late_night)
            fraud_score += 0.3
        
        # Normalize fraud score to be between 0 and 1
        fraud_score = min(fraud_score, 1.0)
        
        # Add fraud detection results to the transaction
        result = transaction.copy()
        result['is_fraudulent'] = len(fraud_types) > 0
        result['fraud_types'] = fraud_types
        result['fraud_score'] = fraud_score
        
        return result


class DatabaseHandler:
    """Handles database operations for storing transactions"""
    
    def __init__(self):
        self.sqlite_handler = SQLiteHandler()
        
        # Batch storage for transactions
        self.transaction_batch = []
        self.fraud_batch = []
        
        logger.info("Connected to SQLite database")
    
    def ensure_connection(self) -> bool:
        """Ensure database connection is active"""
        try:
            # Test the connection
            conn = self.sqlite_handler.get_connection()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"SQLite connection error: {e}")
            return False
    
    def store_transaction(self, transaction: Dict) -> None:
        """Add a transaction to the batch for storage"""
        # Extract relevant fields for the transactions table
        tx_data = {
            'transaction_id': transaction['transaction_id'],
            'timestamp': transaction['timestamp'],
            'card_number': transaction['card_number'],
            'amount': transaction['amount'],
            'merchant_id': transaction['merchant_id'],
            'merchant_name': transaction['merchant_name'],
            'merchant_category': transaction['merchant_category'],
            'country': transaction['country'],
            'city': transaction['city'],
            'latitude': transaction['latitude'],
            'longitude': transaction['longitude'],
            'is_fraudulent': transaction['is_fraudulent']
        }
        
        self.transaction_batch.append(tx_data)
        
        # If it's fraudulent, also add to the fraud batch
        if transaction['is_fraudulent']:
            fraud_data = tx_data.copy()
            fraud_data['fraud_type'] = ', '.join(transaction['fraud_types'])
            fraud_data['fraud_score'] = transaction['fraud_score']
            self.fraud_batch.append(fraud_data)
        
        # Flush batches if they reach the batch size
        if len(self.transaction_batch) >= BATCH_SIZE:
            self.flush_batches()
    
    def flush_batches(self) -> None:
        """Write batched transactions to the database"""
        if not self.ensure_connection():
            logger.error("Cannot flush batches: database connection failed")
            return
        
        try:
            # Insert regular transactions
            if self.transaction_batch:
                for transaction in self.transaction_batch:
                    self.sqlite_handler.insert_transaction(transaction)
                
                logger.info(f"Stored {len(self.transaction_batch)} transactions")
                self.transaction_batch = []
            
            # Insert fraudulent transactions
            if self.fraud_batch:
                for fraud_data in self.fraud_batch:
                    self.sqlite_handler.insert_fraudulent_transaction(fraud_data)
                
                logger.info(f"Stored {len(self.fraud_batch)} fraudulent transactions")
                self.fraud_batch = []
            
        except Exception as e:
            logger.error(f"Error flushing batches to database: {e}")
    
    def close(self) -> None:
        """Close the database connection"""
        try:
            # Flush any remaining batches
            self.flush_batches()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")


class TransactionProcessor:
    """Processes transactions from Kafka or direct queue"""
    
    def __init__(self):
        self.running = False
        self.thread = None
        self.kafka_consumer = None
        self.fraud_detector = FraudDetector()
        self.db_handler = DatabaseHandler()
        
        # Get the transaction queue from the generator
        self.transaction_queue = get_transaction_queue()
        
        # Initialize Kafka if needed
        if USE_KAFKA:
            self._init_kafka()
    
    def _init_kafka(self) -> None:
        """Initialize Kafka consumer"""
        try:
            self.kafka_consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id=KAFKA_GROUP,
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.kafka_consumer = None
    
    def _process_transaction(self, transaction: Dict) -> None:
        """Process a single transaction"""
        try:
            # Apply fraud detection
            result = self.fraud_detector.detect_fraud(transaction)
            
            # Store in database
            self.db_handler.store_transaction(result)
            
            # Log fraud detection and send alerts
            if result['is_fraudulent']:
                logger.warning(f"FRAUD DETECTED: {result['transaction_id']} - "
                             f"${result['amount']:.2f} at {result['merchant_name']} - "
                             f"Types: {', '.join(result['fraud_types'])}")
                
                # Alert functionality disabled - results will only be displayed on the dashboard
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")

    
    def _process_from_kafka(self) -> None:
        """Process transactions from Kafka"""
        for message in self.kafka_consumer:
            if not self.running:
                break
            
            transaction = message.value
            self._process_transaction(transaction)
    
    def _process_from_queue(self) -> None:
        """Process transactions from direct queue"""
        while self.running:
            try:
                # Try to get a transaction from the queue
                transaction = self.transaction_queue.get(block=True, timeout=1.0)
                self._process_transaction(transaction)
                
            except Empty:
                # No transactions in the queue, wait a bit
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error in queue processing: {e}")
                time.sleep(1.0)  # Wait a bit before retrying
    
    def start(self) -> None:
        """Start the transaction processor in a separate thread"""
        if self.thread and self.thread.is_alive():
            logger.warning("Transaction processor is already running")
            return
        
        self.running = True
        
        # Choose processing method based on configuration
        if USE_KAFKA and self.kafka_consumer:
            self.thread = threading.Thread(target=self._process_from_kafka)
        else:
            self.thread = threading.Thread(target=self._process_from_queue)
        
        self.thread.daemon = True
        self.thread.start()
        logger.info("Transaction processor started")
    
    def stop(self) -> None:
        """Stop the transaction processor"""
        self.running = False
        
        if self.thread:
            self.thread.join(timeout=5.0)
        
        if self.kafka_consumer:
            self.kafka_consumer.close()
        
        # Flush remaining transactions and close DB connection
        self.db_handler.close()
        
        logger.info("Transaction processor stopped")


if __name__ == "__main__":
    # Create and start the transaction processor
    processor = TransactionProcessor()
    
    try:
        processor.start()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        processor.stop()