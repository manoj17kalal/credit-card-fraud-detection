#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Credit Card Transaction Generator

This script simulates real-time credit card transactions using the Faker library.
It can stream data to Kafka or directly to the processing pipeline via a simple queue.
"""

import os
import time
import uuid
import random
import json
import logging
import threading
import datetime
from queue import Queue
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict

import pandas as pd
from faker import Faker
from dotenv import load_dotenv

# Optional Kafka support
try:
    from kafka import KafkaProducer
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
USE_KAFKA = os.getenv('USE_KAFKA', 'false').lower() == 'true' and KAFKA_AVAILABLE
TRANSACTION_FREQUENCY = float(os.getenv('TRANSACTION_FREQUENCY', '1.5'))  # seconds
FRAUD_PROBABILITY = float(os.getenv('FRAUD_PROBABILITY', '0.05'))  # 5% chance of fraud

# Shared queue for direct streaming (when not using Kafka)
transaction_queue = Queue(maxsize=1000)

@dataclass
class Transaction:
    """Data class representing a credit card transaction"""
    transaction_id: str
    timestamp: str
    card_number: str  # Masked except last 4 digits
    amount: float
    merchant_id: str
    merchant_name: str
    merchant_category: str
    country: str
    city: str
    latitude: float
    longitude: float

    def to_dict(self) -> Dict:
        """Convert transaction to dictionary"""
        return asdict(self)

    def to_json(self) -> str:
        """Convert transaction to JSON string"""
        return json.dumps(self.to_dict())

class TransactionGenerator:
    """Generates simulated credit card transactions"""
    
    def __init__(self):
        self.fake = Faker()
        self.running = False
        self.thread = None
        self.kafka_producer = None
        
        # Load merchant data
        self.merchants = self._generate_merchants(100)
        
        # Load user cards data
        self.user_cards = self._generate_user_cards(1000)
        
        # Initialize Kafka if needed
        if USE_KAFKA:
            self._init_kafka()
    
    def _init_kafka(self) -> None:
        """Initialize Kafka producer"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.kafka_producer = None
    
    def _generate_merchants(self, count: int) -> List[Dict]:
        """Generate a list of fake merchants"""
        merchant_categories = [
            "Grocery", "Restaurant", "Gas Station", "Online Shopping", 
            "Electronics", "Travel", "Entertainment", "Healthcare",
            "Clothing", "Home Improvement"
        ]
        
        merchants = []
        for _ in range(count):
            merchant = {
                'id': str(uuid.uuid4()),
                'name': self.fake.company(),
                'category': random.choice(merchant_categories),
                'country': self.fake.country(),
                'city': self.fake.city(),
                'latitude': float(self.fake.latitude()),
                'longitude': float(self.fake.longitude())
            }
            merchants.append(merchant)
        
        return merchants
    
    def _generate_user_cards(self, count: int) -> List[Dict]:
        """Generate a list of user cards"""
        cards = []
        for _ in range(count):
            # Generate a fake credit card number and mask it
            full_card = self.fake.credit_card_number()
            masked_card = f"{'*' * (len(full_card) - 4)}{full_card[-4:]}"
            
            # Assign a home country and city to the card
            home_country = self.fake.country()
            home_city = self.fake.city()
            
            card = {
                'card_number': masked_card,
                'home_country': home_country,
                'home_city': home_city,
                'last_country': home_country,
                'last_city': home_city,
                'last_transaction_time': None,
                'transaction_count_last_hour': 0
            }
            cards.append(card)
        
        return cards
    
    def _generate_normal_transaction(self) -> Transaction:
        """Generate a normal (non-fraudulent) transaction"""
        # Select a random card and merchant
        card = random.choice(self.user_cards)
        merchant = random.choice(self.merchants)
        
        # Generate transaction amount based on merchant category
        if merchant['category'] in ["Electronics", "Travel"]:
            amount = round(random.uniform(100, 2000), 2)
        elif merchant['category'] in ["Online Shopping", "Clothing", "Home Improvement"]:
            amount = round(random.uniform(20, 500), 2)
        else:
            amount = round(random.uniform(5, 100), 2)
        
        # Create transaction
        transaction = Transaction(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.datetime.now().isoformat(),
            card_number=card['card_number'],
            amount=amount,
            merchant_id=merchant['id'],
            merchant_name=merchant['name'],
            merchant_category=merchant['category'],
            country=card['last_country'],  # Use the card's last country
            city=card['last_city'],        # Use the card's last city
            latitude=float(self.fake.latitude()),
            longitude=float(self.fake.longitude())
        )
        
        # Update card's last transaction info
        card['last_transaction_time'] = datetime.datetime.now()
        card['transaction_count_last_hour'] += 1
        
        return transaction
    
    def _generate_fraudulent_transaction(self) -> Transaction:
        """Generate a fraudulent transaction"""
        # Select a random card and merchant
        card = random.choice(self.user_cards)
        merchant = random.choice(self.merchants)
        
        # Choose a fraud pattern
        fraud_type = random.choice([
            "high_amount",
            "foreign_country",
            "rapid_transactions",
            "midnight_spending"
        ])
        
        if fraud_type == "high_amount":
            # Unusually high transaction amount
            amount = round(random.uniform(5000, 15000), 2)
            country = card['last_country']
            city = card['last_city']
        
        elif fraud_type == "foreign_country":
            # Transaction from a different country
            amount = round(random.uniform(100, 1000), 2)
            country = self.fake.country()
            while country == card['home_country']:
                country = self.fake.country()
            city = self.fake.city()
        
        elif fraud_type == "rapid_transactions":
            # Part of multiple rapid transactions
            amount = round(random.uniform(50, 500), 2)
            country = card['last_country']
            city = card['last_city']
            # Simulate multiple transactions by increasing the count
            card['transaction_count_last_hour'] += 3
        
        else:  # midnight_spending
            # Late night spending
            amount = round(random.uniform(100, 2000), 2)
            country = card['last_country']
            city = card['last_city']
        
        # Create transaction
        transaction = Transaction(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.datetime.now().isoformat(),
            card_number=card['card_number'],
            amount=amount,
            merchant_id=merchant['id'],
            merchant_name=merchant['name'],
            merchant_category=merchant['category'],
            country=country,
            city=city,
            latitude=float(self.fake.latitude()),
            longitude=float(self.fake.longitude())
        )
        
        # Update card's last transaction info
        card['last_transaction_time'] = datetime.datetime.now()
        card['last_country'] = country
        card['last_city'] = city
        
        return transaction
    
    def generate_transaction(self) -> Transaction:
        """Generate a transaction (normal or fraudulent)"""
        if random.random() < FRAUD_PROBABILITY:
            return self._generate_fraudulent_transaction()
        else:
            return self._generate_normal_transaction()
    
    def send_to_kafka(self, transaction: Transaction) -> None:
        """Send transaction to Kafka topic"""
        if self.kafka_producer:
            try:
                self.kafka_producer.send(
                    KAFKA_TOPIC, 
                    value=transaction.to_dict()
                )
                logger.debug(f"Sent transaction {transaction.transaction_id} to Kafka")
            except Exception as e:
                logger.error(f"Failed to send to Kafka: {e}")
    
    def send_to_queue(self, transaction: Transaction) -> None:
        """Send transaction to the shared queue"""
        try:
            transaction_queue.put(transaction.to_dict(), block=False)
            logger.debug(f"Added transaction {transaction.transaction_id} to queue")
        except Exception as e:
            logger.error(f"Failed to add to queue: {e}")
    
    def _generation_loop(self) -> None:
        """Main loop for generating transactions"""
        while self.running:
            try:
                # Generate a transaction
                transaction = self.generate_transaction()
                
                # Send to appropriate destination
                if USE_KAFKA:
                    self.send_to_kafka(transaction)
                else:
                    self.send_to_queue(transaction)
                
                # Log the transaction
                logger.info(f"Generated transaction: {transaction.transaction_id} - "
                           f"${transaction.amount:.2f} at {transaction.merchant_name}")
                
                # Wait for next transaction
                time.sleep(random.uniform(
                    max(0.1, TRANSACTION_FREQUENCY - 0.5),
                    TRANSACTION_FREQUENCY + 0.5
                ))
                
            except Exception as e:
                logger.error(f"Error in transaction generation: {e}")
                time.sleep(1)  # Wait a bit before retrying
    
    def start(self) -> None:
        """Start the transaction generator in a separate thread"""
        if self.thread and self.thread.is_alive():
            logger.warning("Transaction generator is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._generation_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info("Transaction generator started")
    
    def stop(self) -> None:
        """Stop the transaction generator"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)
        if self.kafka_producer:
            self.kafka_producer.close()
        logger.info("Transaction generator stopped")


def get_transaction_queue() -> Queue:
    """Get the shared transaction queue"""
    return transaction_queue


if __name__ == "__main__":
    # Create and start the transaction generator
    generator = TransactionGenerator()
    
    try:
        generator.start()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        generator.stop()