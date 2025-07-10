import sqlite3
import os
import logging
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLiteHandler:
    def __init__(self, db_path="fraud_detection.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with schema"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create transactions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    card_number TEXT NOT NULL,
                    amount REAL NOT NULL,
                    merchant_id TEXT NOT NULL,
                    merchant_name TEXT NOT NULL,
                    merchant_category TEXT NOT NULL,
                    country TEXT NOT NULL,
                    city TEXT,
                    latitude REAL,
                    longitude REAL,
                    is_fraudulent INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create fraudulent_transactions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fraudulent_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    card_number TEXT NOT NULL,
                    amount REAL NOT NULL,
                    merchant_id TEXT NOT NULL,
                    merchant_name TEXT NOT NULL,
                    merchant_category TEXT NOT NULL,
                    country TEXT NOT NULL,
                    city TEXT,
                    latitude REAL,
                    longitude REAL,
                    fraud_type TEXT NOT NULL,
                    fraud_score REAL,
                    detection_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
                )
            """)
            
            # Create user_cards table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_cards (
                    card_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    card_number TEXT NOT NULL UNIQUE,
                    last_country TEXT,
                    last_city TEXT,
                    last_transaction_timestamp TEXT,
                    transaction_count_last_hour INTEGER DEFAULT 0,
                    transaction_count_last_day INTEGER DEFAULT 0,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_card_number ON transactions(card_number)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_fraudulent_timestamp ON fraudulent_transactions(timestamp)")
            
            conn.commit()
            conn.close()
            logger.info("SQLite database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing SQLite database: {e}")
            raise
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def insert_transaction(self, transaction_data: Dict[str, Any]):
        """Insert a transaction into the database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO transactions 
                (transaction_id, timestamp, card_number, amount, merchant_id, 
                 merchant_name, merchant_category, country, city, latitude, longitude, is_fraudulent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transaction_data['transaction_id'],
                transaction_data['timestamp'],
                transaction_data['card_number'],
                transaction_data['amount'],
                transaction_data['merchant_id'],
                transaction_data['merchant_name'],
                transaction_data['merchant_category'],
                transaction_data['country'],
                transaction_data.get('city'),
                transaction_data.get('latitude'),
                transaction_data.get('longitude'),
                transaction_data.get('is_fraudulent', False)
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error inserting transaction: {e}")
            raise
    
    def insert_fraudulent_transaction(self, fraud_data: Dict[str, Any]):
        """Insert a fraudulent transaction"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO fraudulent_transactions 
                (transaction_id, timestamp, card_number, amount, merchant_id, 
                 merchant_name, merchant_category, country, city, latitude, longitude, 
                 fraud_type, fraud_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fraud_data['transaction_id'],
                fraud_data['timestamp'],
                fraud_data['card_number'],
                fraud_data['amount'],
                fraud_data['merchant_id'],
                fraud_data['merchant_name'],
                fraud_data['merchant_category'],
                fraud_data['country'],
                fraud_data.get('city'),
                fraud_data.get('latitude'),
                fraud_data.get('longitude'),
                fraud_data['fraud_type'],
                fraud_data.get('fraud_score', 0.0)
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error inserting fraudulent transaction: {e}")
            raise
    
    def get_fraudulent_transactions(self, limit=100):
        """Get recent fraudulent transactions"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM fraudulent_transactions 
                ORDER BY detection_timestamp DESC 
                LIMIT ?
            """, (limit,))
            
            columns = [description[0] for description in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error getting fraudulent transactions: {e}")
            return []
    
    def get_fraud_stats(self):
        """Get fraud statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_frauds,
                    SUM(amount) as total_fraud_amount,
                    AVG(fraud_score) as avg_fraud_score,
                    COUNT(DISTINCT card_number) as affected_cards
                FROM fraudulent_transactions
            """)
            
            result = cursor.fetchone()
            conn.close()
            
            return {
                'total_frauds': result[0] or 0,
                'total_fraud_amount': result[1] or 0.0,
                'avg_fraud_score': result[2] or 0.0,
                'affected_cards': result[3] or 0
            }
            
        except Exception as e:
            logger.error(f"Error getting fraud stats: {e}")
            return {
                'total_frauds': 0,
                'total_fraud_amount': 0.0,
                'avg_fraud_score': 0.0,
                'affected_cards': 0
            }