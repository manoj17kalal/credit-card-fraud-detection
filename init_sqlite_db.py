#!/usr/bin/env python3
"""
SQLite Database Initialization Script
Creates the SQLite database and tables for the fraud detection system.
"""

import os
import sys
import logging
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from db.sqlite_handler import SQLiteHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Initialize the SQLite database"""
    try:
        logger.info("Initializing SQLite database...")
        
        # Create SQLite handler which will initialize the database
        sqlite_handler = SQLiteHandler()
        
        logger.info("SQLite database initialized successfully!")
        logger.info(f"Database location: {sqlite_handler.db_path}")
        
        # Test the connection
        conn = sqlite_handler.get_connection()
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('transactions', 'fraudulent_transactions', 'user_cards')
        """)
        tables = cursor.fetchall()
        
        logger.info(f"Created tables: {[table[0] for table in tables]}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error initializing SQLite database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()