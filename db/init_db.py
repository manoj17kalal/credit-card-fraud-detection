import os
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters from environment variables
db_params = {
    'host': os.environ.get('DB_HOST', 'hopper.proxy.rlwy.net'),
    'database': os.environ.get('DB_NAME', 'railway'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'lSoHicmziijhwJVGBXgsJRJLOBtkMvEo'),
    'port': os.environ.get('DB_PORT', '21941')
}

def init_database():
    """Initialize the database with the schema"""
    try:
        # Connect to the database
        logger.info(f"Connecting to database at {db_params['host']}:{db_params['port']}")
        conn = psycopg2.connect(
            host=db_params['host'],
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            port=db_params['port']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read the schema file
        logger.info("Reading schema file")
        with open('db/schema.sql', 'r') as f:
            schema_sql = f.read()
        
        # Execute the schema SQL
        logger.info("Executing schema SQL")
        cursor.execute(schema_sql)
        
        logger.info("Database initialization completed successfully")
        
        # Close the connection
        cursor.close()
        conn.close()
        
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

if __name__ == "__main__":
    init_database()