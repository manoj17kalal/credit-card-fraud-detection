-- Database schema for credit card fraud detection system

-- Create tables

-- All Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    card_number VARCHAR(19) NOT NULL,  -- Masked card number
    amount DECIMAL(10, 2) NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    merchant_name VARCHAR(100) NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(50),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    is_fraudulent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Flagged/Fraudulent Transactions Table
CREATE TABLE IF NOT EXISTS fraudulent_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    card_number VARCHAR(19) NOT NULL,  -- Masked card number
    amount DECIMAL(10, 2) NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    merchant_name VARCHAR(100) NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(50),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    fraud_type VARCHAR(100) NOT NULL,  -- Type of fraud detected
    fraud_score DECIMAL(5, 2),         -- Score indicating confidence of fraud detection
    detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);

-- User Cards Table (for tracking card usage patterns)
CREATE TABLE IF NOT EXISTS user_cards (
    card_id SERIAL PRIMARY KEY,
    card_number VARCHAR(19) NOT NULL UNIQUE,  -- Masked card number
    last_country VARCHAR(50),
    last_city VARCHAR(50),
    last_transaction_timestamp TIMESTAMP,
    transaction_count_last_hour INT DEFAULT 0,
    transaction_count_last_day INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_card_number ON transactions(card_number);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id ON transactions(merchant_id);
CREATE INDEX IF NOT EXISTS idx_transactions_country ON transactions(country);

CREATE INDEX IF NOT EXISTS idx_fraudulent_timestamp ON fraudulent_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_fraudulent_card_number ON fraudulent_transactions(card_number);
CREATE INDEX IF NOT EXISTS idx_fraudulent_fraud_type ON fraudulent_transactions(fraud_type);

-- Create views for dashboard
CREATE OR REPLACE VIEW fraud_summary_hourly AS
SELECT 
    DATE_TRUNC('hour', timestamp) AS hour,
    COUNT(*) AS fraud_count,
    SUM(amount) AS total_fraud_amount,
    AVG(amount) AS avg_fraud_amount,
    COUNT(DISTINCT card_number) AS unique_cards_affected,
    COUNT(DISTINCT merchant_id) AS unique_merchants_affected
FROM fraudulent_transactions
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;

CREATE OR REPLACE VIEW fraud_by_country AS
SELECT 
    country,
    COUNT(*) AS fraud_count,
    SUM(amount) AS total_fraud_amount
FROM fraudulent_transactions
GROUP BY country
ORDER BY fraud_count DESC;

CREATE OR REPLACE VIEW fraud_by_merchant_category AS
SELECT 
    merchant_category,
    COUNT(*) AS fraud_count,
    SUM(amount) AS total_fraud_amount
FROM fraudulent_transactions
GROUP BY merchant_category
ORDER BY fraud_count DESC;