# CSV Upload Feature Guide

**Author: Manoj Kalal**

## Overview

The Credit Card Fraud Detection System now includes a powerful CSV upload feature that allows you to analyze batch transaction data for fraud detection. This feature processes uploaded CSV files and provides comprehensive fraud analysis results.

## How to Use

### Step 1: Prepare Your CSV File

Your CSV file must contain the following columns:
- `transaction_id`: Unique identifier for each transaction
- `timestamp`: Transaction timestamp (format: YYYY-MM-DD HH:MM:SS)
- `card_number`: Credit card number (can be masked like ****-****-****-1234)
- `amount`: Transaction amount (numeric)
- `merchant_id`: Merchant identifier
- `country`: Country where transaction occurred
- `latitude`: Geographic latitude (numeric)
- `longitude`: Geographic longitude (numeric)

### Step 2: Upload and Analyze

1. Open the Streamlit dashboard
2. Look for the "CSV Upload" section in the sidebar
3. Click "Browse files" and select your CSV file
4. Click "Analyze CSV for Fraud" button
5. Wait for the analysis to complete

### Step 3: Review Results

The system will display:
- **Summary Statistics**: Total transactions, fraud count, fraud rate
- **Detailed Tables**: All transactions and flagged fraudulent transactions
- **Visualizations**:
  - Fraud distribution by merchant category
  - Fraud distribution by country
  - Geographic map showing fraud locations
- **Download Option**: Export results as CSV file

## Sample CSV Format

```csv
transaction_id,timestamp,card_number,amount,merchant_id,country,latitude,longitude
TXN001,2024-01-15 10:30:00,****-****-****-1234,150.50,MERCH001,USA,40.7128,-74.0060
TXN002,2024-01-15 10:31:00,****-****-****-5678,5500.00,MERCH002,USA,40.7128,-74.0060
```

## Fraud Detection Rules

The system applies the following fraud detection rules:

1. **High Amount Transactions**: Transactions over $5,000
2. **Rapid Transactions**: More than 3 transactions from the same card within 30 seconds
3. **Geographic Anomalies**: Transactions from different countries within a short time
4. **Late Night Activity**: Transactions between 12 AM and 6 AM
5. **Duplicate Transactions**: Identical amounts from the same card within 5 minutes

## Tips for Best Results

- Ensure your CSV file has all required columns
- Use consistent date format (YYYY-MM-DD HH:MM:SS)
- Include geographic coordinates for location-based fraud detection
- Larger datasets provide more comprehensive analysis

## Troubleshooting

**Error: Missing required columns**
- Check that your CSV has all required column names
- Column names are case-sensitive

**Error: Invalid date format**
- Ensure timestamps are in YYYY-MM-DD HH:MM:SS format

**Error: Invalid numeric values**
- Check that amount, latitude, and longitude contain valid numbers

## Sample Data

A sample CSV file (`sample_transactions.csv`) is included in the project root for testing purposes.