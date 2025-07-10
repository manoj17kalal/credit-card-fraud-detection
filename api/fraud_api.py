#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
FastAPI Endpoint for Credit Card Fraud Detection System

This module provides API endpoints to fetch fraud transaction data.
It can be deployed separately from the main application.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import psycopg2
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'creditcard')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

# API configuration
API_KEY = os.getenv('API_KEY', 'your-api-key-here')

# Create FastAPI app
app = FastAPI(
    title="Credit Card Fraud Detection API",
    description="API for accessing credit card fraud detection data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


# Pydantic models for API responses
class FraudTransaction(BaseModel):
    """Model for a fraudulent transaction"""
    transaction_id: str = Field(..., description="Unique transaction identifier")
    timestamp: datetime = Field(..., description="Transaction timestamp")
    card_number: str = Field(..., description="Masked credit card number")
    amount: float = Field(..., description="Transaction amount")
    merchant_name: str = Field(..., description="Merchant name")
    merchant_category: str = Field(..., description="Merchant category")
    country: str = Field(..., description="Transaction country")
    city: Optional[str] = Field(None, description="Transaction city")
    fraud_type: str = Field(..., description="Type of fraud detected")
    fraud_score: float = Field(..., description="Fraud confidence score (0-1)")
    detection_timestamp: datetime = Field(..., description="When fraud was detected")


class FraudStats(BaseModel):
    """Model for fraud statistics"""
    total_frauds: int = Field(..., description="Total number of frauds")
    total_amount: float = Field(..., description="Total fraud amount")
    avg_amount: float = Field(..., description="Average fraud amount")
    max_amount: float = Field(..., description="Maximum fraud amount")
    avg_fraud_score: float = Field(..., description="Average fraud score")
    affected_cards: int = Field(..., description="Number of affected cards")


class ErrorResponse(BaseModel):
    """Model for error responses"""
    detail: str


# Dependency for API key validation
async def verify_api_key(api_key: str = Query(..., description="API key for authentication")):
    """Verify the API key"""
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


class DatabaseConnection:
    """Handles database connections and queries"""
    
    def __init__(self):
        self.conn = None
        self.connect()
    
    def connect(self) -> None:
        """Connect to the PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info(f"Connected to database {DB_NAME} on {DB_HOST}")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            self.conn = None
    
    def ensure_connection(self) -> bool:
        """Ensure database connection is active"""
        if self.conn is None:
            self.connect()
        
        if self.conn is None:
            return False
        
        try:
            # Check if connection is still alive
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception:
            # Reconnect if connection is dead
            logger.warning("Database connection lost, reconnecting...")
            try:
                self.conn.close()
            except Exception:
                pass
            self.connect()
            return self.conn is not None
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """Execute a query and return results as a DataFrame"""
        if not self.ensure_connection():
            logger.error("Database connection failed")
            return None
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None
    
    def close(self) -> None:
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")


# Database connection instance
db = DatabaseConnection()


@app.get("/", response_class=JSONResponse)
def root():
    """Root endpoint"""
    return {"message": "Credit Card Fraud Detection API", "version": "1.0.0"}


@app.get(
    "/api/frauds/recent",
    response_model=List[FraudTransaction],
    responses={401: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Get recent fraud transactions",
    description="Retrieve the most recent fraudulent transactions"
)
async def get_recent_frauds(
    limit: int = Query(10, description="Number of transactions to return (max 100)", ge=1, le=100),
    api_key: str = Depends(verify_api_key)
):
    """Get recent fraudulent transactions"""
    # Limit the number of transactions
    if limit > 100:
        limit = 100
    
    query = """
    SELECT 
        transaction_id,
        timestamp,
        card_number,
        amount,
        merchant_name,
        merchant_category,
        country,
        city,
        fraud_type,
        fraud_score,
        detection_timestamp
    FROM 
        fraudulent_transactions
    ORDER BY 
        timestamp DESC
    LIMIT %s
    """
    
    df = db.execute_query(query, (limit,))
    
    if df is None:
        raise HTTPException(status_code=500, detail="Database query failed")
    
    # Convert DataFrame to list of dictionaries
    result = df.to_dict(orient='records')
    return result


@app.get(
    "/api/frauds/stats",
    response_model=FraudStats,
    responses={401: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Get fraud statistics",
    description="Retrieve statistics about fraudulent transactions"
)
async def get_fraud_stats(
    hours: int = Query(24, description="Number of hours to include in statistics", ge=1, le=720),
    api_key: str = Depends(verify_api_key)
):
    """Get fraud statistics for a time period"""
    # Calculate time filter
    time_filter = datetime.now() - timedelta(hours=hours)
    
    query = """
    SELECT 
        COUNT(*) AS total_frauds,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        MAX(amount) AS max_amount,
        AVG(fraud_score) AS avg_fraud_score,
        COUNT(DISTINCT card_number) AS affected_cards
    FROM 
        fraudulent_transactions
    WHERE 
        timestamp >= %s
    """
    
    df = db.execute_query(query, (time_filter,))
    
    if df is None or df.empty:
        raise HTTPException(status_code=500, detail="Database query failed")
    
    # Convert DataFrame row to dictionary
    result = df.iloc[0].to_dict()
    return result


@app.get(
    "/api/frauds/by-country",
    response_class=JSONResponse,
    responses={401: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Get frauds by country",
    description="Retrieve fraud counts grouped by country"
)
async def get_frauds_by_country(
    hours: int = Query(24, description="Number of hours to include", ge=1, le=720),
    limit: int = Query(10, description="Number of countries to return", ge=1, le=100),
    api_key: str = Depends(verify_api_key)
):
    """Get fraud counts by country"""
    # Calculate time filter
    time_filter = datetime.now() - timedelta(hours=hours)
    
    query = """
    SELECT 
        country,
        COUNT(*) AS fraud_count,
        SUM(amount) AS total_amount
    FROM 
        fraudulent_transactions
    WHERE 
        timestamp >= %s
    GROUP BY 
        country
    ORDER BY 
        fraud_count DESC
    LIMIT %s
    """
    
    df = db.execute_query(query, (time_filter, limit))
    
    if df is None:
        raise HTTPException(status_code=500, detail="Database query failed")
    
    # Convert DataFrame to list of dictionaries
    result = df.to_dict(orient='records')
    return result


@app.get(
    "/api/frauds/by-category",
    response_class=JSONResponse,
    responses={401: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Get frauds by merchant category",
    description="Retrieve fraud counts grouped by merchant category"
)
async def get_frauds_by_category(
    hours: int = Query(24, description="Number of hours to include", ge=1, le=720),
    api_key: str = Depends(verify_api_key)
):
    """Get fraud counts by merchant category"""
    # Calculate time filter
    time_filter = datetime.now() - timedelta(hours=hours)
    
    query = """
    SELECT 
        merchant_category,
        COUNT(*) AS fraud_count,
        SUM(amount) AS total_amount
    FROM 
        fraudulent_transactions
    WHERE 
        timestamp >= %s
    GROUP BY 
        merchant_category
    ORDER BY 
        fraud_count DESC
    """
    
    df = db.execute_query(query, (time_filter,))
    
    if df is None:
        raise HTTPException(status_code=500, detail="Database query failed")
    
    # Convert DataFrame to list of dictionaries
    result = df.to_dict(orient='records')
    return result


@app.get(
    "/api/health",
    response_class=JSONResponse,
    summary="API health check",
    description="Check if the API and database connection are working"
)
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "database_connection": False,
        "timestamp": datetime.now().isoformat()
    }
    
    # Check database connection
    if db.ensure_connection():
        health_status["database_connection"] = True
    else:
        health_status["status"] = "degraded"
    
    return health_status


# Shutdown event to close database connection
@app.on_event("shutdown")
def shutdown_event():
    """Close database connection on shutdown"""
    db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fraud_api:app", host="0.0.0.0", port=9000, reload=True)