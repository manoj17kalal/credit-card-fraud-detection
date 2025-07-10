#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Prefect Flow for Credit Card Fraud Detection System

This script defines the orchestration workflow for the credit card fraud detection system.
It schedules and monitors the following tasks:
- Data generation
- Real-time processing
- Database maintenance
- Report generation
"""

import os
import sys
import time
import logging
import smtplib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.deployments import Deployment

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import project modules
from data_generator.simulate_transactions import TransactionGenerator
from processing.real_time_processor import TransactionProcessor, FraudDetector

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

# Email configuration
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME', '')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')
EMAIL_FROM = os.getenv('EMAIL_FROM', '')
EMAIL_TO = os.getenv('EMAIL_TO', '')

# Report directory
REPORT_DIR = os.getenv('REPORT_DIR', str(project_root / 'reports'))


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
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """Execute a query and return results as a DataFrame"""
        if self.conn is None:
            logger.error("No database connection available")
            return None
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None
    
    def execute_command(self, command: str, params: Optional[tuple] = None) -> bool:
        """Execute a command (INSERT, UPDATE, DELETE) and commit changes"""
        if self.conn is None:
            logger.error("No database connection available")
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(command, params)
            self.conn.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Command execution error: {e}")
            self.conn.rollback()
            return False
    
    def close(self) -> None:
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")


@task(retries=3, retry_delay_seconds=30, cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(hours=1))
def generate_daily_report(report_date: Optional[datetime.date] = None) -> str:
    """Generate a daily fraud report"""
    logger = get_run_logger()
    logger.info("Generating daily fraud report")
    
    # Use current date if not specified
    if report_date is None:
        report_date = datetime.date.today() - datetime.timedelta(days=1)
    
    # Format date for display and filenames
    date_str = report_date.strftime('%Y-%m-%d')
    
    # Create report directory if it doesn't exist
    report_dir = Path(REPORT_DIR)
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    db = DatabaseConnection()
    
    # Query for daily fraud statistics
    query = """
    SELECT 
        DATE_TRUNC('hour', timestamp) AS hour,
        COUNT(*) AS fraud_count,
        SUM(amount) AS total_amount,
        AVG(fraud_score) AS avg_fraud_score
    FROM 
        fraudulent_transactions
    WHERE 
        DATE(timestamp) = %s
    GROUP BY 
        DATE_TRUNC('hour', timestamp)
    ORDER BY 
        hour ASC
    """
    
    hourly_data = db.execute_query(query, (date_str,))
    
    if hourly_data is None or hourly_data.empty:
        logger.warning(f"No fraud data found for {date_str}")
        return ""
    
    # Query for fraud by category
    query = """
    SELECT 
        merchant_category,
        COUNT(*) AS fraud_count,
        SUM(amount) AS total_amount,
        AVG(fraud_score) AS avg_fraud_score
    FROM 
        fraudulent_transactions
    WHERE 
        DATE(timestamp) = %s
    GROUP BY 
        merchant_category
    ORDER BY 
        fraud_count DESC
    """
    
    category_data = db.execute_query(query, (date_str,))
    
    # Query for fraud by country
    query = """
    SELECT 
        country,
        COUNT(*) AS fraud_count,
        SUM(amount) AS total_amount,
        AVG(fraud_score) AS avg_fraud_score
    FROM 
        fraudulent_transactions
    WHERE 
        DATE(timestamp) = %s
    GROUP BY 
        country
    ORDER BY 
        fraud_count DESC
    LIMIT 10
    """
    
    country_data = db.execute_query(query, (date_str,))
    
    # Query for overall statistics
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
        DATE(timestamp) = %s
    """
    
    stats_data = db.execute_query(query, (date_str,))
    
    # Close database connection
    db.close()
    
    # Generate report file
    report_file = report_dir / f"fraud_report_{date_str}.html"
    
    # Create HTML report
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Fraud Report - {date_str}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #2c3e50; }}
            .summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .chart {{ margin-bottom: 30px; }}
        </style>
    </head>
    <body>
        <h1>Credit Card Fraud Detection Report</h1>
        <p>Date: {date_str}</p>
    """
    
    # Add summary statistics
    if stats_data is not None and not stats_data.empty:
        stats = stats_data.iloc[0]
        html_content += f"""
        <div class="summary">
            <h2>Summary</h2>
            <table>
                <tr>
                    <th>Total Frauds</th>
                    <th>Total Amount</th>
                    <th>Average Amount</th>
                    <th>Maximum Amount</th>
                    <th>Average Fraud Score</th>
                    <th>Cards Affected</th>
                </tr>
                <tr>
                    <td>{int(stats['total_frauds'])}</td>
                    <td>${stats['total_amount']:,.2f}</td>
                    <td>${stats['avg_amount']:,.2f}</td>
                    <td>${stats['max_amount']:,.2f}</td>
                    <td>{stats['avg_fraud_score']:.2f}</td>
                    <td>{int(stats['affected_cards'])}</td>
                </tr>
            </table>
        </div>
        """
    
    # Add hourly data
    if hourly_data is not None and not hourly_data.empty:
        html_content += """
        <h2>Hourly Fraud Activity</h2>
        <table>
            <tr>
                <th>Hour</th>
                <th>Fraud Count</th>
                <th>Total Amount</th>
                <th>Avg Fraud Score</th>
            </tr>
        """
        
        for _, row in hourly_data.iterrows():
            hour_str = row['hour'].strftime('%H:00')
            html_content += f"""
            <tr>
                <td>{hour_str}</td>
                <td>{int(row['fraud_count'])}</td>
                <td>${row['total_amount']:,.2f}</td>
                <td>{row['avg_fraud_score']:.2f}</td>
            </tr>
            """
        
        html_content += "</table>"
        
        # Generate hourly chart
        plt.figure(figsize=(10, 6))
        plt.bar(hourly_data['hour'].dt.strftime('%H:00'), hourly_data['fraud_count'])
        plt.title('Hourly Fraud Count')
        plt.xlabel('Hour')
        plt.ylabel('Number of Frauds')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        hourly_chart_file = report_dir / f"hourly_chart_{date_str}.png"
        plt.savefig(hourly_chart_file)
        plt.close()
    
    # Add category data
    if category_data is not None and not category_data.empty:
        html_content += """
        <h2>Fraud by Merchant Category</h2>
        <table>
            <tr>
                <th>Category</th>
                <th>Fraud Count</th>
                <th>Total Amount</th>
                <th>Avg Fraud Score</th>
            </tr>
        """
        
        for _, row in category_data.iterrows():
            html_content += f"""
            <tr>
                <td>{row['merchant_category']}</td>
                <td>{int(row['fraud_count'])}</td>
                <td>${row['total_amount']:,.2f}</td>
                <td>{row['avg_fraud_score']:.2f}</td>
            </tr>
            """
        
        html_content += "</table>"
        
        # Generate category chart
        plt.figure(figsize=(10, 6))
        sns.barplot(x='fraud_count', y='merchant_category', data=category_data.head(10))
        plt.title('Top Merchant Categories by Fraud Count')
        plt.xlabel('Number of Frauds')
        plt.ylabel('Merchant Category')
        plt.tight_layout()
        
        category_chart_file = report_dir / f"category_chart_{date_str}.png"
        plt.savefig(category_chart_file)
        plt.close()
    
    # Add country data
    if country_data is not None and not country_data.empty:
        html_content += """
        <h2>Fraud by Country</h2>
        <table>
            <tr>
                <th>Country</th>
                <th>Fraud Count</th>
                <th>Total Amount</th>
                <th>Avg Fraud Score</th>
            </tr>
        """
        
        for _, row in country_data.iterrows():
            html_content += f"""
            <tr>
                <td>{row['country']}</td>
                <td>{int(row['fraud_count'])}</td>
                <td>${row['total_amount']:,.2f}</td>
                <td>{row['avg_fraud_score']:.2f}</td>
            </tr>
            """
        
        html_content += "</table>"
        
        # Generate country chart
        plt.figure(figsize=(10, 6))
        sns.barplot(x='fraud_count', y='country', data=country_data)
        plt.title('Top Countries by Fraud Count')
        plt.xlabel('Number of Frauds')
        plt.ylabel('Country')
        plt.tight_layout()
        
        country_chart_file = report_dir / f"country_chart_{date_str}.png"
        plt.savefig(country_chart_file)
        plt.close()
    
    # Close HTML
    html_content += """
    </body>
    </html>
    """
    
    # Write report to file
    with open(report_file, 'w') as f:
        f.write(html_content)
    
    logger.info(f"Report generated: {report_file}")
    return str(report_file)


@task(retries=2, retry_delay_seconds=60)
def send_email_report(report_file: str) -> bool:
    """Send the generated report via email"""
    logger = get_run_logger()
    
    if not report_file or not os.path.exists(report_file):
        logger.error(f"Report file not found: {report_file}")
        return False
    
    if not SMTP_USERNAME or not SMTP_PASSWORD or not EMAIL_FROM or not EMAIL_TO:
        logger.error("Email configuration is incomplete")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        
        # Get report date from filename
        report_date = os.path.basename(report_file).replace('fraud_report_', '').replace('.html', '')
        msg['Subject'] = f"Credit Card Fraud Report - {report_date}"
        
        # Add text body
        body = f"Please find attached the credit card fraud report for {report_date}."
        msg.attach(MIMEText(body, 'plain'))
        
        # Read HTML report
        with open(report_file, 'r') as f:
            html_content = f.read()
        
        # Add HTML body
        msg.attach(MIMEText(html_content, 'html'))
        
        # Connect to SMTP server
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        
        # Send email
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email report sent to {EMAIL_TO}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to send email report: {e}")
        return False


@task(retries=3, retry_delay_seconds=30)
def clean_old_data(days: int = 90) -> int:
    """Clean old transaction data from the database"""
    logger = get_run_logger()
    logger.info(f"Cleaning transaction data older than {days} days")
    
    # Calculate cutoff date
    cutoff_date = datetime.date.today() - datetime.timedelta(days=days)
    
    # Connect to database
    db = DatabaseConnection()
    
    # Delete old transactions
    command = """
    DELETE FROM transactions 
    WHERE timestamp < %s
    """
    
    success = db.execute_command(command, (cutoff_date,))
    
    if not success:
        logger.error("Failed to clean old transaction data")
        db.close()
        return 0
    
    # Get number of deleted rows
    query = "SELECT pg_catalog.pg_stat_get_tuples_deleted('transactions'::regclass);"
    result = db.execute_query(query)
    
    # Close database connection
    db.close()
    
    if result is not None and not result.empty:
        deleted_count = result.iloc[0, 0]
        logger.info(f"Deleted {deleted_count} old transaction records")
        return deleted_count
    
    return 0


@task(retries=2, retry_delay_seconds=30)
def optimize_database() -> bool:
    """Optimize database by running VACUUM and ANALYZE"""
    logger = get_run_logger()
    logger.info("Optimizing database")
    
    # Connect to database
    db = DatabaseConnection()
    
    # Run VACUUM ANALYZE
    commands = [
        "VACUUM ANALYZE transactions;",
        "VACUUM ANALYZE fraudulent_transactions;",
        "VACUUM ANALYZE user_cards;"
    ]
    
    success = True
    for command in commands:
        if not db.execute_command(command):
            logger.error(f"Failed to execute: {command}")
            success = False
    
    # Close database connection
    db.close()
    
    if success:
        logger.info("Database optimization completed successfully")
    
    return success


@task(retries=2, retry_delay_seconds=30)
def check_system_health() -> Dict[str, Any]:
    """Check the health of the fraud detection system"""
    logger = get_run_logger()
    logger.info("Checking system health")
    
    health_status = {
        'database_connection': False,
        'transaction_count_24h': 0,
        'fraud_count_24h': 0,
        'avg_processing_time': 0.0,
        'system_status': 'unknown'
    }
    
    # Connect to database
    db = DatabaseConnection()
    
    # Check database connection
    if db.conn is not None:
        health_status['database_connection'] = True
    
    # Get transaction count for last 24 hours
    query = """
    SELECT COUNT(*) FROM transactions 
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
    """
    
    result = db.execute_query(query)
    if result is not None and not result.empty:
        health_status['transaction_count_24h'] = int(result.iloc[0, 0])
    
    # Get fraud count for last 24 hours
    query = """
    SELECT COUNT(*) FROM fraudulent_transactions 
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
    """
    
    result = db.execute_query(query)
    if result is not None and not result.empty:
        health_status['fraud_count_24h'] = int(result.iloc[0, 0])
    
    # Get average processing time
    query = """
    SELECT AVG(EXTRACT(EPOCH FROM (detection_timestamp - timestamp))) 
    FROM fraudulent_transactions 
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
    """
    
    result = db.execute_query(query)
    if result is not None and not result.empty and not pd.isna(result.iloc[0, 0]):
        health_status['avg_processing_time'] = float(result.iloc[0, 0])
    
    # Close database connection
    db.close()
    
    # Determine system status
    if not health_status['database_connection']:
        health_status['system_status'] = 'critical'
    elif health_status['transaction_count_24h'] == 0:
        health_status['system_status'] = 'warning'
    else:
        health_status['system_status'] = 'healthy'
    
    logger.info(f"System health status: {health_status['system_status']}")
    return health_status


@flow(name="Daily Fraud Report Flow")
def daily_report_flow():
    """Daily flow for generating and sending fraud reports"""
    logger = get_run_logger()
    logger.info("Starting daily fraud report flow")
    
    # Generate yesterday's report
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    report_file = generate_daily_report(yesterday)
    
    # Send report via email if generated successfully
    if report_file:
        send_email_report(report_file)
    
    logger.info("Daily fraud report flow completed")


@flow(name="Weekly Database Maintenance Flow")
def weekly_maintenance_flow():
    """Weekly flow for database maintenance tasks"""
    logger = get_run_logger()
    logger.info("Starting weekly database maintenance flow")
    
    # Clean old data (keep last 90 days)
    deleted_count = clean_old_data(90)
    
    # Optimize database
    if deleted_count > 0:
        optimize_database()
    
    logger.info("Weekly database maintenance flow completed")


@flow(name="Hourly Health Check Flow")
def hourly_health_check_flow():
    """Hourly flow for system health checks"""
    logger = get_run_logger()
    logger.info("Starting hourly health check flow")
    
    # Check system health
    health_status = check_system_health()
    
    # Alert if system is not healthy
    if health_status['system_status'] != 'healthy':
        logger.warning(f"System health check failed: {health_status}")
        # TODO: Implement alerting mechanism for system health issues
    
    logger.info("Hourly health check flow completed")


def deploy_flows():
    """Deploy all flows to Prefect"""
    # Deploy daily report flow (runs at 1:00 AM every day)
    Deployment.build_from_flow(
        flow=daily_report_flow,
        name="daily-fraud-report",
        schedule=CronSchedule(cron="0 1 * * *"),
        tags=["fraud-detection", "reporting"]
    )
    
    # Deploy weekly maintenance flow (runs at 2:00 AM every Sunday)
    Deployment.build_from_flow(
        flow=weekly_maintenance_flow,
        name="weekly-db-maintenance",
        schedule=CronSchedule(cron="0 2 * * 0"),
        tags=["fraud-detection", "maintenance"]
    )
    
    # Deploy hourly health check flow (runs every hour)
    Deployment.build_from_flow(
        flow=hourly_health_check_flow,
        name="hourly-health-check",
        schedule=IntervalSchedule(interval=datetime.timedelta(hours=1)),
        tags=["fraud-detection", "monitoring"]
    )


if __name__ == "__main__":
    # If run directly, deploy the flows
    deploy_flows()