#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Export Utilities for Credit Card Fraud Detection System

This module provides functionality to export fraud data to various formats
including CSV, Excel, and JSON.
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import pandas as pd
from dotenv import load_dotenv

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Export directory
EXPORT_DIR = os.getenv('EXPORT_DIR', str(project_root / 'exports'))

# Create export directory if it doesn't exist
Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True)


class DataExporter:
    """Exports fraud data to various formats"""
    
    def __init__(self, db_connection):
        """Initialize with a database connection"""
        self.db = db_connection
    
    def _get_fraud_data(self, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
        """Get fraud data for the specified date range"""
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
            latitude,
            longitude,
            fraud_type,
            fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            DATE(timestamp) BETWEEN %s AND %s
        ORDER BY 
            timestamp DESC
        """
        
        data = self.db.execute_query(
            query, 
            (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        
        return data
    
    def _get_fraud_summary(self, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
        """Get fraud summary data for the specified date range"""
        # Daily summary
        daily_query = """
        SELECT 
            DATE(timestamp) AS date,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            AVG(fraud_score) AS avg_fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            DATE(timestamp) BETWEEN %s AND %s
        GROUP BY 
            DATE(timestamp)
        ORDER BY 
            date ASC
        """
        
        daily_data = self.db.execute_query(
            daily_query, 
            (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        
        # Category summary
        category_query = """
        SELECT 
            merchant_category,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
            AVG(fraud_score) AS avg_fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            DATE(timestamp) BETWEEN %s AND %s
        GROUP BY 
            merchant_category
        ORDER BY 
            fraud_count DESC
        """
        
        category_data = self.db.execute_query(
            category_query, 
            (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        
        # Country summary
        country_query = """
        SELECT 
            country,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
            AVG(fraud_score) AS avg_fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            DATE(timestamp) BETWEEN %s AND %s
        GROUP BY 
            country
        ORDER BY 
            fraud_count DESC
        """
        
        country_data = self.db.execute_query(
            country_query, 
            (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        
        return {
            'daily': daily_data,
            'category': category_data,
            'country': country_data
        }
    
    def export_to_csv(self, start_date: datetime.date, end_date: datetime.date) -> Dict[str, str]:
        """Export fraud data to CSV files"""
        logger.info(f"Exporting fraud data to CSV for {start_date} to {end_date}")
        
        # Get data
        fraud_data = self._get_fraud_data(start_date, end_date)
        summary_data = self._get_fraud_summary(start_date, end_date)
        
        # Create date range string for filenames
        date_range = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
        
        # Export detailed fraud data
        fraud_file = os.path.join(EXPORT_DIR, f"fraud_transactions_{date_range}.csv")
        fraud_data.to_csv(fraud_file, index=False)
        
        # Export summary data
        daily_file = os.path.join(EXPORT_DIR, f"fraud_daily_summary_{date_range}.csv")
        summary_data['daily'].to_csv(daily_file, index=False)
        
        category_file = os.path.join(EXPORT_DIR, f"fraud_category_summary_{date_range}.csv")
        summary_data['category'].to_csv(category_file, index=False)
        
        country_file = os.path.join(EXPORT_DIR, f"fraud_country_summary_{date_range}.csv")
        summary_data['country'].to_csv(country_file, index=False)
        
        logger.info(f"CSV export completed: {fraud_file}")
        
        return {
            'transactions': fraud_file,
            'daily_summary': daily_file,
            'category_summary': category_file,
            'country_summary': country_file
        }
    
    def export_to_excel(self, start_date: datetime.date, end_date: datetime.date) -> str:
        """Export fraud data to a single Excel file with multiple sheets"""
        logger.info(f"Exporting fraud data to Excel for {start_date} to {end_date}")
        
        # Get data
        fraud_data = self._get_fraud_data(start_date, end_date)
        summary_data = self._get_fraud_summary(start_date, end_date)
        
        # Create date range string for filename
        date_range = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
        
        # Create Excel file
        excel_file = os.path.join(EXPORT_DIR, f"fraud_report_{date_range}.xlsx")
        
        # Create Excel writer
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            # Write each dataframe to a different worksheet
            fraud_data.to_excel(writer, sheet_name='Transactions', index=False)
            summary_data['daily'].to_excel(writer, sheet_name='Daily Summary', index=False)
            summary_data['category'].to_excel(writer, sheet_name='Category Summary', index=False)
            summary_data['country'].to_excel(writer, sheet_name='Country Summary', index=False)
            
            # Get workbook and add formats
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Format the transaction sheet
            worksheet = writer.sheets['Transactions']
            
            # Set column widths
            worksheet.set_column('A:A', 15)  # transaction_id
            worksheet.set_column('B:B', 20)  # timestamp
            worksheet.set_column('C:C', 20)  # card_number
            worksheet.set_column('D:D', 10)  # amount
            worksheet.set_column('E:E', 25)  # merchant_name
            worksheet.set_column('F:F', 20)  # merchant_category
            worksheet.set_column('G:G', 15)  # country
            worksheet.set_column('H:H', 15)  # city
            worksheet.set_column('I:J', 10)  # lat/long
            worksheet.set_column('K:K', 15)  # fraud_type
            worksheet.set_column('L:L', 10)  # fraud_score
            
            # Write the header with the format
            for col_num, value in enumerate(fraud_data.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Format the daily summary sheet
            worksheet = writer.sheets['Daily Summary']
            worksheet.set_column('A:A', 12)  # date
            worksheet.set_column('B:E', 15)  # metrics
            
            for col_num, value in enumerate(summary_data['daily'].columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Format the category summary sheet
            worksheet = writer.sheets['Category Summary']
            worksheet.set_column('A:A', 25)  # merchant_category
            worksheet.set_column('B:D', 15)  # metrics
            
            for col_num, value in enumerate(summary_data['category'].columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Format the country summary sheet
            worksheet = writer.sheets['Country Summary']
            worksheet.set_column('A:A', 20)  # country
            worksheet.set_column('B:D', 15)  # metrics
            
            for col_num, value in enumerate(summary_data['country'].columns.values):
                worksheet.write(0, col_num, value, header_format)
        
        logger.info(f"Excel export completed: {excel_file}")
        return excel_file
    
    def export_to_json(self, start_date: datetime.date, end_date: datetime.date) -> str:
        """Export fraud data to JSON file"""
        logger.info(f"Exporting fraud data to JSON for {start_date} to {end_date}")
        
        # Get data
        fraud_data = self._get_fraud_data(start_date, end_date)
        summary_data = self._get_fraud_summary(start_date, end_date)
        
        # Create date range string for filename
        date_range = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
        
        # Convert timestamps to strings for JSON serialization
        fraud_data_json = fraud_data.copy()
        if 'timestamp' in fraud_data_json.columns:
            fraud_data_json['timestamp'] = fraud_data_json['timestamp'].astype(str)
        
        # Convert summary data timestamps to strings
        daily_json = summary_data['daily'].copy()
        if 'date' in daily_json.columns:
            daily_json['date'] = daily_json['date'].astype(str)
        
        # Create JSON structure
        export_data = {
            'metadata': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'generated_at': datetime.now().isoformat(),
                'transaction_count': len(fraud_data)
            },
            'transactions': fraud_data_json.to_dict(orient='records'),
            'summary': {
                'daily': daily_json.to_dict(orient='records'),
                'category': summary_data['category'].to_dict(orient='records'),
                'country': summary_data['country'].to_dict(orient='records')
            }
        }
        
        # Write to JSON file
        json_file = os.path.join(EXPORT_DIR, f"fraud_data_{date_range}.json")
        with open(json_file, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"JSON export completed: {json_file}")
        return json_file


# Example usage
if __name__ == "__main__":
    # This would be imported from elsewhere in a real application
    from processing.real_time_processor import DatabaseHandler
    
    # Create database connection
    db_handler = DatabaseHandler()
    
    # Create exporter
    exporter = DataExporter(db_handler)
    
    # Export data for the last 7 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    # Export to different formats
    csv_files = exporter.export_to_csv(start_date, end_date)
    excel_file = exporter.export_to_excel(start_date, end_date)
    json_file = exporter.export_to_json(start_date, end_date)
    
    print(f"CSV files: {csv_files}")
    print(f"Excel file: {excel_file}")
    print(f"JSON file: {json_file}")