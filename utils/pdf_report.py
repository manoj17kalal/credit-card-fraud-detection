#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PDF Report Generator for Credit Card Fraud Detection System

This module provides functionality to generate PDF reports of fraud statistics
and transactions using the FPDF library.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
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

# Report directory
REPORT_DIR = os.getenv('REPORT_DIR', str(project_root / 'reports'))

# Create report directory if it doesn't exist
Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

# Set matplotlib style
plt.style.use('seaborn-v0_8-whitegrid')


class FraudReportPDF(FPDF):
    """Custom PDF class for fraud reports"""
    
    def __init__(self, title: str = "Credit Card Fraud Report"):
        super().__init__()
        self.title = title
        self.set_auto_page_break(auto=True, margin=15)
        self.add_page()
        self.add_font('DejaVu', '', 'DejaVuSansCondensed.ttf', uni=True)
        self.add_font('DejaVu', 'B', 'DejaVuSansCondensed-Bold.ttf', uni=True)
        self.set_font('DejaVu', 'B', 16)
        self.cell(0, 10, self.title, 0, 1, 'C')
        self.ln(10)
    
    def header(self):
        """Page header"""
        # Skip header on first page as we add title manually
        if self.page_no() == 1:
            return
        
        # Logo
        # self.image('logo.png', 10, 8, 33)
        # Title
        self.set_font('DejaVu', 'B', 12)
        self.cell(0, 10, self.title, 0, 1, 'C')
        # Line break
        self.ln(10)
    
    def footer(self):
        """Page footer"""
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        self.set_font('DejaVu', '', 8)
        # Page number
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')
        # Date
        self.cell(0, 10, datetime.now().strftime('%Y-%m-%d %H:%M'), 0, 0, 'R')
    
    def chapter_title(self, title: str):
        """Add a chapter title"""
        self.set_font('DejaVu', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 6, title, 0, 1, 'L', 1)
        self.ln(4)
    
    def section_title(self, title: str):
        """Add a section title"""
        self.set_font('DejaVu', 'B', 10)
        self.cell(0, 6, title, 0, 1, 'L')
        self.ln(2)
    
    def add_paragraph(self, text: str):
        """Add a paragraph of text"""
        self.set_font('DejaVu', '', 10)
        self.multi_cell(0, 5, text)
        self.ln()
    
    def add_table(self, headers: List[str], data: List[List[Any]], widths: Optional[List[int]] = None):
        """Add a table with headers and data"""
        # Calculate column widths if not provided
        if widths is None:
            page_width = self.w - 2 * self.l_margin
            widths = [page_width / len(headers)] * len(headers)
        
        # Table headers
        self.set_font('DejaVu', 'B', 9)
        self.set_fill_color(200, 220, 255)
        for i, header in enumerate(headers):
            self.cell(widths[i], 7, str(header), 1, 0, 'C', 1)
        self.ln()
        
        # Table data
        self.set_font('DejaVu', '', 9)
        self.set_fill_color(255, 255, 255)
        fill = False
        
        for row in data:
            # Check if we need a page break
            if self.get_y() + 7 > self.page_break_trigger:
                self.add_page()
            
            for i, cell in enumerate(row):
                self.cell(widths[i], 7, str(cell), 1, 0, 'L', fill)
            self.ln()
            fill = not fill  # Alternate row colors
        
        self.ln(5)
    
    def add_chart(self, chart_path: str, caption: str = "", w: int = 180, h: int = 90):
        """Add a chart image with caption"""
        # Check if we need a page break
        if self.get_y() + h + 10 > self.page_break_trigger:
            self.add_page()
        
        # Add chart
        self.image(chart_path, x=None, y=None, w=w, h=h)
        
        # Add caption if provided
        if caption:
            self.set_font('DejaVu', 'I', 8)
            self.cell(0, 5, caption, 0, 1, 'C')
        
        self.ln(5)
    
    def add_summary_box(self, title: str, data: Dict[str, Any]):
        """Add a summary box with key statistics"""
        self.set_font('DejaVu', 'B', 10)
        self.set_fill_color(230, 230, 250)
        self.cell(0, 6, title, 0, 1, 'L', 1)
        
        self.set_font('DejaVu', '', 9)
        self.set_fill_color(245, 245, 255)
        
        # Calculate column widths
        page_width = self.w - 2 * self.l_margin
        col_width = page_width / 2
        
        # Add data in two columns
        row_height = 6
        fill = True
        
        # Split data into two columns
        items = list(data.items())
        mid = len(items) // 2 + len(items) % 2
        
        for i in range(mid):
            # Left column
            key, value = items[i]
            self.cell(col_width / 2, row_height, key, 1, 0, 'L', fill)
            self.cell(col_width / 2, row_height, str(value), 1, 0, 'R', fill)
            
            # Right column if available
            if i + mid < len(items):
                key, value = items[i + mid]
                self.cell(col_width / 2, row_height, key, 1, 0, 'L', fill)
                self.cell(col_width / 2, row_height, str(value), 1, 1, 'R', fill)
            else:
                self.ln()
            
            fill = not fill
        
        self.ln(5)


class FraudReportGenerator:
    """Generates PDF reports for fraud detection"""
    
    def __init__(self, db_connection):
        """Initialize with a database connection"""
        self.db = db_connection
    
    def _format_currency(self, value: float) -> str:
        """Format a value as currency"""
        return f"${value:,.2f}"
    
    def _format_percentage(self, value: float) -> str:
        """Format a value as percentage"""
        return f"{value:.2f}%"
    
    def _create_chart_hourly_fraud(self, df: pd.DataFrame, report_date: datetime.date) -> str:
        """Create chart for hourly fraud activity"""
        plt.figure(figsize=(10, 5))
        
        # Create bar chart for fraud count
        ax1 = plt.gca()
        bars = ax1.bar(
            df['hour'].dt.strftime('%H:%M'), 
            df['fraud_count'], 
            color='skyblue',
            label='Fraud Count'
        )
        ax1.set_xlabel('Hour')
        ax1.set_ylabel('Number of Frauds', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.set_xticklabels(df['hour'].dt.strftime('%H:%M'), rotation=45)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax1.annotate(
                f'{int(height)}',
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha='center', va='bottom'
            )
        
        # Create line chart for total amount on secondary y-axis
        ax2 = ax1.twinx()
        ax2.plot(
            df['hour'].dt.strftime('%H:%M'), 
            df['total_amount'], 
            color='red', 
            marker='o',
            linestyle='-',
            label='Total Amount'
        )
        ax2.set_ylabel('Total Amount ($)', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        
        # Add title and legend
        plt.title(f'Hourly Fraud Activity - {report_date.strftime("%Y-%m-%d")}')
        
        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        plt.tight_layout()
        
        # Save chart
        chart_path = os.path.join(REPORT_DIR, f"hourly_fraud_{report_date.strftime('%Y%m%d')}.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path
    
    def _create_chart_category(self, df: pd.DataFrame, report_date: datetime.date) -> str:
        """Create chart for fraud by merchant category"""
        # Sort by fraud count and take top 10
        df = df.sort_values('fraud_count', ascending=True).tail(10)
        
        plt.figure(figsize=(10, 6))
        bars = plt.barh(df['merchant_category'], df['fraud_count'], color='lightblue')
        
        # Add value labels
        for bar in bars:
            width = bar.get_width()
            plt.text(
                width + 0.3, 
                bar.get_y() + bar.get_height()/2, 
                f'{int(width)}',
                ha='left', va='center'
            )
        
        plt.xlabel('Number of Frauds')
        plt.ylabel('Merchant Category')
        plt.title(f'Fraud by Merchant Category - {report_date.strftime("%Y-%m-%d")}')
        plt.tight_layout()
        
        # Save chart
        chart_path = os.path.join(REPORT_DIR, f"category_fraud_{report_date.strftime('%Y%m%d')}.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path
    
    def _create_chart_country(self, df: pd.DataFrame, report_date: datetime.date) -> str:
        """Create chart for fraud by country"""
        # Sort by fraud count and take top 10
        df = df.sort_values('fraud_count', ascending=False).head(10)
        
        plt.figure(figsize=(10, 5))
        
        # Create bar chart with color gradient based on fraud amount
        bars = plt.bar(
            df['country'], 
            df['fraud_count'],
            color=plt.cm.Blues(df['total_amount'] / df['total_amount'].max())
        )
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width()/2, 
                height + 0.3, 
                f'{int(height)}',
                ha='center', va='bottom'
            )
        
        plt.xlabel('Country')
        plt.ylabel('Number of Frauds')
        plt.title(f'Top 10 Countries by Fraud Count - {report_date.strftime("%Y-%m-%d")}')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save chart
        chart_path = os.path.join(REPORT_DIR, f"country_fraud_{report_date.strftime('%Y%m%d')}.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return chart_path
    
    def generate_daily_report(self, report_date: Optional[datetime.date] = None) -> str:
        """Generate a daily fraud report"""
        logger.info("Generating daily fraud PDF report")
        
        # Use current date if not specified
        if report_date is None:
            report_date = datetime.now().date() - timedelta(days=1)
        
        # Format date for display and filenames
        date_str = report_date.strftime('%Y-%m-%d')
        
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
        
        hourly_data = self.db.execute_query(query, (date_str,))
        
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
        
        category_data = self.db.execute_query(query, (date_str,))
        
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
        
        country_data = self.db.execute_query(query, (date_str,))
        
        # Query for overall statistics
        query = """
        SELECT 
            COUNT(*) AS total_frauds,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            MAX(amount) AS max_amount,
            MIN(amount) AS min_amount,
            AVG(fraud_score) AS avg_fraud_score,
            COUNT(DISTINCT card_number) AS affected_cards,
            COUNT(DISTINCT merchant_category) AS affected_categories
        FROM 
            fraudulent_transactions
        WHERE 
            DATE(timestamp) = %s
        """
        
        stats_data = self.db.execute_query(query, (date_str,))
        
        # Query for top fraudulent transactions
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
            fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            DATE(timestamp) = %s
        ORDER BY 
            amount DESC
        LIMIT 10
        """
        
        top_transactions = self.db.execute_query(query, (date_str,))
        
        # Create charts
        hourly_chart = self._create_chart_hourly_fraud(hourly_data, report_date)
        category_chart = self._create_chart_category(category_data, report_date)
        country_chart = self._create_chart_country(country_data, report_date)
        
        # Create PDF report
        pdf = FraudReportPDF(f"Credit Card Fraud Report - {date_str}")
        
        # Add report date and introduction
        pdf.add_paragraph(
            f"This report provides an analysis of fraudulent credit card transactions detected on {date_str}. "
            f"It includes hourly trends, merchant categories, countries, and top fraudulent transactions."
        )
        
        # Add summary statistics
        if stats_data is not None and not stats_data.empty:
            stats = stats_data.iloc[0]
            
            summary_data = {
                "Total Frauds": int(stats['total_frauds']),
                "Total Amount": self._format_currency(stats['total_amount']),
                "Average Amount": self._format_currency(stats['avg_amount']),
                "Maximum Amount": self._format_currency(stats['max_amount']),
                "Minimum Amount": self._format_currency(stats['min_amount']),
                "Avg Fraud Score": f"{stats['avg_fraud_score']:.2f}",
                "Cards Affected": int(stats['affected_cards']),
                "Categories Affected": int(stats['affected_categories'])
            }
            
            pdf.add_summary_box("Summary Statistics", summary_data)
        
        # Add hourly fraud chart
        pdf.chapter_title("Hourly Fraud Activity")
        pdf.add_paragraph(
            "The chart below shows the number of fraudulent transactions and total fraud amount by hour. "
            "This helps identify peak times for fraudulent activity."
        )
        pdf.add_chart(hourly_chart, "Hourly distribution of fraud transactions")
        
        # Add hourly data table
        pdf.section_title("Hourly Fraud Data")
        
        # Format hourly data for table
        table_data = []
        for _, row in hourly_data.iterrows():
            table_data.append([
                row['hour'].strftime('%H:%M'),
                int(row['fraud_count']),
                self._format_currency(row['total_amount']),
                f"{row['avg_fraud_score']:.2f}"
            ])
        
        pdf.add_table(
            ["Hour", "Fraud Count", "Total Amount", "Avg Fraud Score"],
            table_data,
            [25, 25, 70, 40]
        )
        
        # Add merchant category chart and data
        pdf.add_page()
        pdf.chapter_title("Fraud by Merchant Category")
        pdf.add_paragraph(
            "This section shows the distribution of fraudulent transactions across different merchant categories. "
            "Identifying high-risk categories can help focus fraud prevention efforts."
        )
        pdf.add_chart(category_chart, "Top merchant categories by fraud count")
        
        # Format category data for table
        table_data = []
        for _, row in category_data.head(10).iterrows():
            table_data.append([
                row['merchant_category'],
                int(row['fraud_count']),
                self._format_currency(row['total_amount']),
                f"{row['avg_fraud_score']:.2f}"
            ])
        
        pdf.add_table(
            ["Merchant Category", "Fraud Count", "Total Amount", "Avg Fraud Score"],
            table_data,
            [70, 25, 50, 40]
        )
        
        # Add country chart and data
        pdf.add_page()
        pdf.chapter_title("Fraud by Country")
        pdf.add_paragraph(
            "This section shows the distribution of fraudulent transactions across different countries. "
            "Geographic patterns can reveal important insights about fraud origins."
        )
        pdf.add_chart(country_chart, "Top countries by fraud count")
        
        # Format country data for table
        table_data = []
        for _, row in country_data.iterrows():
            table_data.append([
                row['country'],
                int(row['fraud_count']),
                self._format_currency(row['total_amount']),
                f"{row['avg_fraud_score']:.2f}"
            ])
        
        pdf.add_table(
            ["Country", "Fraud Count", "Total Amount", "Avg Fraud Score"],
            table_data,
            [50, 30, 60, 40]
        )
        
        # Add top fraudulent transactions
        pdf.add_page()
        pdf.chapter_title("Top Fraudulent Transactions")
        pdf.add_paragraph(
            "The table below shows the top fraudulent transactions by amount. "
            "These high-value frauds represent the greatest financial risk."
        )
        
        # Format transaction data for table
        table_data = []
        for _, row in top_transactions.iterrows():
            # Mask card number
            masked_card = row['card_number']
            if len(masked_card) > 8:
                masked_card = masked_card[:4] + "****" + masked_card[-4:]
            
            table_data.append([
                masked_card,
                self._format_currency(row['amount']),
                row['merchant_name'],
                row['fraud_type'],
                f"{row['fraud_score']:.2f}"
            ])
        
        pdf.add_table(
            ["Card Number", "Amount", "Merchant", "Fraud Type", "Score"],
            table_data,
            [40, 40, 50, 40, 20]
        )
        
        # Add conclusion
        pdf.add_paragraph(
            "This report highlights key patterns in fraudulent activity detected by our system. "
            "Regular monitoring of these patterns can help improve fraud detection rules and reduce financial losses."
        )
        
        # Save PDF
        report_file = os.path.join(REPORT_DIR, f"fraud_report_{date_str}.pdf")
        pdf.output(report_file)
        
        logger.info(f"PDF report generated: {report_file}")
        return report_file
    
    def generate_weekly_report(self, end_date: Optional[datetime.date] = None) -> str:
        """Generate a weekly fraud report"""
        logger.info("Generating weekly fraud PDF report")
        
        # Use current date if not specified
        if end_date is None:
            end_date = datetime.now().date() - timedelta(days=1)
        
        # Calculate start date (7 days before end date)
        start_date = end_date - timedelta(days=6)
        
        # Format dates for display and filenames
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        week_str = f"{start_str}_to_{end_str}"
        
        # Query for daily fraud statistics
        query = """
        SELECT 
            DATE(timestamp) AS date,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
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
        
        daily_data = self.db.execute_query(query, (start_str, end_str))
        
        if daily_data is None or daily_data.empty:
            logger.warning(f"No fraud data found for week {week_str}")
            return ""
        
        # Additional queries similar to daily report but for the week period
        # ...
        
        # Create PDF report
        pdf = FraudReportPDF(f"Weekly Credit Card Fraud Report - {week_str}")
        
        # Add report content
        # ...
        
        # Save PDF
        report_file = os.path.join(REPORT_DIR, f"weekly_fraud_report_{week_str}.pdf")
        pdf.output(report_file)
        
        logger.info(f"Weekly PDF report generated: {report_file}")
        return report_file


# Example usage
if __name__ == "__main__":
    # This would be imported from elsewhere in a real application
    from processing.real_time_processor import DatabaseHandler
    
    # Create database connection
    db_handler = DatabaseHandler()
    
    # Create report generator
    report_gen = FraudReportGenerator(db_handler)
    
    # Generate daily report for yesterday
    yesterday = datetime.now().date() - timedelta(days=1)
    report_file = report_gen.generate_daily_report(yesterday)
    
    print(f"Report generated: {report_file}")