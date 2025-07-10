#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for PDF Report Generator

This module contains unit tests for the FraudReportGenerator class that handles
generating PDF reports of fraud statistics.
"""

import os
import sys
import unittest
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the FraudReportGenerator class
from utils.pdf_report import FraudReportGenerator, FraudReportPDF


class TestFraudReportPDF(unittest.TestCase):
    """Test cases for the FraudReportPDF class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary directory for reports
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a PDF instance
        self.pdf = FraudReportPDF("Test Report")
    
    def tearDown(self):
        """Clean up after tests"""
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_initialization(self):
        """Test PDF initialization"""
        self.assertEqual(self.pdf.title, "Test Report")
        self.assertEqual(self.pdf.page_no(), 1)  # Should start with one page
    
    def test_chapter_title(self):
        """Test adding chapter title"""
        # This is mostly a smoke test since we can't easily check the PDF content
        self.pdf.chapter_title("Test Chapter")
        # No exception should be raised
    
    def test_section_title(self):
        """Test adding section title"""
        self.pdf.section_title("Test Section")
        # No exception should be raised
    
    def test_add_paragraph(self):
        """Test adding paragraph"""
        self.pdf.add_paragraph("This is a test paragraph.")
        # No exception should be raised
    
    def test_add_table(self):
        """Test adding table"""
        headers = ["Column 1", "Column 2", "Column 3"]
        data = [
            ["Row 1, Col 1", "Row 1, Col 2", "Row 1, Col 3"],
            ["Row 2, Col 1", "Row 2, Col 2", "Row 2, Col 3"]
        ]
        self.pdf.add_table(headers, data)
        # No exception should be raised
    
    def test_add_summary_box(self):
        """Test adding summary box"""
        data = {
            "Total": 100,
            "Average": 50.0,
            "Maximum": 100.0,
            "Minimum": 10.0
        }
        self.pdf.add_summary_box("Test Summary", data)
        # No exception should be raised
    
    def test_output(self):
        """Test PDF output"""
        # Add some content
        self.pdf.add_paragraph("Test content for PDF output.")
        
        # Save the PDF
        output_file = os.path.join(self.temp_dir, "test_output.pdf")
        self.pdf.output(output_file)
        
        # Check that the file was created
        self.assertTrue(os.path.exists(output_file))
        self.assertTrue(os.path.getsize(output_file) > 0)  # File should not be empty


class TestFraudReportGenerator(unittest.TestCase):
    """Test cases for the FraudReportGenerator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary directory for reports
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a mock database handler
        self.mock_db = MagicMock()
        
        # Create sample dataframes for mock returns
        # Hourly data
        self.hourly_data = pd.DataFrame({
            'hour': [datetime(2023, 1, 1, i, 0, 0) for i in range(24)],
            'fraud_count': [i % 10 + 1 for i in range(24)],
            'total_amount': [(i % 10 + 1) * 1000.0 for i in range(24)],
            'avg_fraud_score': [0.7 + (i % 3) * 0.1 for i in range(24)]
        })
        
        # Category data
        self.category_data = pd.DataFrame({
            'merchant_category': ['Electronics', 'Travel', 'Retail', 'Dining', 'Entertainment'],
            'fraud_count': [20, 15, 10, 5, 3],
            'total_amount': [30000.0, 22500.0, 15000.0, 7500.0, 4500.0],
            'avg_fraud_score': [0.8, 0.75, 0.7, 0.65, 0.6]
        })
        
        # Country data
        self.country_data = pd.DataFrame({
            'country': ['USA', 'Canada', 'UK', 'Germany', 'France'],
            'fraud_count': [25, 15, 10, 5, 3],
            'total_amount': [37500.0, 22500.0, 15000.0, 7500.0, 4500.0],
            'avg_fraud_score': [0.75, 0.8, 0.85, 0.7, 0.65]
        })
        
        # Stats data
        self.stats_data = pd.DataFrame({
            'total_frauds': [58],
            'total_amount': [87000.0],
            'avg_amount': [1500.0],
            'max_amount': [5000.0],
            'min_amount': [100.0],
            'avg_fraud_score': [0.75],
            'affected_cards': [30],
            'affected_categories': [5]
        })
        
        # Top transactions data
        self.top_transactions = pd.DataFrame({
            'transaction_id': [f'T00{i}' for i in range(1, 11)],
            'timestamp': [datetime.now() - timedelta(hours=i) for i in range(10)],
            'card_number': [f'1234****{i}{i}{i}{i}' for i in range(1, 11)],
            'amount': [5000.0 - i * 100 for i in range(10)],
            'merchant_name': [f'Merchant {i}' for i in range(1, 11)],
            'merchant_category': ['Electronics', 'Travel', 'Retail', 'Dining', 'Entertainment'] * 2,
            'country': ['USA', 'Canada', 'UK', 'Germany', 'France'] * 2,
            'city': [f'City {i}' for i in range(1, 11)],
            'fraud_type': ['High Amount', 'Unusual Location', 'Rapid Transactions', 'Duplicate Transaction', 'Late Night Spending'] * 2,
            'fraud_score': [0.9 - i * 0.05 for i in range(10)]
        })
        
        # Patch the REPORT_DIR in the pdf_report module
        self.patcher = patch('utils.pdf_report.REPORT_DIR', self.temp_dir)
        self.patcher.start()
        
        # Create a FraudReportGenerator instance with the mock database
        self.generator = FraudReportGenerator(self.mock_db)
    
    def tearDown(self):
        """Clean up after tests"""
        # Stop the patcher
        self.patcher.stop()
        
        # Remove the temporary directory
        shutil.rmtree(self.temp_dir)
    
    def test_format_currency(self):
        """Test the _format_currency method"""
        self.assertEqual(self.generator._format_currency(1234.56), "$1,234.56")
        self.assertEqual(self.generator._format_currency(0), "$0.00")
        self.assertEqual(self.generator._format_currency(1000000), "$1,000,000.00")
    
    def test_format_percentage(self):
        """Test the _format_percentage method"""
        self.assertEqual(self.generator._format_percentage(75.5), "75.50%")
        self.assertEqual(self.generator._format_percentage(0), "0.00%")
        self.assertEqual(self.generator._format_percentage(100), "100.00%")
    
    @patch('matplotlib.pyplot.savefig')
    def test_create_chart_hourly_fraud(self, mock_savefig):
        """Test the _create_chart_hourly_fraud method"""
        # Call the method
        report_date = datetime.now().date()
        result = self.generator._create_chart_hourly_fraud(self.hourly_data, report_date)
        
        # Verify the result
        self.assertTrue(result.endswith('.png'))
        self.assertTrue(os.path.dirname(result) == self.temp_dir)
        
        # Verify the mock was called
        mock_savefig.assert_called_once()
    
    @patch('matplotlib.pyplot.savefig')
    def test_create_chart_category(self, mock_savefig):
        """Test the _create_chart_category method"""
        # Call the method
        report_date = datetime.now().date()
        result = self.generator._create_chart_category(self.category_data, report_date)
        
        # Verify the result
        self.assertTrue(result.endswith('.png'))
        self.assertTrue(os.path.dirname(result) == self.temp_dir)
        
        # Verify the mock was called
        mock_savefig.assert_called_once()
    
    @patch('matplotlib.pyplot.savefig')
    def test_create_chart_country(self, mock_savefig):
        """Test the _create_chart_country method"""
        # Call the method
        report_date = datetime.now().date()
        result = self.generator._create_chart_country(self.country_data, report_date)
        
        # Verify the result
        self.assertTrue(result.endswith('.png'))
        self.assertTrue(os.path.dirname(result) == self.temp_dir)
        
        # Verify the mock was called
        mock_savefig.assert_called_once()
    
    @patch('utils.pdf_report.FraudReportPDF.output')
    def test_generate_daily_report_with_data(self, mock_output):
        """Test the generate_daily_report method with data"""
        # Set up the mocks
        self.mock_db.execute_query.side_effect = [
            self.hourly_data,    # For hourly data query
            self.category_data,  # For category data query
            self.country_data,   # For country data query
            self.stats_data,     # For stats data query
            self.top_transactions  # For top transactions query
        ]
        
        # Mock the chart creation methods
        with patch.object(self.generator, '_create_chart_hourly_fraud', return_value=os.path.join(self.temp_dir, 'hourly.png')) as mock_hourly_chart, \
             patch.object(self.generator, '_create_chart_category', return_value=os.path.join(self.temp_dir, 'category.png')) as mock_category_chart, \
             patch.object(self.generator, '_create_chart_country', return_value=os.path.join(self.temp_dir, 'country.png')) as mock_country_chart:
            
            # Call the method
            report_date = datetime.now().date()
            result = self.generator.generate_daily_report(report_date)
            
            # Verify the result
            self.assertTrue(result.endswith('.pdf'))
            self.assertTrue(os.path.dirname(result) == self.temp_dir)
            
            # Verify the mocks were called
            self.assertEqual(self.mock_db.execute_query.call_count, 5)
            mock_hourly_chart.assert_called_once()
            mock_category_chart.assert_called_once()
            mock_country_chart.assert_called_once()
            mock_output.assert_called_once()
    
    def test_generate_daily_report_no_data(self):
        """Test the generate_daily_report method with no data"""
        # Set up the mock to return empty dataframe
        self.mock_db.execute_query.return_value = pd.DataFrame()
        
        # Call the method
        report_date = datetime.now().date()
        result = self.generator.generate_daily_report(report_date)
        
        # Verify the result
        self.assertEqual(result, "")
        
        # Verify the mock was called
        self.mock_db.execute_query.assert_called_once()
    
    @patch('utils.pdf_report.FraudReportPDF.output')
    def test_generate_weekly_report(self, mock_output):
        """Test the generate_weekly_report method"""
        # Set up the mock to return our sample dataframe
        self.mock_db.execute_query.return_value = self.daily_df = pd.DataFrame({
            'date': [datetime.now().date() - timedelta(days=i) for i in range(7)],
            'fraud_count': [10, 15, 8, 12, 9, 7, 11],
            'total_amount': [15000.0, 22500.0, 12000.0, 18000.0, 13500.0, 10500.0, 16500.0],
            'avg_fraud_score': [0.75, 0.8, 0.7, 0.85, 0.65, 0.9, 0.75]
        })
        
        # Call the method
        end_date = datetime.now().date()
        result = self.generator.generate_weekly_report(end_date)
        
        # Verify the result
        self.assertTrue(result.endswith('.pdf'))
        self.assertTrue(os.path.dirname(result) == self.temp_dir)
        
        # Verify the mock was called
        self.mock_db.execute_query.assert_called_once()
        mock_output.assert_called_once()


if __name__ == '__main__':
    unittest.main()