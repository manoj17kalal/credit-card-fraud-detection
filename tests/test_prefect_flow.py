#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Prefect Flow

This module contains unit tests for the Prefect flow orchestration
that handles scheduling and running tasks for the credit card fraud detection system.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the Prefect flow components
with patch('prefect.flow'):
    with patch('prefect.task'):
        from scheduler.prefect_flow import (
            get_db_connection,
            query_hourly_fraud_data,
            query_fraud_by_category,
            query_fraud_by_country,
            generate_daily_report,
            send_email_report,
            clean_old_transactions,
            optimize_database,
            check_system_health,
            daily_reporting_flow,
            weekly_maintenance_flow,
            hourly_health_check_flow,
            deploy_flows
        )


class TestPrefectFlow(unittest.TestCase):
    """Test cases for the Prefect flow orchestration"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock database connection
        self.mock_db = MagicMock()
        
        # Create sample fraud data by hour
        self.sample_hourly_data = pd.DataFrame({
            'hour': list(range(24)),
            'fraud_count': [0] * 24,
            'total_amount': [0.0] * 24
        })
        # Set some values for testing
        self.sample_hourly_data.loc[3, 'fraud_count'] = 1
        self.sample_hourly_data.loc[3, 'total_amount'] = 5000.0
        self.sample_hourly_data.loc[5, 'fraud_count'] = 1
        self.sample_hourly_data.loc[5, 'total_amount'] = 6000.0
        
        # Create sample fraud data by category
        self.sample_category_data = pd.DataFrame({
            'merchant_category': ['Travel', 'Dining', 'Electronics'],
            'fraud_count': [1, 1, 0],
            'total_amount': [5000.0, 6000.0, 0.0]
        })
        
        # Create sample fraud data by country
        self.sample_country_data = pd.DataFrame({
            'country': ['UK', 'Mexico', 'Brazil'],
            'fraud_count': [1, 1, 0],
            'total_amount': [5000.0, 6000.0, 0.0]
        })
    
    @patch('scheduler.prefect_flow.psycopg2.connect')
    def test_get_db_connection(self, mock_connect):
        """Test database connection function"""
        # Set up the mock
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Call the function with test parameters
        conn = get_db_connection(
            host='test-host',
            database='test-db',
            user='test-user',
            password='test-pass'
        )
        
        # Verify the connection was established with the right parameters
        mock_connect.assert_called_once_with(
            host='test-host',
            database='test-db',
            user='test-user',
            password='test-pass'
        )
        self.assertEqual(conn, mock_conn)
    
    def test_query_hourly_fraud_data(self):
        """Test querying hourly fraud data"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.hour, row.fraud_count, row.total_amount)
            for _, row in self.sample_hourly_data.iterrows()
        ]
        mock_cursor.description = [
            ('hour',), ('fraud_count',), ('total_amount',)
        ]
        
        # Call the function
        result = query_hourly_fraud_data(self.mock_db, date=datetime.now().date())
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('EXTRACT(HOUR FROM timestamp)', query)
        self.assertIn('GROUP BY hour', query)
        self.assertIn('ORDER BY hour', query)
        
        # Verify the result
        self.assertEqual(len(result), 24)  # 24 hours in a day
        self.assertListEqual(list(result.columns), ['hour', 'fraud_count', 'total_amount'])
    
    def test_query_fraud_by_category(self):
        """Test querying fraud data by merchant category"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.merchant_category, row.fraud_count, row.total_amount)
            for _, row in self.sample_category_data.iterrows()
        ]
        mock_cursor.description = [
            ('merchant_category',), ('fraud_count',), ('total_amount',)
        ]
        
        # Call the function
        result = query_fraud_by_category(self.mock_db, date=datetime.now().date())
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('GROUP BY merchant_category', query)
        self.assertIn('ORDER BY fraud_count DESC', query)
        
        # Verify the result
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result.columns), ['merchant_category', 'fraud_count', 'total_amount'])
    
    def test_query_fraud_by_country(self):
        """Test querying fraud data by country"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Configure the mock to return our sample data
        mock_cursor.fetchall.return_value = [
            (row.country, row.fraud_count, row.total_amount)
            for _, row in self.sample_country_data.iterrows()
        ]
        mock_cursor.description = [
            ('country',), ('fraud_count',), ('total_amount',)
        ]
        
        # Call the function
        result = query_fraud_by_country(self.mock_db, date=datetime.now().date())
        
        # Verify the SQL query
        mock_cursor.execute.assert_called_once()
        query = mock_cursor.execute.call_args[0][0]
        self.assertIn('SELECT', query)
        self.assertIn('GROUP BY country', query)
        self.assertIn('ORDER BY fraud_count DESC', query)
        
        # Verify the result
        self.assertEqual(len(result), 3)
        self.assertListEqual(list(result.columns), ['country', 'fraud_count', 'total_amount'])
    
    @patch('scheduler.prefect_flow.query_hourly_fraud_data')
    @patch('scheduler.prefect_flow.query_fraud_by_category')
    @patch('scheduler.prefect_flow.query_fraud_by_country')
    @patch('scheduler.prefect_flow.FraudReportGenerator')
    def test_generate_daily_report(self, mock_report_generator, mock_country, mock_category, mock_hourly):
        """Test generating a daily report"""
        # Set up the mocks
        mock_hourly.return_value = self.sample_hourly_data
        mock_category.return_value = self.sample_category_data
        mock_country.return_value = self.sample_country_data
        
        mock_generator = MagicMock()
        mock_report_generator.return_value = mock_generator
        mock_generator.generate_daily_report.return_value = 'test_report.pdf'
        
        # Call the function
        result = generate_daily_report(
            db_connection=self.mock_db,
            report_date=datetime.now().date(),
            output_dir='/tmp/reports'
        )
        
        # Verify the mocks were called with the right parameters
        mock_hourly.assert_called_once_with(self.mock_db, date=ANY)
        mock_category.assert_called_once_with(self.mock_db, date=ANY)
        mock_country.assert_called_once_with(self.mock_db, date=ANY)
        
        # Verify the report generator was initialized and called correctly
        mock_report_generator.assert_called_once_with(self.mock_db)
        mock_generator.generate_daily_report.assert_called_once_with(
            self.sample_hourly_data,
            self.sample_category_data,
            self.sample_country_data,
            ANY,  # report_date
            '/tmp/reports'
        )
        
        # Verify the result
        self.assertEqual(result, 'test_report.pdf')
    
    @patch('scheduler.prefect_flow.EmailAlerter')
    def test_send_email_report(self, mock_email_alerter):
        """Test sending an email report"""
        # Set up the mock
        mock_alerter = MagicMock()
        mock_email_alerter.return_value = mock_alerter
        
        # Call the function
        send_email_report(
            report_path='test_report.pdf',
            report_date=datetime.now().date(),
            recipients=['test@example.com']
        )
        
        # Verify the email alerter was initialized and called correctly
        mock_email_alerter.assert_called_once()
        mock_alerter.send_report.assert_called_once_with(
            'test_report.pdf',
            ANY,  # subject
            ['test@example.com']
        )
    
    def test_clean_old_transactions(self):
        """Test cleaning old transactions"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call the function
        result = clean_old_transactions(
            db_connection=self.mock_db,
            days_to_keep=30
        )
        
        # Verify the SQL queries
        self.assertEqual(mock_cursor.execute.call_count, 2)  # Two DELETE queries
        
        # Check the first query (transactions)
        query1 = mock_cursor.execute.call_args_list[0][0][0]
        self.assertIn('DELETE FROM transactions', query1)
        self.assertIn('timestamp <', query1)
        
        # Check the second query (fraudulent_transactions)
        query2 = mock_cursor.execute.call_args_list[1][0][0]
        self.assertIn('DELETE FROM fraudulent_transactions', query2)
        self.assertIn('timestamp <', query2)
        
        # Verify the commit was called
        self.mock_db.commit.assert_called_once()
    
    def test_optimize_database(self):
        """Test optimizing the database"""
        # Set up the mock cursor
        mock_cursor = MagicMock()
        self.mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Call the function
        result = optimize_database(db_connection=self.mock_db)
        
        # Verify the SQL queries
        self.assertEqual(mock_cursor.execute.call_count, 3)  # Three VACUUM queries
        
        # Check the queries
        tables = ['transactions', 'fraudulent_transactions', 'user_cards']
        for i, table in enumerate(tables):
            query = mock_cursor.execute.call_args_list[i][0][0]
            self.assertIn(f'VACUUM ANALYZE {table}', query)
    
    @patch('scheduler.prefect_flow.psutil')
    def test_check_system_health(self, mock_psutil):
        """Test checking system health"""
        # Set up the mocks
        mock_psutil.virtual_memory.return_value.percent = 50.0
        mock_psutil.cpu_percent.return_value = 30.0
        mock_psutil.disk_usage.return_value.percent = 40.0
        
        # Call the function
        result = check_system_health()
        
        # Verify the result
        self.assertIsInstance(result, dict)
        self.assertEqual(result['memory_usage_percent'], 50.0)
        self.assertEqual(result['cpu_usage_percent'], 30.0)
        self.assertEqual(result['disk_usage_percent'], 40.0)
        self.assertEqual(result['status'], 'healthy')
    
    @patch('scheduler.prefect_flow.get_db_connection')
    @patch('scheduler.prefect_flow.generate_daily_report')
    @patch('scheduler.prefect_flow.send_email_report')
    def test_daily_reporting_flow(self, mock_send_email, mock_generate_report, mock_get_db):
        """Test the daily reporting flow"""
        # Set up the mocks
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        mock_generate_report.return_value = 'test_report.pdf'
        
        # Call the function
        daily_reporting_flow()
        
        # Verify the mocks were called
        mock_get_db.assert_called_once()
        mock_generate_report.assert_called_once_with(
            db_connection=mock_db,
            report_date=ANY,
            output_dir=ANY
        )
        mock_send_email.assert_called_once_with(
            report_path='test_report.pdf',
            report_date=ANY,
            recipients=ANY
        )
    
    @patch('scheduler.prefect_flow.get_db_connection')
    @patch('scheduler.prefect_flow.clean_old_transactions')
    @patch('scheduler.prefect_flow.optimize_database')
    def test_weekly_maintenance_flow(self, mock_optimize, mock_clean, mock_get_db):
        """Test the weekly maintenance flow"""
        # Set up the mocks
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        
        # Call the function
        weekly_maintenance_flow()
        
        # Verify the mocks were called
        mock_get_db.assert_called_once()
        mock_clean.assert_called_once_with(
            db_connection=mock_db,
            days_to_keep=30
        )
        mock_optimize.assert_called_once_with(db_connection=mock_db)
    
    @patch('scheduler.prefect_flow.check_system_health')
    @patch('scheduler.prefect_flow.EmailAlerter')
    def test_hourly_health_check_flow(self, mock_email_alerter, mock_check_health):
        """Test the hourly health check flow"""
        # Set up the mocks
        mock_alerter = MagicMock()
        mock_email_alerter.return_value = mock_alerter
        
        # Test with healthy system
        mock_check_health.return_value = {
            'memory_usage_percent': 50.0,
            'cpu_usage_percent': 30.0,
            'disk_usage_percent': 40.0,
            'status': 'healthy'
        }
        
        # Call the function
        hourly_health_check_flow()
        
        # Verify the health check was called
        mock_check_health.assert_called_once()
        
        # Verify no alert was sent for healthy system
        mock_email_alerter.assert_not_called()
        
        # Reset the mocks
        mock_check_health.reset_mock()
        mock_email_alerter.reset_mock()
        
        # Test with unhealthy system
        mock_check_health.return_value = {
            'memory_usage_percent': 95.0,
            'cpu_usage_percent': 90.0,
            'disk_usage_percent': 40.0,
            'status': 'warning'
        }
        
        # Call the function
        hourly_health_check_flow()
        
        # Verify the health check was called
        mock_check_health.assert_called_once()
        
        # Verify an alert was sent for unhealthy system
        mock_email_alerter.assert_called_once()
        mock_alerter.send_alert.assert_called_once()
    
    @patch('scheduler.prefect_flow.Deployment')
    def test_deploy_flows(self, mock_deployment):
        """Test deploying the flows"""
        # Set up the mock
        mock_deployment_instance = MagicMock()
        mock_deployment.return_value = mock_deployment_instance
        
        # Call the function
        deploy_flows()
        
        # Verify the deployments were created
        self.assertEqual(mock_deployment.call_count, 3)  # Three flows
        
        # Check the first deployment (daily reporting)
        args1, kwargs1 = mock_deployment.call_args_list[0]
        self.assertEqual(kwargs1['name'], 'daily-fraud-report')
        self.assertEqual(kwargs1['schedule'].cron, '0 6 * * *')  # 6 AM daily
        
        # Check the second deployment (weekly maintenance)
        args2, kwargs2 = mock_deployment.call_args_list[1]
        self.assertEqual(kwargs2['name'], 'weekly-db-maintenance')
        self.assertEqual(kwargs2['schedule'].cron, '0 1 * * 0')  # 1 AM Sunday
        
        # Check the third deployment (hourly health check)
        args3, kwargs3 = mock_deployment.call_args_list[2]
        self.assertEqual(kwargs3['name'], 'hourly-health-check')
        self.assertEqual(kwargs3['schedule'].interval, 3600)  # Every hour
        
        # Verify the deployments were applied
        self.assertEqual(mock_deployment_instance.apply.call_count, 3)


# Main test runner
if __name__ == '__main__':
    unittest.main()