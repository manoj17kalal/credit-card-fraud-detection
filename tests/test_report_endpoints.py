#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit Tests for Report API Endpoints

This module contains unit tests for the FastAPI endpoints in report_endpoints.py
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
from pathlib import Path
from datetime import datetime, timedelta

from fastapi.testclient import TestClient

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import the FastAPI app and router
from api.report_endpoints import router, generate_report_task
from api.fraud_api import app

# Add the router to the app for testing
app.include_router(router, prefix="/api/v1/reports", tags=["reports"])


class TestReportEndpoints(unittest.TestCase):
    """Test cases for the Report API endpoints"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a test client
        self.client = TestClient(app)
        
        # Mock the API key
        self.env_patcher = patch.dict('os.environ', {
            'API_KEY': 'test_api_key'
        })
        self.env_patcher.start()
        
        # Mock the report directory
        self.report_dir = tempfile.mkdtemp()
        self.report_dir_patcher = patch('api.report_endpoints.REPORT_DIR', self.report_dir)
        self.report_dir_patcher.start()
        
        # Create a sample report file
        self.sample_report_path = os.path.join(self.report_dir, 'fraud_report_2023-01-01.pdf')
        with open(self.sample_report_path, 'wb') as f:
            f.write(b'Sample PDF content')
        
        # Mock the FraudReportGenerator
        self.generator_patcher = patch('api.report_endpoints.FraudReportGenerator')
        self.mock_generator_class = self.generator_patcher.start()
        self.mock_generator = MagicMock()
        self.mock_generator_class.return_value = self.mock_generator
        
        # Mock the database connection
        self.db_patcher = patch('api.report_endpoints.get_db_connection')
        self.mock_get_db = self.db_patcher.start()
        self.mock_db = MagicMock()
        self.mock_get_db.return_value = self.mock_db
        
        # Mock the background tasks
        self.background_tasks = {}
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
        self.report_dir_patcher.stop()
        self.generator_patcher.stop()
        self.db_patcher.stop()
        shutil.rmtree(self.report_dir)
    
    def test_generate_daily_report_with_valid_api_key(self):
        """Test the generate daily report endpoint with a valid API key"""
        # Set up the mock to return a report path
        self.mock_generator.generate_daily_report.return_value = self.sample_report_path
        
        # Make the request with the API key
        response = self.client.post(
            "/api/v1/reports/daily",
            json={"date": "2023-01-01"},
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertIn('task_id', data)
        self.assertIn('status', data)
        self.assertEqual(data['status'], 'processing')
    
    def test_generate_daily_report_with_invalid_api_key(self):
        """Test the generate daily report endpoint with an invalid API key"""
        response = self.client.post(
            "/api/v1/reports/daily",
            json={"date": "2023-01-01"},
            headers={"X-API-Key": "invalid_key"}
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "Invalid API key"})
    
    def test_generate_daily_report_without_api_key(self):
        """Test the generate daily report endpoint without an API key"""
        response = self.client.post(
            "/api/v1/reports/daily",
            json={"date": "2023-01-01"}
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"detail": "API key required"})
    
    def test_generate_weekly_report_with_valid_api_key(self):
        """Test the generate weekly report endpoint with a valid API key"""
        # Set up the mock to return a report path
        self.mock_generator.generate_weekly_report.return_value = self.sample_report_path
        
        # Make the request with the API key
        response = self.client.post(
            "/api/v1/reports/weekly",
            json={"end_date": "2023-01-07"},
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertIn('task_id', data)
        self.assertIn('status', data)
        self.assertEqual(data['status'], 'processing')
    
    def test_check_report_status_with_valid_api_key(self):
        """Test the check report status endpoint with a valid API key"""
        # Add a task to the background tasks
        task_id = "test_task_id"
        with patch('api.report_endpoints.BACKGROUND_TASKS', {task_id: {'status': 'completed', 'report_path': self.sample_report_path}}):
            # Make the request with the API key
            response = self.client.get(
                f"/api/v1/reports/status/{task_id}",
                headers={"X-API-Key": "test_api_key"}
            )
            
            # Verify the response
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data['task_id'], task_id)
            self.assertEqual(data['status'], 'completed')
            self.assertEqual(data['report_path'], os.path.basename(self.sample_report_path))
    
    def test_check_report_status_nonexistent_task(self):
        """Test the check report status endpoint with a nonexistent task ID"""
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/reports/status/nonexistent_task",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Task not found"})
    
    def test_download_report_with_valid_api_key(self):
        """Test the download report endpoint with a valid API key"""
        # Make the request with the API key
        response = self.client.get(
            f"/api/v1/reports/download/fraud_report_2023-01-01.pdf",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'Sample PDF content')
        self.assertEqual(response.headers['Content-Type'], 'application/pdf')
        self.assertEqual(response.headers['Content-Disposition'], 'attachment; filename=fraud_report_2023-01-01.pdf')
    
    def test_download_report_nonexistent_file(self):
        """Test the download report endpoint with a nonexistent file"""
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/reports/download/nonexistent.pdf",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Report not found"})
    
    def test_list_reports_with_valid_api_key(self):
        """Test the list reports endpoint with a valid API key"""
        # Make the request with the API key
        response = self.client.get(
            "/api/v1/reports/list",
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)  # Should have our sample report
        self.assertEqual(data[0], 'fraud_report_2023-01-01.pdf')
    
    def test_clean_reports_with_valid_api_key(self):
        """Test the clean reports endpoint with a valid API key"""
        # Make the request with the API key
        response = self.client.delete(
            "/api/v1/reports/clean",
            params={"days": 30},
            headers={"X-API-Key": "test_api_key"}
        )
        
        # Verify the response
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('message', data)
        self.assertIn('deleted_count', data)
        
        # Verify the file was deleted
        self.assertFalse(os.path.exists(self.sample_report_path))
    
    @patch('api.report_endpoints.generate_report_task')
    def test_generate_report_task(self, mock_generate_report_task):
        """Test the generate_report_task function"""
        # Set up the mock to return a report path
        mock_generate_report_task.return_value = self.sample_report_path
        
        # Create a task
        task_id = "test_task_id"
        background_tasks = {task_id: {'status': 'processing', 'report_path': None}}
        
        # Call the function
        with patch('api.report_endpoints.BACKGROUND_TASKS', background_tasks):
            # Use AsyncMock to mock the async function
            async def mock_async_func():
                return self.sample_report_path
            
            mock_generate_report_task.side_effect = mock_async_func
            
            # Run the async function
            import asyncio
            asyncio.run(generate_report_task(task_id, 'daily', '2023-01-01', self.mock_db))
            
            # Verify the task was updated
            self.assertEqual(background_tasks[task_id]['status'], 'completed')
            self.assertEqual(background_tasks[task_id]['report_path'], self.sample_report_path)


if __name__ == '__main__':
    import tempfile
    import shutil
    unittest.main()