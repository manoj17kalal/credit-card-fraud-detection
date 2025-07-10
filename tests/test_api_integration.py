#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Integration Tests for API Components

This module contains integration tests for the FastAPI endpoints
and their interaction with other components of the system.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path
import json
from datetime import datetime, timedelta

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import FastAPI test client
from fastapi.testclient import TestClient

# Import the API modules
with patch('fastapi.Depends'):
    from api.fraud_api import app as fraud_app
    from api.report_endpoints import app as report_app


class TestAPIIntegration(unittest.TestCase):
    """Integration tests for the API components"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create test clients for both APIs
        self.fraud_client = TestClient(fraud_app)
        self.report_client = TestClient(report_app)
        
        # Set up valid API key for testing
        self.valid_api_key = "test-api-key"
        
        # Create a temporary directory for test reports
        self.test_report_dir = Path("./test_reports")
        os.makedirs(self.test_report_dir, exist_ok=True)
        
        # Create a sample PDF file for testing
        self.sample_pdf_path = self.test_report_dir / "sample_report.pdf"
        with open(self.sample_pdf_path, "wb") as f:
            f.write(b"%PDF-1.5\nSample PDF content")
    
    def tearDown(self):
        """Clean up after tests"""
        # Remove test files and directories
        if self.sample_pdf_path.exists():
            os.remove(self.sample_pdf_path)
        
        if self.test_report_dir.exists():
            import shutil
            shutil.rmtree(self.test_report_dir)
    
    @patch('api.fraud_api.get_db_connection')
    @patch('api.fraud_api.API_KEYS', ["test-api-key"])
    def test_fraud_api_integration(self, mock_get_db):
        """Test integration between fraud API endpoints"""
        # Set up the mock database connection
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        
        # Set up mock cursor and data
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Test the health endpoint
        response = self.fraud_client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "healthy", "timestamp": ANY})
        
        # Test the recent frauds endpoint with valid API key
        mock_cursor.fetchall.return_value = [
            ("T001", datetime.now(), "1234****5678", 1000.0, "M001", "Merchant A", "Electronics", "USA", "New York", 40.7128, -74.0060)
        ]
        mock_cursor.description = [
            ("transaction_id",), ("timestamp",), ("card_number",), ("amount",),
            ("merchant_id",), ("merchant_name",), ("merchant_category",),
            ("country",), ("city",), ("latitude",), ("longitude",)
        ]
        
        response = self.fraud_client.get(
            "/api/v1/frauds/recent",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["transaction_id"], "T001")
        
        # Test the fraud statistics endpoint with valid API key
        mock_cursor.fetchall.return_value = [(10, 50000.0, 5)]
        mock_cursor.description = [("total_frauds",), ("total_amount",), ("unique_cards",)]
        
        response = self.fraud_client.get(
            "/api/v1/frauds/statistics",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total_frauds"], 10)
        self.assertEqual(data["total_amount"], 50000.0)
        self.assertEqual(data["unique_cards"], 5)
        
        # Test the frauds by country endpoint with valid API key
        mock_cursor.fetchall.return_value = [
            ("USA", 5, 25000.0),
            ("UK", 3, 15000.0),
            ("Canada", 2, 10000.0)
        ]
        mock_cursor.description = [("country",), ("fraud_count",), ("total_amount",)]
        
        response = self.fraud_client.get(
            "/api/v1/frauds/by-country",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["country"], "USA")
        self.assertEqual(data[0]["fraud_count"], 5)
        
        # Test the frauds by merchant category endpoint with valid API key
        mock_cursor.fetchall.return_value = [
            ("Electronics", 4, 20000.0),
            ("Travel", 3, 15000.0),
            ("Dining", 3, 15000.0)
        ]
        mock_cursor.description = [("merchant_category",), ("fraud_count",), ("total_amount",)]
        
        response = self.fraud_client.get(
            "/api/v1/frauds/by-category",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["merchant_category"], "Electronics")
        self.assertEqual(data[0]["fraud_count"], 4)
        
        # Test with invalid API key
        response = self.fraud_client.get(
            "/api/v1/frauds/recent",
            headers={"X-API-Key": "invalid-key"}
        )
        self.assertEqual(response.status_code, 401)
    
    @patch('api.report_endpoints.get_db_connection')
    @patch('api.report_endpoints.API_KEYS', ["test-api-key"])
    @patch('api.report_endpoints.REPORT_DIR', str(Path("./test_reports")))
    @patch('api.report_endpoints.FraudReportGenerator')
    def test_report_api_integration(self, mock_report_generator, mock_get_db):
        """Test integration between report API endpoints"""
        # Set up the mock database connection
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        
        # Set up mock report generator
        mock_generator = MagicMock()
        mock_report_generator.return_value = mock_generator
        mock_generator.generate_daily_report.return_value = str(self.sample_pdf_path)
        mock_generator.generate_weekly_report.return_value = str(self.sample_pdf_path)
        
        # Test generating a daily report
        response = self.report_client.post(
            "/api/v1/reports/generate/daily",
            headers={"X-API-Key": self.valid_api_key},
            json={"date": datetime.now().strftime("%Y-%m-%d")}
        )
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertIn("task_id", data)
        self.assertIn("status", data)
        self.assertEqual(data["status"], "processing")
        
        # Test generating a weekly report
        response = self.report_client.post(
            "/api/v1/reports/generate/weekly",
            headers={"X-API-Key": self.valid_api_key},
            json={"end_date": datetime.now().strftime("%Y-%m-%d")}
        )
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertIn("task_id", data)
        self.assertIn("status", data)
        self.assertEqual(data["status"], "processing")
        
        # Test checking report status
        # First, add a task to the task_status dictionary
        from api.report_endpoints import task_status
        task_id = "test-task-id"
        task_status[task_id] = {"status": "completed", "report_path": str(self.sample_pdf_path)}
        
        response = self.report_client.get(
            f"/api/v1/reports/status/{task_id}",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "completed")
        self.assertEqual(data["report_path"], str(self.sample_pdf_path))
        
        # Test downloading a report
        response = self.report_client.get(
            f"/api/v1/reports/download/{os.path.basename(self.sample_pdf_path)}",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "application/pdf")
        self.assertEqual(response.content, b"%PDF-1.5\nSample PDF content")
        
        # Test listing reports
        response = self.report_client.get(
            "/api/v1/reports/list",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertIn("sample_report.pdf", data)
        
        # Test cleaning old reports
        response = self.report_client.post(
            "/api/v1/reports/clean",
            headers={"X-API-Key": self.valid_api_key},
            json={"days_to_keep": 7}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("message", data)
        self.assertIn("deleted_count", data)
        
        # Test with invalid API key
        response = self.report_client.get(
            "/api/v1/reports/list",
            headers={"X-API-Key": "invalid-key"}
        )
        self.assertEqual(response.status_code, 401)
    
    @patch('api.fraud_api.get_db_connection')
    @patch('api.report_endpoints.get_db_connection')
    @patch('api.fraud_api.API_KEYS', ["test-api-key"])
    @patch('api.report_endpoints.API_KEYS', ["test-api-key"])
    def test_cross_api_integration(self, mock_report_db, mock_fraud_db):
        """Test integration between fraud and report APIs"""
        # Set up the mock database connections
        mock_fraud_conn = MagicMock()
        mock_fraud_db.return_value = mock_fraud_conn
        
        mock_report_conn = MagicMock()
        mock_report_db.return_value = mock_report_conn
        
        # Set up mock cursor and data for fraud API
        mock_fraud_cursor = MagicMock()
        mock_fraud_conn.cursor.return_value.__enter__.return_value = mock_fraud_cursor
        mock_fraud_cursor.fetchall.return_value = [
            ("T001", datetime.now(), "1234****5678", 1000.0, "M001", "Merchant A", "Electronics", "USA", "New York", 40.7128, -74.0060)
        ]
        mock_fraud_cursor.description = [
            ("transaction_id",), ("timestamp",), ("card_number",), ("amount",),
            ("merchant_id",), ("merchant_name",), ("merchant_category",),
            ("country",), ("city",), ("latitude",), ("longitude",)
        ]
        
        # Test that both APIs use the same API key validation
        # First, check fraud API
        response = self.fraud_client.get(
            "/api/v1/frauds/recent",
            headers={"X-API-Key": self.valid_api_key}
        )
        self.assertEqual(response.status_code, 200)
        
        # Then, check report API
        with patch('api.report_endpoints.REPORT_DIR', str(Path("./test_reports"))):
            response = self.report_client.get(
                "/api/v1/reports/list",
                headers={"X-API-Key": self.valid_api_key}
            )
            self.assertEqual(response.status_code, 200)
        
        # Test with invalid API key on both APIs
        response = self.fraud_client.get(
            "/api/v1/frauds/recent",
            headers={"X-API-Key": "invalid-key"}
        )
        self.assertEqual(response.status_code, 401)
        
        response = self.report_client.get(
            "/api/v1/reports/list",
            headers={"X-API-Key": "invalid-key"}
        )
        self.assertEqual(response.status_code, 401)
    
    @patch('api.fraud_api.get_db_connection')
    @patch('api.fraud_api.API_KEYS', ["test-api-key"])
    def test_cors_middleware(self, mock_get_db):
        """Test CORS middleware configuration"""
        # Set up the mock database connection
        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        
        # Set up mock cursor and data
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [
            ("transaction_id",), ("timestamp",), ("card_number",), ("amount",),
            ("merchant_id",), ("merchant_name",), ("merchant_category",),
            ("country",), ("city",), ("latitude",), ("longitude",)
        ]
        
        # Test CORS headers in response
        response = self.fraud_client.get(
            "/api/v1/frauds/recent",
            headers={
                "X-API-Key": self.valid_api_key,
                "Origin": "http://localhost:8501"  # Simulating request from Streamlit
            }
        )
        self.assertEqual(response.status_code, 200)
        
        # Check CORS headers
        self.assertIn("Access-Control-Allow-Origin", response.headers)
        self.assertIn("Access-Control-Allow-Credentials", response.headers)
        self.assertEqual(response.headers["Access-Control-Allow-Credentials"], "true")
        
        # Test preflight request
        response = self.fraud_client.options(
            "/api/v1/frauds/recent",
            headers={
                "Origin": "http://localhost:8501",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "X-API-Key"
            }
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("Access-Control-Allow-Methods", response.headers)
        self.assertIn("Access-Control-Allow-Headers", response.headers)


# Main test runner
if __name__ == '__main__':
    unittest.main()