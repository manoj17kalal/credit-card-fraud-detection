#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API Endpoints for PDF Report Generation and Download

This module provides FastAPI endpoints to generate and download PDF reports
for the Credit Card Fraud Detection System.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import PDF report generator
from utils.pdf_report import FraudReportGenerator
from api.auth import get_api_key
from processing.real_time_processor import DatabaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/reports", tags=["reports"])

# Report directory
REPORT_DIR = os.getenv('REPORT_DIR', str(project_root / 'reports'))

# Create report directory if it doesn't exist
Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

# Database connection
db_handler = DatabaseHandler()

# Report generator
report_generator = FraudReportGenerator(db_handler)


# Models
class ReportRequest(BaseModel):
    """Request model for report generation"""
    report_type: str  # 'daily' or 'weekly'
    date: Optional[str] = None  # Format: YYYY-MM-DD


class ReportResponse(BaseModel):
    """Response model for report generation"""
    status: str
    report_id: str
    message: str


class ReportStatus(BaseModel):
    """Response model for report status"""
    status: str
    report_id: str
    file_path: Optional[str] = None
    created_at: Optional[str] = None
    error: Optional[str] = None


# In-memory storage for report generation status
# In a production system, this would be stored in a database
report_status = {}


def generate_report_background(report_type: str, date_str: Optional[str], report_id: str):
    """Background task to generate a report"""
    try:
        # Parse date if provided, otherwise use yesterday
        if date_str:
            report_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            report_date = datetime.now().date() - timedelta(days=1)
        
        # Generate report based on type
        if report_type == "daily":
            report_file = report_generator.generate_daily_report(report_date)
        elif report_type == "weekly":
            # For weekly, the date is the end date of the week
            report_file = report_generator.generate_weekly_report(report_date)
        else:
            raise ValueError(f"Unsupported report type: {report_type}")
        
        # Update status
        report_status[report_id] = {
            "status": "completed",
            "file_path": report_file,
            "created_at": datetime.now().isoformat(),
            "error": None
        }
        
        logger.info(f"Report {report_id} generated successfully: {report_file}")
    
    except Exception as e:
        logger.error(f"Error generating report {report_id}: {str(e)}")
        report_status[report_id] = {
            "status": "failed",
            "file_path": None,
            "created_at": datetime.now().isoformat(),
            "error": str(e)
        }


@router.post("/generate", response_model=ReportResponse)
def generate_report(
    request: ReportRequest, 
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key)
):
    """Generate a fraud report asynchronously"""
    # Validate report type
    if request.report_type not in ["daily", "weekly"]:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid report type: {request.report_type}. Must be 'daily' or 'weekly'."
        )
    
    # Validate date format if provided
    if request.date:
        try:
            datetime.strptime(request.date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD."
            )
    
    # Generate unique report ID
    report_id = f"{request.report_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{hash(datetime.now())}"
    
    # Initialize report status
    report_status[report_id] = {
        "status": "processing",
        "file_path": None,
        "created_at": datetime.now().isoformat(),
        "error": None
    }
    
    # Start background task
    background_tasks.add_task(
        generate_report_background,
        request.report_type,
        request.date,
        report_id
    )
    
    return ReportResponse(
        status="processing",
        report_id=report_id,
        message=f"Report generation started. Check status with /reports/status/{report_id}"
    )


@router.get("/status/{report_id}", response_model=ReportStatus)
def check_report_status(report_id: str, api_key: str = Depends(get_api_key)):
    """Check the status of a report generation task"""
    if report_id not in report_status:
        raise HTTPException(
            status_code=404,
            detail=f"Report with ID {report_id} not found"
        )
    
    status_data = report_status[report_id]
    
    return ReportStatus(
        status=status_data["status"],
        report_id=report_id,
        file_path=status_data["file_path"],
        created_at=status_data["created_at"],
        error=status_data["error"]
    )


@router.get("/download/{report_id}")
def download_report(report_id: str, api_key: str = Depends(get_api_key)):
    """Download a generated report"""
    if report_id not in report_status:
        raise HTTPException(
            status_code=404,
            detail=f"Report with ID {report_id} not found"
        )
    
    status_data = report_status[report_id]
    
    if status_data["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report is not ready for download. Current status: {status_data['status']}"
        )
    
    if not status_data["file_path"] or not os.path.exists(status_data["file_path"]):
        raise HTTPException(
            status_code=404,
            detail="Report file not found"
        )
    
    return FileResponse(
        path=status_data["file_path"],
        filename=os.path.basename(status_data["file_path"]),
        media_type="application/pdf"
    )


@router.get("/latest/{report_type}")
def get_latest_report(report_type: str, api_key: str = Depends(get_api_key)):
    """Get the latest generated report of a specific type"""
    if report_type not in ["daily", "weekly"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report type: {report_type}. Must be 'daily' or 'weekly'."
        )
    
    # Find the latest report file
    report_files = []
    for file in os.listdir(REPORT_DIR):
        if file.startswith(f"{report_type}_fraud_report") and file.endswith(".pdf"):
            file_path = os.path.join(REPORT_DIR, file)
            report_files.append((file_path, os.path.getmtime(file_path)))
    
    if not report_files:
        raise HTTPException(
            status_code=404,
            detail=f"No {report_type} reports found"
        )
    
    # Sort by modification time (newest first)
    report_files.sort(key=lambda x: x[1], reverse=True)
    latest_report = report_files[0][0]
    
    return FileResponse(
        path=latest_report,
        filename=os.path.basename(latest_report),
        media_type="application/pdf"
    )


@router.delete("/clean", status_code=204)
def clean_old_reports(days: int = 30, api_key: str = Depends(get_api_key)):
    """Clean reports older than specified days"""
    if days < 1:
        raise HTTPException(
            status_code=400,
            detail="Days parameter must be at least 1"
        )
    
    cutoff_time = datetime.now() - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()
    
    deleted_count = 0
    for file in os.listdir(REPORT_DIR):
        if file.endswith(".pdf"):
            file_path = os.path.join(REPORT_DIR, file)
            if os.path.getmtime(file_path) < cutoff_timestamp:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting file {file_path}: {str(e)}")
    
    # Also clean up the status dictionary
    to_delete = []
    for report_id, status_data in report_status.items():
        if status_data["created_at"]:
            created_at = datetime.fromisoformat(status_data["created_at"])
            if created_at < cutoff_time:
                to_delete.append(report_id)
    
    for report_id in to_delete:
        del report_status[report_id]
    
    return Response(status_code=204)