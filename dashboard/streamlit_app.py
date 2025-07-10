#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Streamlit Dashboard for Credit Card Fraud Detection

This application provides a real-time dashboard for monitoring credit card fraud.
It connects to the database and displays transactions, fraud statistics, and visualizations.

Author: Manoj Kalal
"""

import os
import time
import json
import logging
import datetime
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import folium_static
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.sqlite_handler import SQLiteHandler
from processing.real_time_processor import FraudDetector

# Optional authentication
try:
    import streamlit_authenticator as stauth
    AUTH_AVAILABLE = True
except ImportError:
    AUTH_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'creditcard')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
USE_AUTH = os.getenv('USE_AUTH', 'false').lower() == 'true' and AUTH_AVAILABLE
REFRESH_INTERVAL = int(os.getenv('REFRESH_INTERVAL', '5'))  # seconds

# Page configuration
st.set_page_config(
    page_title="Credit Card Fraud Detection Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Authentication configuration
if USE_AUTH:
    # Default credentials (should be changed in production)
    default_username = os.getenv('AUTH_USERNAME', 'admin')
    default_password = os.getenv('AUTH_PASSWORD', 'admin')
    default_name = os.getenv('AUTH_NAME', 'Admin User')
    
    # Create a hashed password
    hashed_passwords = stauth.Hasher([default_password]).generate()
    
    # Authentication configuration
    auth_config = {
        'credentials': {
            'usernames': {
                default_username: {
                    'name': default_name,
                    'password': hashed_passwords[0]
                }
            }
        },
        'cookie': {
            'name': 'fraud_dashboard_auth',
            'key': 'some_signature_key',
            'expiry_days': 30
        }
    }
    
    # Create the authenticator
    authenticator = stauth.Authenticate(
        auth_config['credentials'],
        auth_config['cookie']['name'],
        auth_config['cookie']['key'],
        auth_config['cookie']['expiry_days']
    )


class DatabaseConnection:
    """Handles database connections and queries"""
    
    def __init__(self):
        try:
            self.sqlite_handler = SQLiteHandler()
            logger.info("Connected to SQLite database")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            st.error(f"Failed to connect to database: {e}")
            self.sqlite_handler = None
    
    def ensure_connection(self) -> bool:
        """Ensure database connection is active"""
        if self.sqlite_handler is None:
            return False
        
        try:
            # Test the connection
            conn = self.sqlite_handler.get_connection()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"SQLite connection error: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[pd.DataFrame]:
        """Execute a query and return results as a DataFrame"""
        if not self.ensure_connection():
            st.error("Database connection failed")
            return None
        
        try:
            conn = self.sqlite_handler.get_connection()
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            st.error(f"Query execution error: {e}")
            return None
    
    def close(self) -> None:
        """Close the database connection"""
        try:
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")


class Dashboard:
    """Main dashboard functionality"""
    
    def __init__(self):
        self.db = DatabaseConnection()
        
        # Initialize session state for filters
        if 'time_range' not in st.session_state:
            st.session_state.time_range = '24h'
        if 'merchant_filter' not in st.session_state:
            st.session_state.merchant_filter = 'All'
        if 'country_filter' not in st.session_state:
            st.session_state.country_filter = 'All'
        if 'min_amount' not in st.session_state:
            st.session_state.min_amount = 0.0
        if 'max_amount' not in st.session_state:
            st.session_state.max_amount = 10000.0
        if 'refresh_interval' not in st.session_state:
            st.session_state.refresh_interval = REFRESH_INTERVAL
        if 'auto_refresh' not in st.session_state:
            st.session_state.auto_refresh = True
    
    def get_time_filter(self) -> datetime:
        """Get the timestamp for filtering based on selected time range"""
        now = datetime.now()
        time_range = st.session_state.time_range
        
        if time_range == '1h':
            return now - timedelta(hours=1)
        elif time_range == '6h':
            return now - timedelta(hours=6)
        elif time_range == '12h':
            return now - timedelta(hours=12)
        elif time_range == '24h':
            return now - timedelta(hours=24)
        elif time_range == '7d':
            return now - timedelta(days=7)
        elif time_range == '30d':
            return now - timedelta(days=30)
        else:
            return now - timedelta(hours=24)  # Default to 24 hours
    
    def get_fraud_transactions(self) -> Optional[pd.DataFrame]:
        """Get fraudulent transactions based on filters"""
        time_filter = self.get_time_filter()
        merchant_filter = st.session_state.merchant_filter
        country_filter = st.session_state.country_filter
        min_amount = st.session_state.min_amount
        max_amount = st.session_state.max_amount
        
        query = """
        SELECT 
            ft.transaction_id,
            ft.timestamp,
            ft.card_number,
            ft.amount,
            ft.merchant_name,
            ft.merchant_category,
            ft.country,
            ft.city,
            ft.latitude,
            ft.longitude,
            ft.fraud_type,
            ft.fraud_score,
            ft.detection_timestamp
        FROM 
            fraudulent_transactions ft
        WHERE 
            ft.timestamp >= ?
        """
        
        params = [time_filter]
        
        # Add merchant filter if not 'All'
        if merchant_filter != 'All':
            query += " AND ft.merchant_category = ?"
            params.append(merchant_filter)
        
        # Add country filter if not 'All'
        if country_filter != 'All':
            query += " AND ft.country = ?"
            params.append(country_filter)
        
        # Add amount filters
        query += " AND ft.amount >= ? AND ft.amount <= ?"
        params.extend([min_amount, max_amount])
        
        # Order by timestamp
        query += " ORDER BY ft.timestamp DESC"
        
        # Execute the query
        df = self.db.execute_query(query, tuple(params))
        
        if df is not None and not df.empty:
            # Convert timestamp columns to datetime
            for col in ['timestamp', 'detection_timestamp']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
        
        return df
    
    def process_csv_file(self, uploaded_file) -> None:
        """Process uploaded CSV file for fraud detection"""
        try:
            # Disable auto-refresh during CSV processing
            st.session_state.auto_refresh = False
            
            # Read the CSV file
            df = pd.read_csv(uploaded_file)
            
            # Display file info
            st.success(f"Successfully loaded CSV file with {len(df)} transactions")
            
            # Show a preview of the data
            st.subheader("Data Preview")
            st.dataframe(df.head(10))
            
            # Required columns for fraud detection
            required_columns = [
                'transaction_id', 'timestamp', 'card_number', 'amount',
                'merchant_name', 'merchant_category', 'country', 'city',
                'latitude', 'longitude'
            ]
            
            # Check if all required columns are present
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"Missing required columns: {', '.join(missing_columns)}")
                st.info("Required columns: " + ", ".join(required_columns))
                return
            
            # Initialize fraud detector
            fraud_detector = FraudDetector()
            
            # Process each transaction for fraud detection
            fraud_results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, row in df.iterrows():
                # Update progress
                progress = (idx + 1) / len(df)
                progress_bar.progress(progress)
                status_text.text(f"Processing transaction {idx + 1} of {len(df)}")
                
                # Create transaction dictionary
                transaction = {
                    'transaction_id': str(row['transaction_id']),
                    'timestamp': row['timestamp'],
                    'card_number': str(row['card_number']),
                    'amount': float(row['amount']),
                    'merchant_name': str(row['merchant_name']),
                    'merchant_category': str(row['merchant_category']),
                    'country': str(row['country']),
                    'city': str(row['city']),
                    'latitude': float(row['latitude']) if pd.notna(row['latitude']) else 0.0,
                    'longitude': float(row['longitude']) if pd.notna(row['longitude']) else 0.0
                }
                
                # Detect fraud
                is_fraud, fraud_type, fraud_score = fraud_detector.detect_fraud(transaction)
                
                if is_fraud:
                    fraud_results.append({
                        'transaction_id': transaction['transaction_id'],
                        'timestamp': transaction['timestamp'],
                        'card_number': transaction['card_number'],
                        'amount': transaction['amount'],
                        'merchant_name': transaction['merchant_name'],
                        'merchant_category': transaction['merchant_category'],
                        'country': transaction['country'],
                        'city': transaction['city'],
                        'latitude': transaction['latitude'],
                        'longitude': transaction['longitude'],
                        'fraud_type': fraud_type,
                        'fraud_score': fraud_score
                    })
            
            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()
            
            # Display results
            if fraud_results:
                st.subheader(f"Fraud Detection Results - {len(fraud_results)} Fraudulent Transactions Found")
                
                # Convert to DataFrame for display
                fraud_df = pd.DataFrame(fraud_results)
                
                # Display summary statistics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Fraudulent Transactions", len(fraud_results))
                
                with col2:
                    total_fraud_amount = fraud_df['amount'].sum()
                    st.metric("Total Fraud Amount", f"${total_fraud_amount:,.2f}")
                
                with col3:
                    avg_fraud_score = fraud_df['fraud_score'].mean()
                    st.metric("Average Fraud Score", f"{avg_fraud_score:.2f}")
                
                with col4:
                    fraud_rate = (len(fraud_results) / len(df)) * 100
                    st.metric("Fraud Rate", f"{fraud_rate:.2f}%")
                
                # Display fraud transactions table
                st.subheader("Fraudulent Transactions Details")
                
                # Format the DataFrame for display
                display_df = fraud_df.copy()
                display_df['amount'] = display_df['amount'].apply(lambda x: f"${x:,.2f}")
                display_df['fraud_score'] = display_df['fraud_score'].apply(lambda x: f"{x:.2f}")
                
                st.dataframe(display_df, use_container_width=True)
                
                # Create visualizations
                st.subheader("Fraud Analysis Visualizations")
                
                # Fraud by category
                fraud_by_category = fraud_df.groupby('merchant_category').agg({
                    'transaction_id': 'count',
                    'amount': 'sum',
                    'fraud_score': 'mean'
                }).rename(columns={
                    'transaction_id': 'fraud_count',
                    'amount': 'total_amount',
                    'fraud_score': 'avg_fraud_score'
                }).reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_category = px.bar(
                        fraud_by_category,
                        x='merchant_category',
                        y='fraud_count',
                        title='Fraud Count by Merchant Category',
                        color='avg_fraud_score',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig_category, use_container_width=True)
                
                with col2:
                    # Fraud by country
                    fraud_by_country = fraud_df.groupby('country').agg({
                        'transaction_id': 'count',
                        'amount': 'sum'
                    }).rename(columns={
                        'transaction_id': 'fraud_count',
                        'amount': 'total_amount'
                    }).reset_index().sort_values('fraud_count', ascending=False).head(10)
                    
                    fig_country = px.bar(
                        fraud_by_country,
                        x='country',
                        y='fraud_count',
                        title='Top 10 Countries by Fraud Count',
                        color='total_amount',
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig_country, use_container_width=True)
                
                # Fraud map
                if not fraud_df.empty and 'latitude' in fraud_df.columns and 'longitude' in fraud_df.columns:
                    st.subheader("Fraud Locations Map")
                    
                    # Create a map centered on the mean coordinates
                    center_lat = fraud_df['latitude'].mean()
                    center_lon = fraud_df['longitude'].mean()
                    
                    m = folium.Map(location=[center_lat, center_lon], zoom_start=2)
                    
                    # Add markers for each fraud transaction
                    for _, row in fraud_df.iterrows():
                        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                            folium.CircleMarker(
                                location=[row['latitude'], row['longitude']],
                                radius=8,
                                popup=f"""Transaction ID: {row['transaction_id']}<br>
                                         Amount: ${row['amount']:,.2f}<br>
                                         Merchant: {row['merchant_name']}<br>
                                         Fraud Type: {row['fraud_type']}<br>
                                         Fraud Score: {row['fraud_score']:.2f}""",
                                color='red',
                                fill=True,
                                fillColor='red',
                                fillOpacity=0.7
                            ).add_to(m)
                    
                    folium_static(m)
                
                # Download option
                st.subheader("Download Results")
                csv_data = fraud_df.to_csv(index=False)
                st.download_button(
                    label="Download Fraud Results as CSV",
                    data=csv_data,
                    file_name=f"fraud_detection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
            else:
                st.success("No fraudulent transactions detected in the uploaded file!")
                st.info(f"Analyzed {len(df)} transactions - all appear to be legitimate.")
            
            # Mark CSV processing as complete
            st.session_state.csv_processed = True
            st.balloons()  # Celebrate successful processing
            
        except Exception as e:
            st.error(f"Error processing CSV file: {str(e)}")
            logger.error(f"CSV processing error: {e}")
    
    def get_fraud_summary(self) -> Optional[pd.DataFrame]:
        """Get fraud summary statistics"""
        time_filter = self.get_time_filter()
        
        query = """
        SELECT 
            datetime(timestamp, 'start of hour') AS hour,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
            AVG(fraud_score) AS avg_fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            timestamp >= ?
        GROUP BY 
            datetime(timestamp, 'start of hour')
        ORDER BY 
            hour ASC
        """
        
        return self.db.execute_query(query, (time_filter,))
    
    def get_merchant_categories(self) -> List[str]:
        """Get list of merchant categories for filtering"""
        query = """
        SELECT DISTINCT merchant_category 
        FROM fraudulent_transactions 
        ORDER BY merchant_category
        """
        
        df = self.db.execute_query(query)
        if df is not None and not df.empty:
            categories = df['merchant_category'].tolist()
            return ['All'] + categories
        return ['All']
    
    def get_countries(self) -> List[str]:
        """Get list of countries for filtering"""
        query = """
        SELECT DISTINCT country 
        FROM fraudulent_transactions 
        ORDER BY country
        """
        
        df = self.db.execute_query(query)
        if df is not None and not df.empty:
            countries = df['country'].tolist()
            return ['All'] + countries
        return ['All']
    
    def get_fraud_by_category(self) -> Optional[pd.DataFrame]:
        """Get fraud statistics by merchant category"""
        time_filter = self.get_time_filter()
        
        query = """
        SELECT 
            merchant_category,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
            AVG(fraud_score) AS avg_fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            timestamp >= ?
        GROUP BY 
            merchant_category
        ORDER BY 
            fraud_count DESC
        """
        
        return self.db.execute_query(query, (time_filter,))
    
    def get_fraud_by_country(self) -> Optional[pd.DataFrame]:
        """Get fraud statistics by country"""
        time_filter = self.get_time_filter()
        
        query = """
        SELECT 
            country,
            COUNT(*) AS fraud_count,
            SUM(amount) AS total_amount,
            AVG(fraud_score) AS avg_fraud_score
        FROM 
            fraudulent_transactions
        WHERE 
            timestamp >= ?
        GROUP BY 
            country
        ORDER BY 
            fraud_count DESC
        """
        
        return self.db.execute_query(query, (time_filter,))
    
    def get_fraud_stats(self) -> Dict[str, Any]:
        """Get overall fraud statistics"""
        time_filter = self.get_time_filter()
        
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
            timestamp >= ?
        """
        
        df = self.db.execute_query(query, (time_filter,))
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return {
            'total_frauds': 0,
            'total_amount': 0.0,
            'avg_amount': 0.0,
            'max_amount': 0.0,
            'avg_fraud_score': 0.0,
            'affected_cards': 0
        }
    
    def render_sidebar(self) -> None:
        """Render the sidebar with filters"""
        st.sidebar.title("Dashboard Controls")
        
        # Time range filter
        st.sidebar.subheader("Time Range")
        time_options = {
            '1h': 'Last Hour',
            '6h': 'Last 6 Hours',
            '12h': 'Last 12 Hours',
            '24h': 'Last 24 Hours',
            '7d': 'Last 7 Days',
            '30d': 'Last 30 Days'
        }
        selected_time = st.sidebar.selectbox(
            "Select time range:",
            options=list(time_options.keys()),
            format_func=lambda x: time_options[x],
            index=list(time_options.keys()).index(st.session_state.time_range)
        )
        st.session_state.time_range = selected_time
        
        # Merchant category filter
        st.sidebar.subheader("Merchant Category")
        merchant_categories = self.get_merchant_categories()
        selected_merchant = st.sidebar.selectbox(
            "Select merchant category:",
            options=merchant_categories,
            index=merchant_categories.index(st.session_state.merchant_filter) 
                if st.session_state.merchant_filter in merchant_categories else 0
        )
        st.session_state.merchant_filter = selected_merchant
        
        # Country filter
        st.sidebar.subheader("Country")
        countries = self.get_countries()
        selected_country = st.sidebar.selectbox(
            "Select country:",
            options=countries,
            index=countries.index(st.session_state.country_filter) 
                if st.session_state.country_filter in countries else 0
        )
        st.session_state.country_filter = selected_country
        
        # Amount range filter
        st.sidebar.subheader("Amount Range")
        min_val, max_val = st.sidebar.slider(
            "Select amount range ($):",
            min_value=0.0,
            max_value=10000.0,
            value=(st.session_state.min_amount, st.session_state.max_amount),
            step=100.0
        )
        st.session_state.min_amount = min_val
        st.session_state.max_amount = max_val
        
        # CSV Upload Section
        st.sidebar.subheader("CSV File Upload")
        uploaded_file = st.sidebar.file_uploader(
            "Upload CSV file for fraud detection",
            type=['csv'],
            help="Upload a CSV file containing transaction data to analyze for fraud"
        )
        
        if uploaded_file is not None:
            # Check if CSV has been processed
            if hasattr(st.session_state, 'csv_processed') and st.session_state.csv_processed:
                st.sidebar.success("✅ CSV analysis completed!")
                if st.sidebar.button("Analyze New CSV"):
                    st.session_state.csv_processed = False
                    st.rerun()
            else:
                if st.sidebar.button("Analyze CSV for Fraud", type="primary"):
                    with st.sidebar:
                        with st.spinner("Processing CSV file..."):
                            self.process_csv_file(uploaded_file)
        
        # Auto-refresh settings
        st.sidebar.subheader("Refresh Settings")
        auto_refresh = st.sidebar.checkbox(
            "Auto-refresh",
            value=st.session_state.auto_refresh
        )
        st.session_state.auto_refresh = auto_refresh
        
        if auto_refresh:
            refresh_interval = st.sidebar.slider(
                "Refresh interval (seconds):",
                min_value=5,
                max_value=60,
                value=st.session_state.refresh_interval,
                step=5
            )
            st.session_state.refresh_interval = refresh_interval
        
        # Manual refresh button
        if st.sidebar.button("Refresh Now"):
            st.rerun()
        
        # About section
        st.sidebar.markdown("---")
        st.sidebar.subheader("About")
        st.sidebar.info(
            "This dashboard displays real-time credit card fraud detection results. "
            "It connects to a PostgreSQL database and shows fraudulent transactions, "
            "statistics, and visualizations."
        )
    
    def render_header(self) -> None:
        """Render the dashboard header"""
        col1, col2 = st.columns([1, 3])
        
        with col1:
            st.image("https://img.icons8.com/color/96/000000/bank-card-front-side.png", width=80)
        
        with col2:
            st.title("Credit Card Fraud Detection Dashboard")
            st.markdown("Real-time monitoring of fraudulent transactions")
    
    def render_stats_cards(self) -> None:
        """Render statistics cards"""
        stats = self.get_fraud_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Frauds Detected",
                value=f"{int(stats['total_frauds'])}"
            )
        
        with col2:
            # Handle case when total_amount might be None or 0
            total_amount = stats['total_amount'] if stats['total_amount'] is not None else 0.0
            st.metric(
                label="Total Fraud Amount",
                value=f"${total_amount:,.2f}"
            )
        
        with col3:
            # Handle case when avg_fraud_score might be None
            avg_score = stats['avg_fraud_score'] if stats['avg_fraud_score'] is not None else 0.0
            st.metric(
                label="Average Fraud Score",
                value=f"{avg_score:.2f}"
            )
        
        with col4:
            st.metric(
                label="Cards Affected",
                value=f"{int(stats['affected_cards'])}"
            )
    
    def render_fraud_map(self, df: pd.DataFrame) -> None:
        """Render a map with fraud locations"""
        if df is None or df.empty:
            st.warning("No fraud data available for the map")
            return
        
        # Create a map centered at the mean of coordinates
        center_lat = df['latitude'].mean()
        center_lon = df['longitude'].mean()
        
        # Default to a world map if no data
        if pd.isna(center_lat) or pd.isna(center_lon):
            center_lat, center_lon = 0, 0
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=2)
        
        # Add markers for each fraud transaction
        for _, row in df.iterrows():
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                continue
                
            # Format popup content
            popup_content = f"""
            <b>Transaction ID:</b> {row['transaction_id']}<br>
            <b>Amount:</b> ${row['amount']:,.2f}<br>
            <b>Merchant:</b> {row['merchant_name']}<br>
            <b>Category:</b> {row['merchant_category']}<br>
            <b>Location:</b> {row['city']}, {row['country']}<br>
            <b>Fraud Type:</b> {row['fraud_type']}<br>
            <b>Fraud Score:</b> {row['fraud_score']:.2f}<br>
            <b>Time:</b> {row['timestamp']}
            """
            
            # Add marker with popup
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"${row['amount']:,.2f} at {row['merchant_name']}",
                icon=folium.Icon(color='red', icon='credit-card', prefix='fa')
            ).add_to(m)
        
        # Display the map
        st.subheader("Fraud Locations")
        folium_static(m, width=1100, height=500)
    
    def render_fraud_by_time(self, df: pd.DataFrame) -> None:
        """Render fraud by time chart"""
        if df is None or df.empty:
            st.warning("No fraud data available for time analysis")
            return
        
        # Convert hour to datetime for better formatting
        df['hour'] = pd.to_datetime(df['hour'])
        
        # Create a line chart for fraud count over time
        fig = px.line(
            df,
            x='hour',
            y='fraud_count',
            title='Fraud Transactions Over Time',
            labels={'hour': 'Time', 'fraud_count': 'Number of Frauds'},
            markers=True
        )
        
        # Add a line for total amount
        fig.add_trace(
            go.Scatter(
                x=df['hour'],
                y=df['total_amount'],
                name='Total Amount ($)',
                yaxis='y2',
                line=dict(color='red', dash='dot')
            )
        )
        
        # Update layout for dual y-axis
        fig.update_layout(
            yaxis=dict(title='Number of Frauds'),
            yaxis2=dict(
                title='Total Amount ($)',
                overlaying='y',
                side='right',
                showgrid=False
            ),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_fraud_by_category(self, df: pd.DataFrame) -> None:
        """Render fraud by merchant category chart"""
        if df is None or df.empty:
            st.warning("No fraud data available for category analysis")
            return
        
        # Sort by fraud count
        df = df.sort_values('fraud_count', ascending=True)
        
        # Create a horizontal bar chart
        fig = px.bar(
            df,
            y='merchant_category',
            x='fraud_count',
            title='Fraud by Merchant Category',
            labels={'merchant_category': 'Category', 'fraud_count': 'Number of Frauds'},
            orientation='h',
            color='avg_fraud_score',
            color_continuous_scale='Reds',
            text='fraud_count'
        )
        
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(height=400)
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_fraud_by_country(self, df: pd.DataFrame) -> None:
        """Render fraud by country chart"""
        if df is None or df.empty:
            st.warning("No fraud data available for country analysis")
            return
        
        # Sort by fraud count and take top 10
        df = df.sort_values('fraud_count', ascending=False).head(10)
        
        # Create a bar chart
        fig = px.bar(
            df,
            x='country',
            y='fraud_count',
            title='Top 10 Countries by Fraud Count',
            labels={'country': 'Country', 'fraud_count': 'Number of Frauds'},
            color='total_amount',
            color_continuous_scale='Blues',
            text='fraud_count'
        )
        
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(height=400)
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_fraud_table(self, df: pd.DataFrame) -> None:
        """Render table of fraudulent transactions"""
        if df is None or df.empty:
            st.warning("No fraudulent transactions found with the current filters")
            return
        
        st.subheader("Recent Fraudulent Transactions")
        
        # Format the DataFrame for display
        display_df = df.copy()
        
        # Format timestamp
        display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Format amount as currency
        display_df['amount'] = display_df['amount'].apply(lambda x: f"${x:,.2f}")
        
        # Select and rename columns for display
        display_df = display_df[[
            'transaction_id', 'timestamp', 'card_number', 'amount',
            'merchant_name', 'country', 'fraud_type', 'fraud_score'
        ]].rename(columns={
            'transaction_id': 'Transaction ID',
            'timestamp': 'Time',
            'card_number': 'Card Number',
            'amount': 'Amount',
            'merchant_name': 'Merchant',
            'country': 'Country',
            'fraud_type': 'Fraud Type',
            'fraud_score': 'Fraud Score'
        })
        
        # Display the table
        st.dataframe(display_df, use_container_width=True)
    
    def render_dashboard(self) -> None:
        """Render the main dashboard"""
        # Get data
        fraud_transactions = self.get_fraud_transactions()
        fraud_summary = self.get_fraud_summary()
        fraud_by_category = self.get_fraud_by_category()
        fraud_by_country = self.get_fraud_by_country()
        
        # Render header
        self.render_header()
        
        # Render stats cards
        self.render_stats_cards()
        
        # Render fraud map
        if fraud_transactions is not None and not fraud_transactions.empty:
            self.render_fraud_map(fraud_transactions)
        
        # Render charts in two columns
        col1, col2 = st.columns(2)
        
        with col1:
            if fraud_summary is not None and not fraud_summary.empty:
                self.render_fraud_by_time(fraud_summary)
            
            if fraud_by_country is not None and not fraud_by_country.empty:
                self.render_fraud_by_country(fraud_by_country)
        
        with col2:
            if fraud_by_category is not None and not fraud_by_category.empty:
                self.render_fraud_by_category(fraud_by_category)
        
        # Render fraud transactions table
        if fraud_transactions is not None:
            self.render_fraud_table(fraud_transactions)
    
    def run(self) -> None:
        """Run the dashboard application"""
        # Handle authentication if enabled
        if USE_AUTH:
            name, authentication_status, username = authenticator.login('Login', 'main')
            
            if authentication_status == False:
                st.error('Username/password is incorrect')
                return
            elif authentication_status == None:
                st.warning('Please enter your username and password')
                return
        
        # Render sidebar
        self.render_sidebar()
        
        # Render main dashboard
        self.render_dashboard()
        
        # Auto-refresh if enabled (but not during CSV processing)
        if (st.session_state.auto_refresh and 
            not (hasattr(st.session_state, 'csv_processed') and st.session_state.csv_processed)):
            time.sleep(0.1)  # Small delay to prevent excessive CPU usage
            st.empty()
            
            # Add a countdown timer
            placeholder = st.empty()
            for i in range(st.session_state.refresh_interval, 0, -1):
                placeholder.markdown(f"Refreshing in {i} seconds...")
                time.sleep(1)
            
            placeholder.empty()
            st.rerun()


if __name__ == "__main__":
    dashboard = Dashboard()
    dashboard.run()