# 🔐 Live Credit Card Fraud Detection System

**Author: Manoj Kalal**

A real-time credit card fraud detection system that simulates live transaction streams, processes them in real-time, applies fraud detection rules, stores results in a database, and displays flagged transactions on a live dashboard with instant alerts.

## 📘 Project Summary

This project implements a fully working real-time credit card fraud detection system using open-source and free-tier cloud tools. The system simulates live transaction streams, processes them in real time, applies fraud detection rules, stores results in cloud storage, and displays flagged transactions on a live dashboard with real-time alerts.

As a Data Engineering project, it builds a scalable and automated real-time data pipeline that:
- Simulates credit card transactions
- Processes and filters fraudulent activity
- Stores clean and flagged transactions
- Displays real-time dashboards
- Sends instant fraud alerts via Telegram or Email
- Uses zero-cost tools with live deployment

## 🛠️ Tech Stack (All Free Tools)

| Component | Tool |
|-----------|------|
| Data Generation | Python + Faker |
| Data Streaming | Apache Kafka (Docker) or WebSocket in Streamlit |
| Real-Time Processing | Pandas-based logic |
| Storage | SQLite (Local database) |
| Dashboard | Streamlit (Live hosted via Streamlit Cloud) |
| Alerts | Telegram Bot & SMTP Email |
| Orchestration | Prefect Cloud (Free Tier) |
| Deployment | Streamlit Cloud / Render.com / Railway.app |

## Architecture
![Architecture Diagram](architecture_diagram.svg)

## 🔁 System Components

### 1. Data Simulation

The system uses the Faker library in Python to simulate realistic credit card transactions with the following fields:
- transaction_id
- timestamp
- card_number (masked)
- amount
- merchant_id
- country
- latitude
- longitude

Transactions are streamed every 1-2 seconds to mimic real-time data using either Kafka (Docker) or a simple Streamlit loop with threading.

### 2. Real-Time Data Processing

The system processes transactions in real-time using Python with Pandas to apply the following fraud rules:
- More than 3 transactions within 30 seconds
- Unusual geo-location (e.g., IP from a different country suddenly)
- Amount over $5000
- Duplicate transactions within short time
- Rapid spend after 12AM local time

### 3. Data Storage

Processed data is stored in SQLite (local database) with three main tables:
- All Transactions
- Flagged/Fraudulent Transactions
- User Cards

### 4. Alerts System

The system sends instant alerts for fraudulent transactions using:
- Telegram Bot (using BotFather)
- Email alerts (using Python's smtplib)

### 5. Real-Time Dashboard

A Streamlit Dashboard displays:
- **CSV Upload Feature**: Upload transaction CSV files for batch fraud analysis
- Table showing latest 10 flagged transactions
- Charts: fraud count per hour, amount trends, map for locations
- Filters: time range, merchant, location
- Download processed results as CSV

### 6. Orchestration & Scheduling

Prefect Cloud (free tier) is used to:
- Schedule nightly reports
- Monitor ETL job success/failure
- Auto-clean old data every week

## 🚀 How to Run Locally

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- PostgreSQL (local or hosted)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/credit-card-fraud-detection.git
   cd credit-card-fraud-detection
   ```

2. Set up environment variables (create a `.env` file):
   ```
   # Database (SQLite - no configuration needed)
   # DB_PATH=db/fraud_detection.db (automatically created)
   
   # Telegram Bot
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   
   # Email
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USERNAME=your_email@gmail.com
   SMTP_PASSWORD=your_app_password
   EMAIL_FROM=your_email@gmail.com
   EMAIL_TO=recipient@example.com
   
   # API
   API_KEY=your_api_key
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Initialize the SQLite database:
   ```bash
   python init_sqlite_db.py
   ```

### Running with Docker

1. Build and start the containers:
   ```bash
   docker-compose up -d
   ```

2. Access the dashboard at http://localhost:8501

### Running without Docker

1. Start the transaction generator:
   ```bash
   python -m data_generator.simulate_transactions
   ```

2. Start the transaction processor:
   ```bash
   python -m processing.real_time_processor
   ```

3. Start the Streamlit dashboard:
   ```bash
   streamlit run dashboard/streamlit_app.py
   ```

4. Start the API server:
   ```bash
   uvicorn api.fraud_api:app --host 0.0.0.0 --port 9000
   ```

## 🌐 Live Dashboard

Access the live dashboard at: [https://credit-card-fraud-detection.streamlit.app/](https://credit-card-fraud-detection.streamlit.app/)

## 📱 Screenshots

### Dashboard
![Dashboard](screenshots/dashboard.png)

### Fraud Alerts
![Fraud Alerts](screenshots/fraud_alerts.png)

## 🔧 How to Simulate Data & Modify Fraud Rules

### Simulating Data

You can modify the transaction generation parameters in `data_generator/simulate_transactions.py`:

```python
# Adjust transaction frequency
TRANSACTION_FREQUENCY_SECONDS = 2  # Generate a transaction every 2 seconds

# Adjust fraud probability
FRAUD_PROBABILITY = 0.05  # 5% chance of generating a fraudulent transaction
```

### Modifying Fraud Rules

Fraud detection rules can be modified in `processing/real_time_processor.py`:

```python
# High amount threshold
HIGH_AMOUNT_THRESHOLD = 5000  # Flag transactions over $5000

# Rapid transaction threshold
RAPID_TRANSACTION_COUNT = 3  # Number of transactions
RAPID_TRANSACTION_SECONDS = 30  # Time window in seconds

# Late night spending (24-hour format)
LATE_NIGHT_START_HOUR = 0  # 12 AM
LATE_NIGHT_END_HOUR = 5  # 5 AM
```

## 🗄️ Setting up PostgreSQL on Railway

1. Create a Railway account at [railway.app](https://railway.app/)
2. Create a new project and add a PostgreSQL database
3. Get your database credentials from the "Connect" tab
4. Set the database environment variables in your `.env` file or Railway environment variables

## 🤖 Telegram Bot Setup

1. Create a new bot using BotFather on Telegram:
   - Open Telegram and search for @BotFather
   - Send `/newbot` command
   - Follow the instructions to create a bot
   - Copy the API token provided

2. Get your Chat ID:
   - Send a message to @userinfobot
   - Copy your ID

3. Set the Telegram environment variables:
   ```
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   ```

## 📧 Email Alerts Setup

1. If using Gmail, enable 2-factor authentication
2. Generate an App Password:
   - Go to your Google Account > Security
   - Under "Signing in to Google," select App Passwords
   - Generate a new app password for "Mail"

3. Set the email environment variables:
   ```
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USERNAME=your_email@gmail.com
   SMTP_PASSWORD=your_app_password
   EMAIL_FROM=your_email@gmail.com
   EMAIL_TO=recipient@example.com
   ```

## 📂 Directory Structure

```
credit-card-fraud-detection/
├── data_generator/
│   └── simulate_transactions.py
├── processing/
│   └── real_time_processor.py
├── alerts/
│   ├── telegram_bot.py
│   └── email_alert.py
├── dashboard/
│   └── streamlit_app.py
├── db/
│   └── schema.sql
├── scheduler/
│   └── prefect_flow.py
├── utils/
│   ├── data_export.py
│   └── pdf_report.py
├── api/
│   ├── fraud_api.py
│   └── report_endpoints.py
├── tests/
│   ├── test_*.py
├── Dockerfile
├── requirements.txt
├── README.md
└── architecture_diagram.svg
```

## 🔍 API Endpoints

The system provides a FastAPI-based API for accessing fraud data:

- `GET /health` - Health check endpoint
- `GET /api/v1/frauds/recent` - Get recent fraudulent transactions
- `GET /api/v1/frauds/stats` - Get fraud statistics
- `GET /api/v1/frauds/by-country` - Get frauds by country
- `GET /api/v1/frauds/by-category` - Get frauds by merchant category
- `GET /api/v1/reports/generate/{report_type}` - Generate a fraud report (daily/weekly)
- `GET /api/v1/reports/status/{report_id}` - Check report generation status
- `GET /api/v1/reports/download/{report_id}` - Download a generated report

All API endpoints require an API key to be passed in the `X-API-Key` header.

## 🧪 Running Tests

Run the test suite with:

```bash
python -m unittest discover tests
```

Or run specific test files:

```bash
python -m unittest tests/test_fraud_detector.py
```

## 🔄 Orchestration with Prefect

The system uses Prefect for orchestration with the following flows:

1. Daily Reporting Flow - Generates and sends daily fraud reports
2. Weekly Maintenance Flow - Cleans old data and optimizes the database
3. Hourly Health Check Flow - Monitors system health

To deploy the flows:

```bash
python -m scheduler.prefect_flow deploy
```

## 💼 Key Skills Showcased

- Real-Time Stream Processing (Kafka)
- Data Orchestration (Prefect)
- Cloud Deployment (Streamlit, Railway)
- Data Modeling & SQL (PostgreSQL)
- Data Alerting and Monitoring
- End-to-End Project Ownership

## 📊 Bonus Features

- Interactive map showing fraud transactions using Plotly
- Dashboard authentication using streamlit-authenticator
- PDF report generation using FPDF
- API endpoints using FastAPI
- Data export to CSV, Excel, and JSON formats

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgements

- [Faker](https://faker.readthedocs.io/) for generating realistic test data
- [Streamlit](https://streamlit.io/) for the interactive dashboard
- [Prefect](https://www.prefect.io/) for workflow orchestration
- [FastAPI](https://fastapi.tiangolo.com/) for API development
- [Railway](https://railway.app/) for database hosting