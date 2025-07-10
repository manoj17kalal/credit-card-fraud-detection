# Streamlit App Deployment Instructions

Author: Manoj Kalal

## IMPORTANT: Streamlit Apps Cannot Run on GitHub Pages!

**GitHub Pages only serves static files, but Streamlit apps require a server to run. You need to deploy to Streamlit Community Cloud instead.**

## Steps to Deploy to Streamlit Community Cloud (FREE)

### 1. Create GitHub Repository (Already Done)

✅ Your repository is already created at: `https://github.com/manoj17kalal/credit-card-fraud-detection`

### 2. Push Your Code to GitHub

Run these commands in your terminal:

```bash
# Add the remote origin
git remote add origin https://github.com/manoj17kalal/credit-card-fraud-detection.git

# Rename the default branch to main (if needed)
git branch -M main

# Push the code to GitHub
git push -u origin main
```

### 3. Deploy to Streamlit Community Cloud

1. **Go to Streamlit Community Cloud**: Visit [share.streamlit.io](https://share.streamlit.io)
2. **Sign up/Login**: Use your GitHub account to sign up
3. **Authorize GitHub**: Allow Streamlit to access your GitHub repositories
4. **Create New App**: Click "New app" button
5. **Fill App Details**:
   - Repository: `manoj17kalal/credit-card-fraud-detection`
   - Branch: `main`
   - Main file path: `dashboard/streamlit_app.py`
6. **Deploy**: Click "Deploy!" button

### 4. Your Live App URL

After deployment, your app will be available at:
`https://manoj17kalal-credit-card-fraud-detection-dashboardstreamlit-app-xyz123.streamlit.app/`

(The exact URL will be provided by Streamlit Community Cloud)

### 3. Repository Structure

Your repository will include:

```
credit-card-fraud-detection/
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
├── .gitignore                  # Git ignore rules
├── CSV_UPLOAD_GUIDE.md         # CSV upload feature guide
├── DEPLOYMENT_INSTRUCTIONS.md  # This file
├── dashboard/
│   ├── streamlit_app.py        # Main Streamlit application
│   └── init_sqlite_db.py       # Database initialization
├── processing/
│   ├── real_time_processor.py  # Fraud detection logic
│   └── simulate_transactions.py # Transaction generator
├── alerts/
│   ├── email_alert.py          # Email notification system
│   └── telegram_bot.py         # Telegram bot integration
├── api/
│   └── fraud_api.py            # FastAPI endpoints
├── orchestration/
│   ├── airflow_dag.py          # Apache Airflow DAG
│   └── prefect_flow.py         # Prefect workflow
├── utils/
│   ├── data_export.py          # Data export utilities
│   └── pdf_report.py           # PDF report generation
├── tests/                      # Test files
└── sample_transactions.csv     # Sample data for testing
```

### 4. Key Features to Highlight

- **Real-time Fraud Detection**: Advanced ML-based fraud detection algorithms
- **Interactive Dashboard**: Streamlit-based dashboard with real-time visualizations
- **CSV Upload Feature**: Upload and analyze transaction files for batch fraud detection
- **Multiple Alert Systems**: Email and Telegram notifications
- **Scalable Architecture**: Kafka for streaming, SQLite for storage
- **Comprehensive Testing**: Full test suite included
- **Production Ready**: Docker support and orchestration tools

### 5. LinkedIn Showcase Tips

1. **Repository URL**: `https://github.com/manoj17kalal/credit-card-fraud-detection`
2. **Live Demo**: Your deployed Streamlit app URL (from Streamlit Community Cloud)
3. **Key Technologies**: Python, Streamlit, Kafka, SQLite, Docker, FastAPI
4. **Highlight**: CSV upload feature for batch fraud analysis
5. **Screenshots**: Include dashboard screenshots in your LinkedIn post

### 6. Running the Application

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database
python dashboard/init_sqlite_db.py

# Run the dashboard
streamlit run dashboard/streamlit_app.py
```

### 7. Sample LinkedIn Post

```
🚀 Excited to share my latest project: Real-time Credit Card Fraud Detection System!

🔍 Key Features:
✅ Real-time fraud detection with ML algorithms
✅ Interactive Streamlit dashboard with live visualizations
✅ CSV upload for batch fraud analysis
✅ Email & Telegram alert systems
✅ Scalable architecture with Kafka & SQLite

🛠️ Tech Stack: Python, Streamlit, Kafka, SQLite, Docker, FastAPI

📊 The system can process thousands of transactions and identify fraudulent patterns in real-time, helping financial institutions protect their customers.

🔗 GitHub: https://github.com/manoj17kalal/credit-card-fraud-detection
🌐 Live Demo: [Your Streamlit App URL]

#MachineLearning #FraudDetection #Python #Streamlit #DataScience #FinTech
```

---

**Important Notes:**
- GitHub Pages cannot host Streamlit apps - use Streamlit Community Cloud instead
- Your live app URL will be provided after deploying to Streamlit Community Cloud
- Make sure your repository is public for Streamlit Community Cloud deployment