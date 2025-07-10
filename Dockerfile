# Use Python 3.9 as the base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create necessary directories
RUN mkdir -p /app/reports

# Expose ports
EXPOSE 8501  # Streamlit
EXPOSE 9000  # API (if implemented)

# Create entrypoint script
RUN echo '#!/bin/bash\n\n\
# Start services based on the COMPONENT environment variable\n\
case "$COMPONENT" in\n\
  "dashboard")\n    echo "Starting Streamlit dashboard..."\n    cd /app && streamlit run dashboard/streamlit_app.py\n    ;;\n\
  "generator")\n    echo "Starting transaction generator..."\n    cd /app && python data_generator/simulate_transactions.py\n    ;;\n\
  "processor")\n    echo "Starting transaction processor..."\n    cd /app && python processing/real_time_processor.py\n    ;;\n\
  "scheduler")\n    echo "Starting Prefect scheduler..."\n    cd /app && python scheduler/prefect_flow.py\n    ;;\n\
  "api")\n    echo "Starting API server..."\n    cd /app && uvicorn api.fraud_api:app --host 0.0.0.0 --port 9000\n    ;;\n\
  *)\n    echo "Unknown component: $COMPONENT"\n    echo "Available components: dashboard, generator, processor, scheduler, api"\n    exit 1\n    ;;\n\
esac' > /app/entrypoint.sh

# Make entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]