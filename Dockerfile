# ──────────────────────────────────────────────────────────────────────────────
# Dockerfile
# Containerises the GCP Security Monitoring pipeline.
# Base image: python:3.11-slim (lightweight, production-ready)
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies needed by some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (Docker layer caching: only re-install if requirements change)
COPY requirements.txt .

# Install Python dependencies
# --no-cache-dir: don't cache packages (keeps image smaller)
# We skip Airflow here for the slim container — the pipeline runs standalone
RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    numpy==1.26.4 \
    pyarrow==16.1.0 \
    google-cloud-bigquery==3.25.0 \
    google-cloud-bigquery-storage==2.25.0 \
    google-auth==2.30.0 \
    requests==2.31.0 \
    anthropic==0.34.0 \
    python-dotenv==1.0.1 \
    colorlog==6.8.2 \
    pytest==8.2.2 \
    pytest-cov==5.0.0

# Copy the full project into the container
COPY . .

# Create the logs directory (persists pipeline logs)
RUN mkdir -p logs

# Set Python path so imports work correctly
ENV PYTHONPATH=/app

# Expose a port in case we add a REST API later (Cloud Run expects 8080)
EXPOSE 8080

# Default command: run the full pipeline
CMD ["python", "-m", "src.orchestration.orchestrator"]
