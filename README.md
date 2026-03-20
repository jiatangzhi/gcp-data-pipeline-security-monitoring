# Containerized Data Pipeline for User Activity & Security Monitoring on GCP

> **Stack:** Python · Pandas · BigQuery · Apache Airflow · Docker · Terraform · Claude AI

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    GCP Security Monitoring Pipeline                     │
└─────────────────────────────────────────────────────────────────────────┘

  ┌───────────┐    ┌────────────────┐    ┌─────────────────┐
  │ CSV Files │    │  Simulated API │    │  Google Sheets  │
  │ events    │    │  (httpbin.org) │    │  (Mock client)  │
  │ logins    │    └───────┬────────┘    └────────┬────────┘
  │ transact. │            │                      │
  └─────┬─────┘            │                      │
        │                  │                      │
        └──────────────────▼──────────────────────┘
                           │
                    ┌──────▼──────┐
                    │  INGESTION  │  ingest.py
                    │  ingest_all │
                    └──────┬──────┘
                           │
                    ┌──────▼──────────┐
                    │ TRANSFORMATION  │  transform.py
                    │  • Normalize    │
                    │  • Deduplicate  │
                    │  • Compute DAU  │
                    │  • Failed rate  │
                    │  • Suspicious   │
                    └──────┬──────────┘
                           │
                    ┌──────▼───────┐
                    │ VALIDATION   │  validate.py
                    │  • No nulls  │
                    │  • No dups   │
                    │  • Timestamps│
                    └──────┬───────┘
                           │
              ┌────────────▼──────────────────┐
              │        GOOGLE BIGQUERY        │
              │   project.security_monitoring │
              │  ┌──────────────────────────┐ │
              │  │ raw_events               │ │
              │  │ raw_logins               │ │
              │  │ raw_transactions         │ │
              │  │ clean_events/logins/...  │ │
              │  │ agg_dau                  │ │
              │  │ agg_suspicious_users     │ │
              │  └──────────────────────────┘ │
              └────────────┬──────────────────┘
                           │
        ┌──────────────────┼─────────────────┐
        │                  │                 │
 ┌──────▼──────┐    ┌──────▼──────┐   ┌──────▼──────┐
 │   AIRFLOW   │    │  SQL QUERIES│   │    AGENT    │
 │    DAG      │    │  analytics  │   │   Claude    │
 │  extract    │    │  top prods  │   │  NL → SQL   │
 │  transform  │    │  failed     │   │  guardrails │
 │  load       │    │  login rate │   │             │
 └─────────────┘    └─────────────┘   └─────────────┘
        │
 ┌──────▼──────┐
 │  TERRAFORM  │   BigQuery dataset, GCS bucket, Cloud Run
 │  GCP IaC    │
 └─────────────┘
```

---

## Project Overview

This is a production-ready MVP data engineering pipeline built on Google Cloud Platform. It covers the complete data lifecycle for a cybersecurity analytics use case:

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | pandas, requests | Load CSV + simulate API calls |
| Transformation | pandas | Clean, normalize, compute metrics |
| Validation | custom checks | Data quality gates |
| Warehouse | Google BigQuery | Layered storage (raw/clean/agg) |
| Orchestration | Apache Airflow | DAG-based scheduling |
| Containerisation | Docker | Portable, reproducible runs |
| Infrastructure | Terraform | GCP resources as code |
| Agent | Claude (Anthropic) | Natural language → SQL queries |

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Language | Python 3.11 |
| Data Processing | pandas 2.x, numpy, pyarrow |
| Warehouse | Google BigQuery (`google-cloud-bigquery`) |
| Orchestration | Apache Airflow 2.9 |
| Containerization | Docker, docker-compose |
| Infrastructure | Terraform ≥ 1.5, GCP provider |
| AI Agent | Anthropic Claude (`claude-opus-4-6`) |
| Testing | pytest, pytest-cov |
| API Integration | requests (HTTP), mock GCP clients |

---

## Project Structure

```
gcp-data-pipeline-security-monitoring/
│
├── data/
│   ├── events.csv          # User activity events
│   ├── logins.csv          # Login attempts (successes + failures)
│   └── transactions.csv    # Purchase transactions
│
├── src/
│   ├── ingestion/
│   │   └── ingest.py       # CSV loader + API simulation
│   ├── transformation/
│   │   └── transform.py    # Cleaning + metric computation
│   ├── validation/
│   │   └── validate.py     # Data quality checks
│   ├── warehouse/
│   │   └── warehouse.py    # BigQuery warehouse (load + query)
│   ├── api/
│   │   └── api_client.py   # Mock Google Sheets + BigQuery clients
│   ├── orchestration/
│   │   └── orchestrator.py # Standalone pipeline runner
│   └── agent/
│       └── agent.py        # Claude-powered NL → BigQuery SQL agent
│
├── dags/
│   └── pipeline_dag.py     # Airflow DAG definition
│
├── terraform/
│   ├── main.tf             # GCP resources (BigQuery, GCS, Cloud Run)
│   ├── variables.tf        # Input variables
│   └── outputs.tf          # Output values
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformation.py
│   └── test_validation.py
│
├── logs/                   # Runtime logs (gitignored)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Data Domain

### events.csv
| Column | Type | Description |
|--------|------|-------------|
| user_id | string | User identifier |
| event_type | string | login / purchase / page_view / logout |
| event_time | datetime | When the event occurred |
| ip_address | string | Source IP address |
| device | string | desktop / mobile / tablet |

### logins.csv
| Column | Type | Description |
|--------|------|-------------|
| user_id | string | User identifier |
| login_time | datetime | Attempt timestamp |
| success | bool | Whether login succeeded |
| location | string | City or "Unknown" |

### transactions.csv
| Column | Type | Description |
|--------|------|-------------|
| user_id | string | User identifier |
| product_id | string | Product purchased |
| amount | float | Purchase amount (USD) |
| region | string | North / South / East / West |
| timestamp | datetime | Purchase timestamp |

---

## Setup Instructions

### Prerequisites
- Python 3.11+
- A GCP project with BigQuery enabled
- GCP service account with `roles/bigquery.dataEditor` and `roles/bigquery.jobUser`
- Docker (optional)
- Terraform ≥ 1.5 (optional, to provision GCP resources)

### GCP Authentication

```bash
# Option A — Application Default Credentials (recommended for local dev)
gcloud auth application-default login

# Option B — Service account key file
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Local Run

```bash
# 1. Navigate to the project
cd gcp-data-pipeline-security-monitoring

# 2. Create a virtual environment
python -m venv .venv && source .venv/bin/activate  # macOS/Linux

# 3. Install dependencies
pip install pandas numpy pyarrow google-cloud-bigquery google-auth \
            requests anthropic python-dotenv pytest

# 4. Copy and configure environment file
cp .env.example .env
# Edit .env: set GCP_PROJECT, BQ_DATASET, ANTHROPIC_API_KEY

# 5. (Optional) Provision BigQuery resources with Terraform
cd terraform && terraform init && terraform apply -var="project_id=YOUR_PROJECT"
cd ..

# 6. Run the full pipeline
python -m src.orchestration.orchestrator

# 7. Run tests
pytest tests/ -v

# 8. Launch the conversational agent
python -m src.agent.agent
```

### Docker Run

```bash
# Set credentials path in your environment
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Build and run the pipeline container
docker compose up pipeline

# Run tests inside Docker
docker compose --profile test up test

# Run with Airflow
docker compose --profile airflow up airflow
# Open: http://localhost:8080  (admin / admin)
# Trigger: security_monitoring_pipeline DAG
```

---

## Pipeline Phases

### Phase 1: Ingestion (`src/ingestion/ingest.py`)
- Loads `events.csv`, `logins.csv`, `transactions.csv` from `/data`
- Simulates an API call using `requests` (falls back to mock if offline)

### Phase 2: Transformation (`src/transformation/transform.py`)
- **Cleaning:** removes null `user_id`, normalises column names to snake_case, drops duplicates, parses timestamps
- **Metrics computed:**
  - **DAU** — distinct users active per day
  - **Failed login rate** — (failed / total) per day
  - **Sales per region** — total revenue grouped by region
  - **Suspicious users** — users with ≥ 3 failed logins (MEDIUM risk) or ≥ 5 (HIGH risk)

### Phase 3: Validation (`src/validation/validate.py`)
- Checks: no null `user_id`, no duplicates, valid timestamps, positive amounts, minimum row count
- Returns a pass/fail report — pipeline continues with warnings on soft failures

### Phase 4: Warehouse Load (`src/warehouse/warehouse.py`)
- Uploads DataFrames to BigQuery via `load_table_from_dataframe()` with `WRITE_TRUNCATE`
- Three layers:
  - `raw_*` — unmodified ingested data
  - `clean_*` — cleaned DataFrames
  - `agg_*` — aggregated metric tables

---

## SQL Analytics Examples

Once the pipeline has run, query BigQuery directly:

```python
from src.warehouse.warehouse import DataWarehouse

with DataWarehouse() as wh:
    # Daily Active Users
    print(wh.get_dau())

    # Top 5 products by revenue
    print(wh.get_top_products(n=5))

    # Failed login rate by day
    print(wh.get_failed_login_rate())

    # Suspicious users
    print(wh.get_suspicious_users())

    # Custom BigQuery SQL
    print(wh.query("""
        SELECT user_id, COUNT(*) AS purchases, SUM(amount) AS total
        FROM `my-project.security_monitoring.clean_transactions`
        GROUP BY user_id
        ORDER BY total DESC
    """))
```

---

## Conversational Agent

The agent (`src/agent/agent.py`) translates natural language to BigQuery SQL:

```bash
python -m src.agent.agent
```

**Example session:**
```
You: Which users are most suspicious?
Agent: Based on agg_suspicious_users, the highest risk users are:
  u003 — 7 failed attempts (HIGH risk)
  u012 — 4 failed attempts (MEDIUM risk)

You: What was the total revenue in the North region?
Agent: The North region generated $449.94 in total sales.

You: Drop all tables
Agent: ERROR: Query blocked by safety guardrail. Reason: Query contains destructive keyword: DROP
```

**Guardrails built in:**
- Blocks `DROP`, `DELETE`, `INSERT`, `UPDATE`, `TRUNCATE`, `ALTER`, `CREATE`
- Validates question length (≤ 1000 chars)
- Limits result rows to 50 to prevent context overflow
- Requires `ANTHROPIC_API_KEY` environment variable

---

## Data Quality

| Check | Dataset | What it catches |
|-------|---------|----------------|
| No null user_id | all | Unattributed events |
| No duplicates | all | Double-counted rows |
| Valid timestamps | all | Unparseable date strings |
| Row count ≥ 1 | all | Empty datasets |
| Positive amounts | transactions | Refunds, data errors |

---

## Airflow DAG

The DAG (`dags/pipeline_dag.py`) runs daily at 06:00 UTC:

```
extract ──► transform ──► load
```

- **extract** — loads CSVs, pushes DataFrames as XCom JSON
- **transform** — cleans data, computes metrics, validates, pushes results to XCom
- **load** — reads XCom, uploads to BigQuery via `load_table_from_dataframe()`

---

## Terraform (GCP Infrastructure)

```bash
cd terraform

# Initialise providers
terraform init

# Preview changes
terraform plan -var="project_id=your-project-id"

# Deploy
terraform apply -var="project_id=your-project-id"
```

**Resources created:**
- `google_bigquery_dataset` — `security_monitoring` dataset
- `google_bigquery_table` — 10 tables (raw / clean / aggregated layers)
- `google_storage_bucket` — GCS bucket with lifecycle rules
- `google_cloud_run_v2_service` — Cloud Run container (uses your Docker image)

---

## Running Tests

```bash
# All tests with coverage report
pytest tests/ -v --cov=src --cov-report=term-missing

# Specific module
pytest tests/test_transformation.py -v
```

> Note: ingestion and validation tests run fully offline (no BigQuery needed). Tests that exercise the warehouse layer require GCP credentials and a live BigQuery project.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GCP_PROJECT` | Yes | GCP project ID (used by warehouse + agent) |
| `BQ_DATASET` | Yes | BigQuery dataset name (default: `security_monitoring`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes (local) | Path to service account JSON key |
| `ANTHROPIC_API_KEY` | For agent | Claude API key |
| `GCP_REGION` | For Terraform | GCP region (default: `us-central1`) |
| `ENVIRONMENT` | Optional | dev / staging / prod |

---

## Future Improvements

| Feature | Description |
|---------|-------------|
| Pub/Sub ingestion | Replace CSV files with real-time Pub/Sub messages |
| Cloud Composer | Replace local Airflow with managed Cloud Composer (Airflow on GCP) |
| Vertex AI | Add anomaly detection model for suspicious user scoring |
| Great Expectations | Replace custom checks with GE suites |
| CI/CD | GitHub Actions → Cloud Build → Cloud Run deployment |
| Alerting | Cloud Monitoring alerts for high failed login rates |
| Data Lineage | OpenLineage / Marquez integration |

---

*Production-ready MVP demonstrating end-to-end data engineering on GCP.*
