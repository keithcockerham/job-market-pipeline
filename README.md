# Job Market Data Pipeline

A production-ready data engineering pipeline that collects, cleans, and visualizes job market data from multiple APIs. Built to include end-to-end data engineering including API integration, ETL orchestration, data quality management, and infrastructure automation.

![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=flat&logo=plotly&logoColor=white)

## Project Overview

This pipeline automatically collects data science and analytics job postings from three major job APIs, processes them through a cleaning pipeline, and stores them in PostgreSQL for analysis. A companion dashboard visualizes key insights including geographic distribution, salary trends, and market composition.

### Key Features

- **Automated Collection**: Nightly scheduled runs via Apache Airflow
- **Multi-Source Integration**: Adzuna, USAJobs, and Jooble APIs
- **Data Quality Pipeline**: Salary normalization, location standardization, deduplication
- **Containerized Infrastructure**: Docker Compose with 4 services
- **Interactive Dashboard**: GitHub Pages with Plotly.js visualizations

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Airflow                           │
│                    (Scheduled: 11 PM Daily)                     │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│  Adzuna API   │       │ USAJobs API   │       │  Jooble API   │
│  (Private)    │       │  (Federal)    │       │ (Aggregator)  │
└───────────────┘       └───────────────┘       └───────────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                ▼
                    ┌───────────────────┐
                    │   JSON Staging    │
                    │  /data/raw/*.json │
                    └───────────────────┘
                                │
                                ▼
                    ┌───────────────────┐
                    │  PostgreSQL DB    │
                    │   raw_jobs table  │
                    └───────────────────┘
                                │
                                ▼
                    ┌───────────────────┐
                    │  Data Cleaning    │
                    │  - Salary norm    │
                    │  - Location std   │
                    │  - Deduplication  │
                    └───────────────────┘
                                │
                                ▼
                    ┌───────────────────┐
                    │  PostgreSQL DB    │
                    │ cleaned_jobs table│
                    └───────────────────┘
                                │
                                ▼
                    ┌───────────────────┐
                    │    Dashboard      │
                    │  GitHub Pages     │
                    └───────────────────┘
```

## Technology Stack

| Category | Technologies |
|----------|-------------|
| **Orchestration** | Apache Airflow 2.8.1 |
| **Database** | PostgreSQL 15 |
| **Containerization** | Docker, Docker Compose |
| **Language** | Python 3.11 |
| **Data Processing** | Pandas, SQLAlchemy |
| **Visualization** | Plotly.js |
| **Hosting** | GitHub Pages |

## Project Structure

```
job-market-pipeline/
├── dags/
│   └── job_collection_dag.py    # Main Airflow DAG
├── scrapers/
│   ├── adzuna_client.py         # Adzuna API client
│   ├── usajobs_client.py        # USAJobs API client
│   ├── jooble_client.py         # Jooble API client
│   ├── custom_adzuna.py         # Adzuna collection script
│   ├── custom_usajobs.py        # USAJobs collection script
│   ├── custom_jooble.py         # Jooble collection script
│   └── data_cleaning.py         # Cleaning transformations
├── docs/
│   ├── index.html               # Dashboard homepage
│   ├── pipeline.html            # Architecture page
│   ├── insights.html            # Data insights page
│   ├── job-market-charts.js     # Plotly chart definitions
│   ├── dashboard_data.json      # Aggregated data for charts
│   └── style.css                # Dashboard styling
├── data/
│   └── raw/                     # Temporary JSON staging
├── logs/                        # Airflow & API logs
├── docker-compose.yml           # Multi-container config
├── Dockerfile.airflow           # Custom Airflow image
├── requirements.txt             # Python dependencies
├── .env                         # API credentials (not in repo)
├── TLDR.md
└── README.md
```

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.11+
- API keys for Adzuna, USAJobs, and Jooble

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/job-market-pipeline.git
cd job-market-pipeline
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```env
# Adzuna API
ADZUNA_API_ID=your_app_id
ADZUNA_API_KEY=your_api_key

# USAJobs API
USAJOBS_API_KEY=your_api_key
USAJOBS_EMAIL=your_email@example.com

# Jooble API
JOOBLE_API_KEY=your_api_key

# PostgreSQL
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=job_market
```

### 3. Build and Start Containers

```bash
# Build custom Airflow image
docker-compose build

# Start all services
docker-compose up -d

# Verify containers are running
docker-compose ps
```

### 4. Initialize Airflow

```bash
# Run database migrations
docker-compose run --rm airflow-webserver airflow db migrate

# Create admin user
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 5. Access Services

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **PostgreSQL**: localhost:5433 (postgres/postgres)

### 6. Configure Airflow Connection

In Airflow UI → Admin → Connections → Add:

| Field | Value |
|-------|-------|
| Connection Id | `job_market_postgres` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema | `job_market` |
| Login | `postgres` |
| Password | `postgres` |
| Port | `5432` |

## Pipeline Tasks

The DAG consists of 8 tasks:

| Task | Description | Duration |
|------|-------------|----------|
| `collect_adzuna` | Query Adzuna API (parallel) | ~17 min |
| `collect_usajobs` | Query USAJobs API (parallel) | ~17 min |
| `collect_jooble` | Query Jooble API (parallel) | ~17 min |
| `load_raw_to_db` | Load JSON to raw_jobs table | ~1 min |
| `cleanup_json_files` | Delete processed JSON files | <1 min |
| `clean_data` | Run cleaning transformations | ~2 min |
| `verify_cleaned_data` | Validate data quality | <1 min |
| `generate_summary` | Log run statistics | <1 min |

**Total Runtime**: ~23 minutes (collection runs in parallel)

## Data Cleaning Transformations

| Transformation | Issue Addressed | Records Affected |
|---------------|-----------------|------------------|
| K Notation Fix | "$150k" → "$150,000" | ~6,300 |
| Hourly to Annual | "$40/hr" → "$83,200/yr" | ~1,300 |
| Monthly to Annual | "$3,000/mo" → "$36,000/yr" | ~150 |
| Weekly to Annual | "$1,000/wk" → "$52,000/yr" | ~45 |
| Location Parsing | "Houston, Texas" → "Houston, TX" | All |
| State Standardization | Full names → 2-letter codes | ~2,000 |
| Job Type Extraction | Free text → categories | ~5,500 |
| Salary Max Imputation | Missing max → min + median spread | ~1,500 |
| Deduplication | Remove duplicate job_ids | ~59,000 |

## Key Insights

From analysis of 44,000+ job postings:

- **Top States**: CA (8,500+), TX (5,200+), NY (4,800+)
- **Salary Premium**: Private sector pays ~45% more than federal ($140K vs $96K avg)
- **Job Types**: 78% Full-time, 12% Contract, 10% Other
- **Data Coverage**: 51 locations (50 states + DC)

## Common Commands

```bash
# Start/stop containers
docker-compose up -d
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler

# Trigger DAG manually
docker exec airflow_scheduler airflow dags trigger job_collection_pipeline

# Check DAG status
docker exec airflow_scheduler airflow dags list-runs -d job_collection_pipeline

# Query database
docker exec job_market_postgres psql -U postgres -d job_market -c "SELECT COUNT(*) FROM cleaned_jobs"

# Restart scheduler after DAG changes
docker-compose restart airflow-scheduler
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing in UI | Check for import errors: `docker exec airflow_scheduler airflow dags list-import-errors` |
| Database connection failed | Verify container name is `postgres` not `localhost` in Airflow connection |
| Package not found | Rebuild image: `docker-compose build --no-cache` |
| Port 5432 in use | Pipeline uses 5433 externally → 5432 internally |
| Rate limit exceeded | Adzuna: 250 req/day. Increase delays or reduce search terms |

## API Rate Limits

| API | Limit | Strategy |
|-----|-------|----------|
| Adzuna | 250 requests/day | 50 results/page, 2s delays |
| USAJobs | No strict limit | Be polite, 2s delays |
| Jooble | Unknown | 2s delays, monitor responses |

## License

MIT License - feel free to use this project as a template for your own data engineering project.

## Author

**Keith**  
Data Scientist → Solutions Engineer  
[GitHub](https://github.com/keithcockerham) | [LinkedIn](https://linkedin.com/in/kcockerham)

---

*Built as a project for data engineering including ETL pipeline development, API integration, data quality management, and infrastructure automation.*
