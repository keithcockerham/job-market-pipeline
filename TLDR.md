# TL;DR - Job Market Data Pipeline

## What Is It?
An automated data pipeline that collects 44,000+ job postings nightly from 3 APIs, cleans the data, stores it in PostgreSQL, and visualizes insights on a dashboard.

## Tech Stack
**Docker** → **Airflow** → **Python** → **PostgreSQL** → **Plotly.js/GitHub Pages**

## The Pipeline (23 min runtime)
```
[Adzuna + USAJobs + Jooble APIs] (parallel, 17 min)
         ↓
    Load to raw_jobs
         ↓
    Clean & Transform
         ↓
    Load to cleaned_jobs
         ↓
    Dashboard JSON
```

## Key Numbers
| Metric | Value |
|--------|-------|
| Job postings | 44,000+ |
| Data sources | 3 APIs |
| States covered | 50 + DC |
| Pipeline runtime | ~23 minutes |
| Schedule | Nightly at 11 PM |

## Methods Used
- Docker multi-container orchestration
- Apache Airflow DAG development
- REST API integration & rate limiting
- ETL pipeline design
- Data cleaning & transformation (Pandas)
- PostgreSQL database management
- Error handling & retry logic
- Infrastructure as code

## Files That Matter
- `docker-compose.yml` - Infrastructure
- `dags/job_collection_dag.py` - Orchestration
- `scrapers/data_cleaning.py` - Transformations
- `docs/` - Dashboard

