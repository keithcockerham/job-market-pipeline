import os
import pandas as pd
import numpy as np
import json
import re
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def generate_dashboard_data(output_path='/opt/airflow/docs/dashboard_data.json'):

    engine = create_engine('postgresql://postgres:postgres@localhost:5433/job_market')
    
    df = pd.read_sql("SELECT * FROM cleaned_jobs", engine)
    
    def get_source(url):
        if pd.isna(url):
            return None
        url = url.lower()
        if 'adzuna' in url:
            return 'Adzuna'
        elif 'usajobs.gov' in url:
            return 'USAJobs'
        elif 'jooble.org' in url:
            return 'Jooble'
        else:
            return None

    df['source_clean'] = df['job_url'].apply(get_source)
    df = df.dropna(subset=['source_clean'])
    
    df['salary_mid'] = (df['salary_min'] + df['salary_max']) / 2

    total_jobs = int(len(df))
    
    count_by_state = (
        df.groupby('location_state')
        .size()
        .sort_values(ascending=False)
        .to_dict()
    )
    
    avg_salary_by_state = (
        df[df['salary_mid'].notna()]
        .groupby('location_state')['salary_mid']
        .mean()
        .round(0)
        .astype(int)
        .to_dict()
    )
    
    avg_salary_by_source = (
        df[df['salary_mid'].notna()]
        .groupby('source_clean')['salary_mid']
        .mean()
        .round(0)
        .astype(int)
        .to_dict()
    )
    
    count_by_source = (
        df.groupby('source_clean')
        .size()
        .to_dict()
    )
    
    count_by_type = (
        df.groupby('job_type')
        .size()
        .sort_values(ascending=False)
        .to_dict()
    )
    
    job_type_by_source = (
        df.groupby(['source_clean', 'job_type'])
        .size()
        .unstack(fill_value=0)
        .to_dict(orient='index')
    )
    
    count_by_company = (
        df.groupby('company')
        .size()
        .sort_values(ascending=False)
        .head(15)
        .to_dict()
    )
    
    count_by_title = (
        df.groupby('title')
        .size()
        .sort_values(ascending=False)
        .head(15)
        .to_dict()
    )
    
    dashboard_data = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'total_jobs': total_jobs,
            'sources': list(count_by_source.keys()),
            'states_covered': len(count_by_state)
        },
        'total_jobs': total_jobs,
        'count_by_state': count_by_state,
        'avg_salary_by_state': avg_salary_by_state,
        'avg_salary_by_source': avg_salary_by_source,
        'count_by_source': count_by_source,
        'count_by_type': count_by_type,
        'job_type_by_source': job_type_by_source,
        'count_by_company': count_by_company,
        'count_by_title': count_by_title
    }
    
    with open(output_path, 'w') as f:
        json.dump(dashboard_data, f, indent=2)

    return dashboard_data

env_path = r'D:\Projects\job-market-pipeline\.env'
load_dotenv(dotenv_path=env_path, override=True)

connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(connection_string)

sql_query = "SELECT * FROM cleaned_jobs WHERE source != 'mock';"
jobs = pd.read_sql(sql_query, engine)
sql_query_raw = "SELECT * FROM raw_jobs WHERE source != 'mock';"
raw_data = pd.read_sql(sql_query_raw, engine)

generate_dashboard_data(output_path='D:\\Projects\\job-market-pipeline\\docs\\dashboard_data.json')