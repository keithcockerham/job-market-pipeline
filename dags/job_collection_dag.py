from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/scrapers')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job_collection_pipeline',
    default_args=default_args,
    description='Daily job market data collection and cleaning pipeline',
    schedule_interval='0 22 * * *',  
    catchup=False,
    max_active_runs=1,
)

def collect_adzuna_data():
    import subprocess
    
    result = subprocess.run(
        ['python', '/opt/airflow/scrapers/custom_adzuna.py'],
        cwd='/opt/airflow/scrapers',
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception("Adzuna collection failed")
    
    return "success"

collect_adzuna = PythonOperator(
    task_id='collect_adzuna',
    python_callable=collect_adzuna_data,
    dag=dag,
)

def collect_usajobs_data():

    import subprocess
    
    result = subprocess.run(
        ['python', '/opt/airflow/scrapers/custom_usajobs.py'],
        cwd='/opt/airflow/scrapers',
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception("USAJobs collection failed")
    
    return "success"

collect_usajobs = PythonOperator(
    task_id='collect_usajobs',
    python_callable=collect_usajobs_data,
    dag=dag,
)

def collect_jooble_data():
    import subprocess
    
    result = subprocess.run(
        ['python', '/opt/airflow/scrapers/custom_jooble.py'],
        cwd='/opt/airflow/scrapers',
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception("Jooble collection failed")
    
    return "success"

collect_jooble = PythonOperator(
    task_id='collect_jooble',
    python_callable=collect_jooble_data,
    dag=dag,
)

def cleanup_json_files():

    import glob
    import os
    
    json_files = glob.glob('/opt/airflow/data/raw/*.json')
    
    deleted_count = 0
    for json_file in json_files:
        try:
            os.remove(json_file)
            print(f"  Deleted: {json_file}")
            deleted_count += 1
        except Exception as e:
            print(f"  Error deleting {json_file}: {e}")
    
    return f"success: {deleted_count} files deleted"

cleanup_jsons = PythonOperator(
    task_id='cleanup_json_files',
    python_callable=cleanup_json_files,
    dag=dag,
)

def load_raw_to_db():

    import json
    import glob
    import pandas as pd
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    
    load_dotenv('/opt/airflow/.env')
    
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_host = 'postgres'
    db_port = '5432'
    db_name = os.getenv('DB_NAME', 'job_market')
    
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
 
    existing_ids = pd.read_sql("SELECT DISTINCT job_id FROM raw_jobs", engine)
    existing_set = set(existing_ids['job_id'].tolist())
   
    json_files = glob.glob('/opt/airflow/data/raw/*.json')
    
    total_loaded = 0
    total_skipped = 0
    
    for json_file in json_files:
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        if not data:
            continue
        
        df = pd.DataFrame(data)
        
        if 'job_id' not in df.columns:
            continue
        
        before_count = len(df)
        df = df[~df['job_id'].isin(existing_set)]
        skipped = before_count - len(df)
        total_skipped += skipped
        
        if len(df) == 0:
            continue
        
        df = df.drop_duplicates(subset=['job_id'], keep='first')
        
        new_ids = set(df['job_id'].tolist())
        existing_set.update(new_ids)
        
        try:
            df.to_sql('raw_jobs', engine, if_exists='append', index=False, method='multi')
            total_loaded += len(df)
        except Exception as e:
            print(f"  Error inserting: {e}")
    
    return f"success: {total_loaded} jobs loaded, {total_skipped} duplicates skipped"

load_raw = PythonOperator(
    task_id='load_raw_to_db',
    python_callable=load_raw_to_db,
    dag=dag,
)

def clean_data():

    import subprocess
    
    result = subprocess.run(
        ['python', '/opt/airflow/scrapers/data_cleaning.py'],
        cwd='/opt/airflow/scrapers',
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception("Data cleaning failed")
    
    return "success"

clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

def verify_cleaned_data():

    import pandas as pd
    from sqlalchemy import create_engine
    from dotenv import load_dotenv
    import os
    
    load_dotenv('/opt/airflow/.env')
    
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    connection_string = f'postgresql://{db_user}:{db_password}@postgres:5432/job_market'
    engine = create_engine(connection_string)
    
    raw_count = pd.read_sql("SELECT COUNT(*) as count FROM raw_jobs", engine).iloc[0, 0]
    clean_count = pd.read_sql("SELECT COUNT(*) as count FROM cleaned_jobs", engine).iloc[0, 0]
    
    if clean_count == 0:
        raise Exception("ERROR: cleaned_jobs table is empty!")
    
    if clean_count > raw_count:
        raise Exception(f"ERROR: More cleaned ({clean_count}) than raw ({raw_count}) - something is wrong!")
    
    columns = pd.read_sql("SELECT * FROM cleaned_jobs LIMIT 1", engine).columns.tolist()
    required = ['job_id', 'title', 'company', 'location_state', 'salary_min', 'source']
    missing = [col for col in required if col not in columns]
    
    if missing:
        raise Exception(f"ERROR: Missing required columns: {missing}")
        
    return "success"

verify_cleaned = PythonOperator(
    task_id='verify_cleaned_data',
    python_callable=verify_cleaned_data,
    dag=dag,
)

def generate_dashboard_data():
    """Generate JSON file for dashboard visualizations"""
    import subprocess
    
    print("Generating dashboard data...")
    
    result = subprocess.run(
        ['python', '/opt/airflow/scrapers/generate_dashboard_data.py'],
        cwd='/opt/airflow/scrapers',
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception("Dashboard data generation failed")
    
    print("Dashboard data generated")
    return "success"

generate_dashboard = PythonOperator(
    task_id='generate_dashboard_data',
    python_callable=generate_dashboard_data,
    dag=dag,
)

def generate_summary():

    from sqlalchemy import create_engine
    import pandas as pd
    from dotenv import load_dotenv
    
    load_dotenv('/opt/airflow/.env')
    
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    connection_string = f'postgresql://{db_user}:{db_password}@postgres:5432/job_market'
    engine = create_engine(connection_string)
  
    raw_count = pd.read_sql("SELECT COUNT(*) as count FROM raw_jobs", engine).iloc[0, 0]
    clean_count = pd.read_sql("SELECT COUNT(*) as count FROM cleaned_jobs", engine).iloc[0, 0]
    
    by_source = pd.read_sql("SELECT source, COUNT(*) as count FROM raw_jobs GROUP BY source", engine)
    
    return "success"

summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag,
)

[collect_adzuna, collect_usajobs, collect_jooble] >> load_raw >> cleanup_jsons >> clean >> verify_cleaned >> generate_dashboard >> summary