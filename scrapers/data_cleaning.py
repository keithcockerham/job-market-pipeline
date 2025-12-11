import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

STATE_NAME_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC'
}

STATE_CODES = set(STATE_NAME_TO_CODE.values())


def get_engine():

    load_dotenv('/opt/airflow/.env')
    
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_host = 'postgres'
    db_port = '5432'
    db_name = os.getenv('DB_NAME', 'job_market')
    
    return create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


def clean_empty_values(df):

    df = df.copy()
    df.replace([r"^\s*$", r"(?i)^null$", r"(?i)^none$", r"(?i)^nan$"], np.nan, regex=True, inplace=True)
    return df


def normalize_full_state_names(value):
  
    if pd.isna(value):
        return np.nan
    value_clean = str(value).strip()
    return STATE_NAME_TO_CODE.get(value_clean, np.nan)


def clean_location(df):
 
    df = df.copy()
    
    df[['location_city', 'location2']] = df['location'].str.split(',', n=1, expand=True)
    df['location2'] = df['location2'].str.strip()
    
    df['location_state'] = np.where(
        df['location2'].str.strip().str.upper().isin(STATE_CODES),
        df['location2'].str.strip().str.upper(),
        np.nan
    )
    
    df['location_else'] = np.where(df['location_state'].isna(), df['location2'], np.nan)
    
    state_names = set(STATE_NAME_TO_CODE.keys())
    mask = (
        df['location_state'].isna() &
        df['location_else'].notna() &
        ~df['location_else'].str.contains(r'[|,]', na=False) &
        df['location_else'].isin(state_names)
    )
    df.loc[mask, 'location_state'] = df.loc[mask, 'location_else'].map(STATE_NAME_TO_CODE)
    
    mask = (
    df['location_state'].isna() & 
    df['search_location'].notna() & 
    df['search_location'].str.strip().str.upper().isin(STATE_CODES)
)
    df.loc[mask, 'location_state'] = df.loc[mask, 'search_location'].str.strip().str.upper()
    
    df.drop(columns=['location_else', 'location2'], inplace=True, errors='ignore')
    
    return df


def extract_job_type_clean(df):

    df = df.copy()
    
    clean_mappings = {
        'Full-time': ['full-time', 'full time', 'fulltime', 'ft', 'permanent'],
        'Part-time': ['part-time', 'part time', 'parttime', 'pt'],
        'Contract': ['contract', 'contractor', 'contractual'],
        'Temporary': ['temporary', 'temp'],
        'Internship': ['internship', 'intern'],
    }
    
    def extract_type(value):
        if pd.isna(value):
            return np.nan
        value_lower = str(value).lower()
        for clean_type, patterns in clean_mappings.items():
            for pattern in patterns:
                if pattern in value_lower:
                    return clean_type
        return np.nan
    
    df['job_type_clean'] = df['job_type'].apply(extract_type)
    df.drop(columns=['job_type'], inplace=True, errors='ignore')
    df.rename(columns={'job_type_clean': 'job_type'}, inplace=True)
    
    return df


def fix_k_notation(df):
  
    df = df.copy()
    
    has_k = df['salary_text'].str.contains('k', case=False, na=False)
    needs_fix = has_k & (df['salary_min'].notna()) & (df['salary_min'] < 1000)
    
    df.loc[needs_fix, 'salary_min'] = df.loc[needs_fix, 'salary_min'] * 1000
    df.loc[needs_fix, 'salary_max'] = df.loc[needs_fix, 'salary_max'] * 1000
    
    return df


def fix_hourly_wages(df):

    df = df.copy()
    
    has_hourly = df['salary_text'].str.contains('per hour|/hour|an hour', case=False, na=False)
    needs_fix = has_hourly & (df['salary_min'].notna()) & (df['salary_min'] < 200)
    
    df.loc[needs_fix, 'salary_min'] = df.loc[needs_fix, 'salary_min'] * 2080
    df.loc[needs_fix, 'salary_max'] = df.loc[needs_fix, 'salary_max'] * 2080
    
    return df


def fix_monthly_wages(df):
 
    df = df.copy()
    
    has_monthly = df['salary_text'].str.contains('per month|/month|a month', case=False, na=False)
    needs_fix = has_monthly & (df['salary_min'].notna()) & (df['salary_min'] < 20000)
    
    df.loc[needs_fix, 'salary_min'] = df.loc[needs_fix, 'salary_min'] * 12
    df.loc[needs_fix, 'salary_max'] = df.loc[needs_fix, 'salary_max'] * 12
    
    return df


def fix_weekly_wages(df):

    df = df.copy()
    
    has_weekly = df['salary_text'].str.contains('per week|/week|a week', case=False, na=False)
    needs_fix = has_weekly & (df['salary_min'].notna()) & (df['salary_min'] < 5000)
    
    df.loc[needs_fix, 'salary_min'] = df.loc[needs_fix, 'salary_min'] * 52
    df.loc[needs_fix, 'salary_max'] = df.loc[needs_fix, 'salary_max'] * 52
    
    return df


def drop_per_unit_wages(df):
    df = df.copy()
    
    per_unit = df['salary_text'].str.contains('per unit', case=False, na=False)
    
    df = df[~per_unit].copy()
    
    return df


def impute_missing_max_by_source(df):
    df = df.copy()
    
    has_both = df['salary_min'].notna() & df['salary_max'].notna()
    spreads = df[has_both].copy()
    spreads['spread'] = spreads['salary_max'] - spreads['salary_min']
    
    median_spreads = spreads.groupby('source')['spread'].median().to_dict()
    
    only_min = df['salary_min'].notna() & df['salary_max'].isna()
    
    imputed = 0
    for source in df['source'].unique():
        mask = only_min & (df['source'] == source)
        if mask.any():
            spread = median_spreads.get(source, 10000)
            
            if spread == 0:
                df.loc[mask, 'salary_max'] = df.loc[mask, 'salary_min']
            else:
                df.loc[mask, 'salary_max'] = df.loc[mask, 'salary_min'] + spread
            
            imputed += mask.sum()

    return df


def run_full_cleaning(df):
 
    df = clean_empty_values(df)
    
    df = clean_location(df)

    df = extract_job_type_clean(df)
    
    df = fix_k_notation(df)
    df = fix_hourly_wages(df)
    df = fix_monthly_wages(df)
    df = fix_weekly_wages(df)
    df = drop_per_unit_wages(df)
    df = impute_missing_max_by_source(df)
    
    cols_to_drop = ['salary_period', 'salary_currency', 'created_at', 'updated_at', 'is_validated', 'is_duplicate', 'is_valid', 'validation_errors', 'source_url']
    for col in cols_to_drop:
        if col in df.columns:
            df.drop(columns=[col], inplace=True) 

    before = len(df)
    df = df.dropna(subset=['location_state'])
    
    if 'Muse' in df['source'].values:
        before = len(df)
        df = df[df['source'] != 'Muse']
    
    return df


def clean_and_load():
    engine = get_engine()
    
    df = pd.read_sql("SELECT * FROM raw_jobs", engine)
    
    df_clean = run_full_cleaning(df)
    
    df_clean = df_clean.sort_values('scraped_at', ascending=False).drop_duplicates(subset=['job_id'], keep='first')
    
    df_clean.to_sql('cleaned_jobs', engine, if_exists='replace', index=False, method='multi')
    
    return len(df_clean)


if __name__ == "__main__":
    clean_and_load()