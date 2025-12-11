import json
import time
from datetime import datetime
from adzuna_client import AdzunaClient

def collect_multi_strategy():
       
    client = AdzunaClient()
    all_jobs = []
    seen_ids = set()
  
    search_terms = [
        "data scientist",
        "data science",
        "machine learning",
        "ML engineer",
        "data engineer",
        "analytics",
        "data analyst",
        "AI engineer",
        "statistics"
    ]
    state_codes = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]
    
    for location in state_codes:
        for term in search_terms:
            try:
                jobs = client.collect_jobs(
                    what=term,
                    where=location,
                    max_pages=1 
                )
                
                new_jobs = 0
                for job in jobs:
                    if job['job_id'] not in seen_ids:
                        all_jobs.append(job)
                        seen_ids.add(job['job_id'])
                        new_jobs += 1
                
                time.sleep(2)  
                
            except Exception as e:
                print(f"Error: {e}")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'../data/raw/adzuna_{timestamp}.json'
    
    with open(filename, 'w') as f:
        json.dump(all_jobs, f, indent=2)
        
    search_query_counts = {}
    for job in all_jobs:
        query = job.get('search_query', 'unknown')
        search_query_counts[query] = search_query_counts.get(query, 0) + 1
        
    return all_jobs

if __name__ == "__main__":
    jobs = collect_multi_strategy()
  
