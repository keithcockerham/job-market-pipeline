import requests
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/adzuna.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AdzunaClient:
     
    BASE_URL = "http://api.adzuna.com/v1/api/jobs"  
    
    def __init__(self, app_id: str = None, app_key: str = None, country: str = "us"):
    
        self.app_id = app_id or os.getenv('ADZUNA_API_ID')
        self.app_key = app_key or os.getenv('ADZUNA_API_KEY')
        self.country = country
        
        if not self.app_id or not self.app_key:
            raise ValueError(
                "Adzuna credentials not found! "
            )
        
        logger.info(f"Initialized Adzuna client for country: {country}")
    
    def _build_url(self, endpoint: str) -> str:
        return f"{self.BASE_URL}/{self.country}/{endpoint}"
    
    def _make_request(self, url: str, params: Dict) -> Dict:
     
        params['app_id'] = self.app_id
        params['app_key'] = self.app_key
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.error("Rate limit exceeded!")
            elif response.status_code == 401:
                logger.error("Authentication failed!")
            else:
                logger.error(f"HTTP error: {e}")
            raise
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def search_jobs(
        self,
        what: str = "data scientist",
        where: str = "Houston, TX",
        results_per_page: int = 50,  
        page: int = 1,
        max_days_old: int = None,  
        full_time: bool = None,
        salary_min: int = None,
        salary_max: int = None
    ) -> Dict:
       
        url = self._build_url("search")
        
        params = {
            'what': what,
            'where': where,
            'results_per_page': min(results_per_page, 50),  
        }
        
        if page > 1:
            params['page'] = page
        
        if max_days_old is not None:
            params['max_days_old'] = max_days_old
        if full_time is not None:
            params['full_time'] = 1 if full_time else 0
        if salary_min:
            params['salary_min'] = salary_min
        if salary_max:
            params['salary_max'] = salary_max
        
        logger.info(f"Searching: '{what}' in '{where}' (page {page})")
        
        response = self._make_request(url, params)
        
        logger.info(
            f"Found {response.get('count', 0)} total results, "
            f"returned {len(response.get('results', []))} jobs"
        )
        
        return response
    
    def get_job_details(self, job_id: str) -> Dict:
      
        url = self._build_url(f"details/{job_id}")
        params = {}
        
        logger.info(f"Fetching job details: {job_id}")
        return self._make_request(url, params)
    
    def transform_to_schema(self, adzuna_job: Dict) -> Dict:

        location_data = adzuna_job.get('location', {})
        location_parts = []
        if location_data.get('display_name'):
            location_parts.append(location_data['display_name'])
        elif location_data.get('area'):
            location_parts.extend(location_data['area'])
        location = ', '.join(location_parts) if location_parts else None
        
        salary_min = adzuna_job.get('salary_min')
        salary_max = adzuna_job.get('salary_max')
        salary_text = None
        
        if salary_min and salary_max:
            salary_text = f"${salary_min:,.0f} - ${salary_max:,.0f} a year"
        elif salary_min:
            salary_text = f"From ${salary_min:,.0f} a year"
        elif salary_max:
            salary_text = f"Up to ${salary_max:,.0f} a year"
        
        contract_type = adzuna_job.get('contract_type', '')
        job_type_map = {
            'full_time': 'Full-time',
            'part_time': 'Part-time',
            'contract': 'Contract',
            'permanent': 'Full-time'
        }
        job_type = job_type_map.get(contract_type.lower(), contract_type)
        
        created_date = adzuna_job.get('created')
        posted_date = created_date.split('T')[0] if created_date else None
        
        return {
            'job_id': adzuna_job.get('id'),
            'title': adzuna_job.get('title'),
            'company': adzuna_job.get('company', {}).get('display_name'),
            'location': location,
            'salary_text': salary_text,
            'salary_min': salary_min,
            'salary_max': salary_max,
            'salary_currency': 'USD',
            'salary_period': 'year',
            'description_snippet': adzuna_job.get('description', ''),
            'posted_date': posted_date,
            'job_type': job_type,
            'job_url': adzuna_job.get('redirect_url'),
            'source': 'Adzuna',
            'scraped_at': datetime.now().isoformat(),
            'search_location': None,  
            'search_query': None      
        }
    
    def collect_jobs(
        self,
        what: str = "data scientist",
        where: str = "Houston", 
        max_pages: int = 8,  
        delay_between_pages: float = 1.0,
        **kwargs
    ) -> List[Dict]:

        all_jobs = []
        
        for page in range(1, max_pages + 1):
            try:
                response = self.search_jobs(
                    what=what,
                    where=where,
                    page=page,
                    **kwargs
                )
                
                results = response.get('results', [])
                
                if not results:
                    logger.info(f"No more results on page {page}, stopping")
                    break
                
                for job in results:
                    transformed = self.transform_to_schema(job)
                    transformed['search_location'] = where
                    transformed['search_query'] = what
                    all_jobs.append(transformed)
                
                logger.info(f"Page {page}: collected {len(results)} jobs")
                
                total_results = response.get('count', 0)
                results_per_page = response.get('results_per_page', 50)
                max_page = (total_results // results_per_page) + 1
                
                if page >= max_page:
                    logger.info(f"Reached last page ({page} of {max_page})")
                    break
                
                if page < max_pages:
                    logger.info(f"Waiting {delay_between_pages}s before next page...")
                    time.sleep(delay_between_pages)
            
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                break
        
        logger.info(f"Total jobs collected: {len(all_jobs)}")
        return all_jobs
    
    def get_category_stats(self) -> Dict:
  
        url = self._build_url("categories")
        params = {}
        
        logger.info("Fetching category statistics")
        return self._make_request(url, params)
    
    def get_location_stats(self, what: str = "data scientist") -> Dict:
     
        url = self._build_url("top_companies")
        params = {'what': what}
        
        logger.info(f"Fetching location stats for: {what}")
        return self._make_request(url, params)


def main():

    try:
        client = AdzunaClient(country="us")
    except ValueError as e:
        print(f"\nError: {e}")
        return
    
    print("\n📡 Collecting job postings...")
    jobs = client.collect_jobs(
        what="data scientist",
        where="Houston",
        max_pages=8,  
        delay_between_pages=1.0
    )
    
    output_file = '../data/raw/adzuna_jobs.json'
    with open(output_file, 'w') as f:
        json.dump(jobs, f, indent=2)

if __name__ == "__main__":
    main()
