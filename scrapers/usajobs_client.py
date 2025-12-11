"""
USAJobs.gov API Client - Federal Government Job Data
Official API for US federal government job listings
"""

import requests
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

# Load environment variables

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/usajobs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class USAJobsClient:
    """
    Official USAJobs.gov API client for federal job data
    
    API Documentation: https://developer.usajobs.gov/
    Rate Limits: No explicit limit, but be respectful
    """
    
    BASE_URL = "https://data.usajobs.gov/api"
    
    def __init__(self, api_key: str = None, email: str = None):

        self.api_key = api_key or os.getenv('USAJOBS_API_KEY')
        self.email = email or os.getenv('USAJOBS_EMAIL')
        
        if not self.api_key:
            raise ValueError(
                "USAJobs API key not found! "
                "Set USAJOBS_API_KEY in .env file"
            )
        
        if not self.email:
            raise ValueError(
                "Email not found! "
                "Set USAJOBS_EMAIL in .env file"
            )
        
        logger.info("Initialized USAJobs client")
    
    def _get_headers(self) -> Dict[str, str]:
        """Build request headers"""
        return {
            'Authorization-Key': self.api_key,
            'User-Agent': self.email,
            'Host': 'data.usajobs.gov'
        }
    
    def _make_request(self, endpoint: str, params: Dict) -> Dict:

        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=15
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                logger.error("Authentication failed! Check your API key.")
            elif response.status_code == 403:
                logger.error("Access forbidden! Check your email in User-Agent.")
            else:
                logger.error(f"HTTP error: {e}")
            raise
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def search_jobs(
        self,
        keyword: str = "data scientist",
        location_name: str = None,
        results_per_page: int = 500,
        page: int = 1,
        date_posted: int = 30,
        position_offering_type_code: str = None,
        remote_indicator: bool = None,
        travel_percentage: str = None,
        salary_min: int = None,
        salary_max: int = None
    ) -> Dict:
    
        params = {
            'Keyword': keyword,
            'ResultsPerPage': min(results_per_page, 500),  
            'Page': page,
            'DatePosted': date_posted
        }
        
        if location_name:
            params['LocationName'] = location_name
        if position_offering_type_code:
            params['PositionOfferingTypeCode'] = position_offering_type_code
        if remote_indicator:
            params['RemoteIndicator'] = 'true'
        if travel_percentage:
            params['TravelPercentage'] = travel_percentage
        if salary_min:
            params['SalaryBucket'] = f"{salary_min}-{salary_max or 999999}"
        
        logger.info(
            f"Searching: '{keyword}' in '{location_name or 'All locations'}' "
            f"(page {page})"
        )
        
        response = self._make_request("search", params)
        
        search_result = response.get('SearchResult', {})
        result_count = search_result.get('SearchResultCount', 0)
        items = search_result.get('SearchResultItems', [])
        
        logger.info(f"Found {result_count} total results, returned {len(items)} jobs")
        
        return response
    
    def transform_to_schema(self, usajobs_item: Dict) -> Dict:
      
        matched_object = usajobs_item.get('MatchedObjectDescriptor', {})
        position_title = matched_object.get('PositionTitle', '')
        position_id = matched_object.get('PositionID', '')
        
        org_name = matched_object.get('OrganizationName', '')
        dept_name = matched_object.get('DepartmentName', '')
        company = org_name or dept_name
        
        locations = matched_object.get('PositionLocation', [])
        if locations and len(locations) > 0:
            loc = locations[0]  # Use first location
            city = loc.get('CityName', '')
            state = loc.get('StateName', '')
            location = f"{city}, {state}" if city and state else (city or state)
        else:
            location = None
        
        salary_data = matched_object.get('PositionRemuneration', [])
        salary_text = None
        salary_min = None
        salary_max = None
        
        if salary_data and len(salary_data) > 0:
            sal = salary_data[0]
            min_sal = sal.get('MinimumRange')
            max_sal = sal.get('MaximumRange')
            rate_interval = sal.get('RateIntervalCode', '')  # PA = Per Annum
            
            if min_sal and max_sal:
                salary_text = f"${float(min_sal):,.0f} - ${float(max_sal):,.0f} a year"
                salary_min = float(min_sal)
                salary_max = float(max_sal)
            elif min_sal:
                salary_text = f"From ${float(min_sal):,.0f} a year"
                salary_min = float(min_sal)
            
            if rate_interval not in ['PA', 'PER YEAR', '']:
                salary_text = f"{salary_text} ({rate_interval})" if salary_text else None
        
        position_offering = matched_object.get('PositionOfferingType', [])
        job_type = None
        if position_offering:
            offering = position_offering[0]
            offering_name = offering.get('Name', '')
            type_map = {
                'Permanent': 'Full-time',
                'Temporary': 'Temporary',
                'Term': 'Contract',
                'Intermittent': 'Part-time'
            }
            job_type = type_map.get(offering_name, offering_name)
        
        publication_start = matched_object.get('PublicationStartDate', '')
        posted_date = publication_start.split('T')[0] if publication_start else None
        
        qualifications = matched_object.get('QualificationSummary', '')
        user_area = matched_object.get('UserArea', {})
        details = user_area.get('Details', {})
        summary = details.get('JobSummary', '')
        description = summary or qualifications
        
        position_uri = matched_object.get('PositionURI', '')
        apply_uri = matched_object.get('ApplyURI', [])
        job_url = position_uri or (apply_uri[0] if apply_uri else None)
        
        is_remote = matched_object.get('PositionLocationDisplay', '').lower() == 'remote'
        
        return {
            'job_id': position_id,
            'title': position_title,
            'company': company,
            'location': location,
            'salary_text': salary_text,
            'salary_min': salary_min,
            'salary_max': salary_max,
            'salary_currency': 'USD',
            'salary_period': 'year',
            'description_snippet': description[:500] if description else None,  # Truncate
            'posted_date': posted_date,
            'job_type': job_type,
            'job_url': job_url,
            'source': 'USAJobs',
            'scraped_at': datetime.now().isoformat(),
            'search_location': None,  # Set when calling
            'search_query': None      # Set when calling
        }
    
    def collect_jobs(
        self,
        keyword: str = "data scientist",
        location_name: str = None,
        max_pages: int = 3,
        delay_between_pages: float = 1.0,
        **kwargs
    ) -> List[Dict]:
   
        all_jobs = []
        
        for page in range(1, max_pages + 1):
            try:
    
                response = self.search_jobs(
                    keyword=keyword,
                    location_name=location_name,
                    page=page,
                    **kwargs
                )
                
                search_result = response.get('SearchResult', {})
                items = search_result.get('SearchResultItems', [])
                
                if not items:
                    logger.info(f"No more results on page {page}, stopping")
                    break
                
                for item in items:
                    try:
                        transformed = self.transform_to_schema(item)
                        transformed['search_location'] = location_name
                        transformed['search_query'] = keyword
                        all_jobs.append(transformed)
                    except Exception as e:
                        logger.warning(f"Error transforming job: {e}")
                        continue
                
                logger.info(f"Page {page}: collected {len(items)} jobs")
                
                result_count = search_result.get('SearchResultCount', 0)
                results_per_page = search_result.get('SearchResultCountAll', 500)
                
                if len(all_jobs) >= result_count:
                    logger.info(f"Collected all {result_count} available jobs")
                    break
                
                if page < max_pages:
                    logger.info(f"Waiting {delay_between_pages}s before next page...")
                    time.sleep(delay_between_pages)
            
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                break
        
        logger.info(f"Total federal jobs collected: {len(all_jobs)}")
        return all_jobs


def main():
 
    print("USAJobs.gov API Data Collection")
    print("=" * 80)
    
    try:
        client = USAJobsClient()
    except ValueError as e:
        print(f"\nError: {e}")
        return
    search_terms = [
        "data scientist",
        "data science",
        "machine learning",
        "ML engineer",
        "data engineer",
        "analytics",
        "data analyst",
        "AI engineer",
        "research scientist",
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
        for search_term in search_terms:

            jobs = client.collect_jobs(
                keyword=search_term,
                location_name=location,
                max_pages=2,  
                date_posted=30,  
                delay_between_pages=1.0
            )
    
    output_file = './data/raw/usajobs_jobs.json'
    with open(output_file, 'w') as f:
        json.dump(jobs, f, indent=2)

if __name__ == "__main__":
    main()
