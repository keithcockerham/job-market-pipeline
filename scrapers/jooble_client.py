# jooble_client.py

import os
import re
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/opt/airflow/logs/jooble.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class JoobleClient:
    """
    Client for Jooble REST API.

    Docs:
      - https://jooble.org/api/about
      - POST https://jooble.org/api/{api_key}
    """

    BASE_URL = "https://jooble.org/api"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("JOOBLE_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Jooble API key not found! "
                "Set JOOBLE_API_KEY in your environment or pass api_key explicitly."
            )
        logger.info("Initialized Jooble client")

    def _make_request(self, payload: Dict) -> Dict:
   
        url = f"{self.BASE_URL}/{self.api_key}"
        logger.debug(f"POST {url} payload={payload}")

        try:
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error from Jooble: {e} (status={resp.status_code})")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to Jooble failed: {e}")
            raise

    def search_jobs(
        self,
        what: str = "data scientist",
        where: str = "Houston, TX",
        page: int = 1,
        results_per_page: int = 50,
        radius: Optional[str] = None,
        salary_min: Optional[int] = None,
        companysearch: bool = False,
    ) -> Dict:
 
        payload: Dict[str, object] = {
            "keywords": what,
            "location": where,
            "page": str(page),
            "ResultOnPage": str(results_per_page),
            "companysearch": "true" if companysearch else "false",
        }

        if radius is not None:
            payload["radius"] = str(radius)
        if salary_min is not None:
            payload["salary"] = int(salary_min)

        logger.info(f"Jooble search: '{what}' in '{where}' (page {page})")
        return self._make_request(payload)

    @staticmethod
    def _parse_salary(salary_str: Optional[str]):
    
        if not salary_str:
            return None, None, None

        try:
            parts = salary_str.strip().split()
            if not parts:
                return None, None, None

            currency = parts[-1]
            # Grab all numeric substrings
            nums = re.findall(r"[\d.,]+", salary_str)
            if not nums:
                return None, None, currency

            vals = [float(n.replace(",", "")) for n in nums]
            if len(vals) == 1:
                return vals[0], None, currency
            return vals[0], vals[1], currency
        except Exception:
            return None, None, None

    def transform_to_schema(self, job: Dict) -> Dict:
    
        salary_str = job.get("salary")
        salary_min, salary_max, currency = self._parse_salary(salary_str)

        updated = job.get("updated")
        posted_date = updated.split("T")[0] if updated else None

        return {
            "job_id": job.get("id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "salary_text": salary_str,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": currency,
            "salary_period": "year",  # Jooble doesn't specify; you can refine later
            "description_snippet": job.get("snippet", ""),
            "posted_date": posted_date,
            "job_type": job.get("type"),
            "job_url": job.get("link"),
            "source": job.get("source") or "Jooble",
            "scraped_at": datetime.now().isoformat(),
            "search_location": None,
            "search_query": None,
        }

    def collect_jobs(
        self,
        what: str = "data scientist",
        where: str = "Houston, TX",
        max_pages: int = 8,
        results_per_page: int = 50,
        delay_between_pages: float = 1.0,
        **kwargs,
    ) -> List[Dict]:
  
        all_jobs: List[Dict] = []

        for page in range(1, max_pages + 1):
            try:
                response = self.search_jobs(
                    what=what,
                    where=where,
                    page=page,
                    results_per_page=results_per_page,
                    **kwargs,
                )

                jobs = response.get("jobs", [])
                total_count = response.get("totalCount", 0)

                if not jobs:
                    logger.info(f"Jooble: no more results on page {page}, stopping")
                    break

                for job in jobs:
                    transformed = self.transform_to_schema(job)
                    transformed["search_location"] = where
                    transformed["search_query"] = what
                    all_jobs.append(transformed)

                logger.info(
                    f"Jooble page {page}: collected {len(jobs)} jobs "
                    f"(totalCount={total_count})"
                )

                # Rough page limit based on totalCount / results_per_page
                if results_per_page > 0:
                    max_page = (total_count // results_per_page) + 1
                    if page >= max_page:
                        logger.info(f"Jooble reached last page ({page} of {max_page})")
                        break

                if page < max_pages:
                    logger.info(
                        f"Jooble: waiting {delay_between_pages}s before next page..."
                    )
                    time.sleep(delay_between_pages)

            except Exception as e:
                logger.error(f"Jooble error on page {page}: {e}")
                break

        logger.info(f"Jooble total jobs collected: {len(all_jobs)}")
        return all_jobs


def main():
    try:
        client = JoobleClient()
    except ValueError as e:
        print(f"\nError: {e}")
        return

    print("\n📡 Collecting Jooble job postings...")
    jobs = client.collect_jobs(
        what="data scientist",
        where="Houston, TX",
        max_pages=3,
        results_per_page=50,
        delay_between_pages=1.0,
    )

    output_file = "../data/raw/jooble_jobs.json"
    with open(output_file, "w") as f:
        json.dump(jobs, f, indent=2)

    print(f"Saved {len(jobs)} jobs to {output_file}")


if __name__ == "__main__":
    main()
