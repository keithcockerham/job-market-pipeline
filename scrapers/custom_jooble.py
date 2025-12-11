# custom_jooble.py

import json
import time
from datetime import datetime
from jooble_client import JoobleClient

def collect_multi_strategy():
    client = JoobleClient()
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
        "statistics",
    ]
    state_codes = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    ]
    for location in state_codes:
        for term in search_terms:
            try:
                jobs = client.collect_jobs(
                    what=term,
                    where=location,
                    max_pages=1,
                    results_per_page=50,
                )

                new_jobs = 0
                for job in jobs:
                    jid = job.get("job_id")
                    if jid and jid not in seen_ids:
                        all_jobs.append(job)
                        seen_ids.add(jid)
                        new_jobs += 1

                print(f"Jooble: {location} / '{term}' -> {new_jobs} new jobs")

                time.sleep(2)  # be nice to their API

            except Exception as e:
                print(f"Jooble error for {location} / '{term}': {e}")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'../data/raw/jooble_{timestamp}.json'
    with open(filename, "w") as f:
        json.dump(all_jobs, f, indent=2)

    print(f"Jooble: collected {len(all_jobs)} total jobs")
    return all_jobs


if __name__ == "__main__":
    collect_multi_strategy()
