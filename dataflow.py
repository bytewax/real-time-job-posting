import requests
import json
from dataclasses import dataclass
from typing import List
import time 
import os
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax import operators as op
from bytewax.testing import run_main

from dotenv import load_dotenv
load_dotenv("./source.env")
api_key = os.getenv("api_key")
cache = {}

def fetch_job_listings(geo_code):
    """Generator to fetch job listings synchronously."""
    url = "https://jsearch.p.rapidapi.com/search"

    querystring = {"query":geo_code,"page":"1","num_pages":"1"}

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    cache_key = f"{geo_code}"
    if cache_key in cache:
        # Return cached data if available
        for job in cache[cache_key]:
            yield job
    else:
        # Fetch and cache data if not available
        fetched_data = []
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=10)
            data = response.json()
            if data["status"] == "OK":
                for job in data["data"]:
                    fetched_data.append(job)
                    yield job
                cache[cache_key] = fetched_data  # Cache the fetched data
            else:
                print(f"API error: {data.get('error', 'Unknown error')}")
            time.sleep(35)  # Delay to respect rate limits
        except requests.exceptions.Timeout:
            print("Request timed out")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")


class LinkedInPartition(StatefulSourcePartition):
    """Custom partition for LinkedIn job listings."""
    def __init__(self, geo_id):
        self.geo_id = geo_id

    def next_batch(self):
        return list(fetch_job_listings(self.geo_id))  # Fetch and return job listings as a list

    def snapshot(self):
        return None

@dataclass
class LinkedInSource(FixedPartitionedSource):
    """Source class to manage multiple partitions for fetching LinkedIn job listings."""
    geo_codes: List[str]

    def list_parts(self) -> List[str]:
        return self.geo_codes

    def build_part(self, step_id, for_key, _resume_state) -> LinkedInPartition:
        return LinkedInPartition(for_key)

# Initialize the Dataflow
flow = Dataflow("linkedin_jobs")
inp = op.input(
    "input", flow, LinkedInSource(["AI Engineer"])  
)
op.inspect("inspect", inp)
run_main(flow)
