import os
import json
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

api_client = ApiClient(
    host=os.getenv('DATABRICKS_HOST'),
    token=os.getenv('DATABRICKS_TOKEN')
)

jobs_api = JobsApi(api_client)

def get_jobs_config(job_json):
    with open(f'./job_configs/{job_json}', "r") as stream:
        job_config = json.load(stream)
    return job_config


payload = get_jobs_config('job_config.json')

jobs_api.create_job(payload, None, '2.1')
