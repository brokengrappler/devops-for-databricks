import requests  # noqa: E902
import os
import json

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}

def start_dbrks_cluster(DBRKS_CLUSTER_ID):
    DBRKS_START_ENDPOINT = 'api/2.0/clusters/start'
    print(type(DBRKS_CLUSTER_ID))
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_START_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=DBRKS_CLUSTER_ID
    )
    if response.status_code != 200:
        raise Exception(json.loads(response.content))
    print(f'Starting {DBRKS_CLUSTER_ID}')
    return DBRKS_CLUSTER_ID


def restart_dbrks_cluster(DBRKS_CLUSTER_ID):
    DBRKS_RESTART_ENDPOINT = 'api/2.0/clusters/restart'
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_RESTART_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=json.loads(DBRKS_CLUSTER_ID)
    )
    if response.status_code != 200:
        raise Exception(json.loads(response.content))
    print(f'Starting {DBRKS_CLUSTER_ID}')
    return DBRKS_CLUSTER_ID
