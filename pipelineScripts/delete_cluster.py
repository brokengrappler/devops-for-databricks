import requests  # noqa: E902
import os
import json

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}


def delete_dbrks_cluster(DBRKS_CLUSTER_ID):
    DBRKS_RESTART_ENDPOINT = 'api/2.0/clusters/permanent-delete'
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_RESTART_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=DBRKS_CLUSTER_ID
    )
    if response.status_code != 200:
        raise Exception(json.loads(response.content))
    return response.status_code
