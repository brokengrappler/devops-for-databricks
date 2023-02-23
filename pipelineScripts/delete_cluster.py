import requests  # noqa: E902
import os
import json

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}


def delete_dbrks_cluster(DBRKS_CLUSTER_ID):
    '''
    Permanentely deletes a cluster
    :param DBRKS_CLUSTER_ID:
        json string of cluster_id key value
        e.g., '{ "cluster_id": "1234-567890-frays123" }'
    :return:
        {}
    '''
    DBRKS_RESTART_ENDPOINT = 'api/2.0/clusters/permanent-delete'
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_RESTART_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=json.dumps(DBRKS_CLUSTER_ID)
    )
    if response.status_code != 200:
        raise Exception(json.loads(response.content))
    return response.json()
