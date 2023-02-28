import requests
import os
import json

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}

def get_dbrks_cluster_info(DBRKS_CLUSTER_ID):
    '''

    :param DBRKS_CLUSTER_ID:
    :return:
    '''
    DBRKS_INFO_ENDPOINT = 'api/2.0/clusters/get'
    response = requests.get(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_INFO_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=DBRKS_CLUSTER_ID
    )
    print(response.url)
    if response.status_code == 200:
        return json.loads(response.content)
    else:
        raise Exception(response.json())
