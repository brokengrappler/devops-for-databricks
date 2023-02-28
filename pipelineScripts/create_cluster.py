import requests
import os
import json
from manage_cluster_state import manage_dbrks_cluster_state

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}


def create_cluster(config_json):
    '''
    Send post to create cluster on Databricks
    :param config_json:
    :return:
        json object of cluster id (ex format '{"cluster_id": "0223-172311-p3mk0u5w"}')
    '''
    DBRKS_START_ENDPOINT = 'api/2.0/clusters/create'
    with open(f'./cluster_configs/{config_json}', "r") as stream:
        cluster_config = json.load(stream)
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_START_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=json.dumps(cluster_config)
    )
    if response.status_code != 200:
        raise Exception(response.status_code)
    print(response.json())
    return response.json()
       

def list_clusters():
    ### not used yet ###
    DBRKS_ENDPOINT = 'api/2.0/clusters/list'
    response = requests.get(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS
    )
    if response.status_code != 200:
        raise Exception(response.content)
    else:
        return response.json()


def set_cluster_access():
    # test for myself
    pass


def main_create_cluster():
    cluster_id = create_cluster('basic-cluster.json')
    manage_dbrks_cluster_state(json.dumps(cluster_id))


if __name__ == '__main__':
    main_create_cluster()
