import requests
import time
import os
import json

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}


def create_cluster(config_json):
    '''
    Send post to create cluster on Databricks
    :param config_json:
    :return:
        json object of cluster id (ex format {'cluster_id': '0221-141820-74a5ejmc'})
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
    if response.status_code == 200:
        return json.loads(response.content)
    else:
        raise Exception(response.json())


def set_cluster_access():
    # test for myself
    pass


def manage_dbrks_cluster_state(cluster_id):
    '''
    Provide status update on cluster creation based on cluster state
    returned from get_dbriks_cluster_info
    :param cluster_id:
    :return:
    '''
    await_cluster = True
    cluster_restarted = False
    start_time = time.time()
    loop_time = 1200  # 20 Minutes
    cluster_state = get_dbrks_cluster_info(cluster_id)['state']
    while await_cluster:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > loop_time:
            raise Exception(f'Error: Loop took over {loop_time} seconds to run.')
        if cluster_state == 'TERMINATED':
            print('Starting Terminated Cluster')
            raise ValueError("Failed to create cluster, cluster terminated")
        elif cluster_state == 'RESTARTING':
            print('Cluster is Restarting')
            time.sleep(60)
        elif cluster_state == 'PENDING':
            print('Cluster is Pending Start')
            time.sleep(60)
        else:
            print('Cluster is Running')
            await_cluster = False


def main_create_cluster():
    cluster_id = create_cluster('basic-cluster.json')
    manage_dbrks_cluster_state(json.dumps(cluster_id))


if __name__ == '__main__':
    main_create_cluster()
