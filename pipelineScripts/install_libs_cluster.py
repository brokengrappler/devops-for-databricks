import os
import json
import requests
from manage_cluster_state import manage_dbrks_cluster_state

DBRKS_REQ_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['DATABRICKS_TOKEN']
}

def create_lib_request_body(lib_json):
    with open(f'./lib_configs/{lib_json}', "r") as stream:
        lib_config = json.load(stream)
    return lib_config


def install_standard_libs(cluster_id):
    DBRKS_INSTALL_ENDPOINT = 'api/2.0/libraries/install'
    manage_dbrks_cluster_state(cluster_id)
    lib_config_dict = create_lib_request_body('standard_libs.json')
    cluster_dict = json.loads(cluster_id)
    body = cluster_dict | lib_config_dict
    print(body)
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_INSTALL_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        data=json.dumps(body)
    )
    if response.status_code != 200:
        raise Exception(response.content)
    else:
        print(response.content)


def get_library_status(cluster_id):
    DBRKS_LIBLIST_ENDPOINT = 'api/2.0/libraries/install'
    cluster_dict = json.loads(cluster_id)
    cluster_value = cluster_dict['cluster_id']
    print(cluster_value)
    response = requests.post(
        f"{os.environ['DATABRICKS_HOST']}{DBRKS_LIBLIST_ENDPOINT}",
        headers=DBRKS_REQ_HEADERS,
        params=cluster_value
    )
    if response.status_code != 200:
        raise Exception(response.content)
    else:
        print(response.status_code)
