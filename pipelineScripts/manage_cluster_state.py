import time
from cluster_status import get_dbrks_cluster_info
import restart_cluster as rs

def manage_dbrks_cluster_state(cluster_id):
    '''
    Provide status update on cluster creation based on cluster state
    returned from get_dbriks_cluster_info
    :param
        cluster_id:
    '''
    print(cluster_id)
    await_cluster = True
    start_time = time.time()
    loop_time = 1200  # 20 Minutes
    while await_cluster:
        current_time = time.time()
        elapsed_time = current_time - start_time
        cluster_state = get_dbrks_cluster_info(cluster_id)['state']
        if elapsed_time > loop_time:
            raise Exception(f'Error: Loop took over {loop_time} seconds to run.')
        if cluster_state == 'TERMINATED':
            rs.start_dbrks_cluster(cluster_id)
            print(f'Cluster was in status TERMINATED. Restarting {cluster_id}')
            time.sleep(60)
        elif cluster_state == 'RESTARTING':
            print('Cluster is Restarting')
            time.sleep(60)
        elif cluster_state == 'PENDING':
            print('Cluster is Pending Start')
            time.sleep(60)
        else:
            print('Cluster is Running')
            await_cluster = False
