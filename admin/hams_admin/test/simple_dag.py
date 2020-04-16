from __future__ import print_function
from hams_admin import HamsConnection, DockerContainerManager
from hams_admin.deployers import python as python_deployer
import json
import requests
from datetime import datetime
import time
import numpy as np
import signal
import sys





# Stop Hams on Ctrl-C
def signal_handler(signal, frame):
    print("Stopping Hams...")
    hams_conn = HamsConnection(DockerContainerManager())
    hams_conn.stop_all()
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    hams_conn = HamsConnection(DockerContainerManager())
    hams_conn.start_hams()


    f = open("../applications/simpledag/dag_description","r")
    dag_description = f.read()
    f.close()
    

    hams_conn.deploy_DAG("simpledag", "test", dag_description)


    #time.sleep(2)

    # For batch inputs set this number > 1
    # batch_size = 1

    # try:
    #     while True:
    #         if batch_size > 1:
    #             predict(
    #                 hams_conn.get_query_addr(),
    #                 [list(np.random.random(200)) for i in range(batch_size)],
    #                 batch=True)
    #         else:
    #             predict(hams_conn.get_query_addr(), np.random.random(200))
    #         time.sleep(0.2)
    # except Exception as e:
    #     hams_conn.stop_all()