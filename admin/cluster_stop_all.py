from __future__ import print_function
from hams_admin import Hams
Connection, DockerContainerManager
from hams_admin.deployers import python as python_deployer
import json
import requests
from datetime import datetime
import time
import numpy as np
import signal
import sys


if __name__ == '__main__':
    hams_conn = HamsConnection(DockerContainerManager())
 #   python_deployer.create_endpoint(hams_conn, "simple-example", "doubles",
 #                                   feature_sum)


    hams_conn.connect_host("202.45.128.174", "2375")
    hams_conn.connect_host("202.45.128.175", "2375")
    hams_conn.stop_all(graceful=False)
    time.sleep(2)

