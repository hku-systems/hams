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


if __name__ == '__main__':
    hams_conn = HamsConnection(DockerContainerManager())
 #   python_deployer.create_endpoint(hams_conn, "simple-example", "doubles",
 #                                   feature_sum)


    hams_conn.stop_all(graceful=False)
    time.sleep(2)

