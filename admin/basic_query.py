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


def predict(addr, x, batch=False):
    url = "http://%s/simple-example/predict" % addr

    if batch:
        req_json = json.dumps({'input_batch': x})
    else:
        req_json = json.dumps({'input': list(x)})

    headers = {'Content-type': 'application/json'}
    start = datetime.now()
    r = requests.post(url, headers=headers, data=req_json)
    end = datetime.now()
    latency = (end - start).total_seconds() * 1000.0
    print("'%s', %f ms" % (r.text, latency))


def feature_sum(xs):
    return [str(sum(x)) for x in xs]


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
#    python_deployer.create_endpoint(hams_conn, "simple-example", "doubles",
#                                    feature_sum)


    time.sleep(2)

