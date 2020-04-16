import base64
import os
import time
from concurrent import futures
import threading

import argparse
import sys
import datetime

import requests, json, numpy as np


from multiprocessing import Process, Queue, Lock

from google.protobuf.timestamp_pb2 import Timestamp

import grpc

#from hams_admin.grpcclient import grpc_client
from hams_admin.rpc import (management_pb2, management_pb2_grpc, model_pb2,
                               model_pb2_grpc, prediction_pb2,
                               prediction_pb2_grpc)

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)


""" 
python imagequery_concurrent_client.py --worker 1 --ip 172.18.0.1 --port 22223 --system oursystem
"""

def oursystem(ip, port, inputt):
    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = prediction_pb2_grpc.ProxyServerStub(channel)
    response = stub.downstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = inputt)))
    return response.status


def withoutproxy(ip, port, inputt):
    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = model_pb2_grpc.PredictServiceStub(channel)
    response = stub.Predict(model_pb2.input(inputStream=inputt, inputType="String"))

    return response.outputStream

def hams(ip, port, inputt):
    headers = {"Content-type": "application/json"}
    requests.post("http://{}:{}/default/predict".format(ip,port), headers=headers, data=json.dumps({"input": [inputt]}).json())
    # docker exec -it boat_container curl -X POST -d '{ "input": "1" }' --header "Content-Type:application/json" 127.0.0.1:8080/predict
    return "Raft OK"

def bigball(ip, port, inputt):
    # Better to use hams to process bigball container.
    return "bigball"
 
 
# Producer function that places data on the Queue
def producer(queue, lock, ip, port, inputt_list, func):
    # Synchronize access to the console
    with lock:
        print('Starting worker => {}'.format(os.getpid()))
         
    # Query and return output on the Queue
    for inputt in inputt_list:
        #time.sleep(random.randint(0, 10))
        output = func(ip, port, inputt)
        #queue.put(output)

        with lock:
            print("Input {} returns Output: {}".format(inputt, output))
 
    # Synchronize access to the console
    with lock:
        print('Worker {} exiting...'.format(os.getpid()))
 
# Currently no need
# The consumer function takes data off of the Queue
def consumer(queue, lock):
    # Synchronize access to the console
    with lock:
        print('Starting consumer => {}'.format(os.getpid()))
     
    # Run indefinitely
    while True:
        time.sleep(random.randint(0, 2))
        # If the queue is empty, queue.get() will block until the queue has data
        output = queue.get()
 
        # Synchronize access to the console
        with lock:
            print('{} got {}'.format(os.getpid(), output))
 
 
def main():


    parser = argparse.ArgumentParser(description='concurrent client')

    parser.add_argument('--worker', nargs=1, type=int, help="Worker num")
    parser.add_argument('--ip', nargs=1, type=str, help="Ip address of your query frontend")
    parser.add_argument('--port', nargs=1, type=str, help="Port of your query frontend, for Hams, put an arbitrary INT")
    parser.add_argument('--system', nargs=1, type=str, help="System name: oursystem/withoutproxy/hams")
                       
    args = parser.parse_args()
     
    # Generate your inputt list here
    inputt_total = list(range(100))
    import random
    random.shuffle(inputt_total)
    inputt_total = [str(i) for i in inputt_total]
    print("inputt_total: " , inputt_total)


    # Get configuration
    work_num = args.worker[0]
    ip = args.ip[0]
    port = args.port[0]
    system = args.system[0]
 
    # Create the Queue object
    queue = Queue()
     
    # Create a lock object to synchronize resource access
    lock = Lock()
 
    producers = []
    consumers = []
 
    thismodule = sys.modules[__name__]

    for i in range(work_num):

        # Slice the input_total to $work_num lists
        inputt_list = inputt_total[i::work_num]
        # Create our producer processes by passing the producer function and it's arguments
        producers.append(Process(target=producer, args=(queue, lock, ip, port, inputt_list, getattr(thismodule, system))))
 
    # Create consumer processes
    #for i in range(work_num):
    #    p = Process(target=consumer, args=(queue, lock))
         
        # This is critical! The consumer function has an infinite loop
        # Which means it will never exit unless we set daemon to true
    #    p.daemon = True
    #    consumers.append(p)
 
    # Start the producers and consumer
    # The Python VM will launch new independent processes for each Process object

    start = time.time()

    for p in producers:
        p.start()
 
    #for c in consumers:
    #    c.start()
 
    # Like threading, we have a join() method that synchronizes our program
    for p in producers:
        p.join()
 
    end = time.time()

    print('Finished %d requests with time:'%(len(inputt_total)))
    print(end-start)
    print('Parent process exiting...')



if __name__ == '__main__':
    main()
