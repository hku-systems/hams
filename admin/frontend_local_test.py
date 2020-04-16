import base64
import os
import time
from concurrent import futures
import threading

from google.protobuf.timestamp_pb2 import Timestamp

import grpc

#from hams_admin.grpcclient import grpc_client
from hams_admin.rpc import (management_pb2, management_pb2_grpc, model_pb2,
                               model_pb2_grpc, prediction_pb2,
                               prediction_pb2_grpc)

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)



def consume(ip, port, inputt):

    inputt = str(inputt)

    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = prediction_pb2_grpc.ProxyServerStub(channel)
    response = stub.downstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = inputt)))

    return response.status




def produce(ip, port, seq_id):
    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = prediction_pb2_grpc.ProxyServerStub(channel)
    time = Timestamp()
    time.GetCurrentTime()
    response = stub.outputstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = "Produced output"),src_uri = "localhost", seq = seq_id, req_id =1, timestamp = time))
    print('Response\n{res}'.format(res=response.status))

    return response.status

def main():

    ip = "localhost"
    port = "22224"


    # We can use a with statement to ensure threads are cleaned up promptly
    with futures.ThreadPoolExecutor(max_workers=64) as executor:
    # Start the load operations and mark each future with its URL
        future_to_excute = {executor.submit(consume, ip, port, inputt): inputt for inputt in range(0,10)}
        future_to_produce = {executor.submit(produce, ip, port, inputt): inputt for inputt in range(0,10)}
        for future in futures.as_completed(future_to_excute):
            inputt = future_to_excute[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%d generated an exception: %s' % (inputt, exc))
            else:
                print('Request %d received output:\n%s' % (inputt, data))

        time.sleep(3)

        for future in futures.as_completed(future_to_excute):
            inputt = future_to_excute[future]
            data = future.result()
            #print("data:%s"%(data))

    return 



if __name__ == '__main__':
    main()
