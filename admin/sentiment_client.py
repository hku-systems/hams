import base64
import os
import time
from concurrent import futures
import threading

import argparse
import sys

from google.protobuf.timestamp_pb2 import Timestamp
from timeit import default_timer as timer
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

def image_string(image):
    image_encode=cv2.imencode('.jpg',image)[1]
    imagelist=image_encode.tolist()
    image_string=json.dumps(imagelist)
    return image_string

def string_image(imagestring):
    image_list=json.loads(imagestring)
    arr=np.array(image_list)
    arr=np.uint8(arr)
    image=cv2.imdecode(arr,cv2.IMREAD_COLOR)
    return image

def main():

    ip = sys.argv[1]
    port = sys.argv[2]

    # We can use a with statement to ensure threads are cleaned up promptly
    
    with futures.ThreadPoolExecutor(max_workers=2048) as executor:
    # Start the load operations and mark each future with its URL
        
        inputt_list = [str(i) for i in range(72)]
        start=timer()
        future_to_excute = {executor.submit(consume, ip, port, inputt): inputt for inputt in inputt_list}

        for future in futures.as_completed(future_to_excute):
            inputt = future_to_excute[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%s generated an exception: %s' % (str(inputt), exc))
            else:
                print('Request %s received output:\n%s' % (str(inputt), data))
        end=timer()

    
    print("\n[INFO] Total time: "+str(end-start))
    return 


if __name__ == '__main__':
    main()
