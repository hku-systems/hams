import base64
import os
import time
from concurrent import futures
import threading

from google.protobuf.timestamp_pb2 import Timestamp

import grpc

from hams_admin import (HamsConnection, DockerContainerManager,
                           graph_parser, redis_client)
from hams_admin.deployers import python as python_deployer
#from hams_admin.grpcclient import grpc_client
from hams_admin.rpc import (management_pb2, management_pb2_grpc, model_pb2,
                               model_pb2_grpc, prediction_pb2,
                               prediction_pb2_grpc)

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)


class ProxyServerServicer(prediction_pb2_grpc.ProxyServerServicer):

    def __init__(self, proxy_name, proxy_port):

        self.event_dict = {}
        self.output_dict = {}
        self.timestamp_dict = {}

        self.seq_id = 0
        self.request_id = 0

        self.seq_mutex = threading.Lock()

        self.entry_proxy_name = proxy_name
        self.entry_proxy_port = proxy_port

        return 

    def downstream(self, request, context): ## Consumer 

       #logging.info("Start processing request...")

        #### received a request 
        ####  stub.downstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = stock_name),src_uri = "localhost", seq = 1, req_id =1, timestamp = Timestamp().GetCurrentTime()))

        ## Sequence Lock - Guarantee that sequence are consistent
        self.seq_mutex.acquire()
        temp_seq = self.seq_id
        self.seq_id = self.seq_id+1
        self.seq_mutex.release()

        ##
        call_time = Timestamp()
        call_time.GetCurrentTime()
        ## Generate request
        temp_request = prediction_pb2.request(input_ = request.input_, src_uri="localhost", seq=temp_seq, req_id = self.request_id, timestamp = call_time )

        ## Create conditional variable
        # Currently use seq id as thread id 
        thread_id = temp_seq
        condition = threading.Event() #Condition()
        self.event_dict[thread_id] = condition

        #logging.info(self.event_dict[thread_id])

        ## Send request to downstream proxy

        # channel = grpc.insecure_channel('%s:%s'%(self.entry_proxy_name, self.entry_proxy_port))
        # stub = prediction_pb2_grpc.ProxyServerStub(channel)
        # response = stub.downstream(temp_request)
        # logging.info('Response\n{res}'.format(res=response.status)) 
        # 
        logging.info("Received consume request%s"%(request.input_.inputStream))      

        #logging.info(type(self.event_dict.get(thread_id, None)))
        ## Wait for task finish
        event = self.event_dict.get(thread_id, None) 
            #event.acquire()
        event.wait()
            #event.release()

        ## Produce return

        output = self.output_dict[thread_id]
        ret_time = self.timestamp_dict[thread_id]

        ## Generate return string
        ret = "Call Time: %s Return Time: %s Output: %s"%(str(call_time), str(ret_time), str(output))

        ## Clean cache
        del self.output_dict[thread_id] 
        del self.event_dict[thread_id]
        del self.timestamp_dict[thread_id]

        logging.info("Finished processing request with ret: %s"%(ret))

        return model_pb2.response(status = ret)


    def outputstream(self, request, contex): ## Producer 

        ## Get the sequence number

        thread_id = request.seq

        event = self.event_dict.get(thread_id, None)

        ## Set the output
        self.output_dict[thread_id] = request.input_.inputStream
        self.timestamp_dict[thread_id] = request.timestamp
        ## Notify all'
        #event.acquire()
        #event.notifyAll()
        #event.release()
        logging.info("Received outputstream and set event")
        event.set()
        return model_pb2.response(status="Frontend received outputstream")


def serve():

    proxy_name = None #os.environ["ENTRY_PROXY_NAME"]
    proxy_port = None #os.environ["ENTRY_PROXY_PORT"]
    #redis_name = os.environ["REDIS_IP"]
    #redis_port = os.environ["REDIS_PORT"]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
    service = ProxyServerServicer(proxy_name, proxy_port)
    prediction_pb2_grpc.add_ProxyServerServicer_to_server(service,server)
    server.add_insecure_port('[::]:22224')
#    server.add_insecure_port('[::]:{port}'.format(port=model_port))
    server.start()
    logging.info("Frontend Server Started")

    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
