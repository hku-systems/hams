import base64
import os
import time
from concurrent import futures
from threading import Thread,Lock

from google.protobuf.timestamp_pb2 import Timestamp

import grpc

from hams_admin import (HamsConnection, DockerContainerManager,
                           graph_parser, redis_client)
from hams_admin.deployers import python as python_deployer
#from hams_admin.grpcclient import grpc_client
from hams_admin.rpc import (management_pb2, management_pb2_grpc, model_pb2,
                               model_pb2_grpc, prediction_pb2,
                               prediction_pb2_grpc)



class ManagementServerServicer(management_pb2_grpc.ManagementServerServicer):
    

    def __init__(self, redis_name, redis_port):

        self.admin = HamsConnection(DockerContainerManager())

        self.mutex = Lock()

        #self.redis_name=redis_name
        #self.redis_port=redis_port

        self.redis_client = redis_client.RedisClient(self.admin.logger, redis_name, redis_port)

    def AddRuntimeDAG(self, request, context):
        self.redis_client.add_runtime_dag(request)
        return management_pb2.Response(status="AddRuntimeDAGSuccessful")

    def GetRuntimeDAG(self, request, context):

        old_runtime_dag = self.redis_client.get_runtime_dag(request)

        return management_pb2.RuntimeDAGInfo(file=old_runtime_dag)

    def ReportContainerFailure(self, request, context):

        print ("=========== ReportContainerFailure ============")

        fail_host_ip = request.hostip
        fail_model_id = request.modelid
        fail_app_id = request.appid

        fail_container_name = request.modelname

        #####################################################
        ##  Step 1 : Get Current runtime DAG
        #####################################################

        self.admin.logger.info("[Recovery] Get existing runtime DAG")

        self.mutex.acquire(timeout=60)

        fail_dag_name = fail_app_id.split('-')[0]
        fail_dag_version = fail_app_id.split('-')[1]
        fail_dag_id = '1'

        old_runtime_dag = self.redis_client.get_runtime_dag(management_pb2.RuntimeDAGInfo(name=fail_dag_name, version=fail_dag_version, id=fail_dag_id))

        #docker_client = self.admin.cm.get_docker_client(fail_host_ip)

        print(old_runtime_dag)

        runtime_infos = graph_parser.get_model_from_dag(old_runtime_dag, int(fail_model_id))

        if not graph_parser.is_running(runtime_infos, fail_container_name):
            re = management_pb2.FailureResponse(status="Error")
            self.mutex.release()
            return re

        #####################################################
        ##  Step 2: Boot up a new container 
        #####################################################

        model_name,model_version,model_image = graph_parser.get_name_version(runtime_infos) 

        isstateful = graph_parser.is_stateful(runtime_infos)
        isprimary = False

        if isstateful:
            self.admin.logger.info("[Recovery] Stateful Failure")
            isprimary = graph_parser.is_primary(runtime_infos, fail_container_name)
            if isprimary:
                self.admin.logger.info("[Recovery] Primary Failure")
            else:
                self.admin.logger.info("[Recovery] Backup Failure")
            

        self.admin.logger.info("[Recovery] Booting up new container instances")          
        ## here we safely init the model container with know proxy 
        container_name, container_id, scheduled_host = self.admin.cm.add_replica(model_name, model_version, "22222", model_image)

        container_ip = self.admin.cm.get_container_ip(scheduled_host, container_id)

        proxy_name, proxy_id = self.admin.cm.set_proxy("ai-proxy:latest",container_name, container_ip, scheduled_host, recovery=True)

        proxy_ip = self.admin.cm.get_container_ip(scheduled_host, proxy_id)

        time.sleep(1)

        self.admin.cm.grpc_client("zsxhku/grpcclient", "--setproxy %s %s %s %s"%(container_ip, "22222", proxy_name, "22223"))

        self.admin.logger.info('[Recovery] Set proxy in Model Container ')

        #####################################################
        ##  Step 3: Update runtime DAG 
        #####################################################

        self.admin.logger.info('[Recovery] Updating old runtime dag')

        new_runtime_dag = graph_parser.gen_new_runtime_dag(old_runtime_dag, model_name, model_version, isstateful, isprimary, container_name, container_id, container_ip, proxy_name, proxy_id, proxy_ip )
        
        self.redis_client.update_runtime_dag(management_pb2.RuntimeDAGInfo(name=fail_dag_name, version=fail_dag_version, id=fail_dag_id, file=new_runtime_dag, format="old"))
    
        #grpc_client.UpdateRuntimeDAG(fail_app_id, )
        self.mutex.release()

        #####################################################
        ##  Step 4: Return the old/updated runtime DAG  
        #####################################################

        self.admin.logger.info("[Recovery] Returning new runtime dag")

        status = ""
        re = management_pb2.FailureResponse(newruntimedag=new_runtime_dag, isstateful=isstateful, isprimary=isprimary, status=status, modelid = request.modelid)
        return re

        
def serve():

    #model_name = os.environ["MODEL_NAME"]
    #model_port = os.environ["MODEL_PORT"]
    redis_name = os.environ["REDIS_IP"]
    redis_port = os.environ["REDIS_PORT"]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    service = ManagementServerServicer(redis_name, redis_port)
    management_pb2_grpc.add_ManagementServerServicer_to_server(service,server)
    server.add_insecure_port('[::]:55555')

#    server.add_insecure_port('[::]:{port}'.format(port=model_port))
    server.start()
    print("Management Daemon Started")
    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
