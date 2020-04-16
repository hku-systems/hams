import redis

#import config 

from .rpc import (model_pb2, management_pb2, prediction_pb2, proxy_pb2)

from google.protobuf.json_format import MessageToDict

from enum import Enum


MODEL_DB = 0
MODEL_CONTAINER_DB = 1
PROXY_CONTAINER_DB = 2
DAG_DB = 3
RUNTIME_DAG_DB = 4
APP_DB = 5
APP_DAG_LINK_DB = 6

class RedisClient():

    def __init__(self, logger, redis_host, redis_port):
        self.logger = logger
        self.redis_host = redis_host
        self.redis_port = redis_port

        self.redis_model_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=MODEL_DB, decode_responses=True)
        self.redis_model_container_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=MODEL_CONTAINER_DB, decode_responses=True)
        self.redis_proxy_container_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=PROXY_CONTAINER_DB, decode_responses=True)
        self.redis_dag_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=DAG_DB, decode_responses=True)
        self.redis_runtime_dag_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=RUNTIME_DAG_DB, decode_responses=True)
        self.redis_app_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=APP_DB, decode_responses=True)
        self.redis_app_dag_link_db = redis.Redis(host=self.redis_host, port=self.redis_port, db=APP_DAG_LINK_DB, decode_responses=True)


    #####################################################################
    ##                          SET
    #####################################################################

    def add_model(self, request):
        model_id = request.modelname+'-'+request.modelversion       
        model = MessageToDict(request)
        self.logger.info("[REDIS] Add model")
        print(model)
        self.redis_model_db.hmset(model_id, model)
        return 

    def add_model_container(self, request):
        model_container_id = request.modelname + '-' + request.modelversion + '-' + str(request.replicaid)
        model_container = MessageToDict(request)
        self.logger.info("[REDIS] Add model container")
        print(model_container)
        self.redis_model_container_db.hmset(model_container_id,model_container)
        return

    def add_proxy_container(self,request):
        proxy_id = request.proxyname
        proxy = MessageToDict(request)
        self.logger.info("[REDIS] Add proxy container")
        print(proxy)
        self.redis_proxy_container_db.hmset(proxy_id, proxy)
        return

    
    def add_dag(self, request):
        dag_id = request.name + '-' + request.version
        dag = MessageToDict(request)
        self.logger.info("[REDIS] Add dag")
        print(dag)
        self.redis_dag_db.hmset(dag_id,dag)
        return

    
    def add_runtime_dag(self, request):
        runtime_dag_id = request.name + '-' + request.version + '-' + request.id
        runtime_dag = MessageToDict(request)

        self.logger.info("[REDIS] Add runtime dag")
        print(runtime_dag)

        self.redis_runtime_dag_db.hmset(runtime_dag_id, runtime_dag)
        return

    
    def add_application(self, request):
        app_id = request.name + '-' + request.version
        app = MessageToDict(request)
        self.redis_app_db.hmset(app_id, app)
        
        return

    def add_application_dag_link(self, request):
        link_id = request.appname + '-' + request.appversion
        link = MessageToDict(request)
        self.redis_app_dag_link_db.hmset(link_id, link)
        return

    #####################################################################
    ##                          UPDATE
    #####################################################################

    def update_runtime_dag(self, request):


        runtime_dag_id = request.name + '-' + request.version + '-' + request.id
        runtime_dag = MessageToDict(request)

        self.logger.info("[REDIS] Update runtime dag")
        print(runtime_dag)

        self.redis_runtime_dag_db.hmset(runtime_dag_id, runtime_dag)
        return


    #####################################################################
    ##                          GET
    #####################################################################

    def get_runtime_dag(self, request):
        runtime_dag_id = request.name + '-' + request.version + '-' + request.id

        runtime_dag = self.redis_runtime_dag_db.hmget(runtime_dag_id, ['file'])[0]
        
        self.logger.info("[REDIS] Get runtime dag")
        print(runtime_dag)

        return runtime_dag


   