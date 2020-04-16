from __future__ import absolute_import, division, print_function
import logging
import docker
import tempfile
import requests
from requests.exceptions import RequestException
import json
import pprint
import time
import re
import os
import tarfile
import sys
from cloudpickle import CloudPickler
import pickle
import numpy as np


from google.protobuf.json_format import MessageToDict

if sys.version_info < (3, 0):
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    PY3 = False
else:
    from io import BytesIO as StringIO
    PY3 = True

import grpc

from .rpc import model_pb2_grpc
from .rpc import model_pb2

from .rpc import prediction_pb2_grpc
from .rpc import prediction_pb2

from .rpc import management_pb2
from .rpc import management_pb2_grpc

from .container_manager import CONTAINERLESS_MODEL_IMAGE, ClusterAdapter
from .exceptions import HamsException, UnconnectedException
from .version import __version__, __registry__
from . import graph_parser

DEFAULT_LABEL = []
DEFAULT_PREDICTION_CACHE_SIZE_BYTES = 33554432
HAMS_TEMP_DIR = "/tmp/hams"  # Used Internally for Test; Not Windows Compatible

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%y-%m-%d:%H:%M:%S',
    level=logging.INFO)

# logging.basicConfig(
#     format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
#     datefmt='%y-%m-%d:%H:%M:%S',
#     level=logging.INFO)

logger = logging.getLogger(__name__)

deploy_regex_str = "[a-z0-9]([-a-z0-9]*[a-z0-9])?\Z"
deployment_regex = re.compile(deploy_regex_str)


def _validate_versioned_model_name(name, version):
    # author: Dan Crankshaw, ucbrise lab, https://github.com/ucbrise/clipper
    if deployment_regex.match(name) is None:
        raise HamsException(
            "Invalid value: {name}: a model name must be a valid DNS-1123 "
            " subdomain. It must consist of lower case "
            "alphanumeric characters, '-' or '.', and must start and end with "
            "an alphanumeric character (e.g. 'example.com', regex used for "
            "validation is '{reg}'".format(name=name, reg=deploy_regex_str))
    if deployment_regex.match(version) is None:
        raise HamsException(
            "Invalid value: {version}: a model version must be a valid DNS-1123 "
            " subdomain. It must consist of lower case "
            "alphanumeric characters, '-' or '.', and must start and end with "
            "an alphanumeric character (e.g. 'example.com', regex used for "
            "validation is '{reg}'".format(
                version=version, reg=deploy_regex_str))


class HamsConnection(object):
    def __init__(self, container_manager):

        self.connected = False
        self.cm = container_manager

        #############TEST################

        self.runtime_dag = ""

        self.lock = False

        #################################


        self.logger = ClusterAdapter(logger, {
            'cluster_name': self.cm.cluster_identifier
        })

    def start_hams(self,
                      mgmt_frontend_image='{}/management_frontend:{}'.format(
                          __registry__, __version__),
                      cache_size=DEFAULT_PREDICTION_CACHE_SIZE_BYTES):

        try:
            self.cm.start_hams(mgmt_frontend_image)
                        
            # while True:
            #     try:
            #         query_frontend_url = "http://{host}/metrics".format(
            #             host=self.cm.get_query_addr())
            #         mgmt_frontend_url = "http://{host}/admin/ping".format(
            #             host=self.cm.get_admin_addr())
            #         for name, url in [('query frontend', query_frontend_url), 
            #                          ('management frontend', mgmt_frontend_url)]:
            #             r = requests.get(url, timeout=5)
            #             if r.status_code != requests.codes.ok:
            #                 raise RequestException(
            #                     "{name} end point {url} health check failed".format(name=name, url=url))
            #         break
            #     except RequestException as e:
            #         self.logger.info("Hams still initializing: \n {}".format(e))
            #         time.sleep(1)
            self.logger.info("Hams is running")
            self.connected = True
        except HamsException as e:
            self.logger.warning("Error starting Hams: {}".format(e.msg))
            raise e

    def connect(self):
        """Connect to a running Hams cluster."""

        self.cm.connect()
        self.connected = True
        self.logger.info(
            "Successfully connected to Hams cluster at {}".format(
                self.cm.get_query_addr()))

  
    def build_and_deploy_DAG(self,
                             name,
                             version,
                             dag_description,
                             labels):
        if not self.connected:
            raise UnconnectedException()
        



    def deploy_model(self,
                     name,
                     version,
                     input_type,
                     image,
                     labels=None,
                     num_replicas=1,
                     batch_size=-1):
        if not self.connected:
            raise UnconnectedException()
        version = str(version)
        _validate_versioned_model_name(name, version)
        self.cm.deploy_model(
            name=name,
            version=version,
            input_type=input_type,
            image=image,
            num_replicas=num_replicas)
        # self.register_model(
        #     name,
        #     version,
        #     input_type,
        #     image=image,
        #     labels=labels,
        #     batch_size=batch_size)
        self.logger.info("Done deploying model {name}:{version}.".format(
            name=name, version=version))


    def connect_host(self, host_ip, host_port):
        self.cm.connect_host(host_ip, "2375")


    def add_model(self,
                  model_name,
                  model_version, 
                  image, 
                  input_type="string", 
                  output_type="string", 
                  stateful=False):

        modelinfo = management_pb2.ModelInfo(modelname=model_name,
                                             modelversion=model_version,
                                             image=image,
                                             inputtype=input_type,
                                             outputtype=output_type,
                                             stateful=stateful).SerializeToString()
                                             
        self.cm.grpc_client("zsxhku/grpcclient", "--addmodel %s %s %s "%("localhost","33333", modelinfo))

        return 

    def deploy_DAG(self, name, version, dag_description=None, runtime=""):


        if not self.connected:
            raise UnconnectedException()

       # model_info = self.get_all_models()

        dag_description_ = dag_description

        #self.logger.info("dag_description: %s"%(dag_description_))

        #if(dag_description==None):
        #    dag_description_=self.get_dag_description()

        nodes_list = graph_parser.get_all_nodes(dag_description_)

        
        container_info = []
        proxy_info = []
        backup_info = []

        count = 1
        for model_info in nodes_list:

            model_name,model_version,model_image = graph_parser.get_name_version(model_info)

            container_name, container_id, host = self.cm.add_replica(model_name, model_version, "22222", model_image, runtime=runtime)
            self.logger.info("Started %s with container %s:%s (HOST:%s)"%(model_name, container_name, container_id, host))
            container_ip = self.cm.get_container_ip(host, container_id)
            proxy_name, proxy_id = self.cm.set_proxy("ai-proxy:latest", container_name, container_ip, host)
            ## get the ip of the instances 
            proxy_ip = self.cm.get_container_ip(host, proxy_id)

            proxy_info.append([proxy_name,proxy_id,proxy_ip])
            container_info.append([container_name, container_id, container_ip])


            if graph_parser.is_stateful(model_info):
                backup_name, backup_id, backup_host = self.cm.add_replica(model_name, model_version, "22222", model_image)
                self.logger.info("[Backup] Started %s with container %s:%s (HOST:%s)"%(model_name, backup_name, backup_id, backup_host))
                backup_ip = self.cm.get_container_ip(backup_host, backup_id)
                backup_proxy_name, backup_proxy_id = self.cm.set_proxy("ai-proxy:latest", backup_name, backup_ip, backup_host)
                backup_proxy_ip= self.cm.get_container_ip(backup_host, backup_proxy_id)
                backup_info.append([backup_name, backup_id, backup_ip, backup_proxy_name, backup_proxy_id, backup_proxy_ip])
            else:
                backup_info.append([])

            #self.cm.check_container_status(host, container_id, 0.3, 20)
            #self.cm.check_container_status(host, proxy_id, 0.3, 20)

            #time.sleep(25)

            #self.logger.info("proxy_ip:%s"%(proxy_ip))

            self.cm.grpc_client("zsxhku/grpcclient", "--setmodel %s %s %s %s %s %s"%(proxy_ip, "22223", container_name, count, container_ip, "22222" ))
            self.logger.info('[DEPLOYMENT] Finished setting model info to proxy')
            if(graph_parser.is_stateful(model_info)):
                self.cm.grpc_client("zsxhku/grpcclient", "--setmodel %s %s %s %s %s %s"%(backup_info[-1][-1], "22223", backup_info[-1][0], count, backup_info[-1][2], "22222" ))
                self.logger.info('[DEPLOYMENT][Backup] Finished setting model info to proxy')
            count += 1
            

            # self.cm.grpc_client("zsxhku/grpcclient", "--setproxy %s %s %s %s"%(container_ip, "22222", proxy_name, "22223"))
            # self.logger.info('[DEPLOYMENT] Finished setting proxy info to model')
            # if(graph_parser.is_stateful(model_info)):
            #    self.cm.grpc_client("zsxhku/grpcclient", "--setproxy %s %s %s %s"%(backup_info[-1][2], "22222", backup_info[-1][3], "22223"))
            #    self.logger.info('[DEPLOYMENT][Backup] Finished setting proxy info to model')
 
        runtime_dag_id = name+version+str(1)

        ## Starting frontend 
        frontend_name, frontend_container_id = self.cm.add_frontend("localhost", "mxschen/frontend",runtime_dag_id, proxy_info[0][2], "22223", max_workers=2048)

        frontend_ip = self.cm.get_container_ip("localhost", frontend_container_id)

        frontend_info = [frontend_name, frontend_container_id, frontend_ip]

        self.logger.info("[DEPLOYMENT] ################ Started Frontend #################")
        #expand the dag description with the model/proxy instances info 
        expanded_dag = graph_parser.expand_dag(dag_description_, name, version, container_info, proxy_info, backup_info, frontend_info)

        self.runtime_dag = expanded_dag



        # TODO: need to modularize
        self.cm.grpc_client("zsxhku/grpcclient", "--addruntimedag %s %s %s %s %s %s %s"%('1', name, version, 'old' , self.cm.admin_ip, self.cm.admin_port, expanded_dag))


        self.logger.info("Added new runtime DAG to admin daemon\n%s"%(expanded_dag))

        #tells the proxy runtime dag info
        for tup in proxy_info:
            proxy_name = tup[0]
            proxy_id = tup[1]
            proxy_ip = tup[2]

            self.cm.grpc_client("zsxhku/grpcclient", "--setdag %s %s %s"%(proxy_ip, "22223", expanded_dag))
            self.logger.info('[DEPLOYMENT] Finished setting DAG for proxy {proxy_name} '.format(proxy_name=proxy_name))

        #tells the backups runtime dag info
        for tup in backup_info:
            if tup:
                self.cm.grpc_client("zsxhku/grpcclient", "--setdag %s %s %s"%(tup[-1], "22223", expanded_dag))
                self.logger.info('[DEPLOYMENT][Backup] Finished setting DAG for proxy {proxy_name} '.format(proxy_name=tup[-1]))



        return



    def get_query_addr(self):

        if not self.connected:
            raise UnconnectedException()
        return self.cm.get_query_addr()




  

    def stop_all_model_containers(self):
        self.cm.stop_all_model_containers()
        self.logger.info("Stopped all Hams model containers")

    def stop_all(self, graceful=True):
        self.cm.stop_all(graceful=graceful)
        self.logger.info(
            "Stopped all Hams cluster and all model containers")

