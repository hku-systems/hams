from __future__ import absolute_import, division, print_function

import socket

import docker
import logging
import os
import sys
import random
import time
import json
import tempfile
from ..container_manager import (
    create_model_container_label, parse_model_container_label,
    ContainerManager, HAMS_DOCKER_LABEL, HAMS_MODEL_CONTAINER_LABEL,
    HAMS_QUERY_FRONTEND_CONTAINER_LABEL,
    HAMS_MGMT_FRONTEND_CONTAINER_LABEL, HAMS_INTERNAL_RPC_PORT,
    HAMS_INTERNAL_QUERY_PORT, HAMS_INTERNAL_MANAGEMENT_PORT,
    HAMS_INTERNAL_METRIC_PORT, HAMS_INTERNAL_REDIS_PORT,
    HAMS_DOCKER_PORT_LABELS, HAMS_METRIC_CONFIG_LABEL, ClusterAdapter)
from ..exceptions import HamsException
from requests.exceptions import ConnectionError
from .. import graph_parser



logger = logging.getLogger(__name__)


class DockerContainerManager(ContainerManager):
    def __init__(self,
                 cluster_name="default-cluster",
                 docker_ip_address="localhost",
                 hams_query_port=1337,
                 hams_management_port=1338,
                 hams_rpc_port=7000,
                 redis_ip=None,
                 redis_port=6379,
                 docker_network="hams_network",
                 extra_container_kwargs={}):

        self.cluster_name = cluster_name
        self.cluster_identifier = cluster_name  # For logging purpose
        self.public_hostname = docker_ip_address
        self.hams_query_port = hams_query_port
        self.hams_management_port = hams_management_port
        self.hams_rpc_port = hams_rpc_port
        self.redis_ip = redis_ip
        self.admin_ip = None
        self.admin_port = None
        self.proxy_image = "ai-proxy"
        if redis_ip is None:
            self.external_redis = False
        else:
            self.external_redis = True
        self.redis_port = redis_port
        if docker_network is "host":
            raise HamsException(
                "DockerContainerManager does not support running on the "
                "\"host\" docker network. Please pick a different network name"
            )
        self.docker_network = docker_network

        self.docker_client = docker.from_env()
        self.extra_container_kwargs = extra_container_kwargs.copy()

        self.host_list = {"localhost":self.docker_client}
        self.container_count = 0

        #self.h_list = {"202.45.128.174","202.45.128.175"}
        self.h_list = {}

        self.logger = ClusterAdapter(logger, {
            'cluster_name': self.cluster_identifier
        })

        for h_ip in self.h_list:
            self.connect_host(h_ip, "2375")


        # Merge Hams-specific labels with any user-provided labels
        if "labels" in self.extra_container_kwargs:
            self.common_labels = self.extra_container_kwargs.pop("labels")
            self.common_labels.update({
                HAMS_DOCKER_LABEL: self.cluster_name
            })
        else:
            self.common_labels = {HAMS_DOCKER_LABEL: self.cluster_name}

        container_args = {
            "network": self.docker_network,
            "detach": True,
        }

        self.extra_container_kwargs.update(container_args)

        
    def start_hams(self,
                      mgmt_frontend_image):

        try:
            self.docker_client.networks.create(
                self.docker_network, check_duplicate=True)
        except docker.errors.APIError:
            self.logger.debug(
                "{nw} network already exists".format(nw=self.docker_network))
        except ConnectionError:
            msg = "Unable to Connect to Docker. Please Check if Docker is running."
            raise HamsException(msg)

        containers_in_cluster = self.docker_client.containers.list(
            filters={
                'label': [
                    '{key}={val}'.format(
                        key=HAMS_DOCKER_LABEL, val=self.cluster_name)
                ]
            })

        if len(containers_in_cluster) > 0:
            raise HamsException(
                "Cluster {} cannot be started because it already exists. "
                "Please use HamsConnection.connect() to connect to it.".
                format(self.cluster_name))

        if not self.external_redis:
            self.logger.info("Starting managed Redis instance in Docker")
            self.redis_port = 33333
            #find_unbound_port(self.redis_port)
            redis_labels = self.common_labels.copy()
            redis_labels[HAMS_DOCKER_PORT_LABELS['redis']] = str(
                self.redis_port)
            redis_container = self.docker_client.containers.run(
                'redis:alpine',
                "redis-server --port %s" % self.redis_port,
                name="redis-{}".format(random.randint(
                    0, 100000)),  # generate a random name
                ports={
                    '%s/tcp' % self.redis_port: self.redis_port
                },
                tty=True,
                labels=redis_labels,
                **self.extra_container_kwargs)
            self.redis_ip = redis_container.name

        # mgmt_cmd = "--redis_ip={redis_ip} --redis_port={redis_port}".format(
        #     redis_ip=self.redis_ip, redis_port=HAMS_INTERNAL_REDIS_PORT)
        #self.hams_management_port = 55555
        #find_unbound_port(
        #    self.hams_management_port)


        mgmt_labels = self.common_labels.copy()
        mgmt_labels[HAMS_MGMT_FRONTEND_CONTAINER_LABEL] = ""
        mgmt_labels[HAMS_DOCKER_PORT_LABELS['management']] = str(
            self.hams_management_port)
        admin_container = self.docker_client.containers.run(
            "zsxhku/admindocker",
            name="admin-daemon-{}".format(random.randint(0, 100000)),  # generate a random name
            ports={
                '%s/tcp' % HAMS_INTERNAL_MANAGEMENT_PORT:self.hams_management_port
            },
            environment={'REDIS_IP':self.redis_ip, 'REDIS_PORT':self.redis_port},
            volumes = {'/var/run/docker.sock':{'bind':'/var/run/docker.sock', 'mode':'rw'}},
            labels=mgmt_labels,
            tty=True,
            **self.extra_container_kwargs)

        self.admin_ip = admin_container.name
        self.admin_port = "55555"


        self.connect()

    def connect(self):
        """
        Use the cluster name to update ports. Because they might not match as in
        start_hams the ports might be changed.
        :return: None
        """
        containers = self.docker_client.containers.list(
            filters={
                'label': [
                    '{key}={val}'.format(
                        key=HAMS_DOCKER_LABEL, val=self.cluster_name)
                ]
            })
        all_labels = {}
        for container in containers:
            all_labels.update(container.labels)

        self.redis_port = all_labels[HAMS_DOCKER_PORT_LABELS['redis']]
        self.hams_management_port = all_labels[HAMS_DOCKER_PORT_LABELS[
             'management']]
        #self.hams_query_port = all_labels[HAMS_DOCKER_PORT_LABELS[
        #     'query_rest']]
        #self.hams_rpc_port = all_labels[HAMS_DOCKER_PORT_LABELS[
        #     'query_rpc']]
        #self.prometheus_port = all_labels[HAMS_DOCKER_PORT_LABELS['metric']]
        #self.prom_config_path = all_labels[HAMS_METRIC_CONFIG_LABEL]

    def connect_host(self, host_ip, host_port):

        docker_client = docker.DockerClient(base_url='tcp://{ip}:{port}'.format(ip=host_ip, port=host_port), tls=False)
        
        self.host_list[host_ip]=docker_client

        self.logger.info('Suffcessfully connected to remote docker daemon on host:{ip}'.format(ip=host_ip))


        return 


    def deploy_model(self, name, version, input_type, image, num_replicas=1):
        # Parameters
        # ----------
        # image : str
        #     The fully specified Docker imagesitory to deploy. If using a custom
        #     registry, the registry name must be prepended to the image. For example,
        #     "localhost:5000/my_model_name:my_model_version" or
        #     "quay.io/my_namespace/my_model_name:my_model_version"
        #self.set_num_replicas(name, version, input_type, image, num_replicas)
        return

    def _get_replicas(self, name, version):
        containers = self.docker_client.containers.list(
            filters={
                "label": [
                    "{key}={val}".format(
                        key=HAMS_DOCKER_LABEL, val=self.cluster_name),
                    "{key}={val}".format(
                        key=HAMS_MODEL_CONTAINER_LABEL,
                        val=create_model_container_label(name, version))
                ]
            })
        return containers

    def get_num_replicas(self, name, version):
        return len(self._get_replicas(name, version))

    def get_host_client(self, host_ip):
        return self.host_list.get(host_ip, self.docker_client)

    def set_proxy(self, image, model_container_label, model_ip, host_ip, recovery=False, remove=True):

        proxy_name = model_container_label + '-proxy'
        env_vars = {
            "PROXY_NAME": proxy_name,
            "PROXY_VERSION": "test",
            "PROXY_PORT": "22223",
            "ADMIN_IP":self.admin_ip,
            "ADMIN_PORT":self.admin_port,
            "RECOVERY": str(recovery),
            "CACHE_SIZE_MB": "2"
        }

        labels = self.common_labels.copy()
        labels[HAMS_MODEL_CONTAINER_LABEL] = proxy_name
        labels[HAMS_DOCKER_LABEL] = self.cluster_name


        host_client = self.get_host_client(host_ip)

        container = host_client.containers.run(
            image,
            name=proxy_name,
            environment=env_vars,
            labels=labels,
            tty=True,     
            #remove=remove,      
            **self.extra_container_kwargs)

        #<Container: d15d870463>
        container_id = str(container)[12:-1]
        return proxy_name, container_id

    def add_frontend(self, host_ip, image, runtime_dag_id, entry_proxy_ip, entry_proxy_port, max_workers=64, stateful=False, remove=True):

        frontend_name = runtime_dag_id + '-frontend-'+str(random.randint(1,100000))
        env_vars = {
            "FRONTEND_NAME": frontend_name,
            "ENTRY_PROXY_NAME": entry_proxy_ip,
            "ENTRY_PROXY_PORT": "22223",
            "ADMIN_IP":self.admin_ip,
            "ADMIN_PORT":self.admin_port,
            "MAX_WORKERS":str(max_workers),
            "RECOVERY": str(stateful),
            "CACHE_SIZE_MB": "2"
        }

        labels = self.common_labels.copy()
        labels[HAMS_MODEL_CONTAINER_LABEL] = frontend_name
        labels[HAMS_DOCKER_LABEL] = self.cluster_name


        host_client = self.get_host_client(host_ip)

        container = host_client.containers.run(
            image,
            name=frontend_name,
            environment=env_vars,
            labels=labels,
            tty=True,
            remove=remove,           
            **self.extra_container_kwargs)

        #<Container: d15d870463>
        container_id = str(container)[12:-1]
        return frontend_name, container_id

    

    def get_container_ip(self, host_ip, container_id):
        
        host_client = self.get_host_client(host_ip)

        meta = host_client.api.inspect_container(container_id)
        ip = meta['NetworkSettings']['Networks']['hams_network']['IPAddress']
        self.logger.info("Got container {id} IP:{meta}".format(id=container_id, meta=ip))
        return ip


    def schedule_host(self):
        
        # Round Robin, default is self.docker_client

        selected_host = list(self.host_list.keys())[(self.container_count % len(self.host_list) )-1]
        selected_client = self.host_list.get(selected_host, self.docker_client)

        return selected_host, selected_client

    def add_replica(self, model_name, model_version, model_port, image, proxy_name="", proxy_port="", remove=True, runtime=""):

            "MODEL_NAME": model_name,
            "MODEL_VERSION": model_version,
            # NOTE: assumes this container being launched on same machine
            # in same docker network as the query frontend
            "MODEL_PORT": "22222",
            "PROXY_NAME": proxy_name,
            "PROXY_PORT": proxy_port
        }

        # modelname_version
        model_container_label = create_model_container_label(model_name, model_version)
        labels = self.common_labels.copy()
        labels[HAMS_MODEL_CONTAINER_LABEL] = model_container_label
        labels[HAMS_DOCKER_LABEL] = self.cluster_name

        model_container_name = model_container_label + '-{}'.format(
            random.randint(0, 100000))

        #model_container_name = model_container_label
        #self.logger.info("[DOCKER MANAGEMENT] Scheduling a host")
        scheduled_host, scheduled_client = self.schedule_host()
        #print(scheduled_client)
        self.logger.info("[DOCKER MANAGEMENT] Scheduled container on %s"%(scheduled_host))

       # scheduled_client = 

        container = scheduled_client.containers.run(
            image,
            name=model_container_name,
            environment=env_vars,
            labels=labels,
            runtime=runtime,
            remove= remove,
            #tty=True,
            **self.extra_container_kwargs)

        #<Container: d15d870463>
        container_id = str(container)[12:-1]

        self.container_count = self.container_count + 1

        return model_container_name, container_id, scheduled_host



    def get_logs(self, logging_dir):
        containers = self.docker_client.containers.list(
            filters={
                "label":
                "{key}={val}".format(
                    key=HAMS_DOCKER_LABEL, val=self.cluster_name)
            })
        logging_dir = os.path.abspath(os.path.expanduser(logging_dir))

        log_files = []
        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)
            self.logger.info("Created logging directory: %s" % logging_dir)
        for c in containers:
            log_file_name = "image_{image}:container_{id}.log".format(
                image=c.image.short_id, id=c.short_id)
            log_file = os.path.join(logging_dir, log_file_name)
            if sys.version_info < (3, 0):
                with open(log_file, "w") as lf:
                    lf.write(c.logs(stdout=True, stderr=True))
            else:
                with open(log_file, "wb") as lf:
                    lf.write(c.logs(stdout=True, stderr=True))
            log_files.append(log_file)
        return log_files

    def stop_models(self, models):
        containers = self.docker_client.containers.list(
            filters={
                "label": [
                    HAMS_MODEL_CONTAINER_LABEL, "{key}={val}".format(
                        key=HAMS_DOCKER_LABEL, val=self.cluster_name)
                ]
            })
        for c in containers:
            c_name, c_version = parse_model_container_label(
                c.labels[HAMS_MODEL_CONTAINER_LABEL])
            if c_name in models and c_version in models[c_name]:
                c.stop()

    def stop_all_model_containers(self):
        containers = self.docker_client.containers.list(
            filters={
                "label": [
                    HAMS_MODEL_CONTAINER_LABEL, "{key}={val}".format(
                        key=HAMS_DOCKER_LABEL, val=self.cluster_name)
                ]
            })
        for c in containers:
            c.stop()

    def stop_all(self, graceful=True):
        for host, client in self.host_list.items():
            containers = client.containers.list(
                filters={
                    "label":
                    "{key}={val}".format(
                        key=HAMS_DOCKER_LABEL, val=self.cluster_name)
                })
            for c in containers:
                if graceful:
                    c.stop()
                else:
                    c.kill()

    def get_admin_addr(self):
        return "{host}:{port}".format(
            host=self.public_hostname, port=self.hams_management_port)

    def get_query_addr(self):
        return "{host}:{port}".format(
            host=self.public_hostname, port=self.hams_query_port)

    #def get_metric_addr(self):
    #    return "{host}:{port}".format(
    #        host=self.public_hostname, port=self.prometheus_port)

    def grpc_client(self, image, arg_list):
        c = self.docker_client.containers.run(
            image,
            arg_list,
            environment=["PYTHONUNBUFFERED=0"],
            remove=True,
            **self.extra_container_kwargs)
        #print(c_id.id)
        self.gen_container_log(c)
       # c.remove()

        
    def gen_container_log(self, c):
        log = ""
        for x in c.logs(stream=True):
            log = log+str(x)
        self.logger.info("[GRPC] Call returns with logs: %s"%(log.rstrip()))

    def check_container_status(self, host_ip, container_id, timeout, threshold):

        client = self.host_list.get(host_ip, self.docker_client)

        c = client.containers.get(container_id)

        count = 0;

        while c.status != "running":
            count = count+1
            if (count < threshold):
                self.logger.info("Container is not runnint, retrying...")
                time.sleep(timeout)
            else:
                return "timeout"
        return "running"

    def get_docker_client(self, host_ip):

        return self.host_list.get(host_ip, self.docker_client)




        

# author: Dan Crankshaw, ucbrise lab, https://github.com/ucbrise/clipper
def find_unbound_port(start=None,
                      increment=False,
                      port_range=(10000, 50000),
                      verbose=False,
                      logger=None):
    """
    Find a unbound port.

    Parameters
    ----------
    start : int
        The port number to start with. If this port is unbounded, return this port.
        If None, start will be a random port.
    increment : bool
        If True, find port by incrementing start port; else, random search.
    port_range : tuple
        The range of port for random number generation
    verbose : bool
        Verbose flag for logging
    logger: logging.Logger
    """
    while True:
        if not start:
            start = random.randint(*port_range)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", start))
            # Make sure we clean up after binding
            del sock
            return start
        except socket.error as e:
            if verbose and logger:
                logger.info("Socket error: {}".format(e))
                logger.info(
                    "randomly generated port %d is bound. Trying again." %
                    start)

        if increment:
            start += 1
        else:
            start = random.randint(*port_range)
