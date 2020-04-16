import abc
from .exceptions import HamsException
import logging

# Constants
HAMS_INTERNAL_QUERY_PORT = 1337
HAMS_INTERNAL_MANAGEMENT_PORT = 1338
HAMS_INTERNAL_RPC_PORT = 7000
HAMS_INTERNAL_METRIC_PORT = 1390
HAMS_INTERNAL_REDIS_PORT = 6379

HAMS_DOCKER_LABEL = "ai.hams.container.label"
HAMS_NAME_LABEL = "ai.hams.name"
HAMS_MODEL_CONTAINER_LABEL = "ai.hams.model_container.label"
HAMS_QUERY_FRONTEND_CONTAINER_LABEL = "ai.hams.query_frontend.label"
HAMS_MGMT_FRONTEND_CONTAINER_LABEL = "ai.hams.management_frontend.label"
HAMS_QUERY_FRONTEND_ID_LABEL = "ai.hams.query_frontend.id"
CONTAINERLESS_MODEL_IMAGE = "NO_CONTAINER"

HAMS_DOCKER_PORT_LABELS = {
    'redis': 'ai.hams.redis.port',
    'query_rest': 'ai.hams.query_frontend.query.port',
    'query_rpc': 'ai.hams.query_frontend.rpc.port',
    'management': 'ai.hams.management.port',
    'metric': 'ai.hams.metric.port'
}
HAMS_METRIC_CONFIG_LABEL = 'ai.hams.metric.config'

# NOTE: we use '_' as the delimiter because kubernetes allows the use
# '_' in labels but not in deployment names. We force model names and
# versions to be compliant with both limitations, so this gives us an extra
# character to use when creating labels.
_MODEL_CONTAINER_LABEL_DELIMITER = "_"


class ClusterAdapter(logging.LoggerAdapter):
    """
    This adapter adds cluster name to logging format.

    Usage
    -----
        In ContainerManager init process, do:
            self.logger = ClusterAdapter(logger, {'cluster_name': self.cluster_name})
    """

    # def process(self, msg, kwargs):
    #     return "[{}] {}".format(self.extra['cluster_name'], msg), kwargs

    def process(self, msg, kwargs):
        return "{}".format(msg), kwargs


def create_model_container_label(name, version):
    return "{name}{delim}{version}".format(
        name=name, delim=_MODEL_CONTAINER_LABEL_DELIMITER, version=version)


def parse_model_container_label(label):
    splits = label.split(_MODEL_CONTAINER_LABEL_DELIMITER)
    if len(splits) != 2:
        raise HamsException(
            "Unable to parse model container label {}".format(label))
    return splits


class ContainerManager(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def start_hams(self, query_frontend_image, mgmt_frontend_image,
                      frontend_exporter_image, cache_size,
                      qf_http_thread_pool_size, qf_http_timeout_request,
                      qf_http_timeout_content, 
                      num_frontend_replicas):
        return

    @abc.abstractmethod
    def connect(self):
        return

    @abc.abstractmethod
    def connect_host(self, host_ip, host_port):
        return

    @abc.abstractmethod
    def deploy_model(self, name, version, input_type, image):
        return

    @abc.abstractmethod
    def add_replica(self, name, version, input_type, image,  proxy_name="", proxy_port="", remove=True, runtime=""):
        return

    @abc.abstractmethod
    def set_proxy(self, image, model_container_label, model_ip, host_ip):
        return

    @abc.abstractmethod
    def get_num_replicas(self, name, version):
        return

    @abc.abstractmethod
    def get_logs(self, logging_dir):
        return

    @abc.abstractmethod
    def stop_models(self, models):
        return

    @abc.abstractmethod
    def stop_all_model_containers(self):
        return

    @abc.abstractmethod
    def stop_all(self, graceful=True):
        pass

    @abc.abstractmethod
    def get_admin_addr(self):
        return

    @abc.abstractmethod
    def get_query_addr(self):
        return

    @abc.abstractmethod
    def get_container_ip(self, host_ip, container_id):
        return

    @abc.abstractmethod
    def grpc_client(self, image, arg_list):
        return

    @abc.abstractmethod
    def check_container_status(self, host_ip, container_id, timeout, threshold):
        return

    @abc.abstractmethod
    def get_docker_client(self, host_ip):
        return

    @abc.abstractmethod
    def add_frontend(self, host_ip, image, runtime_dag_id, entry_proxy_ip, entry_proxy_port, max_workers=64,stateful=False, remove=True):
        return

