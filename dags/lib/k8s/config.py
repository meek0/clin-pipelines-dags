import kubernetes
import os
from airflow.models import Variable


k8s_namespace = Variable.get('kubernetes_namespace')


def k8s_load_config():
    if os.getenv('K8S_LOCAL_CONFIG', 'false') == 'true':
        kubernetes.config.load_kube_config()
    else:
        kubernetes.config.load_incluster_config()
