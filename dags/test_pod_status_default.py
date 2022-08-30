import kubernetes
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib import config
from lib.config import K8sContext


with DAG(
    dag_id='test_pod_status_default',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    def _test_pod_status_default():
        config.k8s_load_config(K8sContext.DEFAULT)
        k8s_client = kubernetes.client.CoreV1Api()
        list = k8s_client.list_namespaced_pod(
            namespace=config.k8s_namespace,
            limit=10,
        )
        logging.info('pods:')
        for item in list.items:
            logging.info(f'{item.metadata.name} ({item.status.phase})')

    test_pod_status_default = PythonOperator(
        task_id='test_pod_status_default',
        python_callable=_test_pod_status_default,
    )

    test_pod_status_default
