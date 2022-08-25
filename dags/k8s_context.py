import kubernetes
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.k8s import k8s_load_config


with DAG(
    dag_id='k8s_context',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    def context():
        k8s_load_config()
        contexts, active_context = kubernetes.config.list_kube_config_contexts()
        logging.info('contexts')
        logging.info(contexts)
        logging.info('active_context')
        logging.info(active_context)

    k8s_context = PythonOperator(
        task_id='k8s_context',
        python_callable=context,
    )

    k8s_context
