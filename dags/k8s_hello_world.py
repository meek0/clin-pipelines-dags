import kubernetes
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from lib.etl import config
from lib.k8s import k8s_load_config

default_args = {
    "owner": "jcostanza",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jcostanza@ferlab.bio"
}

namespace = config.k8s_namespace


def _spark_task_check(ti):
    k8s_load_config()
    k8s_client = kubernetes.client.CoreV1Api()
    pod = k8s_client.list_namespaced_pod(
        namespace=namespace,
        limit=3,
    )
    logging.info(pod)


with DAG(
    dag_id='k8s_hello_world',
    start_date=days_ago(2),
    schedule_interval=None,
    catchup=False
) as dag:

    task_hello_world = KubernetesPodOperator(
        namespace=namespace,
        image='alpine',
        cmds=["sh", "-c", "echo 'Hello WOrld!'"],
        name="say-hello",
        is_delete_operator_pod=False,
        task_id="say-hello",
        get_logs=True,
    )

    spark_task_check = PythonOperator(
        task_id="spark_task_check",
        python_callable=_spark_task_check,
    )

    task_hello_world >> spark_task_check
