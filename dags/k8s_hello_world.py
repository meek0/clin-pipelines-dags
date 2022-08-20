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


def _pod_list():
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

    hello_world = KubernetesPodOperator(
        task_id="say_hello",
        is_delete_operator_pod=True,
        namespace=namespace,
        cluster_context=config.k8s_context.default,
        name="say_hello",
        image='alpine',
        cmds=["sh", "-c", "echo 'Hello World!'"],
        get_logs=True,
    )

    pod_list = PythonOperator(
        task_id="pod_list",
        python_callable=_pod_list,
    )

    hello_world >> pod_list
