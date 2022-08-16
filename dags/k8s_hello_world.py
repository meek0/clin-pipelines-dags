from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    "owner": "jcostanza",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jcostanza@ferlab.bio"
}

with DAG("k8s_hello_world", start_date=days_ago(2),
    schedule_interval=None, catchup=False) as dag:
        task_hello_world = KubernetesPodOperator(
            namespace='default',
            image='alpine',
            cmds=["sh", "-c", "echo 'Hello world!' "],
            name="say-hello",
            is_delete_operator_pod=False,
            in_cluster=True,
            task_id="say-helo",
            get_logs=True,
        )