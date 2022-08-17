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
            kubernetes_conn_id='kubernetes',
            image='alpine',
            cmds=["sh", "-c", "echo 'Hello WOrld!'"],
            name="say-hello",
            is_delete_operator_pod=False,
            task_id="say-hello",
            get_logs=True,
        )