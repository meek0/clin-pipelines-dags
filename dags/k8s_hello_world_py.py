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
            namespace='cqgc-etl',
            image="Python:3.6",
            cmds=["Python","-c"],
            arguments=["print('hello world')"]
            name="test-say-hello",
            in_cluster=True,
            cluster_context= 'kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc',
            task_id="test-say-hello-world",
            get_logs=True,
            dag=dag
        )