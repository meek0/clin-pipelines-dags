from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from lib.etl import config


with DAG(
    dag_id='k8s_pod_operator',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    k8s_pod_operator = KubernetesPodOperator(
        task_id='k8s_pod_operator',
        namespace=config.k8s_namespace,
        cluster_context='kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc',
        name='k8s_pod_operator',
        image='alpine',
        cmds=['echo', 'hello'],
        arguments=[],
        is_delete_operator_pod=True,
    )

    k8s_pod_operator
