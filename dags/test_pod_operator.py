from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from lib.etl import config


with DAG(
    dag_id='test_pod_operator',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    k8s_pod_operator = KubernetesPodOperator(
        task_id='test_pod_operator',
        is_delete_operator_pod=True,
        namespace=config.k8s_namespace,
        cluster_context=config.k8s_context.get('default'),
        name='k8s-pod-operator',
        image='alpine',
        cmds=['echo', 'hello'],
        arguments=[],
    )

    k8s_pod_operator
