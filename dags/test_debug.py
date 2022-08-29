from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from lib.etl import config


with DAG(
    dag_id='test_debug',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    test_debug = KubernetesPodOperator(
        task_id='test_debug',
        is_delete_operator_pod=False,
        namespace=config.k8s_namespace,
        cluster_context=config.k8s_context.get('default'),
        name='test-debug',
        image='alpine',
        cmds=['echo', 'hello'],
        arguments=[],
    )

    test_debug
