from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from lib.etl import config


with DAG(
    dag_id='test_context_etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    test_context_etl = KubernetesPodOperator(
        is_delete_operator_pod=False,
        task_id='test_context_etl',
        namespace=config.k8s_namespace,
        cluster_context=config.k8s_context.get('etl'),
        name='test-context-etl',
        image='alpine',
        cmds=['echo', 'hello', 'etl'],
        arguments=[],
    )

    test_context_etl
