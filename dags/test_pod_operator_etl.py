from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from lib import config
from lib.config import K8sContext


with DAG(
    dag_id='test_pod_operator_etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    test_pod_operator_etl = KubernetesPodOperator(
        task_id='test_pod_operator_etl',
        name='test-pod-operator-etl',
        is_delete_operator_pod=True,
        in_cluster=config.k8s_in_cluster(K8sContext.ETL),
        config_file=config.k8s_config_file(K8sContext.ETL),
        cluster_context=config.k8s_cluster_context(K8sContext.ETL),
        namespace=config.k8s_namespace,
        image='alpine',
        cmds=['echo', 'hello'],
        arguments=[],
    )

    test_pod_operator_etl
