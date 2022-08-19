from airflow import DAG
from datetime import datetime
from lib.etl import config
from lib.etl.etl_task import etl_task


with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    task = etl_task(
        group_id='etl',
        parent_id='',
        environment=config.environment,
        k8s_namespace=config.k8s_namespace,
        k8s_context=config.k8s_context,
        k8s_service_account=config.k8s_service_account,
        spark_image=config.spark_image,
        spark_jar=config.spark_jar,
        batch_id=config.batch_id,
    )

    task
