from airflow import DAG
from datetime import datetime
from lib.etl import config
from lib.etl.config import K8sContext
from lib.etl.operators.spark import SparkOperator


with DAG(
    dag_id='test_spark_log',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    test_spark_log = SparkOperator(
        task_id='test_spark_log',
        name='test-spark-log',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.fail.Fail',
        spark_config='raw-fhir-etl',
        arguments=[f'config/{config.environment}.conf', 'initial', 'all'],
    )

    test_spark_log
