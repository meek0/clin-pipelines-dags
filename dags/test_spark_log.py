from airflow import DAG
from datetime import datetime
from lib import config
from lib.config import K8sContext
from lib.operators.spark import SparkOperator


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
        spark_config='enriched-etl',
        arguments=[f'config/{config.environment}.conf', 'default'],
    )

    test_spark_log
