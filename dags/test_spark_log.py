import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.etl import config
from lib.etl.config import K8sContext
from lib.etl.operators.spark import SparkOperator


with DAG(
    dag_id='test_spark_log',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    environment = config.environment

    def _test():
        logging.info('hello')

    test = PythonOperator(
        task_id='test',
        python_callable=_test,
    )

    log = SparkOperator(
        task_id='log',
        name='etl-test-spark-log',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.fail.Fail',
        spark_config='raw-fhir-etl',
        arguments=[f'config/{environment}.conf', 'initial', 'all'],
    )

    test >> log
