from airflow import DAG
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from airflow.models.param import Param

if (config.show_test_dags):

    with DAG(
        dag_id='test_spark_fail',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'spark_jar': Param('', type='string'),
        },
    ) as dag:

        def spark_jar() -> str:
            return '{{ params.spark_jar }}'
            
        test_spark_fail = SparkOperator(
            task_id='test_spark_fail',
            name='test-spark-fail',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.fail.Fail',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[f'config/{env}.conf', 'default'],
        )
