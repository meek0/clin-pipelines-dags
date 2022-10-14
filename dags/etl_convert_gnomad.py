from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack


with DAG(
    dag_id='etl_convert_gnomad',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    convert_gnomad = SparkOperator(
        task_id='convert_gnomad',
        name='etl-convert-gnomad',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.migration.ConvertGnomadTablesToDelta',
        spark_config='enriched-etl',
        arguments=[
            f'config/{env}.conf', 'initial',
        ],
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion,
    )
