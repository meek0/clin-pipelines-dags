from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack


with DAG(
    dag_id='etl_import_omim',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    table = SparkOperator(
        task_id='table',
        name='etl-import-omim-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['omim'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion,
    )
