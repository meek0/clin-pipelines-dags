import logging
from datetime import datetime
from itertools import chain

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib.config import env, s3_conn_id, basespace_illumina_credentials, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_import import stream_upload_to_s3, get_s3_file_version

with DAG(
        dag_id='etl_import_genes',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        max_active_tasks=1,  # Only one task can be scheduled at a time
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    
    genes_table = SparkOperator(
        task_id='genes_table',
        name='etl-import-genes-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[f'config/{env}.conf', 'default', 'genes'],
        on_execute_callback=Slack.notify_dag_start
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    genes_table >> slack
