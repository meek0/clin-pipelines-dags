import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import env
from lib.franklin import (FranklinStatus, attach_vcf_to_analysis,
                          export_bucket, extract_from_name_aliquot_id,
                          get_analysis_status, get_completed_analysis,
                          get_franklin_http_conn, get_metadata_content,
                          group_families_from_metadata, import_bucket,
                          post_create_analysis, transfer_vcf_to_franklin,
                          vcf_suffix, writeS3AnalysesStatus)
from lib.groups.franklin_create import FranklinCreate
from lib.groups.franklin_update import FranklinUpdate
from lib.slack import Slack
from sensors.franklin import FranklinAPISensor

with DAG(
        dag_id='etl_import_franklin',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        params={
            'batch_id': Param('', type='string'),
        },
) as dag:
    
    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def validate_params(batch_id):
        if batch_id == '':
            raise AirflowFailException('DAG param "batch_id" is required')

    params = PythonOperator(
        task_id='params',
        op_args=[batch_id()],
        python_callable=validate_params,
        on_execute_callback=Slack.notify_dag_start,
    )

    create = FranklinCreate(
        group_id='create',
        batch_id=batch_id(),
    )

    update = FranklinUpdate(
        group_id='update',
        batch_id=batch_id(),
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )
    
    params >> create >> update >> slack