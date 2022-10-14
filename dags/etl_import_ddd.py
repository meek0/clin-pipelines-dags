import logging
import re
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import file_md5, http_get, http_get_file


with DAG(
    dag_id='etl_import_ddd',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        foo = http_get_file('https://www.ebi.ac.uk/gene2phenotype/downloads/DDG2P.csv.gz', 'DDG2P.csv.gz')
        logging.info(foo)

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-ddd-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['ddd'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
