import logging
import re
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get, http_get_file


with DAG(
    dag_id='etl_import_hpo',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        hpo_url = 'https://github.com/obophenotype/human-phenotype-ontology/releases'
        hpo_file = 'genes_to_phenotype.txt'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/hpo/{hpo_file}'

        # Get latest version
        html = http_get(hpo_url).text
        latest_ver = re.search(f'/download/(v.+)/{hpo_file}', html).group(1)
        logging.info(f'HPO latest version: {latest_ver}')

        # Get imported version
        imported_ver = None
        if s3.check_for_key(f'{s3_key}.version', s3_bucket):
            imported_ver = s3.read_key(f'{s3_key}.version', s3_bucket)
        logging.info(f'HPO imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(
            f'{hpo_url}/download/{latest_ver}/{hpo_file}', hpo_file
        )

        # Upload file to S3
        s3.load_file(hpo_file, s3_key, s3_bucket, replace=True)
        s3.load_string(
            latest_ver, f'{s3_key}.version', s3_bucket, replace=True
        )
        logging.info(f'New HPO imported version: {latest_ver}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-hpo-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['hpo'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
