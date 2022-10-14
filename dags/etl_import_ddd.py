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
from lib.utils import http_get_file


with DAG(
    dag_id='etl_import_ddd',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        ddd_url = 'https://www.ebi.ac.uk/gene2phenotype/downloads'
        ddd_file = 'DDG2P.csv.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/ddd/{ddd_file}'

        # Download file
        http_get_file(f'{ddd_url}/{ddd_file}', ddd_file)

        # Get latest version
        file = open(ddd_file, 'rb')
        first_line = str(file.readline())
        file.close()
        latest_ver = re.search('DDG2P_([0-9_]+)\.csv', first_line).group(1)
        logging.info(f'DDD latest version: {latest_ver}')

        # Get imported version
        imported_ver = None
        if s3.check_for_key(f'{s3_key}.version', s3_bucket):
            imported_ver = s3.read_key(f'{s3_key}.version', s3_bucket)
        logging.info(f'DDD imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Upload file to S3
        s3.load_file(ddd_file, s3_key, s3_bucket, replace=True)
        s3.load_string(
            latest_ver, f'{s3_key}.version', s3_bucket, replace=True
        )
        logging.info(f'New DDD imported version: {latest_ver}')

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
