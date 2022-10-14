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
    dag_id='etl_import_dbsnp',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        dbsnp_url = 'https://ftp.ncbi.nih.gov/snp/latest_release/VCF'
        dbsnp_file = 'GCF_000001405.39.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/dbsnp/{dbsnp_file}'

        # Get latest release MD5 checksum
        md5_text = http_get(f'{dbsnp_url}/{dbsnp_file}.md5').text
        md5_hash = re.search('^([0-9a-f]+)', md5_text).group(1)

        # Get latest s3 MD5 checksum
        s3_md5 = None
        if s3.check_for_key(f'{s3_key}.md5', s3_bucket):
            s3_md5 = s3.read_key(f'{s3_key}.md5', s3_bucket)
        logging.info(f'dbsnp imported MD5 hash: {s3_md5}')

        # Skip task if up to date
        if s3_md5 == md5_hash:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{dbsnp_url}/{dbsnp_file}', dbsnp_file)

        # Verify MD5 checksum
        download_md5_hash = file_md5(dbsnp_file)
        if download_md5_hash != md5_hash:
            raise AirflowFailException('MD5 checksum verification failed')

        # Upload file to S3
        s3.load_file(dbsnp_file, s3_key, s3_bucket, replace=True)
        s3.load_string(
            md5_hash, f'{s3_key}.md5', s3_bucket, replace=True
        )
        logging.info(f'New dbsnp imported MD5 hash: {md5_hash}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-dbsnp-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.public.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['dbsnp'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
