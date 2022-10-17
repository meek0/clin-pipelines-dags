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
from lib.utils import file_md5, http_get, http_get_file


with DAG(
    dag_id='etl_import_human_genes',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        refseq_url = 'https://ftp.ncbi.nlm.nih.gov/refseq/H_sapiens'
        refseq_file = 'Homo_sapiens.gene_info.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/refseq/{refseq_file}'

        # Get latest s3 MD5 checksum
        s3_md5 = None
        if s3.check_for_key(f'{s3_key}.md5', s3_bucket):
            s3_md5 = s3.read_key(f'{s3_key}.md5', s3_bucket)
        logging.info(f'refseq imported MD5 hash: {s3_md5}')

        # Download file
        http_get_file(f'{refseq_url}/{refseq_file}', refseq_file)

        # Verify MD5 checksum
        download_md5_hash = file_md5(refseq_file)
        if download_md5_hash == s3_md5:
            raise AirflowSkipException()

        # Upload file to S3
        s3.load_file(refseq_file, s3_key, s3_bucket, replace=True)
        s3.load_string(
            download_md5_hash, f'{s3_key}.md5', s3_bucket, replace=True
        )
        logging.info(f'New refseq imported MD5 hash: {download_md5_hash}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-human-genes-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['refseq_human_genes'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
