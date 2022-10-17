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
    dag_id='etl_import_orphanet',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        orphanet_url = 'https://www.orphadata.com/data/xml'
        genes_file = 'en_product6.xml'
        diseases_file = 'en_product9_ages.xml'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key_genes = f'raw/landing/orphanet/{genes_file}'
        s3_key_diseases = f'raw/landing/orphanet/{diseases_file}'

        # Get latest s3 MD5 checksum
        s3_md5_genes = None
        if s3.check_for_key(f'{s3_key_genes}.md5', s3_bucket):
            s3_md5_genes = s3.read_key(f'{s3_key_genes}.md5', s3_bucket)
        logging.info(f'orphanet genes imported MD5 hash: {s3_md5_genes}')

        s3_md5_diseases = None
        if s3.check_for_key(f'{s3_key_diseases}.md5', s3_bucket):
            s3_md5_diseases = s3.read_key(f'{s3_key_diseases}.md5', s3_bucket)
        logging.info(f'orphanet diseases imported MD5 hash: {s3_md5_diseases}')

        # Download file
        http_get_file(f'{orphanet_url}/{genes_file}', genes_file)
        http_get_file(f'{orphanet_url}/{diseases_file}', diseases_file)

        # Verify MD5 checksum
        updated_genes = False
        download_md5_hash_genes = file_md5(genes_file)
        if download_md5_hash_genes != s3_md5_genes:
            # Upload file to S3
            s3.load_file(genes_file, s3_key_genes, s3_bucket, replace=True)
            s3.load_string(
                download_md5_hash_genes, f'{s3_key_genes}.md5', s3_bucket, replace=True
            )
            logging.info(f'New orphanet genes imported MD5 hash: {download_md5_hash_genes}')
            updated_genes = True

        # Verify MD5 checksum
        updated_diseases = False
        download_md5_hash_diseases = file_md5(diseases_file)
        if download_md5_hash_diseases != s3_md5_diseases:
            # Upload file to S3
            s3.load_file(diseases_file, s3_key_diseases, s3_bucket, replace=True)
            s3.load_string(
                download_md5_hash_diseases, f'{s3_key_diseases}.md5', s3_bucket, replace=True
            )
            logging.info(f'New orphanet diseases imported MD5 hash: {download_md5_hash_diseases}')
            updated_diseases = True

        if not updated_diseases and not updated_genes:
            raise AirflowSkipException()
       

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-orphanet-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['orphanet'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
