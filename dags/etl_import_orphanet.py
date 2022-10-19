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
from lib.utils_import import get_s3_file_md5, download_and_check_md5, load_to_s3


with DAG(
    dag_id='etl_import_orphanet',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        url = 'https://www.orphadata.com/data/xml'
        genes_file = 'en_product6.xml'
        diseases_file = 'en_product9_ages.xml'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key_genes = f'raw/landing/orphanet/{genes_file}'
        s3_key_diseases = f'raw/landing/orphanet/{diseases_file}'

        # Get latest s3 MD5 checksum
        s3_md5_genes = get_s3_file_md5(s3, s3_bucket, s3_key_genes)
        logging.info(f'Current genes imported MD5 hash: {s3_md5_genes}')

        s3_md5_diseases = get_s3_file_md5(s3, s3_bucket, s3_key_diseases)
        logging.info(f'Current diseases imported MD5 hash: {s3_md5_diseases}')

        # Download file
        download_md5_genes = download_and_check_md5(url, genes_file, None)
        download_md5_diseases = download_and_check_md5(url, diseases_file, None)

        # Verify MD5 checksum
        updated_genes = False
        if download_md5_genes != s3_md5_genes:
            # Upload file to S3
            load_to_s3(s3, s3_bucket, s3_key_genes, file, download_md5_genes)
            logging.info(f'New genes imported MD5 hash: {download_md5_genes}')
            updated_genes = True

        # Verify MD5 checksum
        updated_diseases = False
        if download_md5_diseases != s3_md5_diseases:
            # Upload file to S3
            load_to_s3(s3, s3_bucket, s3_key_diseases, file, download_md5_diseases)
            logging.info(f'New diseases imported MD5 hash: {download_md5_diseases}')
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
