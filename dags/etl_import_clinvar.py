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
from lib.hooks.slack import SlackHook
from lib.operators.spark import SparkOperator
from lib.utils import file_md5, http_get, http_get_file


with DAG(
    dag_id='etl_import_clinvar',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': SlackHook.notify_task_failure,
    },
) as dag:

    def _file():
        clinvar_url = 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38'
        clinvar_file = 'clinvar.vcf.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/clinvar/{clinvar_file}'

        # Get MD5 checksum
        md5_text = http_get(f'{clinvar_url}/{clinvar_file}.md5').text
        md5_hash = re.search('^([0-9a-f]+)', md5_text).group(1)

        # Get latest version
        latest_ver = re.search('clinvar_([0-9]+)\.vcf', md5_text).group(1)
        logging.info(f'ClinVar latest version: {latest_ver}')

        # Get imported version
        imported_ver = None
        if s3.check_for_key(f'{s3_key}.version', s3_bucket):
            imported_ver = s3.read_key(f'{s3_key}.version', s3_bucket)
        logging.info(f'ClinVar imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{clinvar_url}/{clinvar_file}', clinvar_file)

        # Verify MD5 checksum
        download_md5_hash = file_md5(clinvar_file)
        if download_md5_hash != md5_hash:
            raise AirflowFailException('MD5 checksum verification failed')

        # Upload file to S3
        s3.load_file(clinvar_file, s3_key, s3_bucket, replace=True)
        s3.load_string(
            latest_ver, f'{s3_key}.version', s3_bucket, replace=True
        )
        logging.info(f'New ClinVar imported version: {latest_ver}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=SlackHook.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-clinvar-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.public.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['clinvar'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=SlackHook.notify_dag_success,
    )

    file >> table
