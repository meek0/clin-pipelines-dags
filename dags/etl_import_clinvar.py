import logging
import os
import time
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.config import env
from lib.utils import file_content, http_get_file


with DAG(
    dag_id='etl_import_clinvar',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    def _import_clinvar():

        url = 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/'
        filename = 'clinvar.vcf.gz'
        md5_filename = f'{filename}.md5'

        http_get_file(f'{url}{md5_filename}', md5_filename)
        md5 = file_content(md5_filename).split(' ', 1)[0]

        logging.info(f'{md5}')

        # s3 = S3Hook('minio')
        # s3.load_file(
        #     path,
        #     key=path,
        #     bucket_name='airflow',
        # )

    import_clinvar = PythonOperator(
        task_id='import_clinvar',
        python_callable=_import_clinvar,
    )
