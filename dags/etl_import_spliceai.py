import logging
from datetime import datetime
from itertools import chain

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import env, s3_conn_id, basespace_illumina_credentials
from lib.slack import Slack
from lib.utils import http_get_file, file_md5
from lib.utils_import import get_s3_file_md5, load_to_s3_with_md5

with DAG(
        dag_id='etl_import_spliceai',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    def _file():
        # file_name -> file_id
        indel = {
            "spliceai_scores.raw.indel.hg38.vcf.gz": 16525003580,
            "spliceai_scores.raw.indel.hg38.vcf.gz.tbi": 16525276839
        }
        snv = {
            "spliceai_scores.raw.snv.hg38.vcf.gz": 16525380715,
            "spliceai_scores.raw.snv.hg38.vcf.gz.tbi": 16525505189
        }

        s3 = S3Hook(s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        updated = False

        def s3_key(file_name):
            return f'raw/landing/spliceai/{file_name}'

        def url(id):
            return f'https://api.basespace.illumina.com/v1pre3/files/{id}/content'

        for file_name, file_id in chain(indel.items(), snv.items()):
            # Get latest S3 MD5 checksum
            s3_md5 = get_s3_file_md5(s3, s3_bucket, s3_key(file_name))
            logging.info(f'Current {file_name} imported MD5 hash: {s3_md5}')

            # Download file
            http_get_file(
                url(file_id),
                file_name,
                headers={'x-access-token': f'{basespace_illumina_credentials}'}
            )

            # Verify MD5 checksum
            download_md5 = file_md5(file_name)
            if download_md5 != s3_md5:
                # Upload file to S3
                load_to_s3_with_md5(s3, s3_bucket, s3_key(file_name), file_name, download_md5)
                logging.info(f'New {file_name} imported MD5 hash: {download_md5}')
                updated = True

        # If no files have been updated, skip task
        if not updated:
            raise AirflowSkipException()


    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )
