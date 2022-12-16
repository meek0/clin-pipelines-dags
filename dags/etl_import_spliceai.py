import logging
from datetime import datetime
from itertools import chain

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import env, s3_conn_id, basespace_illumina_credentials
from lib.slack import Slack
from lib.utils_import import get_s3_file_md5, stream_upload_to_s3

with DAG(
        dag_id='etl_import_spliceai',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    def _file():
        import http.client as http_client
        http_client.HTTPConnection.debuglevel = 1

        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        requests_log.addHandler(ch)

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
            logging.info(f'Current file {file_name} imported MD5 hash: {s3_md5}')

            # Download file
            stream_upload_to_s3(
                s3, s3_bucket, s3_key(file_name), url(file_id),
                headers={'x-access-token': f'{basespace_illumina_credentials}'},
                replace=True
            )
            logging.info(f'File {file_name} uploaded to S3')

            # Upload MD5 checksum to S3
            new_s3_md5 = get_s3_file_md5(s3, s3_bucket, s3_key(file_name))
            s3.load_string(new_s3_md5, f'{s3_key(file_name)}.md5', s3_bucket, replace=True)

        # Verify MD5 checksum
            if new_s3_md5 != s3_md5:
                logging.info(f'New {file_name} imported MD5 hash: {new_s3_md5}')
                updated = True

        # If no files have been updated, skip task
        if not updated:
            raise AirflowSkipException()


    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion
    )
