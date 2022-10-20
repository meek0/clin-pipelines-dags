import logging
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
from lib.utils_import import get_s3_file_md5, download_and_check_md5, load_to_s3_with_md5


with DAG(
    dag_id='etl_import_ensembl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        url = 'http://ftp.ensembl.org/pub/current_tsv/homo_sapiens'
        types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
        updated = False

        for type in types:

            file = f'Homo_sapiens.GRCh38.108.{type}.tsv.gz'

            s3 = S3Hook(config.s3_conn_id)
            s3_bucket = f'cqgc-{env}-app-datalake'
            s3_key = f'raw/landing/ensembl/{file}'

            # Get latest s3 MD5 checksum
            s3_md5 = get_s3_file_md5(s3, s3_bucket, s3_key)
            logging.info(f'Current {type} imported MD5 hash: {s3_md5}')

            # Download file
            download_md5 = download_and_check_md5(url, file, None)

            # Verify MD5 checksum
            if download_md5 != s3_md5:
                # Upload file to S3
                load_to_s3_with_md5(s3, s3_bucket, s3_key, file, download_md5)
                logging.info(f'New {type} imported MD5 hash: {download_md5}')
                updated = True


        if not updated:
            raise AirflowSkipException()
       

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-ensembl-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='ensembl_mapping',
        arguments=['orphanet'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
