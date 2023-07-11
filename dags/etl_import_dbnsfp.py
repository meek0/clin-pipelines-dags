import logging
import re
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_import import get_s3_file_md5, download_and_check_md5, load_to_s3_with_md5


with DAG(
    dag_id='etl_import_dbnsfp',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    # Following steps explain how to get and clean-up the raw zip files
    # 1 - download dbNSFP4.3a.zip from https://sites.google.com/site/jpopgen/dbNSFP
    # 2 - extract ZIP content on local inside a folder
    # 3 - keep only files with pattern: dbNSFP4.3a_variant.chr*.gz
    # 4 - for every of those files, apply the following shell script:
    # (this script exists because: some columns have un-compatible names for HIVE (SQL like naming convention) and the easiest way was a 'sed' 
    # command cause the files are big also the script deploy on S3 QA when it's done, can take some time with the VPN upload speed limit. 
    # You can run multiple conversions at once to go quicker, one per local CPU unit is fine)

    '''
    gunzip $1.gz
    sed -i -e '1s/pos(1-based)/position_1-based/' -e '1s/hg19_pos(1-based)/hg19_pos_1-based/' -e '1s/hg18_pos(1-based)/phg18_pos_1-based/' $1
    gzip $1
    aws --profile cqgc-qa --endpoint https://s3.cqgc.hsj.rtss.qc.ca s3 cp $1.gz s3://cqgc-qa-app-datalake/raw/landing/dbNSFP/$1.gz
    '''

    raw = SparkOperator(
        task_id='raw',
        name='etl-import-dbnsfp-raw-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[f'config/{env}.conf', 'default', 'dbnsfp_raw'],
        on_execute_callback=Slack.notify_dag_start,
    )

    enriched = SparkOperator(
        task_id='enriched',
        name='etl-import-dbnsfp-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[f'config/{env}.conf', 'default', 'dbnsfp'],
        on_success_callback=Slack.notify_dag_completion,
    )

    raw >> enriched
