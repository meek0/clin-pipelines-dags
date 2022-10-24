import logging
import re
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from lib import config
from lib.config import env
from lib.slack import Slack
from lib.utils import http_get, http_get_file
from lib.utils_import import get_s3_file_version


with DAG(
    dag_id='etl_import_topmed_bravo',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    url = 'https://bravo.sph.umich.edu'
    path = 'hg38/downloads'
    file_prefix = 'bravo-dbsnp-chr'
    file_ext = '.vcf.gz'

    s3 = S3Hook(config.s3_conn_id)
    s3_bucket = f'cqgc-{env}-app-datalake'
    s3_key_prefix = f'raw/landing/topmed/'

    chromosomes = list(range(1, 23)) + list('X')

    def _init():
        # Get latest version
        html = http_get(url).text
        latest_ver = re.search('bravo.sph.umich.edu/(.+)/hg38/', html).group(1)
        logging.info(f'TOPMed Bravo latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(
            s3, s3_bucket, f'{s3_key_prefix}{file_prefix}{file_ext}'
        )
        logging.info(f'TOPMed Bravo imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        return latest_ver

    init = PythonOperator(
        task_id='init',
        python_callable=_init,
        on_execute_callback=Slack.notify_dag_start,
    )

    @task
    def file(chromosome: str, **kwargs):
        latest_ver = kwargs['ti'].xcom_pull(task_ids=['init'])[0]
        logging.info(latest_ver)
        logging.info(chromosome)

    files = file.expand(chromosome=chromosomes)

    def _release():
        logging.info('release')

    release = PythonOperator(
        task_id='release',
        python_callable=_release,
    )

    # table = SparkOperator(
    #     task_id='table',
    #     name='etl-import-topmed-bravo-table',
    #     k8s_context=K8sContext.ETL,
    #     spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
    #     spark_config='enriched-etl',
    #     arguments=['topmed_bravo'],
    #     trigger_rule=TriggerRule.ALL_SUCCESS,
    #     on_success_callback=Slack.notify_dag_completion,
    # )

    init >> files >> release
