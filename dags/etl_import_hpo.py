import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.utils import http_get, http_get_file
from lib.utils_etl import batch_id, color, skip_import, spark_jar
from lib.utils_import import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_hpo',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', type=['null', 'string']),
        'spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure
    },
    ) as dag:

    params_validate = validate_color(color())

    def _file(file):
        url = 'https://github.com/obophenotype/human-phenotype-ontology/releases'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/hpo/{file}'

        # Get latest version
        html = http_get(url).text
        latest_ver = re.search(f'/download/(v.+)/{file}', html).group(1)
        logging.info(f'{file} latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'{file} imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{url}/download/{latest_ver}/{file}', file)

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New {file} imported version: {latest_ver}')


    download_hpo_genes = PythonOperator(
        task_id='download_hpo_genes',
        python_callable=_file,
        op_args=['genes_to_phenotype.txt'],
        on_execute_callback=Slack.notify_dag_start,
    )

    download_hpo_obo = PythonOperator(
        task_id='download_hpo_obo',
        python_callable=_file,
        op_args=['hp-fr.obo'],
    )

    normalized_hpo_genes = SparkOperator(
        task_id='normalized_hpo_genes',
        name='etl-import-hpo-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'hpo',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_hpo_table',
        ],
    )

    normalized_hpo_obo = SparkOperator(
        task_id='normalized_hpo_obo',
        name='etl-import-hpo-obo-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'hpo-obo',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_hpo_table',
        ],
    )

    publish_hpo_to_fhir = PipelineOperator(
        task_id='publish_hpo_to_fhir',
        name='publish-hpo-to-fhir',
        k8s_context=K8sContext.DEFAULT,
        color=color,
        arguments=[
            'bio.ferlab.clin.etl.PublishHpoToFhir', 'hpo'
        ],
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> download_hpo_genes >> download_hpo_obo >> normalized_hpo_genes >> normalized_hpo_obo >> publish_hpo_to_fhir >> slack
