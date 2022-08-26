from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.etl import config
from lib.etl.config import K8sContext
from lib.etl.operators.aws import AwsOperator
from lib.etl.operators.curl import CurlOperator
from lib.etl.operators.fhir import FhirOperator
from lib.etl.operators.fhir_csv import FhirCsvOperator
from lib.etl.operators.postgres import PostgresOperator
from lib.etl.operators.wait import WaitOperator
from lib.k8s.operators.deployment_pause import K8sDeploymentPauseOperator
from lib.k8s.operators.deployment_restart import K8sDeploymentRestartOperator
from lib.k8s.operators.deployment_resume import K8sDeploymentResumeOperator


with DAG(
    dag_id='etl_cleanup',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', enum=['', 'blue', 'green']),
    },
) as dag:

    environment = config.environment

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def _params_validate(color):
        if environment == 'qa':
            if color == '':
                raise AirflowFailException('DAG param "color" is needed')
        else:
            raise AirflowFailException(
                f'DAG run is forbidden in {environment} environment'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[color()],
        python_callable=_params_validate,
    )

    fhir_pause = K8sDeploymentPauseOperator(
        task_id='fhir_pause',
        deployment='fhir-server' + color('-'),
    )

    db_tables_delete = PostgresOperator(
        task_id='db_tables_delete',
        name='etl-cleanup-db-tables-delete',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        cmds=[
            'psql', '-d', 'fhir' + color('_'), '-c',
            '''
            DO $$$DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END$$$;
            ''',
        ],
    )

    fhir_resume = K8sDeploymentResumeOperator(
        task_id='fhir_resume',
        deployment='fhir-server' + color('-'),
    )

    fhir_restart = K8sDeploymentRestartOperator(
        task_id='fhir_restart',
        deployment='fhir-server' + color('-'),
    )

    es_indices_delete = CurlOperator(
        task_id='es_indices_delete',
        name='etl-cleanup-es-indices-delete',
        k8s_context=K8sContext.DEFAULT,
        arguments=[
            '-f', '-X', 'DELETE',
            f'http://elasticsearch:9200/clin-{environment}-prescriptions' + color('-') +
            f',clin-{environment}-patients' + color('-') +
            f',clin-{environment}-analyses' + color('-') +
            f',clin-{environment}-sequencings' + color('-') +
            '?ignore_unavailable=true',
        ],
    )

    s3_download_delete = AwsOperator(
        task_id='s3_download_delete',
        name='etl-cleanup-s3-download-delete',
        k8s_context=K8sContext.DEFAULT,
        arguments=[
            's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
            f's3://cqgc-{environment}-app-download' + color('/') + '/',
            '--recursive',
        ],
    )

    s3_datalake_delete = AwsOperator(
        task_id='s3_datalake_delete',
        name='etl-cleanup-s3-datalake-delete',
        k8s_context=K8sContext.DEFAULT,
        arguments=[
            's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
            f's3://cqgc-{environment}-app-datalake/', '--recursive', '--exclude', '"*"',
            '--include', '"normalized/*"',
            '--include', '"enriched/*"',
            '--include', '"raw/landing/fhir/*"',
            '--include', '"es_index/*"',
        ],
    )

    wait_120 = WaitOperator(
        task_id='wait_120',
        time='120',
    )

    fhir_ig_publish = FhirOperator(
        task_id='fhir_ig_publish',
        name='etl-cleanup-fhir-init-ig-publish',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
    )

    wait_30 = WaitOperator(
        task_id='wait_30',
        time='30',
    )

    fhir_csv_import = FhirCsvOperator(
        task_id='fhir_csv_import',
        name='etl-cleanup-fhir-csv-import',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=['-f', 'nanuq.yml'],
    )

    params_validate >> fhir_pause >> db_tables_delete >> fhir_resume >> fhir_restart >> es_indices_delete >> s3_download_delete >> s3_datalake_delete >> wait_120 >> fhir_ig_publish >> wait_30 >> fhir_csv_import