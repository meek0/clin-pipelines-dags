from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib import config
from lib.config import Env, K8sContext
from lib.operators.aws import AwsOperator
from lib.operators.curl import CurlOperator
from lib.operators.fhir import FhirOperator
from lib.operators.fhir_csv import FhirCsvOperator
from lib.operators.k8s_deployment_pause import K8sDeploymentPauseOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.k8s_deployment_resume import K8sDeploymentResumeOperator
from lib.operators.postgres import PostgresOperator
from lib.operators.wait import WaitOperator


env = config.environment
if env == Env.QA:

    with DAG(
        dag_id='etl_cleanup',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'color': Param('', enum=['', 'blue', 'green']),
        },
    ) as dag:

        def color(prefix: str = '') -> str:
            return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

        def _params_validate(color):
            if env == Env.QA:
                if color == '':
                    raise AirflowFailException(
                        f'DAG param "color" is required in {env} environment'
                    )
            else:
                raise AirflowFailException(
                    f'DAG run is forbidden in {env} environment'
                )

        params_validate = PythonOperator(
            task_id='params_validate',
            op_args=[color()],
            python_callable=_params_validate,
        )

        fhir_pause = K8sDeploymentPauseOperator(
            task_id='fhir_pause',
            k8s_context=K8sContext.DEFAULT,
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
            k8s_context=K8sContext.DEFAULT,
            deployment='fhir-server' + color('-'),
        )

        fhir_restart = K8sDeploymentRestartOperator(
            task_id='fhir_restart',
            k8s_context=K8sContext.DEFAULT,
            deployment='fhir-server' + color('-'),
        )

        es_indices_delete = CurlOperator(
            task_id='es_indices_delete',
            name='etl-cleanup-es-indices-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '-f', '-X', 'DELETE',
                f'http://elasticsearch:9200/clin-{env}-prescriptions' + color('-') +
                f',clin-{env}-patients' + color('-') +
                f',clin-{env}-analyses' + color('-') +
                f',clin-{env}-sequencings' + color('-') +
                '?ignore_unavailable=true',
            ],
        )

        s3_download_delete = AwsOperator(
            task_id='s3_download_delete',
            name='etl-cleanup-s3-download-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{env}-app-download' + color('/') + '/',
                '--recursive',
            ],
        )

        s3_datalake_delete = AwsOperator(
            task_id='s3_datalake_delete',
            name='etl-cleanup-s3-datalake-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{env}-app-datalake/', '--recursive', '--exclude', '*',
                '--include', 'normalized/*',
                '--include', 'enriched/*',
                '--include', 'raw/landing/fhir/*',
                '--include', 'es_index/*',
            ],
        )

        wait_2m = WaitOperator(
            task_id='wait_2m',
            time='2m',
        )

        fhir_ig_publish = FhirOperator(
            task_id='fhir_ig_publish',
            name='etl-cleanup-fhir-init-ig-publish',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
        )

        wait_30s = WaitOperator(
            task_id='wait_30s',
            time='30s',
        )

        fhir_csv_import = FhirCsvOperator(
            task_id='fhir_csv_import',
            name='etl-cleanup-fhir-csv-import',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
            arguments=['-f', 'nanuq.yml'],
        )

        params_validate >> fhir_pause >> db_tables_delete >> fhir_resume >> fhir_restart >> es_indices_delete >> s3_download_delete >> s3_datalake_delete >> wait_2m >> fhir_ig_publish >> wait_30s >> fhir_csv_import
