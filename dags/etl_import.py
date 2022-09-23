from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.config import env, Env, K8sContext, csv_file_name
from lib.operators.fhir import FhirOperator
from lib.operators.fhir_csv import FhirCsvOperator
from lib.operators.wait import WaitOperator


with DAG(
    dag_id='etl_import',
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
        elif color != '':
            raise AirflowFailException(
                f'DAG param "color" is forbidden in {env} environment'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[color()],
        python_callable=_params_validate,
    )

    fhir_ig_publish = FhirOperator(
        task_id='fhir_ig_publish',
        name='etl-import-fhir-ig-publish',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
    )

    wait_30s = WaitOperator(
        task_id='wait_30s',
        time='30s',
    )

    fhir_csv_import = FhirCsvOperator(
        task_id='fhir_csv_import',
        name='etl-import-fhir-csv-import',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=['-f', f'{csv_file_name()}.yml'],
    )

    params_validate >> fhir_ig_publish >> wait_30s >> fhir_csv_import
