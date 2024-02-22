from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from lib import config
from lib.config import Env, K8sContext, env
from lib.operators.fhir import FhirOperator
from lib.operators.fhir_csv import FhirCsvOperator
from lib.operators.wait import WaitOperator
from lib.slack import Slack

with DAG(
    dag_id='etl_import_fhir',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', type=['null', 'string']),
    },
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def color(prefix: str = '') -> str:
        return '{% if params.color and params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def _params_validate(color):
        if env == Env.QA:
            if not color or color == '':
                raise AirflowFailException(
                    f'DAG param "color" is required in {env} environment'
                )
        elif color and color != '':
            raise AirflowFailException(
                f'DAG param "color" is forbidden in {env} environment'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[color()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    ig_publish = FhirOperator(
        task_id='ig_publish',
        name='etl-import-fhir-ig-publish',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
    )

    wait_30s = WaitOperator(
        task_id='wait_30s',
        time='30s',
    )

    csv_import = FhirCsvOperator(
        task_id='csv_import',
        name='etl-import-fhir-csv-import',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=['-f', f'{env}.yml'],
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> ig_publish >> wait_30s >> csv_import
