from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib import config
from lib.config import Env
from lib.etl import ingest_group


env = config.environment
if env == Env.QA:

    with DAG(
        dag_id='etl_qc',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'batch_id':  Param('', type='string'),
            'color': Param('', enum=['', 'blue', 'green']),
        },
    ) as dag:

        def batch_id() -> str:
            return '{{ params.batch_id }}'

        def color(prefix: str = '') -> str:
            return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

        def _params_validate(batch_id, color):
            if batch_id == '':
                raise AirflowFailException('DAG param "batch_id" is required')
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
            op_args=[batch_id(), color()],
            python_callable=_params_validate,
        )

        ingest = ingest_group(
            group_id='ingest',
            batch_id=batch_id(),
            color=color(),
        )

        params_validate >> ingest
