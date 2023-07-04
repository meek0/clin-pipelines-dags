from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib.doc import qa as doc
from lib.groups.qa import qa
from airflow.operators.empty import EmptyOperator
from lib.slack import Slack

with DAG(
    dag_id='etl_qa',
    doc_md=doc.etl_qa,
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'release_id': Param('', type='string'),
        'spark_jar': Param('', type='string'),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
    },
) as dag:

    def release_id() -> str:
        return '{{ params.release_id }}'

    def spark_jar() -> str:
        return '{{ params.spark_jar }}'

    def _params_validate(release_id):
        if release_id == '':
            raise AirflowFailException('DAG param "release_id" is required')

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[release_id()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    qa = qa(
        group_id='qa',
        release_id=release_id(),
        spark_jar=spark_jar(),
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> qa >> slack
