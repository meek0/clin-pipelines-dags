from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib.config import env, es_url, Env, K8sContext
from lib.groups.qa import qa
from lib.groups.ingest import ingest
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='etl_ingest',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id': Param('', type='string'),
        'color': Param('', enum=['', 'blue', 'green']),
        'import': Param('yes', enum=['yes', 'no']),
        'spark_jar': Param('', type='string'),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def spark_jar() -> str:
        return '{{ params.spark_jar }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def skip_import() -> str:
        return '{% if params.batch_id|length and params.import == "yes" %}{% else %}yes{% endif %}'

    def _params_validate(batch_id, color):
        if batch_id == '':
            raise AirflowFailException('DAG param "batch_id" is required')
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
        op_args=[batch_id(), color()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    ingest = ingest(
        group_id='ingest',
        batch_id=batch_id(),
        color=color(),
        skip_import=skip_import(),  # skipping already imported batch is allowed
        skip_batch='', # always compute this batch (purpose of this dag)
        spark_jar=spark_jar(),
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> ingest >> slack
