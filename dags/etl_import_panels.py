from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.config import K8sContext, config_file
from lib.operators.panels import PanelsOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack

with DAG(
    dag_id='etl_import_panels',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
        params={
        'panels': Param('', type='string'),
        'import': Param('yes', enum=['yes', 'no']),
        'debug': Param('no', enum=['yes', 'no']),
        'dryrun': Param('no', enum=['yes', 'no']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def panels() -> str:
        return '{{ params.panels }}'

    def _import() -> str:
        return '{{ params.import }}'

    def debug() -> str:
        return '{% if params.debug == "yes" %}true{% else %}false{% endif %}'

    def dryrun() -> str:
        return '{% if params.dryrun == "yes" %}true{% else %}false{% endif %}'

    def skip_import() -> str:
        return '{% if params.panels|length and params.import == "yes" %}{% else %}yes{% endif %}'

    def skip_etl() -> str:
        return '{% if params.dryrun == "yes" %}yes{% else %}{% endif %}'

    def _params_validate(panels, _import):
        if panels == '' and _import == 'yes':
            raise AirflowFailException(
                'DAG param "panels" is required'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[panels(), _import()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    s3 = PanelsOperator(
        task_id='s3',
        name='etl-s3-panels',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_import(),
        arguments=[
            'org.clin.panels.command.Import', '--file=' + panels(), '--debug=' + debug(), '--dryrun=' + dryrun()
        ],
    )

    panels = SparkOperator(
        task_id='panels',
        name='etl-import-panels',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='raw-import-external-etl',
        skip=skip_etl(),
        arguments=[
            'panels',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_import_panels',
        ],
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    s3 >> panels >> slack