from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.operators.panels import PanelsOperator
from lib.slack import Slack
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

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
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def panels() -> str:
        return '{{ params.panels }}'

    def debug() -> str:
        return '{% if params.debug == "yes" %}true{% else %}false{% endif %}'

    def dryrun() -> str:
        return '{% if params.dryrun == "yes" %}true{% else %}false{% endif %}'

    def skip_import() -> str:
        return '{% if params.panels|length and params.import == "yes" %}{% else %}yes{% endif %}'

    def _params_validate(panels, debug, dryrun):
        if panels == '':
            raise AirflowFailException(
                'DAG param "panels" is required'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[panels(), debug(), dryrun()],
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
        spark_class='bio.ferlab.clin.etl.external.ImportExternal',
        spark_config='raw-import-external-etl',
        arguments=[
            f'config/{env}.conf', 'initial', 'panels',
        ],
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion,
    )

    s3 >> panels