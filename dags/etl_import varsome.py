from airflow import DAG
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack

if env in [Env.PROD]:

    with DAG(
        dag_id='etl_import_varsome',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'batch_id':  Param('', type='string'),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
    ) as dag:

        def batch_id() -> str:
            return '{{ params.batch_id }}'

        def _params_validate(batch_id):
            if batch_id == '':
                raise AirflowFailException('DAG param "batch_id" is required')
        
        params_validate = PythonOperator(
            task_id='params_validate',
            op_args=[batch_id()],
            python_callable=_params_validate,
            on_execute_callback=Slack.notify_dag_start,
        )

        varsome = SparkOperator(
            task_id='varsome',
            name='etl-import-varsome',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.varsome.Varsome',
            spark_config='varsome-etl',
            spark_secret='varsome',
            arguments=[
                f'config/{env}.conf', 'initial', 'all', batch_id()
            ],
            on_success_callback=Slack.notify_dag_completion,
        )

        params_validate >> varsome
