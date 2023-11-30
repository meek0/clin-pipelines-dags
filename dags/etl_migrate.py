from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, K8sContext, batch_ids, env, es_url
from lib.groups.ingest_batch import IngestBatch
from lib.groups.ingest_fhir import IngestFhir
from lib.groups.qa import qa
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack

with DAG(
    dag_id='etl_migrate',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', enum=['', 'blue', 'green']),
        'snv': Param('no', enum=['yes', 'no']),
        'snv_somatic_tumor_only': Param('no', enum=['yes', 'no']),
        'cnv': Param('no', enum=['yes', 'no']),
        'cnv_somatic_tumor_only': Param('no', enum=['yes', 'no']),
        'variants': Param('no', enum=['yes', 'no']),
        'consequences': Param('no', enum=['yes', 'no']),
        'exomiser': Param('no', enum=['yes', 'no']),
        'coverage_by_gene': Param('no', enum=['yes', 'no']),
        'franklin': Param('no', enum=['yes', 'no']),
        'spark_jar': Param('', type='string'),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=4
) as dag:

    def formatSkipCondition(param: str) -> str:
        return '{% if params.'+param+' == "yes" %}{% else %}yes{% endif %}'

    def spark_jar() -> str:
        return '{{ params.spark_jar }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'
    
    def skip_snv() -> str:
        return formatSkipCondition('snv')

    def skip_snv_somatic_tumor_only() -> str:
        return formatSkipCondition('snv_somatic_tumor_only')

    def skip_cnv() -> str:
        return formatSkipCondition('cnv')

    def skip_cnv_somatic_tumor_only() -> str:
        return formatSkipCondition('cnv_somatic_tumor_only')

    def skip_variants() -> str:
        return formatSkipCondition('variants')

    def skip_consequences() -> str:
        return formatSkipCondition('consequences')

    def skip_exomiser() -> str:
        return formatSkipCondition('exomiser')

    def skip_coverage_by_gene() -> str:
        return formatSkipCondition('coverage_by_gene')

    def skip_franklin() -> str:
        return formatSkipCondition('franklin')

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
        on_execute_callback=Slack.notify_dag_start,
    )

    allDags = IngestFhir(
        group_id='fhir',
        batch_id='',
        color=color(),
        skip_import='yes',  # always skip import, not the purpose of that dag
        skip_batch='', # we want to do fhir normalized once
        spark_jar=spark_jar(),
    )

    def migrateBatchId(id):
        return IngestBatch(
            group_id='ingest',
            batch_id=id,
            skip_snv=skip_snv(),
            skip_snv_somatic_tumor_only=skip_snv_somatic_tumor_only(),
            skip_cnv=skip_cnv(),
            skip_cnv_somatic_tumor_only=skip_cnv_somatic_tumor_only(),
            skip_variants=skip_variants(),
            skip_consequences=skip_consequences(),
            skip_exomiser=skip_exomiser(),
            skip_coverage_by_gene=skip_coverage_by_gene(),
            skip_franklin=skip_franklin(),
            spark_jar=spark_jar(),
            batch_id_as_tag=True
        )
    
    # concat every dags inside a loop
    for id in batch_ids:
        batch = migrateBatchId(id)
        allDags >> batch
        allDags = batch

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> allDags >> slack
