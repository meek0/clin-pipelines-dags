from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib.config import env, es_url, Env, K8sContext, indexer_context
from lib.groups.qa import qa
from lib.groups.ingest import ingest
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack


with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id':  Param('', type='string'),
        'release_id': Param('', type='string'),
        'color': Param('', enum=['', 'blue', 'green']),
        'import': Param('yes', enum=['yes', 'no']),
        'notify': Param('no', enum=['yes', 'no']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def release_id() -> str:
        return '{{ params.release_id }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def skip_import() -> str:
        return '{% if params.batch_id|length and params.import == "yes" %}{% else %}yes{% endif %}'

    def skip_batch() -> str:
        return '{% if params.batch_id|length %}{% else %}yes{% endif %}'

    def default_or_initial() -> str:
        return '{% if params.batch_id|length and params.import == "yes" %}default{% else %}initial{% endif %}'

    def skip_notify() -> str:
        return '{% if params.batch_id|length and params.notify == "yes" %}{% else %}yes{% endif %}'

    def _params_validate(release_id, color):
        if release_id == '':
            raise AirflowFailException('DAG param "release_id" is required')
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
        op_args=[release_id(), color()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    ingest = ingest(
        group_id='ingest',
        batch_id=batch_id(),
        color=color(),
        skip_import=skip_import(),
        skip_batch=skip_batch(),
    )

    with TaskGroup(group_id='enrich') as enrich:
        snv = SparkOperator(
            task_id='snv',
            name='etl-enrich-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', default_or_initial(), 'snv',
            ],
        )

        variants = SparkOperator(
            task_id='variants',
            name='etl-enrich-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', default_or_initial(), 'variants',
            ],
        )

        consequences = SparkOperator(
            task_id='consequences',
            name='etl-enrich-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', default_or_initial(), 'consequences',
            ],
        )

        cnv = SparkOperator(
            task_id='cnv',
            name='etl-enrich-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', default_or_initial(), 'cnv',
            ],
        )

        snv >> variants >> consequences >> cnv

    with TaskGroup(group_id='prepare') as prepare:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-prepare-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'gene_centric', release_id(),
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-prepare-gene-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'gene_suggestions', release_id(),
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-prepare-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'variant_centric', release_id(),
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-prepare-variant-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'variant_suggestions', release_id(),
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-prepare-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'cnv_centric', release_id(),
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    qa = qa(
        group_id='qa',
        release_id=release_id(),
    )

    with TaskGroup(group_id='index') as index:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-index-gene-centric',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_gene_centric',
                release_id(),
                'gene_centric_template.json',
                'gene_centric',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-index-gene-suggestions',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_gene_suggestions',
                release_id(),
                'gene_suggestions_template.json',
                'gene_suggestions',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-index-variant-centric',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_variant_centric',
                release_id(),
                'variant_centric_template.json',
                'variant_centric',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-index-variant-suggestions',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_variant_suggestions',
                release_id(),
                'variant_suggestions_template.json',
                'variant_suggestions',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-index-cnv-centric',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_cnv_centric',
                release_id(),
                'cnv_centric_template.json',
                'cnv_centric',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    with TaskGroup(group_id='publish') as publish:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-publish-gene-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_gene_centric',
                release_id(),
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-publish-gene-suggestions',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_gene_suggestions',
                release_id(),
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-publish-variant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_variant_centric',
                release_id(),
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-publish-variant-suggestions',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_variant_suggestions',
                release_id(),
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-publish-cnv-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_cnv_centric',
                release_id(),
            ],
        )

        arranger_remove_project = ArrangerOperator(
            task_id='arranger_remove_project',
            name='etl-publish-arranger-remove-project',
            k8s_context=K8sContext.DEFAULT,
            cmds=[
                'node',
                '--experimental-modules=node',
                '--es-module-specifier-resolution=node',
                'cmd/remove_project.js',
                env,
            ],
        )

        arranger_restart = K8sDeploymentRestartOperator(
            task_id='arranger_restart',
            k8s_context=K8sContext.DEFAULT,
            deployment='arranger',
            on_success_callback=Slack.notify_dag_completion,
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> arranger_remove_project >> arranger_restart

    notify = PipelineOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        skip=skip_notify(),
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier', batch_id(),
        ],
    )

    params_validate >> ingest >> enrich >> prepare >> qa >> index >> publish >> notify
