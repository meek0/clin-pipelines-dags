from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import env, es_url, Env, K8sContext, indexer_context, config_file
from lib.groups.ingest_batch import IngestBatch
from lib.groups.ingest_fhir import IngestFhir
from lib.groups.qa import qa
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
        'spark_jar': Param('', type='string'),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    concurrency=4
) as dag:

    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def release_id() -> str:
        return '{{ params.release_id }}'

    def spark_jar() -> str:
        return '{{ params.spark_jar }}'

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

    ingest_fhir = IngestFhir(
        group_id='fhir',
        batch_id=batch_id(),
        color=color(),
        skip_import=skip_import(),
        skip_batch=skip_batch(),
        spark_jar=spark_jar(),
    )

    ingest_batch = IngestBatch(
        group_id='ingest',
        batch_id=batch_id(),
        skip_snv=skip_batch(),
        skip_snv_somatic_tumor_only=skip_batch(),
        skip_cnv=skip_batch(),
        skip_cnv_somatic_tumor_only=skip_batch(),
        skip_variants=skip_batch(),
        skip_consequences=skip_batch(),
        skip_exomiser=skip_batch(),
        skip_coverage_by_gene=skip_batch(),
        spark_jar=spark_jar(),
    )

    with TaskGroup(group_id='enrich') as enrich:
        snv = SparkOperator(
            task_id='snv',
            name='etl-enrich-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[
                'snv',
                '--config', config_file,
                '--steps', default_or_initial(),
                '--app-name', 'etl_enrich_snv',
            ],
        )

        snv_somatic_tumor_only = SparkOperator(
            task_id='snv_somatic_tumor_only',
            name='etl-enrich-snv-somatic',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[
                'snv_somatic_tumor_only',
                '--config', config_file,
                '--steps', default_or_initial(),
                '--app-name', 'etl_enrich_snv_somatic',
            ],
        )

        variants = SparkOperator(
            task_id='variants',
            name='etl-enrich-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[
                'variants',
                '--config', config_file,
                '--steps', default_or_initial(),
                '--app-name', 'etl_enrich_variants',
            ],
        )

        consequences = SparkOperator(
            task_id='consequences',
            name='etl-enrich-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[
                'consequences',
                '--config', config_file,
                '--steps', default_or_initial(),
                '--app-name', 'etl_enrich_consequences',
            ],
        )

        cnv = SparkOperator(
            task_id='cnv',
            name='etl-enrich-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[
                'cnv',
                '--config', config_file,
                '--steps', default_or_initial(),
                '--app-name', 'etl_enrich_cnv',
            ],
        )

        coverage_by_gene = SparkOperator(
            task_id='coverage_by_gene',
            name='etl-enrich-coverage-by-gene',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            spark_jar=spark_jar(),
            arguments=[
                'coverage_by_gene',
                '--config', config_file,
                '--steps', default_or_initial(),
                '--app-name', 'etl_enrich_coverage_by_gene',
            ],
        )

        snv >> snv_somatic_tumor_only >> variants >> consequences >> cnv >> coverage_by_gene

    with TaskGroup(group_id='prepare') as prepare:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-prepare-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            spark_jar=spark_jar(),
            arguments=[
                'gene_centric',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_prepare_gene_centric',
                '--releaseId', release_id()
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-prepare-gene-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            spark_jar=spark_jar(),
            arguments=[
                'gene_suggestions',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_prepare_gene_suggestions',
                '--releaseId', release_id()
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-prepare-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            spark_jar=spark_jar(),
            arguments=[
                'variant_centric',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_prepare_variant_centric',
                '--releaseId', release_id()
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-prepare-variant-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            spark_jar=spark_jar(),
            arguments=[
                'variant_suggestions',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_prepare_variant_suggestions',
                '--releaseId', release_id()
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-prepare-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            spark_jar=spark_jar(),
            arguments=[
                'cnv_centric',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_prepare_cnv_centric',
                '--releaseId', release_id()
            ],
        )

        coverage_by_gene_centric = SparkOperator(
            task_id='coverage_by_gene',
            name='etl-prepare-coverage-by-gene',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            spark_jar=spark_jar(),
            arguments=[
                'coverage_by_gene_centric',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_prepare_coverage_by_gene',
                '--releaseId', release_id()
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> coverage_by_gene_centric

    qa = qa(
        group_id='qa',
        release_id=release_id(),
        spark_jar=spark_jar(),
    )

    with TaskGroup(group_id='index') as index:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-index-gene-centric',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            spark_jar=spark_jar(),
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
            spark_jar=spark_jar(),
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
            spark_jar=spark_jar(),
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
            spark_jar=spark_jar(),
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
            spark_jar=spark_jar(),
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

        coverage_by_gene_centric = SparkOperator(
            task_id='coverage_by_gene_centric',
            name='etl-index-coverage-by-gene',
            k8s_context=indexer_context,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            spark_jar=spark_jar(),
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_coverage_by_gene_centric',
                release_id(),
                'coverage_by_gene_centric_template.json',
                'coverage_by_gene_centric',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> coverage_by_gene_centric

    with TaskGroup(group_id='publish') as publish:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-publish-gene-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            spark_jar=spark_jar(),
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
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
            spark_jar=spark_jar(),
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
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
            spark_jar=spark_jar(),
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
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
            spark_jar=spark_jar(),
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
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
            spark_jar=spark_jar(),
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_cnv_centric',
                release_id(),
            ],
        )

        coverage_by_gene_centric = SparkOperator(
            task_id='coverage_by_gene_centric',
            name='etl-publish-cnv-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            spark_jar=spark_jar(),
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
            arguments=[
                es_url, '', '',
                f'clin_{env}' + color('_') + '_coverage_by_gene_centric',
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

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> coverage_by_gene_centric >> arranger_remove_project >> arranger_restart

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

    params_validate >> ingest_fhir >> ingest_batch >> enrich >> prepare >> qa >> index >> publish >> notify
