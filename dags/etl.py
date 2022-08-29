from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.etl import config
from lib.etl.config import K8sContext
from lib.etl.operators.pipeline import PipelineOperator
from lib.etl.operators.spark import SparkOperator


with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id':  Param('', type='string'),
        'release': Param('', type='string'),
        'color': Param('', enum=['', 'blue', 'green']),
    },
) as dag:

    env = config.environment

    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def release() -> str:
        return '{{ params.release }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def _params_validate(batch_id, release, color):
        if batch_id == '':
            raise AirflowFailException('DAG param "batch_id" is required')
        if release == '':
            raise AirflowFailException('DAG param "release" is required')
        if env == 'qa':
            if color == '':
                raise AirflowFailException(
                    f'DAG param "color" is required in {env} environment'
                )
        else:
            if color != '':
                raise AirflowFailException(
                    f'DAG param "color" is forbidden in {env} environment'
                )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[batch_id(), release(), color()],
        python_callable=_params_validate,
    )

    with TaskGroup(group_id='ingest') as ingest:

        file_import = PipelineOperator(
            task_id='file_import',
            name='etl-ingest-file-import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-files-import',
            color=color(),
            arguments=[
                'bio.ferlab.clin.etl.FileImport', batch_id(), 'false', 'true',
            ],
        )

        fhir_export = PipelineOperator(
            task_id='fhir_export',
            name='etl-ingest-fhir-export',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-datalake',
            color=color(),
            arguments=[
                'bio.ferlab.clin.etl.FhirExport', 'all',
            ],
        )

        fhir_normalize = SparkOperator(
            task_id='fhir_normalize',
            name='etl-ingest-fhir-normalize',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.fhir.FhirRawToNormalized',
            spark_config='raw-fhir-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'all',
            ],
        )

        vcf_snv = SparkOperator(
            task_id='vcf_snv',
            name='etl-ingest-vcf-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id(), 'snv',
            ],
        )

        vcf_cnv = SparkOperator(
            task_id='vcf_cnv',
            name='etl-ingest-vcf-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id(), 'cnv',
            ],
        )

        vcf_variants = SparkOperator(
            task_id='vcf_variants',
            name='etl-ingest-vcf-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id(), 'variants',
            ],
        )

        vcf_consequences = SparkOperator(
            task_id='vcf_consequences',
            name='etl-ingest-vcf-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id(), 'consequences',
            ],
        )

        file_import >> fhir_export >> fhir_normalize >> vcf_snv >> vcf_cnv >> vcf_variants >> vcf_consequences

    with TaskGroup(group_id='enrich') as enrich:

        variants = SparkOperator(
            task_id='variants',
            name='etl-enrich-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', 'default', 'variants',
            ],
        )

        consequences = SparkOperator(
            task_id='consequences',
            name='etl-enrich-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', 'default', 'consequences',
            ],
        )

        cnv = SparkOperator(
            task_id='cnv',
            name='etl-enrich-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{env}.conf', 'default', 'cnv',
            ],
        )

        variants >> consequences >> cnv

    with TaskGroup(group_id='prepare') as prepare:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-prepare-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'gene_centric', release(),
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-prepare-gene-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'gene_suggestions', release(),
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-prepare-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'variant_centric', release(),
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-prepare-variant-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'variant_suggestions', release(),
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-prepare-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{env}.conf', 'initial', 'cnv_centric', release(),
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    with TaskGroup(group_id='index') as index:

        gene_centric = SparkOperator(
            task_id='gene_centric',
            name='etl-index-variants',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_gene_centric',
                release(),
                'gene_centric_template.json',
                'gene_centric',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-index-suggestions',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_gene_suggestion',
                release(),
                'gene_suggestions_template.json',
                'gene_suggestions',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-index-variant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_variant_centric',
                release(),
                'variant_centric_template.json',
                'variant_centric',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-index-variant-suggestions',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_variant_suggestions',
                release(),
                'variant_suggestions_template.json',
                'variant_suggestions',
                '1900-01-01 00:00:00',
                f'config/{env}.conf',
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-index-cnv-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_cnv_centric',
                release(),
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
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_gene_centric',
                release(),
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-publish-gene-suggestions',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_gene_suggestions',
                release(),
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-publish-variant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_variant_centric',
                release(),
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-publish-variant-suggestions',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_variant_suggestion',
                release(),
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-publish-cnv-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200', '', '',
                f'clin_{env}' + color('_') + '_cnv_centric',
                release(),
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    notify = PipelineOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier', batch_id(),
        ],
    )

    params_validate >> ingest >> enrich >> prepare >> index >> publish >> notify
