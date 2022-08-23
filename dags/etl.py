from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.etl import config
from lib.etl.config import K8sContext
from lib.etl.pipeline_task import pipeline_task
from lib.etl.spark_task import spark_task
from lib.helper import join


with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id':  Param('BATCHID', type='string', minLength=1),
        'release': Param('re_000', type='string', minLength=1),
        'color': Param('', enum=['', 'blue', 'green']),
    },
) as dag:

    environment = config.environment

    def index(name: str) -> str:
        return f'clin_{environment}_' + '{% if params.color|length %}{{ params.color }}_{% endif %}' + name

    with TaskGroup(group_id='ingest') as ingest:

        parent_id = 'ingest'

        file_import = pipeline_task(
            task_id='file_import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{environment}-app-files-import',
            color='{{ params.color }}',
            arguments=[
                'bio.ferlab.clin.etl.FileImport',
                '{{ params.batch_id }}',
                'false',
                'true',
            ],
        )

        fhir_export = pipeline_task(
            task_id='fhir_export',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{environment}-app-datalake',
            color='{{ params.color }}',
            arguments=[
                'bio.ferlab.clin.etl.FhirExport',
                'all',
            ],
        )

        fhir_normalize = spark_task(
            group_id='fhir_normalize',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.fhir.FhirRawToNormalized',
            spark_config='raw-fhir-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'all',
            ],
        )

        vcf_snv = spark_task(
            group_id='vcf_snv',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                '{{ params.batch_id }}',
                'snv',
            ],
        )

        vcf_cnv = spark_task(
            group_id='vcf_cnv',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                '{{ params.batch_id }}',
                'cnv',
            ],
        )

        vcf_variants = spark_task(
            group_id='vcf_variants',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                '{{ params.batch_id }}',
                'variants',
            ],
        )

        vcf_consequences = spark_task(
            group_id='vcf_consequences',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                '{{ params.batch_id }}',
                'consequences',
            ],
        )

        external_panels = spark_task(
            group_id='external_panels',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'panels',
            ],
        )

        external_mane_summary = spark_task(
            group_id='external_mane_summary',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'mane-summary',
            ],
        )

        external_refseq_annotation = spark_task(
            group_id='external_refseq_annotation',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'refseq-annotation',
            ],
        )

        external_refseq_feature = spark_task(
            group_id='external_refseq_feature',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'refseq-feature',
            ],
        )

        # varsome = spark_task(
        #     group_id='varsome',
        #     parent_id=parent_id,
        #     k8s_context=K8sContext.ETL,
        #     spark_class='bio.ferlab.clin.etl.varsome.Varsome',
        #     spark_config='varsome-etl',
        #     spark_secret='varsome',
        #     arguments=[
        #         f'config/{environment}.conf',
        #         'initial',
        #         'all',
        #     ],
        # )

        gene_tables = spark_task(
            group_id='gene_tables',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.CreateGenesTable',
            spark_config='genes-tables-creation',
            arguments=[
                f'config/{environment}.conf',
                'initial',
            ],
        )

        # public_tables = spark_task(
        #     group_id='public_tables',
        #     parent_id=parent_id,
        #     k8s_context=K8sContext.ETL,
        #     spark_class='bio.ferlab.clin.etl.external.CreatePublicTables',
        #     spark_config='public-tables-creation-etl',
        #     arguments=[
        #         f'config/{environment}.conf',
        #         'initial',
        #     ],
        # )

        file_import >> fhir_export >> fhir_normalize >> vcf_snv >> vcf_cnv >> vcf_variants >> vcf_consequences >> external_panels >> external_mane_summary >> external_refseq_annotation >> external_refseq_feature >> gene_tables

    with TaskGroup(group_id='enrich') as enrich:

        parent_id = 'enrich'

        variants = spark_task(
            group_id='variants',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                'variants',
            ],
        )

        consequences = spark_task(
            group_id='consequences',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                'consequences',
            ],
        )

        cnv = spark_task(
            group_id='cnv',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                'cnv',
            ],
        )

        variants >> consequences >> cnv

    with TaskGroup(group_id='prepare') as prepare:

        parent_id = 'prepare'

        gene_centric = spark_task(
            group_id='gene_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'gene_centric',
                '{{ params.release }}',
            ],
        )

        gene_suggestions = spark_task(
            group_id='gene_suggestions',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'gene_suggestions',
                '{{ params.release }}',
            ],
        )

        variant_centric = spark_task(
            group_id='variant_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'variant_centric',
                '{{ params.release }}',
            ],
        )

        variant_suggestions = spark_task(
            group_id='variant_suggestions',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'variant_suggestions',
                '{{ params.release }}',
            ],
        )

        cnv_centric = spark_task(
            group_id='cnv_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'cnv_centric',
                '{{ params.release }}',
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    with TaskGroup(group_id='index') as index:

        parent_id = 'index'

        gene_centric = spark_task(
            group_id='gene_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('gene_centric'),
                '{{ params.release }}',
                'gene_centric_template.json',
                'gene_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        gene_suggestions = spark_task(
            group_id='gene_suggestions',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('gene_suggestion'),
                '{{ params.release }}',
                'gene_suggestions_template.json',
                'gene_suggestions',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        variant_centric = spark_task(
            group_id='variant_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('variant_centric'),
                '{{ params.release }}',
                'variant_centric_template.json',
                'variant_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        variant_suggestions = spark_task(
            group_id='variant_suggestions',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('variant_suggestions'),
                '{{ params.release }}',
                'variant_suggestions_template.json',
                'variant_suggestions',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        cnv_centric = spark_task(
            group_id='cnv_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('cnv_centric'),
                '{{ params.release }}',
                'cnv_centric_template.json',
                'cnv_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    with TaskGroup(group_id='publish') as publish:

        parent_id = 'publish'

        gene_centric = spark_task(
            group_id='gene_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('gene_centric'),
                '{{ params.release }}',
            ],
        )

        gene_suggestions = spark_task(
            group_id='gene_suggestions',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('gene_suggestions'),
                '{{ params.release }}',
            ],
        )

        variant_centric = spark_task(
            group_id='variant_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('variant_centric'),
                '{{ params.release }}',
            ],
        )

        variant_suggestions = spark_task(
            group_id='variant_suggestions',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('variant_suggestion'),
                '{{ params.release }}',
            ],
        )

        cnv_centric = spark_task(
            group_id='cnv_centric',
            parent_id=parent_id,
            k8s_context=K8sContext.DEFAULT,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                index('cnv_centric'),
                '{{ params.release }}',
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    notify = pipeline_task(
        task_id='notify',
        k8s_context=K8sContext.DEFAULT,
        color='{{ params.color }}',
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier',
            '{{ params.batch_id }}',
        ],
    )

    ingest >> enrich >> prepare >> index >> publish >> notify
