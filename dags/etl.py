from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.etl import config
from lib.etl.aws_task import aws_task
from lib.etl.config import K8sContext
from lib.etl.curl_task import curl_task
from lib.etl.pipeline_task import pipeline_task
from lib.etl.postgres_task import postgres_task
from lib.etl.spark_task import spark_task
from lib.k8s import k8s_deployment_pause, k8s_deployment_resume, k8s_deployment_restart


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

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def color_k8s_resource(name: str) -> str:
        return name + '{% if params.color|length %}-{{ params.color }}{% endif %}'

    def color_es_index(name: str) -> str:
        return f'clin_{environment}_' + '{% if params.color|length %}{{ params.color }}_{% endif %}' + name

    with TaskGroup(group_id='cleanup') as cleanup:

        fhir_pause = k8s_deployment_pause(
            task_id='fhir_pause',
            deployment=color_k8s_resource('fhir-server'),
        )

        fhir_resume = k8s_deployment_resume(
            task_id='fhir_resume',
            deployment=color_k8s_resource('fhir-server'),
        )

        db_tables_delete = postgres_task(
            task_id='db_tables_delete',
            k8s_context=K8sContext.DEFAULT,
            dash_color=color('-'),
            cmds=[
                'psql', '-d', 'fhir' + color('_'), '-c',
                '''
                DO $$$DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END$$$;
                ''',
            ],
        )

        fhir_restart = k8s_deployment_restart(
            task_id='fhir_restart',
            deployment=color_k8s_resource('fhir-server'),
        )

        es_indices_delete = curl_task(
            task_id='es_indices_delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '-f', '-X', 'DELETE',
                f'http://elasticsearch:9200/clin-{environment}-prescriptions' + color('-') +
                f',clin-{environment}-patients' + color('-') +
                f',clin-{environment}-analyses' + color('-') +
                f',clin-{environment}-sequencings' + color('-') +
                '?ignore_unavailable=true',
            ],
        )

        s3_download_delete = aws_task(
            task_id='s3_download_delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{environment}-app-download' + color('/') + '/',
                '--recursive',
            ],
        )

        s3_datalake_delete = aws_task(
            task_id='s3_datalake_delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                's3://cqgc-qa-app-datalake/', '--recursive', '--exclude', '"*"',
                '--include', '"normalized/*"',
                '--include', '"enriched/*"',
                '--include', '"raw/landing/fhir/*"',
                '--include', '"es_index/*"',
            ],
        )

        wait_2_min = BashOperator(
            task_id='wait_2_min',
            bash_command='sleep 120',
        )

        fhir_pause >> db_tables_delete >> fhir_resume >> fhir_restart >> es_indices_delete >> s3_download_delete >> s3_datalake_delete >> wait_2_min

    with TaskGroup(group_id='ingest') as ingest:

        parent_id = 'ingest'

        file_import = pipeline_task(
            task_id='file_import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{environment}-app-files-import',
            color=color(),
            dash_color=color('-'),
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
            color=color(),
            dash_color=color('-'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('gene_centric'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('gene_suggestion'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('variant_centric'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('variant_suggestions'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('cnv_centric'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('gene_centric'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('gene_suggestions'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('variant_centric'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('variant_suggestion'),
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
                'http://elasticsearch:9200', '', '',
                color_es_index('cnv_centric'),
                '{{ params.release }}',
            ],
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric

    notify = pipeline_task(
        task_id='notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        dash_color=color('-'),
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier',
            '{{ params.batch_id }}',
        ],
    )

    cleanup >> ingest >> enrich >> prepare >> index >> publish >> notify
