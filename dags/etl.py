from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.etl import config
from lib.etl.aws import AwsOperator
from lib.etl.config import K8sContext
from lib.etl.curl import CurlOperator
from lib.etl.fhir import FhirOperator
from lib.etl.fhir_csv import FhirCsvOperator
from lib.etl.pipeline import PipelineOperator
from lib.etl.postgres import PostgresOperator
from lib.etl.spark import SparkOperator
from lib.etl.wait import WaitOperator
from lib.k8s import K8sDeploymentPauseOperator, K8sDeploymentResumeOperator, K8sDeploymentRestartOperator


with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id':  Param('201106_A00516_0169_AHFM3HDSXY', type='string', minLength=1),
        'release': Param('re_000', type='string', minLength=1),
        'color': Param('green', enum=['', 'blue', 'green']),
    },
) as dag:

    environment = config.environment

    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def release() -> str:
        return '{{ params.release }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    with TaskGroup(group_id='cleanup') as cleanup:

        fhir_pause = K8sDeploymentPauseOperator(
            task_id='fhir_pause',
            deployment='fhir-server' + color('-'),
        )

        db_tables_delete = PostgresOperator(
            task_id='db_tables_delete',
            name='etl-cleanup-db-tables-delete',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
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

        fhir_resume = K8sDeploymentResumeOperator(
            task_id='fhir_resume',
            deployment='fhir-server' + color('-'),
        )

        fhir_restart = K8sDeploymentRestartOperator(
            task_id='fhir_restart',
            deployment='fhir-server' + color('-'),
        )

        es_indices_delete = CurlOperator(
            task_id='es_indices_delete',
            name='etl-cleanup-es-indices-delete',
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

        s3_download_delete = AwsOperator(
            task_id='s3_download_delete',
            name='etl-cleanup-s3-download-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{environment}-app-download' + color('/') + '/',
                '--recursive',
            ],
        )

        s3_datalake_delete = AwsOperator(
            task_id='s3_datalake_delete',
            name='etl-cleanup-s3-datalake-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{environment}-app-datalake/', '--recursive', '--exclude', '"*"',
                '--include', '"normalized/*"',
                '--include', '"enriched/*"',
                '--include', '"raw/landing/fhir/*"',
                '--include', '"es_index/*"',
            ],
        )

        wait = WaitOperator(
            task_id='wait',
            time='120',
        )

        fhir_pause >> db_tables_delete >> fhir_resume >> fhir_restart >> es_indices_delete >> s3_download_delete >> s3_datalake_delete >> wait

    with TaskGroup(group_id='fhir_init') as fhir_init:

        ig_publish = FhirOperator(
            task_id='ig_publish',
            name='etl-fhir-init-ig-publish',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
        )

        wait = WaitOperator(
            task_id='wait',
            time='20',
        )

        csv_import = FhirCsvOperator(
            task_id='csv_import',
            name='etl-fhir-init-csv-import',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
            arguments=['-f', 'nanuq.yml'],
        )

        ig_publish >> wait >> csv_import

    with TaskGroup(group_id='ingest') as ingest:

        file_import = PipelineOperator(
            task_id='file_import',
            name='etl-ingest-file-import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{environment}-app-files-import',
            color=color(),
            arguments=[
                'bio.ferlab.clin.etl.FileImport', batch_id(), 'false', 'true',
            ],
        )

        fhir_export = PipelineOperator(
            task_id='fhir_export',
            name='etl-ingest-fhir-export',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{environment}-app-datalake',
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
                f'config/{environment}.conf', 'initial', 'all',
            ],
        )

        vcf_snv = SparkOperator(
            task_id='vcf_snv',
            name='etl-ingest-vcf-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf', 'default', batch_id(), 'snv',
            ],
        )

        vcf_cnv = SparkOperator(
            task_id='vcf_cnv',
            name='etl-ingest-vcf-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf', 'default', batch_id(), 'cnv',
            ],
        )

        vcf_variants = SparkOperator(
            task_id='vcf_variants',
            name='etl-ingest-vcf-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf', 'default', batch_id(), 'variants',
            ],
        )

        vcf_consequences = SparkOperator(
            task_id='vcf_consequences',
            name='etl-ingest-vcf-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf', 'default', batch_id(), 'consequences',
            ],
        )

        external_panels = SparkOperator(
            task_id='external_panels',
            name='etl-ingest-external-panels',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'panels',
            ],
        )

        external_mane_summary = SparkOperator(
            task_id='external_mane_summary',
            name='etl-ingest-external-mane-summary',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'mane-summary',
            ],
        )

        external_refseq_annotation = SparkOperator(
            task_id='external_refseq_annotation',
            name='etl-ingest-external-refseq-annotation',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'refseq-annotation',
            ],
        )

        external_refseq_feature = SparkOperator(
            task_id='external_refseq_feature',
            name='etl-ingest-external-refseq-feature',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'refseq-feature',
            ],
        )

        # varsome = SparkOperator(
        #     task_id='varsome',
        #     name='etl-ingest-varsome',
        #     k8s_context=K8sContext.ETL,
        #     spark_class='bio.ferlab.clin.etl.varsome.Varsome',
        #     spark_config='varsome-etl',
        #     spark_secret='varsome',
        #     arguments=[
        #         f'config/{environment}.conf', 'initial', 'all',
        #     ],
        # )

        gene_tables = SparkOperator(
            task_id='gene_tables',
            name='etl-ingest-gene-tables',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.external.CreateGenesTable',
            spark_config='genes-tables-creation',
            arguments=[
                f'config/{environment}.conf', 'initial',
            ],
        )

        # public_tables = SparkOperator(
        #     task_id='public_tables',
        #     name='etl-ingest-public-tables',
        #     k8s_context=K8sContext.ETL,
        #     spark_class='bio.ferlab.clin.etl.external.CreatePublicTables',
        #     spark_config='public-tables-creation-etl',
        #     arguments=[
        #         f'config/{environment}.conf', 'initial',
        #     ],
        # )

        file_import >> fhir_export >> fhir_normalize >> vcf_snv >> vcf_cnv >> vcf_variants >> vcf_consequences >> external_panels >> external_mane_summary >> external_refseq_annotation >> external_refseq_feature >> gene_tables

    with TaskGroup(group_id='enrich') as enrich:

        variants = SparkOperator(
            task_id='variants',
            name='etl-enrich-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf', 'default', 'variants',
            ],
        )

        consequences = SparkOperator(
            task_id='consequences',
            name='etl-enrich-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf', 'default', 'consequences',
            ],
        )

        cnv = SparkOperator(
            task_id='cnv',
            name='etl-enrich-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf', 'default', 'cnv',
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
                f'config/{environment}.conf', 'initial', 'gene_centric', release(),
            ],
        )

        gene_suggestions = SparkOperator(
            task_id='gene_suggestions',
            name='etl-prepare-gene-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'gene_suggestions', release(),
            ],
        )

        variant_centric = SparkOperator(
            task_id='variant_centric',
            name='etl-prepare-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'variant_centric', release(),
            ],
        )

        variant_suggestions = SparkOperator(
            task_id='variant_suggestions',
            name='etl-prepare-variant-suggestions',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'variant_suggestions', release(),
            ],
        )

        cnv_centric = SparkOperator(
            task_id='cnv_centric',
            name='etl-prepare-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf', 'initial', 'cnv_centric', release(),
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
                f'clin_{environment}' + color('_') + '_gene_centric',
                release(),
                'gene_centric_template.json',
                'gene_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
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
                f'clin_{environment}' + color('_') + '_gene_suggestion',
                release(),
                'gene_suggestions_template.json',
                'gene_suggestions',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
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
                f'clin_{environment}' + color('_') + '_variant_centric',
                release(),
                'variant_centric_template.json',
                'variant_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
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
                f'clin_{environment}' + color('_') + '_variant_suggestions',
                release(),
                'variant_suggestions_template.json',
                'variant_suggestions',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
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
                f'clin_{environment}' + color('_') + '_cnv_centric',
                release(),
                'cnv_centric_template.json',
                'cnv_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
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
                f'clin_{environment}' + color('_') + '_gene_centric',
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
                f'clin_{environment}' + color('_') + '_gene_suggestions',
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
                f'clin_{environment}' + color('_') + '_variant_centric',
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
                f'clin_{environment}' + color('_') + '_variant_suggestion',
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
                f'clin_{environment}' + color('_') + '_cnv_centric',
                release(),
            ],
        )

        arranger_restart = K8sDeploymentRestartOperator(
            task_id='arranger_restart',
            deployment='arranger',
        )

        wait = WaitOperator(
            task_id='wait',
            time='20',
        )

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> arranger_restart >> wait

    with TaskGroup(group_id='rolling') as rolling:

        es_indices_swap = CurlOperator(
            task_id='es_indices_swap',
            name='etl-rolling-es-indices-swap',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '-f', '-X', 'POST', 'http://elasticsearch:9200/_aliases',
                '-H', '"Content-Type: application/json"', '-d',
                '''
                {{
                    "actions": [
                        {{ "remove": {{ "index": "*", "alias": "clin-{env}-analyses" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin-{env}-sequencings" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_gene_centric" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_cnv_centric" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_gene_suggestions" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_variant_centric" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_variant_suggestions" }} }},
                        {{ "add": {{ "index": "clin-{env}-analyses{dash_color}", "alias": "clin-{env}-analyses" }} }},
                        {{ "add": {{ "index": "clin-{env}-sequencings{dash_color}", "alias": "clin-{env}-sequencings" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_gene_centric_{release}", "alias": "clin_{env}_gene_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_gene_suggestions_{release}", "alias": "clin_{env}_gene_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_variant_centric_{release}", "alias": "clin_{env}_variant_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_cnv_centric_{release}", "alias": "clin_{env}_cnv_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_variant_suggestions_{release}", "alias": "clin_{env}_variant_suggestions" }} }}
                    ]
                }}
                '''.format(
                    env=environment,
                    release=release(),
                    dash_color=color('-'),
                    under_color=color('_'),
                ),
            ],
        )

        arranger_restart = K8sDeploymentRestartOperator(
            task_id='arranger_restart',
            deployment='arranger',
        )

        wait = WaitOperator(
            task_id='wait',
            time='20',
        )

        es_indices_swap >> arranger_restart >> wait

    notify = PipelineOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier', batch_id(),
        ],
    )

    cleanup >> fhir_init >> ingest >> enrich >> prepare >> index >> publish >> rolling >> notify
