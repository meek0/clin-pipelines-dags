from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.etl import config
from lib.etl.spark_task import spark_task


with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    environment = config.environment
    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context
    k8s_service_account = config.k8s_service_account
    spark_image = config.spark_image
    spark_jar = config.spark_jar
    batch_id = config.batch_id
    release = config.release
    color = config.color

    with TaskGroup(group_id='ingest') as ingest:

        parent_id = 'ingest'

        fhir = spark_task(
            group_id='fhir',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            # bio.ferlab.clin.etl.fhir.FhirRawToNormalized
            spark_class='bio.ferlab.clin.etl.fail.Fail',
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
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                batch_id,
                'snv',
            ],
        )

        vcf_cnv = spark_task(
            group_id='vcf_cnv',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                batch_id,
                'cnv',
            ],
        )

        vcf_variants = spark_task(
            group_id='vcf_variants',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                batch_id,
                'variants',
            ],
        )

        vcf_consequences = spark_task(
            group_id='vcf_consequences',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                batch_id,
                'consequences',
            ],
        )

        external_panels = spark_task(
            group_id='external_panels',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
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
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
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
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
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
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.external.ImportExternal',
            spark_config='raw-import-external-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'refseq-feature',
            ],
        )

        varsome = spark_task(
            group_id='varsome',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.varsome.Varsome',
            spark_config='varsome-etl',
            spark_secret='varsome',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'all',
            ],
        )

        gene_tables = spark_task(
            group_id='gene_tables',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.external.CreateGenesTable',
            spark_config='genes-tables-creation',
            arguments=[
                f'config/{environment}.conf',
                'initial',
            ],
        )

        public_tables = spark_task(
            group_id='public_tables',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.external.CreatePublicTables',
            spark_config='public-tables-creation-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
            ],
        )

        fhir >> vcf_snv >> vcf_cnv >> vcf_variants >> vcf_consequences >> external_panels >> external_mane_summary >> external_refseq_annotation >> external_refseq_feature >> varsome >> gene_tables >> public_tables

    with TaskGroup(group_id='enrich') as enrich:

        parent_id = 'enrich'

        enriched_variants = spark_task(
            group_id='enriched_variants',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                'variants',
            ],
        )

        enriched_consequences = spark_task(
            group_id='enriched_consequences',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                'consequences',
            ],
        )

        enriched_cnv = spark_task(
            group_id='enriched_cnv',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config='enriched-etl',
            arguments=[
                f'config/{environment}.conf',
                'default',
                'cnv',
            ],
        )

        enriched_variants >> enriched_consequences >> enriched_cnv

    with TaskGroup(group_id='prepare') as prepare:

        parent_id = 'prepare'

        prepare_gene_centric = spark_task(
            group_id='prepare_gene_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'gene_centric',
                release,
            ],
        )

        prepare_gene_suggestions = spark_task(
            group_id='prepare_gene_suggestions',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'gene_suggestions',
                release,
            ],
        )

        prepare_variant_centric = spark_task(
            group_id='prepare_variant_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'variant_centric',
                release,
            ],
        )

        prepare_variant_suggestions = spark_task(
            group_id='prepare_variant_suggestions',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'variant_suggestions',
                release,
            ],
        )

        prepare_cnv_centric = spark_task(
            group_id='prepare_cnv_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
            spark_config='prepare-index-etl',
            arguments=[
                f'config/{environment}.conf',
                'initial',
                'cnv_centric',
                release,
            ],
        )

        prepare_gene_centric >> prepare_gene_suggestions >> prepare_variant_centric >> prepare_variant_suggestions >> prepare_cnv_centric

    with TaskGroup(group_id='index') as index:

        parent_id = 'index'

        index_gene_centric = spark_task(
            group_id='index_gene_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.qa,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_gene_centric',
                release,
                'gene_centric_template.json',
                'gene_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        index_gene_suggestions = spark_task(
            group_id='index_gene_suggestions',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.qa,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_gene_suggestions',
                release,
                'gene_suggestions_template.json',
                'gene_suggestions',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        index_variant_centric = spark_task(
            group_id='index_variant_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.qa,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_variant_centric',
                release,
                'variant_centric_template.json',
                'variant_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        index_variant_suggestions = spark_task(
            group_id='index_variant_suggestions',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.qa,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_variant_suggestions',
                release,
                'variant_suggestions_template.json',
                'variant_suggestions',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        index_cnv_centric = spark_task(
            group_id='index_cnv_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.qa,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Indexer',
            spark_config='index-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_cnv_centric',
                release,
                'cnv_centric_template.json',
                'cnv_centric',
                '1900-01-01 00:00:00',
                f'config/{environment}.conf',
            ],
        )

        index_gene_centric >> index_gene_suggestions >> index_variant_centric >> index_variant_suggestions >> index_cnv_centric

    with TaskGroup(group_id='publish') as publish:

        parent_id = 'publish'

        publish_gene_centric = spark_task(
            group_id='publish_gene_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_gene_centric',
                release
            ],
        )

        publish_gene_suggestions = spark_task(
            group_id='publish_gene_suggestions',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_gene_suggestions',
                release
            ],
        )

        publish_variant_centric = spark_task(
            group_id='publish_variant_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_variant_centric',
                release
            ],
        )

        publish_variant_suggestions = spark_task(
            group_id='publish_variant_suggestions',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_variant_suggestions',
                release
            ],
        )

        publish_cnv_centric = spark_task(
            group_id='publish_cnv_centric',
            parent_id=parent_id,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.es.Publish',
            spark_config='publish-elasticsearch-etl',
            arguments=[
                'http://elasticsearch:9200',
                '',
                '',
                f'clin_qa_{color}_cnv_centric',
                release
            ],
        )

        publish_gene_centric >> publish_gene_suggestions >> publish_variant_centric >> publish_variant_suggestions >> publish_cnv_centric

    ingest >> enrich >> prepare >> index >> publish
