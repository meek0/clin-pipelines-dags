from airflow.utils.task_group import TaskGroup
from lib.etl.config import K8sContext
from lib.etl.spark_task import spark_task
from lib.helper import task_id


def etl_task(
    group_id: str,
    parent_id: str,
    environment: str,
    k8s_namespace: str,
    k8s_context: K8sContext,
    k8s_service_account: str,
    spark_image: str,
    spark_jar: str,
    batch_id: str,
    release: str,
    color: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as etl_task_group:

        step_3 = spark_task(
            group_id='fhir',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_1 = spark_task(
            group_id='vcf_snv',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_2 = spark_task(
            group_id='vcf_cnv',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_3 = spark_task(
            group_id='vcf_variants',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_4 = spark_task(
            group_id='vcf_consequences',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_5 = spark_task(
            group_id='external_panels',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_6 = spark_task(
            group_id='external_mane_summary',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_7 = spark_task(
            group_id='external_refseq_annotation',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_8 = spark_task(
            group_id='external_refseq_feature',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_9 = spark_task(
            group_id='varsome',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_10 = spark_task(
            group_id='gene_tables',
            parent_id=task_id([parent_id, group_id]),
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

        step_4_11 = spark_task(
            group_id='public_tables',
            parent_id=task_id([parent_id, group_id]),
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

        step_5_1 = spark_task(
            group_id='enriched_variants',
            parent_id=task_id([parent_id, group_id]),
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

        step_5_2 = spark_task(
            group_id='enriched_consequences',
            parent_id=task_id([parent_id, group_id]),
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

        step_5_3 = spark_task(
            group_id='enriched_cnv',
            parent_id=task_id([parent_id, group_id]),
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

        step_6_1 = spark_task(
            group_id='prepare_gene_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_6_2 = spark_task(
            group_id='prepare_gene_suggestions',
            parent_id=task_id([parent_id, group_id]),
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

        step_6_3 = spark_task(
            group_id='prepare_variant_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_6_4 = spark_task(
            group_id='prepare_variant_suggestions',
            parent_id=task_id([parent_id, group_id]),
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

        step_6_5 = spark_task(
            group_id='prepare_cnv_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_7_1 = spark_task(
            group_id='index_gene_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_7_2 = spark_task(
            group_id='index_gene_suggestions',
            parent_id=task_id([parent_id, group_id]),
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

        step_7_3 = spark_task(
            group_id='index_variant_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_7_4 = spark_task(
            group_id='index_variant_suggestions',
            parent_id=task_id([parent_id, group_id]),
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

        step_7_5 = spark_task(
            group_id='index_cnv_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_8_1 = spark_task(
            group_id='publish_gene_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_8_2 = spark_task(
            group_id='publish_gene_suggestions',
            parent_id=task_id([parent_id, group_id]),
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

        step_8_3 = spark_task(
            group_id='publish_variant_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_8_4 = spark_task(
            group_id='publish_variant_suggestions',
            parent_id=task_id([parent_id, group_id]),
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

        step_8_5 = spark_task(
            group_id='publish_cnv_centric',
            parent_id=task_id([parent_id, group_id]),
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

        step_3 >> step_4_1 >> step_4_2 >> step_4_3 >> step_4_4 >> step_4_5 >> step_4_6 >> step_4_7 >> step_4_8 >> step_4_9 >> step_4_10 >> step_4_11 >> step_5_1 >> step_5_2 >> step_5_3 >> step_6_1 >> step_6_2 >> step_6_3 >> step_6_4 >> step_6_5 >> step_7_1 >> step_7_2 >> step_7_3 >> step_7_4 >> step_7_5 >> step_8_1 >> step_8_2 >> step_8_3 >> step_8_4 >> step_8_5

    return etl_task_group
