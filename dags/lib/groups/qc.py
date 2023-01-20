from airflow.utils.task_group import TaskGroup
from lib.config import env_url, Env, K8sContext
from lib.doc import qc as doc
from lib.operators.spark import SparkOperator


def qc(
    group_id: str,
    release_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        filters_snv = SparkOperator(
            task_id='filters_snv',
            doc_md=doc.filters_snv,
            name='etl-qc-filters-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantfilter.FiltersSNV',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        filters_frequency_extra = SparkOperator(
            task_id='filters_frequency_extra',
            doc_md=doc.filters_frequency_extra,
            name='etl-qc-filters-frequency-extra',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantfilter.FiltersFrequencyExtra',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        filters_frequency_missed = SparkOperator(
            task_id='filters_frequency_missed',
            doc_md=doc.filters_frequency_missed,
            name='etl-qc-filters-frequency-missed',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantfilter.FiltersFrequencyMissed',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_variant_centric = SparkOperator(
            task_id='no_null_variant_centric',
            doc_md=doc.no_null_variant_centric,
            name='etl-qc-no-null-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullVariantCentric',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_gene_centric = SparkOperator(
            task_id='no_null_gene_centric',
            doc_md=doc.no_null_gene_centric,
            name='etl-qc-no-null-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullGene',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_cnv_centric = SparkOperator(
            task_id='no_null_cnv_centric',
            doc_md=doc.no_null_cnv_centric,
            name='etl-qc-no-null-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullCNV',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric = SparkOperator(
            task_id='only_null_variant_centric',
            doc_md=doc.only_null_variant_centric,
            name='etl-qc-only-null-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_gene_centric = SparkOperator(
            task_id='only_null_gene_centric',
            doc_md=doc.only_null_gene_centric,
            name='etl-qc-only-null-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullGene',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_cnv_centric = SparkOperator(
            task_id='only_null_cnv_centric',
            doc_md=doc.only_null_cnv_centric,
            name='etl-qc-only-null-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullCNV',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric = SparkOperator(
            task_id='same_value_variant_centric',
            doc_md=doc.same_value_variant_centric,
            name='etl-qc-same-value-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_gene_centric = SparkOperator(
            task_id='same_value_gene_centric',
            doc_md=doc.same_value_gene_centric,
            name='etl-qc-same-value-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueGene',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_cnv_centric = SparkOperator(
            task_id='same_value_cnv_centric',
            doc_md=doc.same_value_cnv_centric,
            name='etl-qc-same-value-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueCNV',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )
        '''
        variants_should_be_annotated = SparkOperator(
            task_id='variants_should_be_annotated',
            doc_md=doc.variants_should_be_annotated,
            name='etl-qc-variants-should-be-annotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldBeAnnotated',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        variants_should_not_be_annotated = SparkOperator(
            task_id='variants_should_not_be_annotated',
            doc_md=doc.variants_should_not_be_annotated,
            name='etl-qc-variants-should-not-be-annotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldNotBeAnnotated',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        variants_should_be_reannotated = SparkOperator(
            task_id='variants_should_be_reannotated',
            doc_md=doc.variants_should_be_reannotated,
            name='etl-qc-variants-should-be-reannotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldBeReannotated',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        variants_should_not_be_reannotated = SparkOperator(
            task_id='variants_should_not_be_reannotated',
            doc_md=doc.variants_should_not_be_reannotated,
            name='etl-qc-variants-should-not-be-reannotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldNotBeReannotated',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )
        '''
        filters_snv >> filters_frequency_extra >> filters_frequency_missed >> no_null_variant_centric >> no_null_gene_centric >> no_null_cnv_centric >> only_null_variant_centric >> only_null_gene_centric >> only_null_cnv_centric >> same_value_variant_centric >> same_value_gene_centric >> same_value_cnv_centric

    return group
