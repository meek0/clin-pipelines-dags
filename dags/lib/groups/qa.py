from airflow.utils.task_group import TaskGroup
from lib.config import env_url, Env, K8sContext
from lib.doc import qa as doc
from lib.operators.spark import SparkOperator


def qa(
    group_id: str,
    release_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        no_dup_snv = SparkOperator(
            task_id='no_dup_snv',
            doc_md=doc.no_dup_snv,
            name='etl-qc-no-dup-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        no_dup_nor_variants = SparkOperator(
            task_id='no_dup_nor_variants',
            doc_md=doc.no_dup_nor_variants,
            name='etl-qc-no-dup-nor-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorVariants',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        no_dup_variants = SparkOperator(
            task_id='no_dup_variants',
            doc_md=doc.no_dup_variants,
            name='etl-qc-no-dup-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariants',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        no_dup_variant_centric = SparkOperator(
            task_id='no_dup_variant_centric',
            doc_md=doc.no_dup_variant_centric,
            name='etl-qc-no-dup-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariantCentric',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        same_list_snv_nor_variants = SparkOperator(
            task_id='same_list_snv_nor_variants',
            doc_md=doc.same_list_snv_nor_variants,
            name='etl-qc-same-list-snv-nor-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndNorVariants',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        same_list_snv_variants = SparkOperator(
            task_id='same_list_snv_variants',
            doc_md=doc.same_list_snv_variants,
            name='etl-qc-same-list-snv-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndVariants',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        same_list_variants_variant_centric = SparkOperator(
            task_id='same_list_variants_variant_centric',
            doc_md=doc.same_list_variants_variant_centric,
            name='etl-qc-same-list-variants-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenVariantsAndVariantCentric',
            spark_config='enriched-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        no_dup_snv >> no_dup_nor_variants >> no_dup_variants >> no_dup_variant_centric >> same_list_snv_nor_variants >> same_list_snv_variants >> same_list_variants_variant_centric

    return group
