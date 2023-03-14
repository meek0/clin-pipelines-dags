from airflow.utils.task_group import TaskGroup
from lib.config import env_url, Env, K8sContext
from lib.doc import qa as doc
from lib.operators.spark import SparkOperator


def qa(
    group_id: str,
    release_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        non_empty_tables = SparkOperator(
            task_id='non_empty_tables',
            doc_md=doc.non_empty_tables,
            name='etl-qc-non-empty-tables',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.tables.NonEmptyTables',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_snv = SparkOperator(
            task_id='no_dup_snv',
            doc_md=doc.no_dup_snv,
            name='etl-qc-no-dup-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_nor_consequences = SparkOperator(
            task_id='no_dup_nor_consequences',
            doc_md=doc.no_dup_nor_consequences,
            name='etl-qc-no-dup-nor-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorConsequences',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_nor_variants = SparkOperator(
            task_id='no_dup_nor_variants',
            doc_md=doc.no_dup_nor_variants,
            name='etl-qc-no-dup-nor-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorVariants',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_consequences = SparkOperator(
            task_id='no_dup_consequences',
            doc_md=doc.no_dup_consequences,
            name='etl-qc-no-dup-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationConsequences',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_variants = SparkOperator(
            task_id='no_dup_variants',
            doc_md=doc.no_dup_variants,
            name='etl-qc-no-dup-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariants',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_variant_centric = SparkOperator(
            task_id='no_dup_variant_centric',
            doc_md=doc.no_dup_variant_centric,
            name='etl-qc-no-dup-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariantCentric',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_dup_cnv_centric = SparkOperator(
            task_id='no_dup_cnv_centric',
            doc_md=doc.no_dup_cnv_centric,
            name='etl-qc-no-dup-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationCNV',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )
        '''
        no_dup_varsome = SparkOperator(
            task_id='no_dup_varsome',
            doc_md=doc.no_dup_varsome,
            name='etl-qc-no-dup-varsome',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVarsome',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )
        '''
        same_list_snv_nor_variants = SparkOperator(
            task_id='same_list_snv_nor_variants',
            doc_md=doc.same_list_snv_nor_variants,
            name='etl-qc-same-list-snv-nor-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndNorVariants',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_list_snv_variants = SparkOperator(
            task_id='same_list_snv_variants',
            doc_md=doc.same_list_snv_variants,
            name='etl-qc-same-list-snv-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndVariants',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_list_variants_variant_centric = SparkOperator(
            task_id='same_list_variants_variant_centric',
            doc_md=doc.same_list_variants_variant_centric,
            name='etl-qc-same-list-variants-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenVariantsAndVariantCentric',
            spark_config='raw-fhir-etl',
            arguments=['clin' + env_url('_'), release_id],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        non_empty_tables >> no_dup_snv >> no_dup_nor_consequences >> no_dup_nor_variants >> no_dup_consequences >> no_dup_variants >> no_dup_variant_centric >> no_dup_cnv_centric >> same_list_snv_nor_variants >> same_list_snv_variants >> same_list_variants_variant_centric

    return group
