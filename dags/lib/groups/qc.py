from airflow.utils.task_group import TaskGroup
from lib import config
from lib.config import Env, K8sContext
from lib.operators.spark import SparkOperator


def qc(
    group_id: str,
    release_id: str,
) -> TaskGroup:

    env = config.environment

    with TaskGroup(group_id=group_id) as group:

        no_dup_snv = SparkOperator(
            task_id='no_dup_snv',
            name='etl-qc-no-dup-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV',
            spark_config='enriched-etl',
            arguments=[f'clin_{env}', release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        no_dup_variant_centric = SparkOperator(
            task_id='no_dup_variant_centric',
            name='etl-qc-no-dup-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariantCentric',
            spark_config='enriched-etl',
            arguments=[f'clin_{env}', release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        same_list_snv_variants = SparkOperator(
            task_id='same_list_snv_variants',
            name='etl-qc-same-list-snv-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndVariants',
            spark_config='enriched-etl',
            arguments=[f'clin_{env}', release_id],
            skip_fail_env=[Env.QA, Env.STAGING],
        )

        no_dup_snv >> no_dup_variant_centric >> same_list_snv_variants

    return group
