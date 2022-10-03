from airflow.utils.task_group import TaskGroup
from lib.config import env_url, Env, K8sContext
from lib.doc import qc as doc
from lib.operators.spark import SparkOperator


def qc(
    group_id: str,
    release_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

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

        filters_frequency_extra >> filters_frequency_missed >> filters_snv

    return group
