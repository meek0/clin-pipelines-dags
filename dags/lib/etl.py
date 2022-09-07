from airflow.utils.task_group import TaskGroup
from lib import config
from lib.config import Env, K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator


def ingest_group(
    group_id: str,
    batch_id: str,
    color: str,
) -> TaskGroup:

    env = config.environment

    with TaskGroup(group_id=group_id) as ingest:

        file_import = PipelineOperator(
            task_id='file_import',
            name='etl-ingest-file-import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-files-import',
            color=color,
            arguments=[
                'bio.ferlab.clin.etl.FileImport', batch_id, 'false', 'true',
            ],
        )

        fhir_export = PipelineOperator(
            task_id='fhir_export',
            name='etl-ingest-fhir-export',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-datalake',
            color=color,
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
                f'config/{env}.conf', 'initial', 'all',
            ],
        )

        vcf_snv = SparkOperator(
            task_id='vcf_snv',
            name='etl-ingest-vcf-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'snv',
            ],
        )

        vcf_cnv = SparkOperator(
            task_id='vcf_cnv',
            name='etl-ingest-vcf-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'cnv',
            ],
        )

        vcf_variants = SparkOperator(
            task_id='vcf_variants',
            name='etl-ingest-vcf-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'variants',
            ],
        )

        vcf_consequences = SparkOperator(
            task_id='vcf_consequences',
            name='etl-ingest-vcf-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'consequences',
            ],
        )

        file_import >> fhir_export >> fhir_normalize >> vcf_snv >> vcf_cnv >> vcf_variants >> vcf_consequences

    return ingest


def qc_group(
    group_id: str,
    release_id: str,
) -> TaskGroup:

    env = config.environment

    with TaskGroup(group_id=group_id) as qc:

        no_dup_snv = SparkOperator(
            task_id='no_dup_snv',
            name='etl-qc-no-dup-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV',
            spark_config='enriched-etl',
            arguments=[f'clin_{env}', release_id],
            skip_fail_env=[Env.QA],
        )

        no_dup_variant_centric = SparkOperator(
            task_id='no_dup_variant_centric',
            name='etl-qc-no-dup-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariantCentric',
            spark_config='enriched-etl',
            arguments=[f'clin_{env}', release_id],
            skip_fail_env=[Env.QA],
        )

        same_list_snv_variants = SparkOperator(
            task_id='same_list_snv_variants',
            name='etl-qc-same-list-snv-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndVariants',
            spark_config='enriched-etl',
            arguments=[f'clin_{env}', release_id],
            skip_fail_env=[Env.QA],
        )

        no_dup_snv >> no_dup_variant_centric >> same_list_snv_variants

    return qc
