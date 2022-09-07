from airflow.utils.task_group import TaskGroup
from lib import config
from lib.config import K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator


def ingest(
    group_id: str,
    batch_id: str,
    color: str,
) -> TaskGroup:

    env = config.environment

    with TaskGroup(group_id=group_id) as group:

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

    return group
