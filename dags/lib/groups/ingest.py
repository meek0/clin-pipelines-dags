from airflow.utils.task_group import TaskGroup
from lib.config import env, K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator


def ingest(
    group_id: str,
    batch_id: str,
    color: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        fhir_import = PipelineOperator(
            task_id='fhir_import',
            name='etl-ingest-fhir-import',
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

        snv = SparkOperator(
            task_id='snv',
            name='etl-ingest-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'snv',
            ],
        )

        cnv = SparkOperator(
            task_id='cnv',
            name='etl-ingest-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'cnv',
            ],
        )

        variants = SparkOperator(
            task_id='variants',
            name='etl-ingest-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'variants',
            ],
        )

        consequences = SparkOperator(
            task_id='consequences',
            name='etl-ingest-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'consequences',
            ],
        )

        fhir_import >> fhir_export >> fhir_normalize >> snv >> cnv >> variants >> consequences

    return group
