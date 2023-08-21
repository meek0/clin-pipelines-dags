from airflow.utils.task_group import TaskGroup

from lib.config import config_file
from lib.config import env, K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator


def IngestFhir(
    group_id: str,
    batch_id: str,
    color: str,
    skip_import: str,
    skip_batch: str,
    spark_jar: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        fhir_import = PipelineOperator(
            task_id='fhir_import',
            name='etl-ingest-fhir-import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-files-import',
            color=color,
            skip=skip_import,
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
            skip=skip_batch,
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
            skip=skip_batch,
            spark_jar=spark_jar,
            arguments=[
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_ingest_fhir_normalize',
                '--destination', 'all'
            ],
        )

        fhir_import >> fhir_export >> fhir_normalize

    return group
