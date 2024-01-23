import logging

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from lib.config import (ClinSchema, ClinVCFSuffix, K8sContext,
                        clin_import_bucket, config_file, env,
                        get_metadata_content, s3_conn_id)
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

        def check_vcfs_by_schema(batch_id, skip):

            if skip:
                raise AirflowSkipException()

            clin_s3 = S3Hook(s3_conn_id)
            metadata = get_metadata_content(clin_s3, batch_id)
            submission_schema = metadata.get('submissionSchema', '')

            snv_vcf_suffix = None
            cnv_vcf_suffix = None
            if submission_schema == ClinSchema.GERMLINE.value:
                snv_vcf_suffix = ClinVCFSuffix.SNV_GERMLINE.value
                cnv_vcf_suffix = ClinVCFSuffix.CNV_GERMLINE.value
            elif submission_schema == ClinSchema.SOMATIC.value:
                snv_vcf_suffix = ClinVCFSuffix.SNV_SOMATIC.value
                cnv_vcf_suffix = ClinVCFSuffix.CNV_SOMATIC.value
            else:
                raise AirflowFailException(f'Invalid submissionSchema: {submission_schema}')
            
            logging.info(f'Schema: {submission_schema}')
            logging.info(f'Expecting SNV VCF(s) suffix: {snv_vcf_suffix}')
            logging.info(f'Expecting CNV VCF(s) suffix: {cnv_vcf_suffix}')

            has_valid_snv_vcf = False
            keys = clin_s3.list_keys(clin_import_bucket, f'{batch_id}/')
            for key in keys:
                if key.endswith(snv_vcf_suffix):
                    logging.info(f'Valid SNV VCF file: {key}')
                    has_valid_snv_vcf = True

            if not has_valid_snv_vcf:
                raise AirflowFailException(f'No valid SNV VCF(s) found')
            
            all_cnv_vcf_valid = True
            for analysis in metadata['analyses']:
                cnv_file = analysis.get('files', {}).get('cnv_vcf')
                if cnv_file:
                    if cnv_file.endswith(cnv_vcf_suffix):
                        logging.info(f'Valid CNV VCF file: {cnv_file}')
                    else:
                        logging.info(f'Invalid CNV VCF file: {cnv_file}')
                        all_cnv_vcf_valid = False

            if not all_cnv_vcf_valid:
                raise AirflowFailException(f'Not all valid CNV VCF(s) found')

        check_vcfs = PythonOperator(
            task_id='validate_vcfs',
            op_args=[batch_id, skip_import],
            python_callable=check_vcfs_by_schema,
        )

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
            spark_config='config-etl-large',
            skip=skip_batch,
            spark_jar=spark_jar,
            arguments=[
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_ingest_fhir_normalize',
                '--destination', 'all'
            ],
        )

        check_vcfs >> fhir_import >> fhir_export >> fhir_normalize

    return group
