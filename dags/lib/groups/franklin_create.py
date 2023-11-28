import logging

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from lib import config
from lib.config import K8sContext, config_file, env
from lib.franklin import (FranklinStatus, analysesDontExist,
                          attach_vcf_to_analysis, get_franklin_token,
                          get_metadata_content, group_families_from_metadata,
                          import_bucket, post_create_analysis,
                          transfer_vcf_to_franklin, vcf_suffix,
                          writeS3AnalysesStatus)


def FranklinCreate(
    group_id: str,
    batch_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        def check_metadata(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            metadata = get_metadata_content(clin_s3, batch_id)
            submission_schema = metadata.get('submissionSchema', '')
            logging.info(f'Schema: {submission_schema}')
            if (submission_schema != 'CQGC_Germline'):
                raise AirflowFailException('Not Germline Batch')

        @task
        def group_families(batch_id):
            check_metadata(batch_id)
            clin_s3 = S3Hook(config.s3_conn_id)
            metadata = get_metadata_content(clin_s3, batch_id)
            [grouped_by_families, without_families] = group_families_from_metadata(metadata)
            logging.info(grouped_by_families)
            return {'families': grouped_by_families, 'no_family': without_families}

        @task
        def upload_files(obj, batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            franklin_s3 = S3Hook(config.s3_franklin)
            aliquot_ids = {}
            matching_keys = clin_s3.list_keys(import_bucket, f'{batch_id}/')
            for key in matching_keys:
                if key.endswith(vcf_suffix):
                    aliquot_ids[key] = transfer_vcf_to_franklin(clin_s3, franklin_s3, key)
            return attach_vcf_to_analysis(obj, aliquot_ids)

        @task
        def create_analysis(obj, batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            franklin_s3 = S3Hook(config.s3_franklin)
            token = get_franklin_token()

            created_ids = []

            '''
            That task will create and save the current analyses status + ids right after creation
            We want to ignore the already created analyses in case we re-start that airflow step
            '''

            for family_id, analyses in obj['families'].items():
                if (analysesDontExist(clin_s3, batch_id, family_id, analyses)):
                    ids = post_create_analysis(family_id, analyses, token, clin_s3, franklin_s3, batch_id)['analysis_ids']
                    writeS3AnalysesStatus(clin_s3, batch_id, family_id, analyses, FranklinStatus.CREATED, ids)
                    created_ids += ids
            
            for patient in obj['no_family']:
                analyses = [patient]
                if (analysesDontExist(clin_s3, batch_id, None, analyses)):
                    ids = post_create_analysis(None, analyses, token, clin_s3, franklin_s3, batch_id)
                    writeS3AnalysesStatus(clin_s3, batch_id, None, analyses, FranklinStatus.CREATED, ids)
                    created_ids += ids

            logging.info(created_ids)
            return True # success !!! 

        create_analysis(
            upload_files(
                group_families(
                    batch_id), 
                batch_id),
            batch_id), 

    return group
