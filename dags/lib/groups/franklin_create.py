import logging

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from lib import config
from lib.config import K8sContext, config_file, env
from lib.franklin import (FranklinStatus, attach_vcf_to_analyses,
                          can_create_analysis, extract_vcf_prefix,
                          get_franklin_token, get_metadata_content,
                          group_families_from_metadata, import_bucket,
                          post_create_analysis, transfer_vcf_to_franklin,
                          vcf_suffix, write_s3_analyses_status)


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
            return submission_schema == 'CQGC_Germline' # do nothing for EXTUM

        '''
        Some rules about tasks:
        - Can't use AirflowSkipException with Task
        - Tasks should always return something
        '''

        @task
        def group_families(batch_id):
            if check_metadata(batch_id):
                clin_s3 = S3Hook(config.s3_conn_id)
                metadata = get_metadata_content(clin_s3, batch_id)
                [grouped_by_families, without_families] = group_families_from_metadata(metadata)
                logging.info(grouped_by_families)
                return {'families': grouped_by_families, 'no_family': without_families}
            return {}

        @task
        def vcf_to_analyses(families, batch_id):
            if check_metadata(batch_id):
                clin_s3 = S3Hook(config.s3_conn_id)
                vcfs = {}
                keys = clin_s3.list_keys(import_bucket, f'{batch_id}/')
                for key in keys:
                    if key.endswith(vcf_suffix):
                        vcfs[key] = extract_vcf_prefix(key)
                return attach_vcf_to_analyses(families, vcfs) # we now have analysis <=> vcf
            return {}

        @task
        def create_analyses(families, batch_id):
            if check_metadata(batch_id):
                clin_s3 = S3Hook(config.s3_conn_id)
                franklin_s3 = S3Hook(config.s3_franklin)
                token = get_franklin_token()

                created_ids = []

                for family_id, analyses in families['families'].items():
                    if (can_create_analysis(clin_s3, batch_id, family_id, analyses)): # already created before
                        transfer_vcf_to_franklin(clin_s3, franklin_s3, analyses)
                        ids = post_create_analysis(family_id, analyses, token, clin_s3, franklin_s3, batch_id)['analysis_ids']
                        write_s3_analyses_status(clin_s3, batch_id, family_id, analyses, FranklinStatus.CREATED, ids)
                        created_ids += ids
                
                for patient in families['no_family']:
                    analyses = [patient]
                    if (can_create_analysis(clin_s3, batch_id, None, analyses)): # already created before
                        transfer_vcf_to_franklin(clin_s3, franklin_s3, analyses)
                        ids = post_create_analysis(None, analyses, token, clin_s3, franklin_s3, batch_id)
                        write_s3_analyses_status(clin_s3, batch_id, None, analyses, FranklinStatus.CREATED, ids)
                        created_ids += ids

                logging.info(created_ids)
                return created_ids # success !!!
            return {}

        create_analyses(
            vcf_to_analyses(
                group_families(
                    batch_id), 
                batch_id),
            batch_id), 

    return group
