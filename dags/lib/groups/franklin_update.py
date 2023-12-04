import logging

from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.franklin import (FranklinStatus, attach_vcf_to_analysis,
                          build_s3_analyses_id_key, build_s3_analyses_ids_key,
                          build_s3_analyses_json_key,
                          build_s3_analyses_status_key, export_bucket,
                          extract_from_name_aliquot_id,
                          extract_param_from_s3_key, get_analysis_status,
                          get_completed_analysis, get_franklin_token,
                          get_metadata_content, group_families_from_metadata,
                          import_bucket, post_create_analysis,
                          transfer_vcf_to_franklin, vcf_suffix,
                          write_s3_analysis_status)
from lib.operators.pipeline import PipelineOperator
from sensors.franklin import FranklinAPISensor


def FranklinUpdate(
    group_id: str,
    batch_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        def download_results(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')

            token = get_franklin_token()
            completed_analyses = []

            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, export_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.READY:    # ignore others status
                        family_id = extract_param_from_s3_key(key, 'family_id') 
                        aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                        id_key = clin_s3.get_key(build_s3_analyses_id_key(batch_id, family_id, aliquot_id), export_bucket)
                        id = id_key.get()['Body'].read().decode('utf-8')

                        json = get_completed_analysis(id, token)
                        json_s3_key = build_s3_analyses_json_key(batch_id, family_id, aliquot_id, id)
                        clin_s3.load_string(json, json_s3_key, export_bucket, replace=True)

                        write_s3_analysis_status(clin_s3, batch_id, family_id, aliquot_id, FranklinStatus.COMPLETED)

                        completed_analyses.append(id)
                        logging.info(f'Download JSON: {len(json)} {json_s3_key}')
            
            logging.info(f'Completed analyses: {completed_analyses}')
        
        def clean_up_clin(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, export_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.COMPLETED:    # ignore others status
                        family_id = extract_param_from_s3_key(key, 'family_id') 
                        aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                        keys_to_delete = [] # delete _FRANKLIN_STATUS_ _FRANKLIN_IDS_ _FRANKLIN_ID_ ...
                        keys_to_delete.append(build_s3_analyses_status_key(batch_id, family_id, aliquot_id))
                        keys_to_delete.append(build_s3_analyses_ids_key(batch_id, family_id, aliquot_id))
                        keys_to_delete.append(build_s3_analyses_id_key(batch_id, family_id, aliquot_id))

                        clin_s3.delete_objects(export_bucket, keys_to_delete)
                        logging.info(f'Delete: {keys_to_delete}')

        def clean_up_franklin(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:  # if any status remains then batch isnt completed yet
                    raise AirflowSkipException('Not all analyses are completed')

            franklin_s3 = S3Hook(config.s3_franklin)
            keys = franklin_s3.list_keys(config.s3_franklin_bucket, f'{env}/{batch_id}')
            for key in keys:
                if key.endswith(vcf_suffix): # delete all VCFs in Franklin bucket
                    franklin_s3.delete_objects(config.s3_franklin_bucket, [key])
                    logging.info(f'Delete: {key}')

        api_sensor = FranklinAPISensor(
            task_id='api_sensor',
            batch_id=batch_id,
            mode='poke',
            soft_fail=True, # SKIP on failure
            poke_interval=5*60, # poke every 5 min for 1 hour
            timeout=1*60*60,
        )

        download_results = PythonOperator(
            task_id='download_results',
            op_args=[batch_id],
            python_callable=download_results,
        )

        clean_up_clin = PythonOperator(
            task_id='clean_up_clin',
            op_args=[batch_id],
            python_callable=clean_up_clin,
        )

        clean_up_franklin = PythonOperator(
            task_id='clean_up_franklin',
            op_args=[batch_id],
            python_callable=clean_up_franklin,
        )

        api_sensor >> download_results >> clean_up_clin >> clean_up_franklin

    return group
