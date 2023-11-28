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
                          buildS3AnalysesIdKey, buildS3AnalysesIdsKey,
                          buildS3AnalysesJSONKey, buildS3AnalysesStatusKey,
                          export_bucket, extract_from_name_aliquot_id,
                          extractParamFromS3Key, get_analysis_status,
                          get_completed_analysis, get_franklin_token,
                          get_metadata_content, group_families_from_metadata,
                          import_bucket, post_create_analysis,
                          transfer_vcf_to_franklin, vcf_suffix,
                          writeS3AnalysisStatus)
from lib.operators.pipeline import PipelineOperator
from sensors.franklin import FranklinAPISensor


def FranklinUpdate(
    group_id: str,
    batch_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        def download_json(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')

            token = get_franklin_token()
            completed_analyses = []

            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, export_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.READY:    # ignore others status
                        family_id = extractParamFromS3Key(key, 'family_id') 
                        aliquot_id = extractParamFromS3Key(key, 'aliquot_id')

                        id_key = clin_s3.get_key(buildS3AnalysesIdKey(batch_id, family_id, aliquot_id), export_bucket)
                        id = id_key.get()['Body'].read().decode('utf-8')

                        json = get_completed_analysis(id, token)
                        json_s3_key = buildS3AnalysesJSONKey(batch_id, family_id, aliquot_id, id)
                        clin_s3.load_string(json, json_s3_key, export_bucket, replace=True)

                        writeS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id, FranklinStatus.COMPLETED)

                        completed_analyses.append(id)
                        logging.info(f'Download JSON: {len(json)} {json_s3_key}')
            
            logging.info(f'Completed analyses: {completed_analyses}')
        
        def clean_up(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, export_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.COMPLETED:    # ignore others status
                        family_id = extractParamFromS3Key(key, 'family_id') 
                        aliquot_id = extractParamFromS3Key(key, 'aliquot_id')

                        keys_to_delete = [] # delete _FRANKLIN_STATUS_ _FRANKLIN_IDS_ _FRANKLIN_ID_ ...
                        keys_to_delete.append(buildS3AnalysesStatusKey(batch_id, family_id, aliquot_id))
                        keys_to_delete.append(buildS3AnalysesIdsKey(batch_id, family_id, aliquot_id))
                        keys_to_delete.append(buildS3AnalysesIdKey(batch_id, family_id, aliquot_id))

                        clin_s3.delete_objects(export_bucket, keys_to_delete)
                        logging.info(f'Delete: {keys_to_delete}')
                    

        api_sensor = FranklinAPISensor(
            task_id='api_sensor',
            batch_id=batch_id,
            poke_interval=5*60, # poke every 5 min for 1 hour
            timeout=1*60*60,
        )

        download = PythonOperator(
            task_id='download',
            op_args=[batch_id],
            python_callable=download_json,
        )

        clean_up = PythonOperator(
            task_id='clean_up',
            op_args=[batch_id],
            python_callable=clean_up,
        )

        api_sensor >> download >> clean_up

    return group
