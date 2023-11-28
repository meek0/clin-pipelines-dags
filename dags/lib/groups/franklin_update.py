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
                          buildS3AnalysesJSONKey, export_bucket,
                          extract_from_name_aliquot_id,
                          extract_from_name_family_id, extractParamFromS3Key,
                          get_analysis_status, get_completed_analysis,
                          get_franklin_http_conn, get_franklin_token,
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

            if len(keys) == 0:  # nothing in that batch about Franklin
                raise AirflowSkipException()

            conn = get_franklin_http_conn()
            token = get_franklin_token(conn)
            completed_analyses = []

            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, export_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.READY:    # ignore others status
                        id_key = clin_s3.get_key(key.replace('_FRANKLIN_STATUS_', '_FRANKLIN_ID_'), export_bucket)
                        id = id_key.get()['Body'].read().decode('utf-8')
                        aliquot_id = extractParamFromS3Key(key, 'aliquot_id')
                        family_id = extractParamFromS3Key(key, 'family_id') 
                        json = get_completed_analysis(conn, id, token)
                        json_s3_key = buildS3AnalysesJSONKey(batch_id, family_id, aliquot_id)
                        clin_s3.load_string(json, json_s3_key, export_bucket, replace=True)
                        writeS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id, FranklinStatus.COMPLETED)
                        completed_analyses.append(id)
                        logging.info(f'Download JSON: {len(json)} {json_s3_key}')
            
            logging.info(f'Completed analyses: {completed_analyses}')

        api_sensor = FranklinAPISensor(
            task_id='api_sensor',
            batch_id=batch_id,
            poke_interval=60,
            timeout=300,
        )

        download = PythonOperator(
            task_id='download',
            op_args=[batch_id],
            python_callable=download_json,
        )

        api_sensor >> download

    return group
