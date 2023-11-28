import logging
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.franklin import (FranklinStatus, attach_vcf_to_analysis,
                          export_bucket, extract_from_name_aliquot_id,
                          extract_from_name_family_id, get_analysis_status,
                          get_completed_analysis, get_franklin_http_conn,
                          get_franklin_token, get_metadata_content,
                          group_families_from_metadata, import_bucket,
                          post_create_analysis, transfer_vcf_to_franklin,
                          vcf_suffix, writeS3AnalysesStatus)
from lib.operators.pipeline import PipelineOperator
from sensors.franklin import FranklinAPISensor


def FranklinUpdate(
    group_id: str,
    batch_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        api_sensor = FranklinAPISensor(
            task_id='api_sensor_task',
            batch_id=batch_id,
            poke_interval=60,
            timeout=300,
        )

        api_sensor
    '''

        @task
        def transfer_from_franklin_to_clin(_batch_id, token):
            obj = build_cases(_batch_id)
            families = obj['families']
            solos = obj['no_family']
            logging.info(obj)
            for family_id, analyses in families.items():
                for analysis in analyses:
                    logging.info(f'analysis {analysis}')
                    if analysis['labAliquotId'] is None:
                        path = f'raw/landing/franklin/batch_id={_batch_id}/family_id={family_id}/aliquot_id=null/analysis.json'
                    else:
                        path = f'raw/landing/franklin/batch_id={_batch_id}/family_id={family_id}/aliquot_id={analysis["labAliquotId"]}/analysis_id={analysis["id"]}/analysis.json'
                    data = get_completed_analysis(analysis["id"], token)
                    clin_s3.load_string(data, path, export_bucket, replace=True)

            for patient in solos:
                logging.info(f'patient is {patient}')
                path = f'raw/landing/franklin/batch_id={_batch_id}/family_id=null/aliquot_id={patient["labAliquotId"].split(" - ")[0]}/analysis_id={patient["id"]}/analysis.json'
                data = get_completed_analysis(patient["id"], token)
                clin_s3.load_string(data, path, export_bucket, replace=True)
            return True


        def get_family_members(keys, family_id):
            family_members = []
            for key in keys:
                if f'family_id={family_id}' in key:
                    family_members.append({'id': key.split('analysis_id=')[1].split('/')[0],
                                           'labAliquotId': key.split('aliquot_id=')[1].split('/')[0]})
            return family_members


        def build_cases(_batch_id):
            to_return = {
                'families': {},
                'no_family': []
            }
            logging.info(f'build cases {_batch_id}')
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={_batch_id}/')
            logging.info(f'keys are {keys}')
            for key in keys:
                if 'family_id=null' not in key:
                    family_id = key.split('family_id=')[1].split('/')[0]
                    to_return['families'][family_id] = get_family_members(keys, family_id)
                else:
                    to_return['no_family'].append({'id': key.split('analysis_id=')[1].split('/')[0],
                                                   'labAliquotId': key.split('aliquot_id=')[1].split('/')[0]})

            logging.info(f'build cases returned {to_return}')
            return to_return
        '''

        #mark_analyses_as_started(
        #    get_analyses_status(
        #create_analysis(
        #    upload_files_to_franklin(
        #        group_families(batch_id()), 
        #        batch_id()),
        #    batch_id()), 
        #    batch_id()) 

    # >> api_sensor >> transfer_from_franklin_to_clin(batch_id(), authenticate())

    return group
