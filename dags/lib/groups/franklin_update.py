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
    skip_update: str,
    status_interval: str,
    status_timeout: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        api_sensor = FranklinAPISensor(
            task_id='api_sensor_task',
            skip=skip_update,
            batch_id=batch_id,
            poke_interval=1,
            timeout=300,
            #status_interval=status_interval,
            #status_timeout=status_timeout
        )

        api_sensor
    '''

    franklin_poke = 900
    franklin_timeout = 10800

    with TaskGroup(group_id='franklin') as franklin:

        @task
        def group_families(batch_id):
            clin_s3 = S3Hook(config.s3_conn_id)
            metadata = get_metadata_content(clin_s3, batch_id)
            [grouped_by_families, without_families] = group_families_from_metadata(metadata)
            logging.info(grouped_by_families)
            return {'families': grouped_by_families, 'no_family': without_families}

        @task
        def upload_files_to_franklin(obj, batch_id):
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
            conn = get_franklin_http_conn()
            token = get_franklin_token(conn)

            created_ids = []


            for family_id, analyses in obj['families'].items():
                if (canCreateAnalyses(clin_s3, batch_id, family_id, analyses)):
                    ids = post_create_analysis(conn, family_id, analyses, token, clin_s3, franklin_s3, batch_id)['analysis_ids']
                    writeS3AnalysesStatus(clin_s3, batch_id, family_id, analyses, FranklinStatus.CREATED, ids)
                    created_ids += ids
            
            for patient in obj['no_family']:
                analyses = [patient]
                if (canCreateAnalyses(clin_s3, batch_id, None, analyses)):
                    ids = post_create_analysis(conn, None, analyses, token, clin_s3, franklin_s3, batch_id)
                    writeS3AnalysesStatus(clin_s3, batch_id, None, analyses, FranklinStatus.CREATED, ids)
                    created_ids += ids

            logging.info(created_ids)
            return True # created_ids can't be used after

        @task
        def get_analyses_status(started_analyses):
            families = started_analyses['families']
            solos = started_analyses['no_family']

            conn = get_franklin_http_conn()
            token = get_franklin_token(conn)

            ids = [] # build one status request with all analysis IDs

            for family_id, analyses in families.items():
                ids += analyses

            for patient in solos:
                ids.append(patient)
                
            status = get_analysis_status(conn, ids, token)
            logging.info(status)
            return status

        @task
        def mark_analyses_as_started(status, batch_id):
            for s in status:
                analysis_id = s['id']
                aliquot_id = extract_from_name_aliquot_id(s['name'])
                family_id = extract_from_name_family_id(s['name'])
                clin_s3.load_string(s['processing_status'],
                    f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/aliquot_id={aliquot_id}/franklin_analysis_id={analysis_id}/_FRANKLIN_STATUS.txt',
                    export_bucket, replace=True)
            return True

   
        api_sensor = FranklinAPISensor(
            task_id='api_sensor_task',
            s3=clin_s3,
            mode='poke',
            poke_interval=franklin_poke,
            timeout=franklin_timeout,
            dag=dag,
            export_bucket=export_bucket
        )
     

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
