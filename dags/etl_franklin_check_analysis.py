import logging

from airflow import DAG

from lib.slack import Slack
from datetime import datetime
from airflow.decorators import task
from lib.franklin import authenticate, check_analysis_exists, copy_files_to_franklin, group_families, start_analysis, \
    get_analyses_status, get_completed_analysis
from sensors.franklin import FranklinAPISensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
import json
from airflow.models.param import Param

with DAG(
        dag_id='etl_franklin_check_analysis',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
        params={
            'batch_id': Param('{batch_id()}', type='string')
            # 'release_id': Param('', type='string'),
            # 'color': Param('', enum=['', 'blue', 'green']),
            # 'import': Param('yes', enum=['yes', 'no']),
            # 'notify': Param('no', enum=['yes', 'no']),
            # 'spark_jar': Param('', type='string'),
        },
) as dag:
    def batch_id() -> str:
        return '123456'
        return '{{ params.batch_id }}'


    @task
    def auth():
        token = authenticate(email=config.franklin_email, password=config.franklin_password)
        return token


    clin_s3 = S3Hook(config.s3_conn_id)
    franklin_s3 = S3Hook(config.s3_franklin)


    @task
    def _group_families():
        metadata_path = f'{batch_id()}/metadata.json'
        f = clin_s3.get_key(metadata_path, 'clin-local')
        file_content = json.loads(f.get()['Body'].read().decode('utf-8'))

        [grouped_by_families, without_families] = group_families(file_content)
        logging.info(grouped_by_families)
        return {'families': grouped_by_families, 'no_family': without_families}


    @task
    def upload_files_to_franklin(obj):
        _families = obj['families']
        solos = obj['no_family']
        for family_id, analyses in _families.items():
            print(f"Family ID: {family_id}")
            for analysis in analyses:
                copy_files_to_franklin(clin_s3, franklin_s3, analysis, batch_id=batch_id())

        for patient in solos:
            copy_files_to_franklin(clin_s3, franklin_s3, patient, batch_id=batch_id())
        return obj


    @task
    def check_file_existence(obj):
        _families = obj['families']
        _no_family = obj['no_family']
        to_create = {
            'no_family': [],
            'families': {},
        }
        for family_id, analyses in _families.items():
            does_exists = False
            for analysis in analyses:
                does_exists = check_analysis_exists(clin_s3, f'{batch_id()}',
                                                    family_id=analysis['patient']['familyId'],
                                                    aliquot_id=analysis['labAliquotId'])
            if does_exists is False:
                to_create['families'][family_id] = analyses

        for patient in _no_family:

            does_exists = check_analysis_exists(clin_s3, f'{batch_id()}',
                                                family_id=None,
                                                aliquot_id=patient['labAliquotId'])
            if does_exists is False:
                to_create['no_family'].append(patient)

        logging.info(f'files to create {to_create}')
        return to_create


    @task
    def _start_analysis(obj, token):
        _families = obj['families']
        solos = obj['no_family']
        started_analyses = {
            'no_family': [],
            'families': {},
        }

        for family_id, analyses in _families.items():
            started_analyses['families'][family_id] = start_analysis(family_id, analyses, token)

        for patient in solos:
            started_analyses['no_family'].append(start_analysis(None, [patient], token))
        return started_analyses


    @task
    def get_statuses(started_analyses, token):
        families = started_analyses['families']
        solos = started_analyses['no_family']
        statuses = {
            'no_family': [],
            'families': {},
        }
        for family_id, analyses in families.items():
            status = get_analyses_status(analyses, token)
            statuses['families'][family_id] = status
        logging.info(statuses)

        for patient in solos:
            statuses['no_family'].append(get_analyses_status([patient], token))
        return statuses


    @task
    def mark_analyses_as_started(obj):
        _families = obj['families']
        solos = obj['no_family']
        for family_id, status in _families.items():
            logging.info(status)
            for s in status:
                if 'family' in s['name']:
                    clin_s3.load_string('',
                                        f'batch_id={batch_id()}/family_id={family_id}/analysis_id={s["id"]}/_IN_PROGRESS.txt',
                                        'clin-local', replace=True)
                else:
                    path = f'batch_id={batch_id()}/family_id={family_id}/aliquot_id={s["name"].split(" - ")[0]}/analysis_id={s["id"]}/_IN_PROGRESS.txt'
                    clin_s3.load_string('', path, 'clin-local', replace=True)

        for p in solos:
            patient = p[0]
            clin_s3.load_string('',
                                f'batch_id={batch_id()}/family_id=/aliquot_id={patient["name"].split(" - ")[0]}/analysis_id={patient["id"]}/_IN_PROGRESS.txt',
                                'clin-local', replace=True)
        return True


    @task
    def list_analysis():
        keys = clin_s3.list_keys('clin-local', f'batch_id={batch_id()}/')
        logging.info(f'list analysis {keys}')
        ids = []
        for key in keys:
            if 'analysis_id=' in key:
                _id = key.split('analysis_id=')[1].split('/')[0]
                ids.append(_id)
        logging.info(f'ids are {ids}')
        return ids


    api_sensor = FranklinAPISensor(
        task_id='api_sensor_task',
        analyses_ids=list_analysis(),
        mode='poke',
        timeout=3600,
        poke_interval=60,
        dag=dag,
    )


    @task
    def transfer_from_franklin_to_clin(obj, token):
        _families = obj['families']
        solos = obj['no_family']
        for family_id, analyses in _families.items():
            for analysis in analyses:
                logging.info(f'analysis {analysis}')
                if analysis['labAliquotId'] is None:
                    path = f'batch_id={batch_id()}/family_id={family_id}/analysis.json'
                else:
                    path = f'batch_id={batch_id()}/family_id={family_id}/aliquot_id={analysis["labAliquotId"]}/analysis_id={analysis["id"]}/analysis.json'
                data = get_completed_analysis(analysis["id"], token)
                clin_s3.load_string(data, path, 'clin-local', replace=True)

        for p in solos:
            patient = p[0]
            path = f'batch_id={batch_id()}/family_id=/aliquot_id={patient["name"].split(" - ")[0]}/analysis_id={patient["id"]}/analysis.json'
            data = get_completed_analysis(patient["id"], token)
            clin_s3.load_string(data, path, 'clin-local', replace=True)
        return True


    @task
    def build_cases(batch_id):
        # {'families': {'LDM_FAM0': [{'id':3, 'labAliquotId': 12345}, {'id':4, 'labAliquotId': 12346}, {'id':5, 'labAliquotId': 12347}, {'id':6, 'labAliquotId': None}]}, 'no_family': []}
        to_return = {
            'families': {},
            'no_family': []
        }

        return {'batch_id': batch_id()}

        # Set task dependencies.
        # list_analysis
        # api_sensor >> transfer_from_franklin_to_clin({'families': {'LDM_FAM0': [{'id':3, 'labAliquotId': 12345}, {'id':4, 'labAliquotId': 12346}, {'id':5, 'labAliquotId': 12347}, {'id':6, 'labAliquotId': None}]}, 'no_family': []}, auth())

        # if 'allo' in 'abc':


    mark_analyses_as_started(
        get_statuses(_start_analysis(upload_files_to_franklin(check_file_existence(_group_families())), auth()),
                     auth())) >> api_sensor >> transfer_from_franklin_to_clin(build_cases(batch_id()), auth())

    # the_sensor([3,4,5,6], auth())
