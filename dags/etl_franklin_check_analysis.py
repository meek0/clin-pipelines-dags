import logging
from airflow import DAG
from airflow.models.baseoperator import chain

from lib.config import env
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
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import ShortCircuitOperator

with DAG(
        dag_id='etl_franklin_check_analysis',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
        render_template_as_native_obj=True,
        params={
            'batch_id': Param('', type='string'),
            'trigger_franklin': Param('yes', enum=['yes', 'no'])
        },
) as dag:
    def batch_id() -> str:
        return '{{ params.batch_id }}'


    def trigger_franklin() -> str:
        return '{{ params.trigger_franklin }}'


    import_bucket = f'cqgc-{env}-app-files-import'
    export_bucket = f'cqgc-{env}-app-datalake'


    @task
    def auth():
        token = authenticate(email=config.franklin_email, password=config.franklin_password)
        return token


    clin_s3 = S3Hook(config.s3_conn_id)
    franklin_s3 = S3Hook(config.s3_franklin)
    franklin_poke = 900
    franklin_timeout = 10800

    with TaskGroup(group_id='franklin') as franklin:
        @task
        def _group_families(_batch_id):
            metadata_path = f'{_batch_id}/metadata.json'
            f = clin_s3.get_key(metadata_path, import_bucket)
            file_content = json.loads(f.get()['Body'].read().decode('utf-8'))

            [grouped_by_families, without_families] = group_families(file_content)
            logging.info(grouped_by_families)
            return {'families': grouped_by_families, 'no_family': without_families}


        @task
        def upload_files_to_franklin(obj, batch):
            _families = obj['families']
            solos = obj['no_family']
            for family_id, analyses in _families.items():
                for analysis in analyses:
                    copy_files_to_franklin(clin_s3, franklin_s3, analysis, batch_id=batch)

            for patient in solos:
                copy_files_to_franklin(clin_s3, franklin_s3, patient, batch_id=batch)
            return obj


        @task
        def check_file_existence(obj, _batch_id):
            _families = obj['families']
            _no_family = obj['no_family']
            to_create = {
                'no_family': [],
                'families': {},
            }
            for family_id, analyses in _families.items():
                does_exists = False
                for analysis in analyses:
                    does_exists = check_analysis_exists(clin_s3, f'{_batch_id}',
                                                        family_id=analysis['patient']['familyId'],
                                                        aliquot_id=analysis['labAliquotId'])
                if does_exists is False:
                    to_create['families'][family_id] = analyses

            for patient in _no_family:

                does_exists = check_analysis_exists(clin_s3, f'{_batch_id}',
                                                    family_id=None,
                                                    aliquot_id=patient['labAliquotId'])
                if does_exists is False:
                    to_create['no_family'].append(patient)

            logging.info(f'files to create {to_create}')
            return to_create


        @task
        def _start_analysis(obj, token, batch):
            _families = obj['families']
            solos = obj['no_family']
            started_analyses = {
                'no_family': [],
                'families': {},
            }

            for family_id, analyses in _families.items():
                started_analyses['families'][f'{family_id}'] = start_analysis(family_id, analyses, token, clin_s3,
                                                                              batch)

            for patient in solos:
                started_analyses['no_family'].append(
                    start_analysis(None, [patient], token, clin_s3, batch)[0])  # only one analysis started on Franklin

            logging.info(started_analyses)
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
        def mark_analyses_as_started(obj, batch):
            _families = obj['families']
            solos = obj['no_family']
            logging.info(obj)
            for family_id, status in _families.items():
                logging.info(status)
                for s in status:
                    if 'family' in s['name']:
                        clin_s3.load_string('',
                                            f'raw/landing/franklin/batch_id={batch}/family_id={family_id}/aliquot_id=null/analysis_id={s["id"]}/_IN_PROGRESS.txt',
                                            export_bucket, replace=True)
                    else:
                        path = f'raw/landing/franklin/batch_id={batch}/family_id={family_id}/aliquot_id={s["name"].split(" - ")[0]}/analysis_id={s["id"]}/_IN_PROGRESS.txt'
                        clin_s3.load_string('', path, export_bucket, replace=True)

            for p in solos:
                logging.info(p)
                patient = p[0]
                clin_s3.load_string('',
                                    f'raw/landing/franklin/batch_id={batch}/family_id=null/aliquot_id={patient["name"].split(" - ")[0]}/analysis_id={patient["id"]}/_IN_PROGRESS.txt',
                                    export_bucket, replace=True)
            return True


        def list_analysis(batch):
            keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch}/')
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
            analyses_ids=list_analysis(batch_id()),
            mode='poke',
            poke_interval=franklin_poke,
            timeout=franklin_timeout,
            dag=dag,
        )


        @task
        def transfer_from_franklin_to_clin(_batch_id, token):
            obj = build_cases(_batch_id)
            _families = obj['families']
            solos = obj['no_family']
            logging.info(obj)
            for family_id, analyses in _families.items():
                for analysis in analyses:
                    logging.info(f'analysis {analysis}')
                    if analysis['labAliquotId'] is None:
                        path = f'raw/landing/franklin/batch_id={batch_id()}/family_id={family_id}/aliquot_id=null/analysis.json'
                    else:
                        path = f'raw/landing/franklin/batch_id={batch_id()}/family_id={family_id}/aliquot_id={analysis["labAliquotId"]}/analysis_id={analysis["id"]}/analysis.json'
                    data = get_completed_analysis(analysis["id"], token)
                    clin_s3.load_string(data, path, export_bucket, replace=True)

            for patient in solos:
                logging.info(f'patient is {patient}')
                path = f'raw/landing/franklin/batch_id={batch_id()}/family_id=null/aliquot_id={patient["labAliquotId"].split(" - ")[0]}/analysis_id={patient["id"]}/analysis.json'
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
            # {'families': {'LDM_FAM0': [{'id':3, 'labAliquotId': 12345}, {'id':4, 'labAliquotId': 12346}, {'id':5, 'labAliquotId': 12347}, {'id':6, 'labAliquotId': None}]}, 'no_family': []}
            to_return = {
                'families': {},
                'no_family': []
            }
            logging.info(f'build cases {_batch_id}')
            keys = clin_s3.list_keys(import_bucket, f'batch_id={_batch_id}/')
            for key in keys:
                if 'family_id=null' not in key:
                    family_id = key.split('family_id=')[1].split('/')[0]
                    to_return['families'][family_id] = get_family_members(keys, family_id)
                else:
                    to_return['no_family'].append({'id': key.split('analysis_id=')[1].split('/')[0],
                                                   'labAliquotId': key.split('aliquot_id=')[1].split('/')[0]})

            logging.info(f'build cases returned {to_return}')
            return to_return


        mark_analyses_as_started(
            get_statuses(
                _start_analysis(
                    upload_files_to_franklin(check_file_existence(_group_families(batch_id()), batch_id()), batch_id()),
                    auth(),
                    batch_id()),
                auth()), batch_id()) >> api_sensor >> transfer_from_franklin_to_clin(batch_id(), auth())


    def check_franklin_needed(_batch_id, trigger):
        metadata_path = f'{_batch_id}/metadata.json'
        logging.info(f'path {metadata_path} || trigger = {trigger}')
        f = clin_s3.get_key(metadata_path, import_bucket)
        file_content = json.loads(f.get()['Body'].read().decode('utf-8'))
        logging.info(f'file content {file_content["submissionSchema"] == "CQGC_Germline"}')
        logging.info(f'trigger {trigger}')
        if trigger == 'yes' and 'CQGC_Germline' == file_content['submissionSchema']:
            return True
        else:
            return False


    cond_true = ShortCircuitOperator(
        task_id="checking_schema_germline",
        python_callable=check_franklin_needed,
        op_args=[batch_id(), trigger_franklin()],
    )

    chain(cond_true, franklin)
