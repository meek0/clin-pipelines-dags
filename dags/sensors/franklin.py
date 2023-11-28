import logging

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from lib import config
from lib.franklin import (FranklinStatus, export_bucket,
                          extract_from_name_aliquot_id,
                          extract_from_name_family_id, get_analysis_status,
                          get_franklin_http_conn, get_franklin_token,
                          writeS3AnalysisStatus)


class FranklinAPISensor(BaseSensorOperator):

    template_fields = BaseSensorOperator.template_fields + (
        'skip', 'batch_id',
    )

    def __init__(self, batch_id, skip: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip = skip
        self.batch_id = batch_id

    '''
    def list_analysis(self):
     
        clin_s3 = S3Hook(config.s3_conn_id)
        matching_keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch}/')

        logging.info(f'list analysis {matching_keys}')

        ids = []
        for key in keys:
            if 'analysis_id=' in key:
                _id = key.split('analysis_id=')[1].split('/')[0]
                ids.append(_id)
        logging.info(f'ids are {ids}')
 
        return matching_keys
        '''

    def poke(self, context):
        if self.skip:
            raise AirflowSkipException()

        batch_id = self.batch_id
        started_analyses = []

        clin_s3 = S3Hook(config.s3_conn_id)
        keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
        for key in keys:
            if '_FRANKLIN_IDS_.txt' in key:
                key_obj = clin_s3.get_key(key, export_bucket)
                started_analyses += key_obj.get()['Body'].read().decode('utf-8').split(',')

        logging.info(f'Started analyses: {len(started_analyses)} {started_analyses}')

        conn = get_franklin_http_conn()
        token = get_franklin_token(conn)
        statuses = get_analysis_status(conn, started_analyses, token)

        readdy_count = 0
        for status in statuses:
            if (status['processing_status'] == 'READY'):
                analysis_id = status['id']
                aliquot_id = extract_from_name_aliquot_id(status['name'])
                family_id = extract_from_name_family_id(status['name'])
                writeS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id, FranklinStatus.READY)
                readdy_count+=1

        logging.info(f'READY: {readdy_count}/{len(statuses)}')

        return readdy_count == len(statuses)
    '''
        try:
            self.analyses_ids = self.list_analysis(context['params']['batch_id'])
            logging.info(f'analysis are {self.analyses_ids} {context}')
            conn = get_franklin_http_conn() # avoid instancing too many
            token = get_franklin_token(conn)

            response = get_analysis_status(conn, self.analyses_ids, token)
            all_ready = False

            for analysis in response:
                logging.info(f'status {analysis["processing_status"]}')
                if analysis['processing_status'] == 'READY':
                    all_ready = True
                else:
                    all_ready = False
                    break

            logging.info(f'all ready {all_ready}')
            return all_ready
        except Exception as e:
            logging.error("API call failed with error: %s", str(e))
            return False
    '''