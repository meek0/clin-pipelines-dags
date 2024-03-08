import logging

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from lib import config
from lib.config import clin_datalake_bucket
from lib.franklin import (FranklinStatus, build_s3_analyses_ids_key,
                          extract_from_name_aliquot_id,
                          extract_from_name_family_id,
                          extract_param_from_s3_key, get_analysis_status,
                          get_franklin_token, write_s3_analysis_status)


class FranklinAPISensor(BaseSensorOperator):

    template_fields = BaseSensorOperator.template_fields + (
        'skip', 'batch_id',
    )

    def __init__(self, batch_id, skip: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip = skip
        self.batch_id = batch_id

    def poke(self, context):
        if self.skip:
            raise AirflowSkipException()

        batch_id = self.batch_id
        clin_s3 = S3Hook(config.s3_conn_id)

        keys = clin_s3.list_keys(clin_datalake_bucket, f'raw/landing/franklin/batch_id={batch_id}/')

        created_analyses = []
        ready_analyses = []

        for key in keys:
            logging.info(f'key: {key}')
            if '_FRANKLIN_STATUS_.txt' in key:
                key_obj = clin_s3.get_key(key, clin_datalake_bucket)
                status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                if status is FranklinStatus.CREATED:    # ignore others status

                    logging.info(f'CREATED: {key}')
                    family_id = extract_param_from_s3_key(key, 'family_id') 
                    aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                    ids_key = clin_s3.get_key(build_s3_analyses_ids_key(batch_id, family_id, aliquot_id), clin_datalake_bucket)
                    ids = ids_key.get()['Body'].read().decode('utf-8').split(',')

                    created_analyses += ids

        # remove duplicated IDs if any
        created_analyses = list(set(created_analyses))
        created_count = len(created_analyses)

        if created_count == 0:
            raise AirflowSkipException('No CREATED analyses')

        token = get_franklin_token()
        statuses = get_analysis_status(created_analyses, token)
        for status in statuses:
            if (status['processing_status'] == 'READY'):

                analysis_id = str(status['id'])

                if (analysis_id in created_analyses):
                    analysis_aliquot_id = extract_from_name_aliquot_id(status['name'])
                    analysis_family_id = extract_from_name_family_id(status['name'])

                    write_s3_analysis_status(clin_s3, batch_id, analysis_family_id, analysis_aliquot_id, FranklinStatus.READY, id=analysis_id)

                    ready_analyses.append(analysis_id)
                else:
                    logging.warn(f'Unexpected analyse ID: {analysis_id} in status response')

        ready_count = len(ready_analyses)

        logging.info(f'Ready analyses: {ready_count}/{created_count} {ready_analyses}')
        return ready_count == created_count  # All created analyses are ready
