import logging

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from lib import config
from lib.franklin import (FranklinStatus, buildS3AnalysesIdsKey, export_bucket,
                          extract_from_name_aliquot_id,
                          extract_from_name_family_id, extractParamFromS3Key,
                          get_analysis_status, get_franklin_token,
                          writeS3AnalysisStatus)


class FranklinAPISensor(BaseSensorOperator):

    template_fields = BaseSensorOperator.template_fields + (
        'batch_id',
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
        created_analyses = []

        keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')

        if len(keys) == 0:  # nothing in that batch about Franklin
            raise AirflowSkipException()

        for key in keys:
            if '_FRANKLIN_STATUS_.txt' in key:
                key_obj = clin_s3.get_key(key, export_bucket)
                status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                if status is FranklinStatus.CREATED:    # ignore others status
                    aliquot_id = extractParamFromS3Key(key, 'aliquot_id')
                    family_id = extractParamFromS3Key(key, 'family_id') 
                    ids_key = clin_s3.get_key(buildS3AnalysesIdsKey(batch_id, family_id, aliquot_id), export_bucket)
                    created_analyses += ids_key.get()['Body'].read().decode('utf-8').split(',')

        # remove all possible duplicated analysis
        created_analyses = list(set(created_analyses))

        logging.info(f'Started analyses: {len(created_analyses)} {created_analyses}')

        if len(created_analyses) == 0:  # All created analyses are ready
            return True

        token = get_franklin_token()
        statuses = get_analysis_status(created_analyses, token)

        ready_count = 0
        for status in statuses:
            if (status['processing_status'] == 'READY'):
                analysis_id = status['id']
                aliquot_id = extract_from_name_aliquot_id(status['name'])
                family_id = extract_from_name_family_id(status['name'])
                writeS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id, FranklinStatus.READY, id=analysis_id)
                ready_count+=1

        logging.info(f'READY: {ready_count}/{len(statuses)}')

        return ready_count == len(statuses)
