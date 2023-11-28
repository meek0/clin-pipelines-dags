import logging

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from lib import config
from lib.franklin import (FranklinStatus, buildS3AnalysesIdsKey, export_bucket,
                          extract_from_name_aliquot_id, extractParamFromS3Key,
                          get_analysis_status, get_franklin_token,
                          writeS3AnalysisStatus)


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

        keys = clin_s3.list_keys(export_bucket, f'raw/landing/franklin/batch_id={batch_id}/')

        token = get_franklin_token()  

        families_done = []  # remember families we already ask for statuses, avoid spamming Franklin  
        ready_analyses = []
        total_created = 0
        total_ready = 0

        for key in keys:
            if '_FRANKLIN_STATUS_.txt' in key:
                key_obj = clin_s3.get_key(key, export_bucket)
                status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                if status is FranklinStatus.CREATED:    # ignore others status

                    family_id = extractParamFromS3Key(key, 'family_id') 
                    aliquot_id = extractParamFromS3Key(key, 'aliquot_id')

                    # SOLO are family_id = None otherwise check already done families
                    if family_id is None or family_id not in families_done:
                        families_done.append(family_id)

                        ids_key = clin_s3.get_key(buildS3AnalysesIdsKey(batch_id, family_id, aliquot_id), export_bucket)
                        ids = ids_key.get()['Body'].read().decode('utf-8').split(',')

                        total_created += len(ids)   # count of IDs may vary DUO, TRIO ...

                        statuses = get_analysis_status(ids, token)
                        for status in statuses:
                            if (status['processing_status'] == 'READY'):

                                total_ready += 1

                                analysis_id = status['id']
                                analysis_aliquot_id = extract_from_name_aliquot_id(status['name'])

                                writeS3AnalysisStatus(clin_s3, batch_id, family_id, analysis_aliquot_id, FranklinStatus.READY, id=analysis_id)

                                ready_analyses.append(analysis_id)

        logging.info(f'Ready analyses: {total_ready}/{total_created} {ready_analyses}')
        return total_ready == total_created;  # All created analyses are ready
