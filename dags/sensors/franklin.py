import logging

from airflow.sensors.base import BaseSensorOperator

from lib import config
from lib.franklin import get_analyses_status, authenticate


class FranklinAPISensor(BaseSensorOperator):
    def __init__(self, s3, export_bucket, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.info(f'franklin sensor init {args} {kwargs}')
        self.s3 = s3
        self.export_bucket = export_bucket

    def list_analysis(self, batch):
        keys = self.s3.list_keys(self.export_bucket, f'raw/landing/franklin/batch_id={batch}/')

        logging.info(f'list analysis {keys}')
        ids = []
        for key in keys:
            if 'analysis_id=' in key:
                _id = key.split('analysis_id=')[1].split('/')[0]
                ids.append(_id)
        logging.info(f'ids are {ids}')
        return ids

    def poke(self, context):
        try:
            self.analyses_ids = self.list_analysis(context['params']['batch_id'])
            logging.info(f'analysis are {self.analyses_ids} {context}')
            token = authenticate(email=config.franklin_email, password=config.franklin_password)

            response = get_analyses_status(self.analyses_ids, token)
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
