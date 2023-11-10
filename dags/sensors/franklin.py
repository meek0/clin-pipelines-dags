import logging

from airflow.sensors.base import BaseSensorOperator
from lib import config
from lib.franklin import get_analyses_status, authenticate


class FranklinAPISensor(BaseSensorOperator):
    def __init__(self, analyses_ids, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.info(f'franklin sensor init {args} {kwargs}')
        self.analyses_ids = analyses_ids

    def poke(self, context):
        try:
            logging.info(f'analysis are {self.analyses_ids}')
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
