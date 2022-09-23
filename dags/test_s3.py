import logging
import requests
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib import config


if (config.show_test_dags):

    with DAG(
        dag_id='test_s3',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
    ) as dag:

        def _test_s3():
            # http://ipv4.download.thinkbroadband.com/5MB.zip
            # http://ipv4.download.thinkbroadband.com/100MB.zip
            # http://ipv4.download.thinkbroadband.com/1GB.zip

            url = 'http://ipv4.download.thinkbroadband.com/5MB.zip'
            path = 'file.zip'

            logging.info('download')
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                with open(path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

            logging.info('upload')
            s3 = S3Hook('minio')
            s3.load_file(
                path,
                key=path,
                bucket_name='airflow',
            )

        test_s3 = PythonOperator(
            task_id='test_s3',
            python_callable=_test_s3,
        )

        test_s3
