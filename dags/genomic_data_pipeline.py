from typing import Pattern

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
import re

# Global config
default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 12),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "plaplante@ferlab.bio",
    "retry_delay": timedelta(minutes=5)
}

BUCKET_KEY: str = 'batches/batch=*/_SUCCESS'
BUCKET_NAME: str = 'clin'
OBJECT_STORE = 'clin_object_store'
BUCKET_REGEXP: Pattern[str] = re.compile('batches/batch=(.*)/_SUCCESS')


def batchInformations(ds, **kwargs):
    from airflow.hooks.S3_hook import S3Hook
    hook = S3Hook(aws_conn_id=OBJECT_STORE, verify=None)
    key: str = hook.get_wildcard_key(BUCKET_KEY, BUCKET_NAME).key
    batch_key: str = key.replace('/_SUCCESS', '')
    batch_id: str = BUCKET_REGEXP.findall(key)[0]
    return {'key': f"{BUCKET_NAME}/{batch_key}", 'batch_id': batch_id}


# DAG
with DAG(dag_id="clin_genomic_data_pipeline",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    start = DummyOperator(task_id='start')

    sensor = S3KeySensor(
        task_id='check_s3_for_file_in_s3',
        bucket_key=BUCKET_KEY,
        wildcard_match=True,
        bucket_name=BUCKET_NAME,
        aws_conn_id=OBJECT_STORE,
        timeout=18 * 60 * 60,
        poke_interval=20,
        dag=dag)

    extractBatchInformations = PythonOperator(
        task_id='extract_batch_informations',
        provide_context=True,
        python_callable=batchInformations,
        dag=dag,
    )

    test = BashOperator(
        task_id='bash_test',
        provide_context=True,
        bash_command="echo {{ task_instance.xcom_pull(task_ids='extract_batch_informations').key }}",
        dag=dag)

    # Replace the following by S3Sensor - check for _success file to launch the flow
    checkDataTaskStatus = KubernetesPodOperator(
        name="check-new-files-on-s3",
        task_id="check-new-files-on-s3",
        namespace="default",
        labels={
            "app": "clin-pipeline-task"
        },
        image="192.168.0.16:5000/clin-pipelines:2020.1",
        image_pull_policy="Always",  # local development - use IfNotPresent in prod
        env_vars={
            "SERVICE_ENDPOINT": "http://192.168.0.16:9000"
        },
        arguments=["check-new-files-on-s3"],
        get_logs=True,
        hostnetwork=True,
        in_cluster=False
    )

    # Create specimen and samples in Fhir
    loadDataIntoHapiFhirTaskStatus = KubernetesPodOperator(
        name="load-metadata-in-fhir",
        task_id="load-metadata-in-fhir",
        namespace="default",
        labels={
            "app": "clin-pipeline-task"
        },
        image="192.168.0.16:5000/clin-pipelines:2020.1",
        image_pull_policy="Always",  # local development - use IfNotPresent in prod
        env_vars={
            "SERVICE_ENDPOINT": "http://192.168.0.16:9000"
        },
        arguments=["load-metadata-in-fhir"],
        get_logs=True,
        hostnetwork=True,
        in_cluster=False
    )

    # Extract patients, specimens and samples to give as input for the ETL
    extractNDJsonFromHapiFhirTaskStatus = KubernetesPodOperator(
        name="extract-fhir-data-for-etl",
        task_id="extract-fhir-data-for-etl",
        namespace="default",
        labels={
            "app": "clin-pipeline-task"
        },
        image="192.168.0.16:5000/clin-pipelines:2020.1",
        image_pull_policy="Always",  # local development - use IfNotPresent in prod
        env_vars={
            "SERVICE_ENDPOINT": "http://192.168.0.16:9000"
        },
        arguments=["extract-fhir-data-for-etl"],
        get_logs=True,
        hostnetwork=True,
        in_cluster=False
    )

    # Trigger ETL
    # See with Jeremy how to launch his dockerized ETL

    # End of the pipeline
    end = DummyOperator(task_id='end')

    sensor.set_upstream(start)
    extractBatchInformations.set_upstream(sensor)
    checkDataTaskStatus.set_upstream(extractBatchInformations)
    test.set_upstream(extractBatchInformations)
    checkDataTaskStatus.set_downstream(loadDataIntoHapiFhirTaskStatus)

    loadDataIntoHapiFhirTaskStatus.set_upstream(checkDataTaskStatus)
    loadDataIntoHapiFhirTaskStatus.set_downstream(extractNDJsonFromHapiFhirTaskStatus)

    extractNDJsonFromHapiFhirTaskStatus.set_upstream(loadDataIntoHapiFhirTaskStatus)
    extractNDJsonFromHapiFhirTaskStatus.set_downstream(end)
