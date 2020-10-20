from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

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

# DAG
with DAG(dag_id="clin_genomic_data_pipeline",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    # Replace the following by S3Sensor - check for _success file to launch the flow
    checkDataTaskStatus = KubernetesPodOperator(
        name="check-new-files-on-s3",
        task_id="check-new-files-on-s3",
        namespace="default",
        labels={
            "app": "clin-pipeline-task"
        },
        image="192.168.0.16:5000/clin-pipelines:2020.1",
        image_pull_policy="Always", # local development - use IfNotPresent in prod
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
        image_pull_policy="Always", # local development - use IfNotPresent in prod
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
        image_pull_policy="Always", # local development - use IfNotPresent in prod
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

    checkDataTaskStatus.set_upstream(start)
    checkDataTaskStatus.set_downstream(loadDataIntoHapiFhirTaskStatus)

    loadDataIntoHapiFhirTaskStatus.set_upstream(checkDataTaskStatus)
    loadDataIntoHapiFhirTaskStatus.set_downstream(extractNDJsonFromHapiFhirTaskStatus)

    extractNDJsonFromHapiFhirTaskStatus.set_upstream(loadDataIntoHapiFhirTaskStatus)
    extractNDJsonFromHapiFhirTaskStatus.set_downstream(end)
