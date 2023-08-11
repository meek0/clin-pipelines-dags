from airflow.utils.task_group import TaskGroup
from lib.config import env_url, Env, K8sContext
from lib.doc import qa as doc
from lib.operators.spark import SparkOperator
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib.config import env, es_url, Env, K8sContext
from lib.groups.qa import qa
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack

def IngestFhir(
    group_id: str,
    batch_id: str,
    color: str,
    skip_import: str,
    skip_batch: str,
    spark_jar: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        fhir_import = PipelineOperator(
            task_id='fhir_import',
            name='etl-ingest-fhir-import',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-files-import',
            color=color,
            skip=skip_import,
            arguments=[
                'bio.ferlab.clin.etl.FileImport', batch_id, 'false', 'true',
            ],
        )

        fhir_export = PipelineOperator(
            task_id='fhir_export',
            name='etl-ingest-fhir-export',
            k8s_context=K8sContext.DEFAULT,
            aws_bucket=f'cqgc-{env}-app-datalake',
            color=color,
            skip=skip_batch,
            arguments=[
                'bio.ferlab.clin.etl.FhirExport', 'all',
            ],
        )

        fhir_normalize = SparkOperator(
            task_id='fhir_normalize',
            name='etl-ingest-fhir-normalize',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.fhir.FhirRawToNormalized',
            spark_config='raw-fhir-etl',
            skip=skip_batch,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'initial', 'all',
            ],
        )

        fhir_import >> fhir_export >> fhir_normalize

    return group
