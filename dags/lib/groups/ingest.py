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

def ingest(
    group_id: str,
    batch_id: str,
    color: str,
    skip_import: str,
    skip_batch: str,
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
            arguments=[
                f'config/{env}.conf', 'initial', 'all',
            ],
        )

        snv = SparkOperator(
            task_id='snv',
            name='etl-ingest-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'snv',
            ],
        )

        snv_somatic = SparkOperator(
            task_id='snv_somatic',
            name='etl-ingest-snv-somatic',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'snv_somatic',
            ],
        )

        cnv = SparkOperator(
            task_id='cnv',
            name='etl-ingest-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'cnv',
            ],
        )

        variants = SparkOperator(
            task_id='variants',
            name='etl-ingest-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'variants',
            ],
        )

        consequences = SparkOperator(
            task_id='consequences',
            name='etl-ingest-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'consequences',
            ],
        )

        exomiser = SparkOperator(
            task_id='exomiser',
            name='etl-ingest-exomiser',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'exomiser',
            ],
        )

        '''
        varsome = SparkOperator(
            task_id='varsome',
            name='etl-ingest-varsome',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.varsome.Varsome',
            spark_config='varsome-etl',
            spark_secret='varsome',
            skip=skip_batch,
            arguments=[
                f'config/{env}.conf', 'default', 'all', batch_id
            ],
            skip_env=[Env.QA, Env.STAGING],
        )
        '''

        fhir_import >> fhir_export >> fhir_normalize >> snv >> snv_somatic >> cnv >> variants >> consequences >> exomiser

    return group
