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

def IngestBatch(
    group_id: str,
    batch_id: str,
    skip_snv: str,
    skip_snv_somatic_tumor_only: str,
    skip_cnv: str,
    skip_cnv_somatic_tumor_only: str,
    skip_variants: str,
    skip_consequences: str,
    skip_exomiser: str,
    spark_jar: str,
    batch_id_as_tag = False,
) -> TaskGroup:

    def getUniqueId(taskOrGrp: str) -> str:
        if batch_id_as_tag:
            return taskOrGrp + '_' + batch_id
        else:
            return taskOrGrp

    with TaskGroup(group_id=getUniqueId(group_id)) as group:

        snv = SparkOperator(
            task_id=getUniqueId('snv'),
            name='etl-ingest-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_snv,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'snv',
            ],
        )

        snv_somatic_tumor_only = SparkOperator(
            task_id=getUniqueId('snv_somatic_tumor_only'),
            name='etl-ingest-snv-somatic',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_snv_somatic_tumor_only,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'snv_somatic_tumor_only',
            ],
        )

        cnv = SparkOperator(
            task_id=getUniqueId('cnv'),
            name='etl-ingest-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_cnv,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'cnv',
            ],
        )

        cnv_somatic_tumor_only = SparkOperator(
            task_id=getUniqueId('cnv_somatic_tumor_only'),
            name='etl-ingest-cnv-somatic',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_cnv_somatic_tumor_only,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'cnv_somatic_tumor_only',
            ],
        )

        variants = SparkOperator(
            task_id=getUniqueId('variants'),
            name='etl-ingest-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_variants,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'variants',
            ],
        )

        consequences = SparkOperator(
            task_id=getUniqueId('consequences'),
            name='etl-ingest-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_consequences,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'consequences',
            ],
        )

        exomiser = SparkOperator(
            task_id=getUniqueId('exomiser'),
            name='etl-ingest-exomiser',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_exomiser,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', batch_id, 'exomiser',
            ],
        )

        '''
        varsome = SparkOperator(
            task_id=getUniqueId('varsome',
            name='etl-ingest-varsome',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.varsome.Varsome',
            spark_config='varsome-etl',
            spark_secret='varsome',
            skip=skip_batch,
            spark_jar=spark_jar,
            arguments=[
                f'config/{env}.conf', 'default', 'all', batch_id
            ],
            skip_env=[Env.QA, Env.STAGING],
        )
        '''

        snv >> snv_somatic_tumor_only >> cnv >> cnv_somatic_tumor_only >> variants >> consequences >> exomiser

    return group
