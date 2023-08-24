from airflow.utils.task_group import TaskGroup

from lib.config import K8sContext
from lib.config import config_file
from lib.operators.spark import SparkOperator


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
    skip_coverage_by_gene: str,
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
                'snv',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_snv',
                '--batchId', batch_id
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
                'snv_somatic_tumor_only',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_snv_somatic',
                '--batchId', batch_id
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
                'cnv',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_cnv',
                '--batchId', batch_id
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
                'cnv_somatic_tumor_only',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_cnv_somatic',
                '--batchId', batch_id
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
                'variants',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_variants',
                '--batchId', batch_id
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
                'consequences',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_consequences',
                '--batchId', batch_id
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
                'exomiser',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_exomiser',
                '--batchId', batch_id
            ],
        )

        coverage_by_gene = SparkOperator(
            task_id=getUniqueId('coverage_by_gene'),
            name='etl-ingest-coverage-by-gene',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='raw-vcf-etl',
            skip=skip_coverage_by_gene,
            spark_jar=spark_jar,
            arguments=[
                'coverage_by_gene',
                '--config', config_file,
                '--steps', 'default',
                '--app-name', 'etl_ingest_coverage_by_gene',
                '--batchId', batch_id
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

        snv >> snv_somatic_tumor_only >> cnv >> cnv_somatic_tumor_only >> variants >> consequences >> exomiser >> coverage_by_gene

    return group
