from airflow import DAG
from datetime import datetime
from lib.etl import config
from lib.etl.config import K8sContext
from lib.etl.operators.spark import SparkOperator


with DAG(
    dag_id='etl_external',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    environment = config.environment

    panels = SparkOperator(
        task_id='panels',
        name='etl-external-panels',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.external.ImportExternal',
        spark_config='raw-import-external-etl',
        arguments=[
            f'config/{environment}.conf', 'initial', 'panels',
        ],
    )

    mane_summary = SparkOperator(
        task_id='mane_summary',
        name='etl-external-mane-summary',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.external.ImportExternal',
        spark_config='raw-import-external-etl',
        arguments=[
            f'config/{environment}.conf', 'initial', 'mane-summary',
        ],
    )

    refseq_annotation = SparkOperator(
        task_id='refseq_annotation',
        name='etl-external-refseq-annotation',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.external.ImportExternal',
        spark_config='raw-import-external-etl',
        arguments=[
            f'config/{environment}.conf', 'initial', 'refseq-annotation',
        ],
    )

    refseq_feature = SparkOperator(
        task_id='refseq_feature',
        name='etl-external-refseq-feature',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.external.ImportExternal',
        spark_config='raw-import-external-etl',
        arguments=[
            f'config/{environment}.conf', 'initial', 'refseq-feature',
        ],
    )

    gene_tables = SparkOperator(
        task_id='gene_tables',
        name='etl-external-gene-tables',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.external.CreateGenesTable',
        spark_config='genes-tables-creation',
        arguments=[
            f'config/{environment}.conf', 'initial',
        ],
    )

    # public_tables = SparkOperator(
    #     task_id='public_tables',
    #     name='etl-external-public-tables',
    #     k8s_context=K8sContext.ETL,
    #     spark_class='bio.ferlab.clin.etl.external.CreatePublicTables',
    #     spark_config='public-tables-creation-etl',
    #     arguments=[
    #         f'config/{environment}.conf', 'initial',
    #     ],
    # )

    # varsome = SparkOperator(
    #     task_id='varsome',
    #     name='etl-external-varsome',
    #     k8s_context=K8sContext.ETL,
    #     spark_class='bio.ferlab.clin.etl.varsome.Varsome',
    #     spark_config='varsome-etl',
    #     spark_secret='varsome',
    #     arguments=[
    #         f'config/{environment}.conf', 'initial', 'all',
    #     ],
    # )

    panels >> mane_summary >> refseq_annotation >> refseq_feature >> gene_tables
