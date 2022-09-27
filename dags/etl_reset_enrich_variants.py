from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator


with DAG(
    dag_id='etl_reset_enrich_variants',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    reset_enrich_variants = SparkOperator(
        task_id='reset_enrich_variants',
        name='etl-reset-enrich-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='enriched-etl',
        arguments=[
            f'config/{env}.conf', 'initial', 'variants',
        ],
    )
