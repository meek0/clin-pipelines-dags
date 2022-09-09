from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator


with DAG(
    dag_id='etl_reset_enrich_cnv',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    etl_reset_enrich_cnv = SparkOperator(
        task_id='etl_reset_enrich_cnv',
        name='etl-reset-enrich-cnv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='enriched-etl',
        arguments=[
            f'config/{env}.conf', 'initial', 'cnv',
        ],
    )
