from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator


with DAG(
    dag_id='test',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    arranger_remove_project = ArrangerOperator(
        task_id='arranger_remove_project',
        name='etl-publish-arranger-remove-project',
        k8s_context=K8sContext.DEFAULT,
        cmds=[
            'node',
            '--experimental-modules=node',
            '--es-module-specifier-resolution=node',
            'cmd/remove_project.js',
            env,
        ],
    )

    # arranger_restart = K8sDeploymentRestartOperator(
    #     task_id='arranger_restart',
    #     k8s_context=K8sContext.DEFAULT,
    #     deployment='arranger',
    # )

    arranger_remove_project >> arranger_restart
