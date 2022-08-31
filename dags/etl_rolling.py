from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib import config
from lib.config import K8sContext
from lib.operators.curl import CurlOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator


with DAG(
    dag_id='etl_rolling',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'release': Param('', type='string'),
        'color': Param('', enum=['', 'blue', 'green']),
    },
) as dag:

    env = config.environment

    def release() -> str:
        return '{{ params.release }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def _params_validate(release, color):
        if release == '':
            raise AirflowFailException(
                'DAG param "release" is required'
            )
        if env == 'qa':
            if color == '':
                raise AirflowFailException(
                    f'DAG param "color" is required in {env} environment'
                )
        else:
            raise AirflowFailException(
                f'DAG run is forbidden in {env} environment'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[release(), color()],
        python_callable=_params_validate,
    )

    es_indices_swap = CurlOperator(
        task_id='es_indices_swap',
        name='etl-rolling-es-indices-swap',
        k8s_context=K8sContext.DEFAULT,
        arguments=[
            '-f', '-X', 'POST', 'http://elasticsearch:9200/_aliases',
            '-H', 'Content-Type: application/json', '-d',
            '''
            {{
                "actions": [
                    {{ "remove": {{ "index": "*", "alias": "clin-{env}-analyses" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin-{env}-sequencings" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin_{env}_gene_centric" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin_{env}_cnv_centric" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin_{env}_gene_suggestions" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin_{env}_variant_centric" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin_{env}_variant_suggestions" }} }},
                    {{ "add": {{ "index": "clin-{env}-analyses{dash_color}", "alias": "clin-{env}-analyses" }} }},
                    {{ "add": {{ "index": "clin-{env}-sequencings{dash_color}", "alias": "clin-{env}-sequencings" }} }},
                    {{ "add": {{ "index": "clin_{env}{under_color}_gene_centric_{release}", "alias": "clin_{env}_gene_centric" }} }},
                    {{ "add": {{ "index": "clin_{env}{under_color}_gene_suggestions_{release}", "alias": "clin_{env}_gene_suggestions" }} }},
                    {{ "add": {{ "index": "clin_{env}{under_color}_variant_centric_{release}", "alias": "clin_{env}_variant_centric" }} }},
                    {{ "add": {{ "index": "clin_{env}{under_color}_cnv_centric_{release}", "alias": "clin_{env}_cnv_centric" }} }},
                    {{ "add": {{ "index": "clin_{env}{under_color}_variant_suggestions_{release}", "alias": "clin_{env}_variant_suggestions" }} }}
                ]
            }}
            '''.format(
                env=env,
                release=release(),
                dash_color=color('-'),
                under_color=color('_'),
            ),
        ],
    )

    arranger_restart = K8sDeploymentRestartOperator(
        task_id='arranger_restart',
        k8s_context=K8sContext.DEFAULT,
        deployment='arranger',
    )

    es_indices_swap >> arranger_restart
