from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.config import env, es_url, Env, K8sContext
from lib.operators.curl import CurlOperator
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.slack import Slack


if env == Env.QA:

    with DAG(
        dag_id='etl_rolling',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('', type='string'),
            'color': Param('', enum=['', 'blue', 'green']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
    ) as dag:

        def release_id() -> str:
            return '{{ params.release_id }}'

        def color(prefix: str = '') -> str:
            return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

        def _params_validate(release_id, color):
            if release_id == '':
                raise AirflowFailException(
                    'DAG param "release_id" is required'
                )
            if env == Env.QA:
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
            op_args=[release_id(), color()],
            python_callable=_params_validate,
            on_execute_callback=Slack.notify_dag_start,
        )

        es_indices_swap = CurlOperator(
            task_id='es_indices_swap',
            name='etl-rolling-es-indices-swap',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '-f', '-X', 'POST', f'{es_url}/_aliases',
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
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_coverage_by_gene_centric" }} }},
                        {{ "add": {{ "index": "clin-{env}-analyses{dash_color}", "alias": "clin-{env}-analyses" }} }},
                        {{ "add": {{ "index": "clin-{env}-sequencings{dash_color}", "alias": "clin-{env}-sequencings" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_gene_centric_{release_id}", "alias": "clin_{env}_gene_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_gene_suggestions_{release_id}", "alias": "clin_{env}_gene_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_variant_centric_{release_id}", "alias": "clin_{env}_variant_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_cnv_centric_{release_id}", "alias": "clin_{env}_cnv_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_variant_suggestions_{release_id}", "alias": "clin_{env}_variant_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_coverage_by_gene_centric_{release_id}", "alias": "clin_{env}_coverage_by_gene_centric" }} }}
                    ]
                }}
                '''.format(
                    env=env,
                    release_id=release_id(),
                    dash_color=color('-'),
                    under_color=color('_'),
                ),
            ],
        )

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

        arranger_restart = K8sDeploymentRestartOperator(
            task_id='arranger_restart',
            k8s_context=K8sContext.DEFAULT,
            deployment='arranger',
            on_success_callback=Slack.notify_dag_completion,
        )

        params_validate >> es_indices_swap >> arranger_remove_project >> arranger_restart
