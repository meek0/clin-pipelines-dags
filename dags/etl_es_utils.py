import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, K8sContext, env, es_url
from lib.groups.es import (format_es_url, test_disk_usage,
                           test_duplicated_by_url)
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.tasks import arranger
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, release_id, skip_if_param_not

with DAG(
        dag_id='etl_es_utils',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'delete_release': Param('no', enum=['yes', 'no']),
            'test_duplicated_variants': Param('no', enum=['yes', 'no']),
            'show_indexes': Param('no', enum=['yes', 'no']),
            'test_disk_usage': Param('no', enum=['yes', 'no']),
            'release_id': Param('', type=['null', 'string']),
            'color': Param('', type=['null', 'string']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
            'trigger_rule': TriggerRule.ALL_DONE,   # as opposed to other dags, we run everything even if a previous task fails
        },
) as dag:


    def show_indexes() -> str:
        return '{{ params.show_indexes or "" }}'

    def _test_disk_usage() -> str:
        return '{{ params.test_disk_usage or "" }}'

    def delete_release() -> str:
        return '{{ params.delete_release or "" }}'

    def test_duplicated_variants() -> str:
        return '{{ params.test_duplicated_variants or "" }}'

    params_validate = validate_color(color())

    def _validate_action_params(delete_release, test_duplicated_variants, release_id):
        if (delete_release == 'yes' or test_duplicated_variants == 'yes') and release_id == '':
            raise AirflowFailException('release_id is required for delete_release')

    params_action_validate = PythonOperator(
        task_id='params_action_validate',
        op_args=[delete_release(), test_duplicated_variants(), release_id()],
        python_callable=_validate_action_params,
    )

    es_delete_release = CurlOperator(
        task_id='es_delete_release',
        name='es-delete-release',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(delete_release(), "yes"),
        arguments=[
            '-k', '--location', '--request', 'DELETE', '{es_url}/clin_{env}{under_color}_gene_suggestions_{release_id},clin_{env}{under_color}_variant_suggestions_{release_id},clin_{env}{under_color}_gene_centric_{release_id},clin_{env}{under_color}_variant_centric_{release_id},clin_{env}{under_color}_cnv_centric_{release_id},clin_{env}{under_color}_coverage_by_gene_centric_{release_id}?ignore_unavailable=true'
            .format(
                es_url=es_url,
                env=env,
                release_id=release_id(),
                under_color=color('_'),
                ),
        ],
    )
    
    es_test_duplicated_release_variant = PythonOperator(
        task_id='es_test_duplicated_release_variant',
        python_callable=test_duplicated_by_url,
        op_args=[
            format_es_url('variant', _color=color(), release_id=release_id()),
            skip_if_param_not(test_duplicated_variants(), "yes")
            ],
        dag=dag,
    )

    es_test_duplicated_release_cnv = PythonOperator(
        task_id='es_test_duplicated_release_cnv',
        python_callable=test_duplicated_by_url,
        op_args=[
            format_es_url('cnv', _color=color(), release_id=release_id()),
            skip_if_param_not(test_duplicated_variants(), "yes")
            ],
        dag=dag,
    )

    es_list_indexes = CurlOperator(
        task_id='es_list_indexes',
        name='es-list-indexes',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(show_indexes(), "yes"),
        arguments=[
            '-k', '--location', '--request', 'GET', f'{es_url}/_cat/indices?h=idx'
        ],
    )


    es_test_disk_usage = PythonOperator(
        task_id='es_test_disk_usage',
        python_callable=test_disk_usage,
        op_args=[
            skip_if_param_not(_test_disk_usage(), "yes")
            ],
        dag=dag,
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> params_action_validate >> es_delete_release >> es_test_duplicated_release_variant >> es_test_duplicated_release_cnv >> es_list_indexes >> es_test_disk_usage >> slack
