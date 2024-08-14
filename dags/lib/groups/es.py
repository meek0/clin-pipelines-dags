import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, K8sContext, env, env_url, es_url
from lib.doc import qc as doc
from lib.operators.curl import CurlOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks import arranger
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, release_id, skip_if_param_not


def format_es_url(index, _color=None, release_id=None):
    url = f'{es_url}/clin_{env}'
    if _color:
        url += color("_")
    url += f'_{index}_centric'
    if release_id:
        url += f'_{release_id}'
    url += '/_search'
    return url

def test_duplicated_by_url(url, skip=None):
    if skip: 
        raise AirflowSkipException()

    headers = {'Content-Type': 'application/json'}
    body = {
            "size" : 0,
            "aggs" : {
                "duplicated" : {
                    "terms" : {
                        "field" : 'hash',
                        "min_doc_count" : 2,
                        "size" : 1
                    }
                }
            }
        }
    response = requests.post(url, headers=headers, json=body, verify=False)
    logging.info(f'ES response: {response.text}')
    buckets = response.json().get('aggregations', {}).get('duplicated', {}).get('buckets', [])
    if not response.ok or len(buckets) > 0:
        raise AirflowFailException('Failed')
    return 

def test_disk_usage(skip=None):
    if skip: 
        raise AirflowSkipException()

    response = requests.get(f'{es_url}/_cat/allocation?v&pretty', verify=False)
    logging.info(f'ES response:\n{response.text}')

    first_node_usage = response.text.split('\n')[1]
    first_node_disk_usage = first_node_usage.split()[5]

    logging.info(f'ES disk usage: {first_node_disk_usage}%')

    if float(first_node_disk_usage) > 75:
        raise AirflowFailException(f'ES disk usage is too high: {first_node_disk_usage}% please delete some old releases')
    return 

def es(
    group_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:


        es_test_duplicated_variant = PythonOperator(
            task_id='es_test_duplicated_variant',
            python_callable=test_duplicated_by_url,
            op_args=[
                format_es_url('variant'),
                ],
        )

        es_test_duplicated_cnv = PythonOperator(
            task_id='es_test_duplicated_cnv',
            python_callable=test_duplicated_by_url,
            op_args=[
                format_es_url('cnv'),
                ],
        )

        es_test_disk_usage = PythonOperator(
            task_id='es_test_disk_usage',
            python_callable=test_disk_usage,
            op_args=[
                ],
        )

        [es_test_duplicated_variant, es_test_duplicated_cnv, es_test_disk_usage]

    return group
