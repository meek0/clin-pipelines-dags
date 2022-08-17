from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_sample_1', default_args=default_args)


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='cqgc-etl',
                                image="Python:3.6",
                                cmds=["Python", "-c"],
                                arguments=["print('hello world')"],
                                labels={"foo": "bar"},
                                name="passing-test",
                                task_id="passing-task",
                                get_logs=True,
                                dag=dag
                                )

passing.set_upstream(start)