from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

with DAG("hello_world", start_date=days_ago(2), 
    schedule_interval=None, catchup=False) as dag:
        task_hello_world = BashOperator(
            task_id="hello_world",
            bash_command="fail_command")

        end = DummyOperator(task_id='end')

        task_hello_world >> end