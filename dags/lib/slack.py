import urllib.parse
from lib import config
from lib import utils
from lib.config import env


class Slack:

    SUCCESS = ':large_green_circle:'
    INFO = ':large_blue_circle:'
    WARNING = ':large_orange_circle:'
    ERROR = ':red_circle:'

    def notify(markdown: str, type=INFO):
        if config.slack_hook_url:
            airflow_link = f' [<{config.base_url}|Airflow>]' if config.base_url else ''
            utils.http_post(config.slack_hook_url, {
                'blocks': [
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': f'{type} *{env.upper()}*{airflow_link}',
                        },
                    },
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': markdown,
                        },
                    },
                ],
            })

    def notify_task_failure(context):
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        task_id = context['task'].task_id
        task_url = Slack.airflow_url(dag_id, run_id, task_id)
        Slack.notify(
            f'Task failure: <{task_url}|{dag_id}.{task_id}>',
            Slack.ERROR,
        )

    def notify_dag_start(context):
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        dag_url = Slack.airflow_url(dag_id, run_id)
        Slack.notify(
            f'DAG start: <{dag_url}|{dag_id}>',
            Slack.INFO,
        )

    def notify_dag_success(context):
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        dag_url = Slack.airflow_url(dag_id, run_id)
        Slack.notify(
            f'DAG success: <{dag_url}|{dag_id}>',
            Slack.SUCCESS,
        )

    def airflow_url(dag_id: str, run_id: str = '', task_id: str = ''):
        params = urllib.parse.urlencode({
            'dag_run_id': run_id,
            'task_id': task_id
        })
        return f'{config.base_url}/dags/{dag_id}/grid?{params}'
