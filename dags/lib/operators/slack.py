from airflow.models.baseoperator import BaseOperator
from lib.hooks.slack import SlackHook


class SlackOperator(BaseOperator):

    def __init__(
        self,
        markdown: str,
        type=SlackHook.INFO,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.markdown = markdown
        self.type = type

    def execute(self, context):
        SlackHook.notify(self.markdown, self.type)
