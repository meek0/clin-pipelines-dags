from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


class WaitOperator(BashOperator):

    def __init__(
        self,
        time: str,
        **kwargs,
    ) -> None:
        super().__init__(
            trigger_rule=TriggerRule.NONE_FAILED,
            bash_command=f'sleep {time}',
            **kwargs,
        )
