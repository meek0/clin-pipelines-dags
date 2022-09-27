from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


class WaitOperator(BashOperator):

    def __init__(
        self,
        time: str,
        trigger_rule=TriggerRule.NONE_FAILED,
        **kwargs,
    ) -> None:
        super().__init__(
            bash_command=f'sleep {time}',
            **kwargs,
        )
