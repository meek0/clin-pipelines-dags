from airflow.operators.bash import BashOperator


class WaitOperator(BashOperator):

    def __init__(
        self,
        time: str,
        **kwargs,
    ) -> None:
        super().__init__(
            bash_command=f'sleep {time}',
            **kwargs,
        )
