from typing import List


def task_id(ids: List[str]) -> str:
    return '.'.join(filter(None, ids))
