from typing import List


def join(string: str, parts: List[str]) -> str:
    return string.join(filter(None, parts))
