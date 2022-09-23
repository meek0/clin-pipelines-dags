import requests
from typing import List


def join(string: str, parts: List[str]) -> str:
    return string.join(filter(None, parts))


def http_get_file(url: str, path: str):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)


def file_content(path: str) -> str:
    with open(path, 'r') as file:
        return file.read()
