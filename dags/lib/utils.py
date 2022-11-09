import hashlib
import requests
from typing import Any, List


def join(string: str, parts: List[str]) -> str:
    return string.join(filter(None, parts))


def http_get(url: str, headers: Any = None) -> requests.Response:
    with requests.get(url, headers=headers) as response:
        response.raise_for_status()
        return response


def http_get_file(url: str, path: str, headers: Any = None, chunk_size: int = 8192) -> None:
    with requests.get(url, headers=headers, stream=True) as response:
        response.raise_for_status()
        with open(path, 'wb') as file:
            for chunk in response.iter_content(chunk_size):
                file.write(chunk)


def http_post(url: str, json: Any = None) -> requests.Response:
    with requests.post(url, json=json) as response:
        response.raise_for_status()
        return response


def file_md5(path: str, chunk_size: int = 8192) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as file:
        for chunk in iter(lambda: file.read(chunk_size), b''):
            md5.update(chunk)
        return md5.hexdigest()
