from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from lib.utils import file_md5, http_get_file

def get_s3_file_md5(s3: S3Hook, s3_bucket: str, s3_key: str) -> str:
  s3_md5 = None
  s3_md5_file = f'{s3_key}.md5'
  if s3.check_for_key(s3_md5_file, s3_bucket):
      s3_md5 = s3.read_key(s3_md5_file, s3_bucket)
  return s3_md5

def download_and_check_md5(url: str, file: str, expected_md5: str) -> None:
  http_get_file(f'{url}/{file}', file)
  md5 = file_md5(file)
  if expected_md5 is not None and md5 != expected_md5:
    raise AirflowFailException('MD5 checksum verification failed')
  return md5

def load_to_s3(s3: S3Hook, s3_bucket: str, s3_key: str, file: str, file_md5: str) -> None:
  s3.load_file(file, s3_key, s3_bucket, replace=True)
  s3.load_string(file_md5, f'{s3_key}.md5', s3_bucket, replace=True)