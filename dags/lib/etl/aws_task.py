from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


def aws_task(
    task_id: str,
    k8s_context: str,
    arguments: List[str] = [],
) -> KubernetesPodOperator:

    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context[k8s_context]
    aws_image = config.aws_image

    return KubernetesPodOperator(
        task_id=task_id,
        is_delete_operator_pod=True,
        namespace=k8s_namespace,
        cluster_context=k8s_context,
        name='aws-task',
        image=aws_image,
        arguments=arguments,
        image_pull_secrets=[
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ],
        env_vars=[
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='download-s3-credentials',
                        key='S3_ACCESS_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='download-s3-credentials',
                        key='S3_SECRET_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_DEFAULT_REGION',
                value='regionone',
            ),
        ],
        volumes=[
            k8s.V1Volume(
                name='minio-ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='minio-ca-certificate',
                    default_mode=0o555,
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='minio-ca-certificate',
                mount_path='/opt/minio-ca',
                read_only=True,
            ),
        ],
    )
