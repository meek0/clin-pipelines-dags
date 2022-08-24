from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


class AwsOperator(KubernetesPodOperator):

    def __init__(
        self,
        k8s_context: str,
        arguments: List[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(
            name='aws-operator',
            **kwargs,
        )
        self.k8s_context = k8s_context
        self.arguments = arguments

    def execute(self, **kwargs):
        k8s_namespace = config.k8s_namespace
        k8s_context = config.k8s_context[self.k8s_context]
        aws_image = config.aws_image

        self.is_delete_operator_pod = True
        self.namespace = k8s_namespace
        self.cluster_context = k8s_context
        self.image = aws_image
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
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
        ]
        self.volumes = [
            k8s.V1Volume(
                name='minio-ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='minio-ca-certificate',
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='minio-ca-certificate',
                mount_path='/opt/minio-ca',
                read_only=True,
            ),
        ]

        super().execute(**kwargs)
