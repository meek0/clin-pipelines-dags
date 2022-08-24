import kubernetes
from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from lib.k8s import k8s_load_config
from typing import List


class SparkOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'spark_config',
        'spark_secret',
    )

    def __init__(
        self,
        k8s_context: str,
        spark_class: str,
        spark_config: str = '',
        spark_secret: str = '',
        arguments: List[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(
            name='spark-operator',
            **kwargs,
        )
        self.k8s_context = k8s_context
        self.spark_class = spark_class
        self.spark_config = spark_config
        self.spark_secret = spark_secret
        self.arguments = arguments

    def execute(self, **kwargs):
        k8s_namespace = config.k8s_namespace
        k8s_context = config.k8s_context[self.k8s_context]
        k8s_service_account = config.k8s_service_account
        spark_image = config.spark_image
        spark_jar = config.spark_jar

        self.is_delete_operator_pod = True
        self.namespace = k8s_namespace
        self.cluster_context = k8s_context
        self.service_account_name = k8s_service_account
        self.image = spark_image
        self.cmds = ['/opt/client-entrypoint.sh']
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='SPARK_CLIENT_POD_NAME',
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(
                        field_path='metadata.name',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='SPARK_JAR',
                value=spark_jar,
            ),
            k8s.V1EnvVar(
                name='SPARK_CLASS',
                value=self.spark_class,
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='spark-defaults',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-defaults',
                ),
            ),
            k8s.V1Volume(
                name='spark-s3-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='spark-s3-credentials',
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='spark-defaults',
                mount_path='/opt/spark-configs/defaults',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='spark-s3-credentials',
                mount_path='/opt/spark-configs/s3-credentials',
                read_only=True,
            ),
        ]

        if self.spark_config:
            self.volumes.append(
                k8s.V1Volume(
                    name=self.spark_config,
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name=self.spark_config,
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name=self.spark_config,
                    mount_path=f'/opt/spark-configs/{self.spark_config}',
                    read_only=True,
                ),
            )
        if self.spark_secret:
            self.volumes.append(
                k8s.V1Volume(
                    name=self.spark_secret,
                    secret=k8s.V1SecretVolumeSource(
                        secret_name=self.spark_secret,
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name=self.spark_secret,
                    mount_path=f'/opt/spark-configs/{self.spark_secret}',
                    read_only=True,
                ),
            )

        result = super().execute(**kwargs)

        k8s_load_config()
        k8s_client = kubernetes.client.CoreV1Api()
        pod = k8s_client.list_namespaced_pod(
            namespace=self.pod.metadata.namespace,
            field_selector=f'metadata.name={self.pod.metadata.name}-driver',
            limit=1,
        )
        if not pod.items:
            raise AirflowFailException('Spark pod not found')

        try:
            k8s_client.delete_namespaced_pod(
                name=f'{self.pod.metadata.name}-driver',
                namespace=self.pod.metadata.namespace,
            )
        except:
            raise AirflowFailException('Spark pod delete failed')

        if pod.items[0].status.phase != 'Succeeded':
            raise AirflowFailException('Spark job failed')

        return result
