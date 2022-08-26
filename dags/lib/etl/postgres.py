from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from lib.utils import join


class PostgresOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'color',
    )

    def __init__(
        self,
        k8s_context: str,
        color: str = '',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.k8s_context = k8s_context
        self.color = color

    def execute(self, **kwargs):
        environment = config.environment
        k8s_namespace = config.k8s_namespace
        k8s_context = config.k8s_context[self.k8s_context]
        postgres_image = config.postgres_image

        self.is_delete_operator_pod = True
        self.namespace = k8s_namespace
        self.cluster_context = k8s_context
        self.image = postgres_image
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='PGUSER',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=f'cqgc-{environment}-postgres-credentials',
                        key='PGUSER',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='PGPASSWORD',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=f'cqgc-{environment}-postgres-credentials',
                        key='PGPASSWORD',
                    ),
                ),
            ),
        ]
        self.env_from = [
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=join(
                        '-', ['fhir-server', self.color, 'db-connection'],
                    ),
                ),
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=f'cqgc-{environment}-postgres-ca-cert',
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='ca-certificate',
                mount_path='/opt/ca',
                read_only=True,
            ),
        ]

        super().execute(**kwargs)
