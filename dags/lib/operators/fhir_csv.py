from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.utils import join


class FhirCsvOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'color',
    )

    def __init__(
        self,
        k8s_context: str,
        color: str = '',
        **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.fhir_csv_image,
            **kwargs,
        )
        self.color = color

    def execute(self, **kwargs):
        env = config.environment

        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='CONFIG__FHIR__URL',
                value='https://' + join('-', ['fhir', self.color]) +
                f'.{env}.cqgc.hsj.rtss.qc.ca/fhir',
            ),
            k8s.V1EnvVar(
                name='CONFIG__FHIR__OAUTH__URL',
                value=f'https://auth.{env}.cqgc.hsj.rtss.qc.ca/auth/realms/clin/protocol/openid-connect/token',
            ),
            k8s.V1EnvVar(
                name='CONFIG__FHIR__OAUTH__CLIENT_ID',
                value='clin-system',
            ),
            k8s.V1EnvVar(
                name='CONFIG__FHIR__OAUTH__CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='keycloak-client-system-credentials',
                        key='client-secret',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='CONFIG__FHIR__OAUTH__UMA_AUDIENCE',
                value='clin-acl',
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='google-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='googlesheets-credentials',
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='google-credentials',
                mount_path='/app/creds',
                read_only=True,
            ),
        ]

        super().execute(**kwargs)
