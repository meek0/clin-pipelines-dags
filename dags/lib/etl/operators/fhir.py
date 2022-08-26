from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from lib.utils import join


class FhirOperator(KubernetesPodOperator):

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
        env = config.environment

        self.is_delete_operator_pod = True
        self.namespace = config.k8s_namespace
        self.cluster_context = config.k8s_context[self.k8s_context]
        self.image = config.fhir_image
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='BASE_URL',
                value='https://' + join('-', ['fhir', self.color]) +
                f'.{env}.cqgc.hsj.rtss.qc.ca/fhir',
            ),
            k8s.V1EnvVar(
                name='OAUTH_URL',
                value=f'https://auth.{env}.cqgc.hsj.rtss.qc.ca/auth/realms/clin/protocol/openid-connect/token',
            ),
            k8s.V1EnvVar(
                name='OAUTH_CLIENT_ID',
                value='clin-system',
            ),
            k8s.V1EnvVar(
                name='OAUTH_CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='keycloak-client-system-credentials',
                        key='client-secret',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='OAUTH_UMA_AUDIENCE',
                value='clin-acl',
            ),
        ]

        super().execute(**kwargs)
