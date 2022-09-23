from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s
from lib import config
from lib.config import env_url
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
        super().__init__(
            trigger_rule=TriggerRule.NONE_FAILED,
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.fhir_image,
            **kwargs,
        )
        self.color = color

    def execute(self, **kwargs):

        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='BASE_URL',
                value='https://' + join('-', ['fhir', self.color]) + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca/fhir',
            ),
            k8s.V1EnvVar(
                name='OAUTH_URL',
                value='https://auth' + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca/auth/realms/clin/protocol/openid-connect/token',
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
