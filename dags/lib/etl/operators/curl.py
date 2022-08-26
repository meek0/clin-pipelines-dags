from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config


class CurlOperator(KubernetesPodOperator):

    def __init__(
        self,
        k8s_context: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.k8s_context = k8s_context

    def execute(self, **kwargs):
        self.is_delete_operator_pod = True
        self.namespace = config.k8s_namespace
        self.cluster_context = config.k8s_context[self.k8s_context]
        self.image = config.curl_image
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]

        super().execute(**kwargs)
