from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


class CurlOperator(KubernetesPodOperator):

    def __init__(
        self,
        k8s_context: str,
        arguments: List[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(
            name='curl-operator',
            **kwargs,
        )
        self.k8s_context = k8s_context
        self.arguments = arguments

    def execute(self, **kwargs):
        k8s_namespace = config.k8s_namespace
        k8s_context = config.k8s_context[k8s_context]
        curl_image = config.curl_image

        self.is_delete_operator_pod = True
        self.namespace = k8s_namespace
        self.cluster_context = k8s_context
        self.image = curl_image
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]

        super().execute(**kwargs)
