from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


def curl_task(
    task_id: str,
    k8s_context: str,
    arguments: List[str] = [],
) -> KubernetesPodOperator:

    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context[k8s_context]
    curl_image = config.curl_image

    return KubernetesPodOperator(
        task_id=task_id,
        is_delete_operator_pod=True,
        namespace=k8s_namespace,
        cluster_context=k8s_context,
        name='curl-task',
        image=curl_image,
        arguments=arguments,
        image_pull_secrets=[
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ],
    )
