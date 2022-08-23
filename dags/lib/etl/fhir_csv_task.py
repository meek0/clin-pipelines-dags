from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


def fhir_csv_task(
    task_id: str,
    k8s_context: str,
    dash_color: str = '',
) -> KubernetesPodOperator:

    environment = config.environment
    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context[k8s_context]
    fhir_csv_image = config.fhir_csv_image

    return KubernetesPodOperator(
        task_id=task_id,
        is_delete_operator_pod=True,
        namespace=k8s_namespace,
        cluster_context=k8s_context,
        name='fhir-csv-task',
        image=fhir_csv_image,
        image_pull_secrets=[
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ],
        env_vars=[
            k8s.V1EnvVar(
                name='CONFIG__FHIR__URL',
                value=f'https://fhir{dash_color}.{environment}.cqgc.hsj.rtss.qc.ca/fhir',
            ),
            k8s.V1EnvVar(
                name='CONFIG__FHIR__OAUTH__URL',
                value=f'https://auth.{environment}.cqgc.hsj.rtss.qc.ca/auth/realms/clin/protocol/openid-connect/token',
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
        ],
        volumes=[
            k8s.V1Volume(
                name='google-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='googlesheets-credentials',
                    default_mode=0o555,
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='google-credentials',
                mount_path='/app/creds',
                read_only=True,
            ),
        ],
    )
