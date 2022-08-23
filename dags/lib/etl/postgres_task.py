from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


def postgres_task(
    task_id: str,
    k8s_context: str,
    dash_color: str = '',
    cmds: List[str] = [],
) -> KubernetesPodOperator:

    environment = config.environment
    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context[k8s_context]
    postgres_image = config.postgres_image

    return KubernetesPodOperator(
        task_id=task_id,
        is_delete_operator_pod=True,
        namespace=k8s_namespace,
        cluster_context=k8s_context,
        name='postgres_task',
        image=postgres_image,
        cmds=cmds,
        image_pull_secrets=[
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ],
        env_vars=[
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
        ],
        env_from=[
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=f'fhir-server{dash_color}-db-connection'
                ),
            ),
        ],
        volumes=[
            k8s.V1Volume(
                name='ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=f'cqgc-{environment}-postgres-ca-cert',
                    default_mode=0o555,
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='ca-certificate',
                mount_path='/opt/ca',
                read_only=True,
            ),
        ],
    )
