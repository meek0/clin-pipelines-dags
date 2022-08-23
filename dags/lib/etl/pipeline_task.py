from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib.etl import config
from typing import List


def pipeline_task(
    task_id: str,
    k8s_context: str,
    aws_bucket: str = '',
    color: str = '',
    dash_color: str = '',
    arguments: List[str] = [],
) -> KubernetesPodOperator:

    environment = config.environment
    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context[k8s_context]
    pipeline_image = config.pipeline_image

    env_vars = [
        k8s.V1EnvVar(
            name='CLIN_URL',
            value=f'https://portail.{environment}.cqgc.hsj.rtss.qc.ca',
        ),
        k8s.V1EnvVar(
            name='FERLOAD_URL',
            value=f'https://ferload.{environment}.cqgc.hsj.rtss.qc.ca',
        ),
        k8s.V1EnvVar(
            name='FHIR_URL',
            value='https://fhir' + dash_color +
            f'.{environment}.cqgc.hsj.rtss.qc.ca/fhir',
        ),
        k8s.V1EnvVar(
            name='KEYCLOAK_URL',
            value=f'https://auth.{environment}.cqgc.hsj.rtss.qc.ca/auth',
        ),
        k8s.V1EnvVar(
            name='KEYCLOAK_AUTHORIZATION_AUDIENCE',
            value='clin-acl',
        ),
        k8s.V1EnvVar(
            name='KEYCLOAK_CLIENT_KEY',
            value='clin-system',
        ),
        k8s.V1EnvVar(
            name='KEYCLOAK_CLIENT_SECRET',
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name='keycloak-client-system-credentials',
                    key='client-secret',
                ),
            ),
        ),
        k8s.V1EnvVar(
            name='MAILER_HOST',
            value='relais.rtss.qc.ca',
        ),
        k8s.V1EnvVar(
            name='MAILER_PORT',
            value='25',
        ),
        k8s.V1EnvVar(
            name='MAILER_SSL',
            value='false',
        ),
        k8s.V1EnvVar(
            name='MAILER_TLS',
            value='false',
        ),
        k8s.V1EnvVar(
            name='MAILER_TLS_REQUIRED',
            value='false',
        ),
        k8s.V1EnvVar(
            name='MAILER_FROM',
            value='cqgc@ssss.gouv.qc.ca',
        ),
        k8s.V1EnvVar(
            name='MAILER_BCC',
            value='clin_test@ferlab.bio',
        ),
    ]
    if aws_bucket:
        env_vars.append([
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-files-processing-credentials',
                        key='S3_ACCESS_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-files-processing-credentials',
                        key='S3_SECRET_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value='https://s3.cqgc.hsj.rtss.qc.ca',
            ),
            k8s.V1EnvVar(
                name='AWS_DEFAULT_REGION',
                value='regionone',
            ),
            k8s.V1EnvVar(
                name='AWS_BUCKET_NAME',
                value=aws_bucket,
            ),
            k8s.V1EnvVar(
                name='AWS_OUTPUT_BUCKET_NAME',
                value=f'cqgc-{environment}-app-download',
            ),
            k8s.V1EnvVar(
                name='AWS_PREFIX',
                value=color,
            ),
        ])

    return KubernetesPodOperator(
        task_id=task_id,
        is_delete_operator_pod=True,
        namespace=k8s_namespace,
        cluster_context=k8s_context,
        name='pipeline-task',
        image=pipeline_image,
        cmds=['/opt/entrypoint/entrypoint.sh', 'java -cp clin-pipelines.jar'],
        arguments=arguments,
        image_pull_secrets=[
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ],
        env_vars=env_vars,
        volumes=[
            k8s.V1Volume(
                name='entrypoint',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-jobs-entrypoint',
                    default_mode=0o555,
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='entrypoint',
                mount_path='/opt/entrypoint',
            ),
        ],
    )
