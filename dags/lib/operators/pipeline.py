from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s
from lib import config
from lib.config import env, env_url
from lib.utils import join


class PipelineOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'aws_bucket',
        'color',
        'skip',
    )

    def __init__(
        self,
        k8s_context: str,
        aws_bucket: str = '',
        color: str = '',
        skip: bool = False,
        trigger_rule=TriggerRule.NONE_FAILED,
        **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.pipeline_image,
            **kwargs,
        )
        self.aws_bucket = aws_bucket
        self.color = color
        self.skip = skip

    def execute(self, **kwargs):

        if self.skip:
            raise AirflowSkipException()

        self.cmds = [
            '/opt/entrypoint/entrypoint.sh',
            'java', '-cp', 'clin-pipelines.jar',
        ]
        self.image_pull_policy = 'Always'
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='CLIN_URL',
                value='https://portail' + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca',
            ),
            k8s.V1EnvVar(
                name='FERLOAD_URL',
                value='https://ferload' + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca',
            ),
            k8s.V1EnvVar(
                name='FHIR_URL',
                value='https://' + join('-', ['fhir', self.color]) + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca/fhir',
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_URL',
                value='https://auth' + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca/auth',
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
                name='AWS_OUTPUT_BUCKET_NAME',
                value=f'cqgc-{env}-app-download',
            ),
            k8s.V1EnvVar(
                name='AWS_PREFIX',
                value=self.color,
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
        self.volumes = [
            k8s.V1Volume(
                name='entrypoint',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-jobs-entrypoint',
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='entrypoint',
                mount_path='/opt/entrypoint',
            ),
        ]

        if self.aws_bucket:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_BUCKET_NAME',
                    value=self.aws_bucket,
                )
            )

        super().execute(**kwargs)
