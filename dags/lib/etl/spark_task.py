import kubernetes
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s
from lib.etl import config
from lib.helper import join
from lib.k8s import k8s_load_config
from typing import List


def spark_task(
    group_id: str,
    parent_id: str,
    k8s_context: str,
    spark_class: str,
    spark_config: str = '',
    spark_secret: str = '',
    arguments: List[str] = [],
) -> TaskGroup:

    k8s_namespace = config.k8s_namespace
    k8s_context = config.k8s_context[k8s_context]
    k8s_service_account = config.k8s_service_account
    spark_image = config.spark_image
    spark_jar = config.spark_jar

    def _spark_job_status(ti):

        task_ids = [join('_', [parent_id, group_id, 'spark_job'])]
        xcom_pod_name = ti.xcom_pull(
            key='pod_name',
            task_ids=task_ids,
        )
        xcom_pod_namespace = ti.xcom_pull(
            key='pod_namespace',
            task_ids=task_ids,
        )
        if not xcom_pod_name or not xcom_pod_namespace:
            raise AirflowFailException('Spark task xcom not found')
        pod_name = xcom_pod_name[0]
        pod_namespace = xcom_pod_namespace[0]

        k8s_load_config()
        k8s_client = kubernetes.client.CoreV1Api()
        pod = k8s_client.list_namespaced_pod(
            namespace=pod_namespace,
            field_selector=f'metadata.name={pod_name}-driver',
            limit=1,
        )
        if not pod.items:
            raise AirflowFailException('Spark task pod not found')

        try:
            k8s_client.delete_namespaced_pod(
                name=f'{pod_name}-driver',
                namespace=pod_namespace,
            )
        except:
            raise AirflowFailException('Spark task pod delete failed')

        if pod.items[0].status.phase != 'Succeeded':
            raise AirflowFailException('Spark task failed')

    with TaskGroup(group_id=group_id) as task_group:

        volumes = [
            k8s.V1Volume(
                name='spark-defaults',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-defaults',
                ),
            ),
            k8s.V1Volume(
                name='spark-s3-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='spark-s3-credentials',
                ),
            ),
        ]
        volume_mounts = [
            k8s.V1VolumeMount(
                name='spark-defaults',
                mount_path='/opt/spark-configs/defaults',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='spark-s3-credentials',
                mount_path='/opt/spark-configs/s3-credentials',
                read_only=True,
            ),
        ]
        if spark_config:
            volumes.append(
                k8s.V1Volume(
                    name=spark_config,
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name=spark_config,
                    ),
                ),
            )
            volume_mounts.append(
                k8s.V1VolumeMount(
                    name=spark_config,
                    mount_path=f'/opt/spark-configs/{spark_config}',
                    read_only=True,
                ),
            )
        if spark_secret:
            volumes.append(
                k8s.V1Volume(
                    name=spark_secret,
                    secret=k8s.V1SecretVolumeSource(
                        secret_name=spark_secret,
                    ),
                ),
            )
            volume_mounts.append(
                k8s.V1VolumeMount(
                    name=spark_secret,
                    mount_path=f'/opt/spark-configs/{spark_secret}',
                    read_only=True,
                ),
            )

        spark_job = KubernetesPodOperator(
            task_id='spark_job',
            is_delete_operator_pod=True,
            namespace=k8s_namespace,
            cluster_context=k8s_context,
            service_account_name=k8s_service_account,
            name='spark_job',
            image=spark_image,
            cmds=['/opt/client-entrypoint.sh'],
            arguments=arguments,
            image_pull_secrets=[
                k8s.V1LocalObjectReference(
                    name='images-registry-credentials',
                ),
            ],
            env_vars=[
                k8s.V1EnvVar(
                    name='SPARK_CLIENT_POD_NAME',
                    value_from=k8s.V1EnvVarSource(
                        field_ref=k8s.V1ObjectFieldSelector(
                            field_path='metadata.name',
                        ),
                    ),
                ),
                k8s.V1EnvVar(
                    name='SPARK_JAR',
                    value=spark_jar,
                ),
                k8s.V1EnvVar(
                    name='SPARK_CLASS',
                    value=spark_class,
                ),
            ],
            volumes=volumes,
            volume_mounts=volume_mounts,
        )

        spark_job_status = PythonOperator(
            task_id='spark_job_status',
            python_callable=_spark_job_status,
        )

        spark_job >> spark_job_status

    return task_group
