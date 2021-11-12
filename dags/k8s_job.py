from typing import List

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s


def curl_job_pod(name: str,
                 namespace: str,
                 image: str = "curlimages/curl"):
    args = [
        "-f",
        "-X",
        "POST",
        "http://elasticsearch-workers:9200/_aliases",
        "-H",
        "Content-Type: application/json", "-d",
        """
            {
            "actions": [
                {"add": { "index": "clin_qa_green_variant_centric_re_000", "alias": "clin_qa_variant_centric"}},
                {"add": { "index": "clin_qa_green_variant_suggestions_re_000", "alias": "clin_qa_variant_suggestions"}},
                {"remove": { "index": "clin_qa_blue_variant_centric_re_000", "alias": "clin_qa_variant_centric"}},
                {"remove": { "index": "clin_qa_blue_variant_suggestions_re_000", "alias": "clin_qa_variant_suggestions"}}
            ]
            }"""
    ]

    return KubernetesPodOperator(
        name=name,
        task_id=name,
        image=image,
        image_pull_policy="Always",
        service_account_name="spark",
        arguments=args,
        namespace=namespace,
        in_cluster=False
    )


def spark_job_pod(name: str,
                  namespace: str,
                  image: str,
                  jar: str,
                  main_class: str,
                  args: list,
                  cluster_type: str):
    env_vars = [
        k8s.V1EnvVar(name='SPARK_JAR', value=jar),
        k8s.V1EnvVar(name='SPARK_CLASS', value=main_class),
        k8s.V1EnvVar(name='SPARK_CLIENT_POD_NAME',
                     value_from=k8s.V1EnvVarSource(field_ref=k8s.V1ObjectFieldSelector(field_path="metadata.name")))
    ]

    volume_mounts = [
        k8s.V1VolumeMount(mount_path=f"/opt/spark-configs/defaults", name="spark-defaults", read_only=True),
        k8s.V1VolumeMount(mount_path=f"/opt/spark-configs/{cluster_type}", name=cluster_type, read_only=True),
        k8s.V1VolumeMount(mount_path=f"/opt/spark-configs/s3-credentials", name="s3-credentials", read_only=True)
    ]

    volumes: List[k8s.V1Volume] = [
        k8s.V1Volume(name="spark-defaults", config_map=k8s.V1ConfigMapVolumeSource(name="spark-defaults")),
        k8s.V1Volume(name=cluster_type, config_map=k8s.V1ConfigMapVolumeSource(name=cluster_type)),
        k8s.V1Volume(name="s3-credentials", secret=k8s.V1SecretVolumeSource(secret_name="s3-credentials"))
    ]

    # containers = [
    #     k8s.V1Container(
    #         args=args,
    #         command=["/opt/client-entrypoint.sh"],
    #         env=env_vars,
    #         image=image,
    #         image_pull_policy="Always",
    #         name=cluster_type,
    #         volume_mounts=volume_mounts
    #     )
    # ]

    # spec = k8s.V1Pod(
    #     api_version="v1",
    #     kind="Pod",
    #     metadata=k8s.V1ObjectMeta(name=name, namespace=NAMESPACE),
    #     spec=k8s.V1PodSpec(
    #         containers=containers,
    #         restart_policy="Never",
    #         volumes=volumes,
    #         service_account_name="spark"
    #     )
    # )

    return KubernetesPodOperator(
        name=name,
        task_id=name,
        # full_pod_spec=spec,
        volumes=volumes,
        env_vars=env_vars,
        volume_mounts=volume_mounts,
        image=image,
        cmds=["/opt/client-entrypoint.sh"],
        image_pull_policy="Always",
        service_account_name="spark",
        arguments=args,
        namespace=namespace,
        in_cluster=False,
        is_delete_operator_pod=False
    )
