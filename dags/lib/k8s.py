import kubernetes
import os
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime


k8s_namespace = Variable.get('kubernetes_namespace')


def k8s_load_config():
    if os.getenv('K8S_LOCAL_CONFIG', 'false') == 'true':
        kubernetes.config.load_kube_config()
    else:
        kubernetes.config.load_incluster_config()


def k8s_deployment_pause(
    task_id: str,
    deployment: str,
):
    def deployment_pause():
        k8s_load_config()
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=deployment,
            namespace=k8s_namespace,
            body={'spec': {'paused': True}},
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=deployment_pause,
    )


def k8s_deployment_resume(
    task_id: str,
    deployment: str,
):
    def deployment_resume():
        k8s_load_config()
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=deployment,
            namespace=k8s_namespace,
            body={'spec': {'paused': False}},
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=deployment_resume,
    )


def k8s_deployment_restart(
    task_id: str,
    deployment: str,
):
    def deployment_restart():
        now = str(datetime.utcnow().isoformat('T') + 'Z')
        k8s_load_config()
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=deployment,
            namespace=k8s_namespace,
            body={
                'spec': {
                    'template': {
                        'metadata': {
                            'annotations': {
                                'kubectl.kubernetes.io/restartedAt': now
                            }
                        }
                    }
                }
            },
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=deployment_restart,
    )
