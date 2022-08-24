import kubernetes
import os
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from datetime import datetime


k8s_namespace = Variable.get('kubernetes_namespace')


def k8s_load_config():
    if os.getenv('K8S_LOCAL_CONFIG', 'false') == 'true':
        kubernetes.config.load_kube_config()
    else:
        kubernetes.config.load_incluster_config()


class K8sDeploymentPauseOperator(BaseOperator):

    template_fields = ('deployment',)

    def __init__(
        self,
        deployment: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment = deployment

    def execute(self, context):
        k8s_load_config()
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=self.deployment,
            namespace=k8s_namespace,
            body={'spec': {'paused': True}},
        )


class K8sDeploymentResumeOperator(BaseOperator):

    template_fields = ('deployment',)

    def __init__(
        self,
        deployment: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment = deployment

    def execute(self, context):
        k8s_load_config()
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=self.deployment,
            namespace=k8s_namespace,
            body={'spec': {'paused': False}},
        )


class K8sDeploymentRestartOperator(BaseOperator):

    template_fields = ('deployment',)

    def __init__(
        self,
        deployment: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.deployment = deployment

    def execute(self, context):
        now = str(datetime.utcnow().isoformat('T') + 'Z')
        k8s_load_config()
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=self.deployment,
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
