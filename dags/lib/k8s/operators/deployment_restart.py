import kubernetes
from airflow.models.baseoperator import BaseOperator
from datetime import datetime
from lib.k8s.config import k8s_load_config, k8s_namespace


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
