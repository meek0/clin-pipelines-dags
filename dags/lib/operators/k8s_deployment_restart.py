import kubernetes
from airflow.models.baseoperator import BaseOperator
from datetime import datetime
from lib import config


class K8sDeploymentRestartOperator(BaseOperator):

    template_fields = ('deployment',)

    def __init__(
        self,
        k8s_context: str,
        deployment: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.k8s_context = k8s_context
        self.deployment = deployment

    def execute(self, _):
        now = str(datetime.utcnow().isoformat('T') + 'Z')
        config.k8s_load_config(self.k8s_context)
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=self.deployment,
            namespace=config.k8s_namespace,
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
