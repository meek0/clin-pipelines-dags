from airflow.models import Variable


class K8sContext:
    etl: str
    qa: str


class K8sContextQa(K8sContext):
    etl = 'kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc'
    qa = 'kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc'


environment = 'qa'

try:
    k8s_namespace = Variable.get('kubernetes_namespace')
except:
    k8s_namespace = 'cqgc-qa'

k8s_context: K8sContext = K8sContextQa
k8s_service_account = 'spark'

spark_image = 'ferlabcrsj/spark:3.1.2'
spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.14/clin-variant-etl.jar'

batch_id = '201106_A00516_0169_AHFM3HDSXY'
release = 're_000'
