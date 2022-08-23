from airflow.exceptions import AirflowConfigException
from airflow.models import Variable


class K8sContext:
    DEFAULT = 'default'
    ETL = 'etl'


environment = Variable.get('environment')
k8s_namespace = Variable.get('kubernetes_namespace')

if environment == 'qa':
    k8s_context = {
        'default': 'kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc',
        'etl': 'kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc',
    }
elif environment == 'staging':
    k8s_context = {
        'default': 'kubernetes-admin-cluster.staging.cqgc@cluster.staging.cqgc',
        'etl': 'kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc',
    }
elif environment == 'prod':
    k8s_context = {
        'default': 'kubernetes-admin-cluster.prod.cqgc@cluster.prod.cqgc',
        'etl': 'kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc',
    }
else:
    raise AirflowConfigException(f'Unexpected environment "{environment}"')

k8s_service_account = 'spark'

pipeline_image = 'ferlabcrsj/clin-pipelines'

spark_image = 'ferlabcrsj/spark:3.1.2'
spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.14/clin-variant-etl.jar'
