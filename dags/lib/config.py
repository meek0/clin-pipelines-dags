import kubernetes
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable


class Env:
    QA = 'qa'
    STAGING = 'staging'
    PROD = 'prod'


class K8sContext:
    DEFAULT = 'default'
    ETL = 'etl'


env = Variable.get('environment')

if env not in [Env.QA, Env.STAGING, Env.PROD]:
    raise AirflowConfigException(f'Unexpected environment "{env}"')

k8s_namespace = Variable.get('kubernetes_namespace')
k8s_context = {
    K8sContext.DEFAULT: Variable.get('kubernetes_context_default', f'airflow-cluster.{env}.cqgc@cluster.{env}.cqgc'),
    K8sContext.ETL: Variable.get('kubernetes_context_etl', None),
}

show_test_dags = Variable.get('show_test_dags', None) == 'yes'

spark_service_account = 'spark'
spark_image = 'ferlabcrsj/spark:3.1.2'
spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.18/clin-variant-etl.jar'

arranger_image = 'ferlabcrsj/clin-arranger:1.3.3'
aws_image = 'amazon/aws-cli'
curl_image = 'curlimages/curl'
fhir_image = 'ferlabcrsj/clin-fhir'
fhir_csv_image = 'ferlabcrsj/csv-to-fhir'
pipeline_image = 'ferlabcrsj/clin-pipelines'
postgres_image = 'ferlabcrsj/postgres-backup:9bb43092f76e95f17cd09f03a27c65d84112a3cd'


def k8s_in_cluster(context: str) -> bool:
    return not k8s_context[context]


def k8s_config_file(context: str) -> str:
    return None if not k8s_context[context] else '~/.kube/config'


def k8s_cluster_context(context: str) -> str:
    return k8s_context[context]


def k8s_load_config(context: str) -> None:
    if not k8s_context[context]:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config(
            config_file=k8s_config_file(context),
            context=k8s_context[context],
        )
