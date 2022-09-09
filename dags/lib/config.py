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
k8s_namespace = Variable.get('kubernetes_namespace')
k8s_context_default = Variable.get('kubernetes_context_default', '')
k8s_context_etl = Variable.get('kubernetes_context_etl', '')

k8s_config = '~/.kube/config'
k8s_service_account = 'spark'
aws_image = 'amazon/aws-cli'
curl_image = 'curlimages/curl'
fhir_image = 'ferlabcrsj/clin-fhir'
fhir_csv_image = 'ferlabcrsj/csv-to-fhir'
pipeline_image = 'ferlabcrsj/clin-pipelines'
postgres_image = 'ferlabcrsj/postgres-backup:9bb43092f76e95f17cd09f03a27c65d84112a3cd'
spark_image = 'ferlabcrsj/spark:3.1.2'
spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.16/clin-variant-etl.jar'

if env == Env.QA:
    k8s_context = {
        K8sContext.DEFAULT: 'airflow-cluster.qa.cqgc@cluster.qa.cqgc',
        K8sContext.ETL: '',
    }
elif env == Env.STAGING:
    k8s_context = {
        K8sContext.DEFAULT: 'airflow-cluster.staging.cqgc@cluster.staging.cqgc',
        K8sContext.ETL: '',
    }
elif env == Env.PROD:
    k8s_context = {
        K8sContext.DEFAULT: 'airflow-cluster.prod.cqgc@cluster.prod.cqgc',
        K8sContext.ETL: '',
    }
else:
    raise AirflowConfigException(f'Unexpected environment "{env}"')

if k8s_context_default:
    k8s_context[K8sContext.DEFAULT] = k8s_context_default

if k8s_context_etl:
    k8s_context[K8sContext.ETL] = k8s_context_etl


def k8s_in_cluster(context: str) -> bool:
    return True if k8s_context[context] == '' else False


def k8s_config_file(context: str) -> str:
    return k8s_config if k8s_context[context] != '' else None


def k8s_cluster_context(context: str) -> str:
    return k8s_context[context] if k8s_context[context] != '' else None


def k8s_load_config(context: str) -> None:
    if k8s_context[context] == '':
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config(
            config_file=k8s_config,
            context=k8s_context[context],
        )
