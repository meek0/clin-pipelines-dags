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
k8s_context = {
    K8sContext.DEFAULT: Variable.get('kubernetes_context_default', None),
    K8sContext.ETL: Variable.get('kubernetes_context_etl', None),
}
base_url = Variable.get('base_url', None)
s3_conn_id = Variable.get('s3_conn_id', None)
slack_hook_url = Variable.get('slack_hook_url', None)
show_test_dags = Variable.get('show_test_dags', None) == 'yes'

arranger_image = 'ferlabcrsj/clin-arranger:1.3.3'
aws_image = 'amazon/aws-cli'
curl_image = 'curlimages/curl'
fhir_csv_image = 'ferlabcrsj/csv-to-fhir'
fhir_image = 'ferlabcrsj/clin-fhir'
pipeline_image = 'ferlabcrsj/clin-pipelines'
postgres_image = 'ferlabcrsj/postgres-backup:9bb43092f76e95f17cd09f03a27c65d84112a3cd'
spark_image = 'ferlabcrsj/spark:3.1.2'
spark_service_account = 'spark'

if env == Env.QA:
    fhir_csv_file = 'qa.yml'
    spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.26/clin-variant-etl.jar'
elif env == Env.STAGING:
    fhir_csv_file = 'staging.yml'
    spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.22/clin-variant-etl.jar'
elif env == Env.PROD:
    fhir_csv_file = 'prod.yml'
    spark_jar = 'https://github.com/Ferlab-Ste-Justine/clin-variant-etl/releases/download/v2.3.22/clin-variant-etl.jar'
else:
    raise AirflowConfigException(f'Unexpected environment "{env}"')


def env_url(prefix: str = '') -> str:
    return f'{prefix}{env}' if env in [Env.QA, Env.STAGING] else ''


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
