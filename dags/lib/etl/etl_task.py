from airflow.utils.task_group import TaskGroup
from lib.etl.config import K8sContext
from lib.etl.spark_task import spark_task
from lib.helper import task_id


def etl_task(
    group_id: str,
    parent_id: str,
    environment: str,
    k8s_namespace: str,
    k8s_context: K8sContext,
    k8s_service_account: str,
    spark_image: str,
    spark_jar: str,
    batch_id: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as etl_task_group:

        step_3 = spark_task(
            group_id='fhir',
            parent_id=task_id([parent_id, group_id]),
            environment=environment,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            # bio.ferlab.clin.etl.fhir.FhirRawToNormalized
            spark_class='bio.ferlab.clin.etl.fail.Fail',
            spark_config='raw-fhir-etl',
            extra_args=['initial', 'all'],
        )

        step_4_1 = spark_task(
            group_id='vcf_snv',
            parent_id=task_id([parent_id, group_id]),
            environment=environment,
            k8s_namespace=k8s_namespace,
            k8s_context=k8s_context.etl,
            k8s_service_account=k8s_service_account,
            spark_image=spark_image,
            spark_jar=spark_jar,
            spark_class='bio.ferlab.clin.etl.vcf.ImportVcf',
            spark_config='raw-vcf-etl',
            extra_args=['default', batch_id, 'snv'],
        )

        step_3 >> step_4_1

    return etl_task_group
