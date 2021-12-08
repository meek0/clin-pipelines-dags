from airflow import DAG
from airflow.utils.dates import days_ago

from k8s_job import spark_job_pod, curl_job_pod

default_args = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

NAMESPACE = "clin-qa"
CLIN_VARIANT_ETL_JAR = "s3a://clin-qa-app-datalake/spark/clin-variant-etl.jar"  # Variable.get('clin_variant_etl_jar')
NORMALIZED_FHIR_ETL_MAIN_CLASS = "bio.ferlab.clin.etl.fhir.FhirRawToNormalized"
NORMALIZED_VCF_ETL_MAIN_CLASS = "bio.ferlab.clin.etl.vcf.ImportVcf"
ENRICHED_ETL_MAIN_CLASS = "bio.ferlab.clin.etl.enriched.RunEnriched"
PREPARE_INDEX_ETL_MAIN_CLASS = "bio.ferlab.clin.etl.es.PrepareIndex"
INDEXER_ETL_MAIN_CLASS = "bio.ferlab.clin.etl.es.Indexer"
SPARK_IMAGE = "ferlabcrsj/spark:3.1.2"
ELEASTICSEARCH_URL = "http://elasticsearch-workers:9200"

RAW_FHIR_ETL_CONF = ["config/qa.conf", "all", "first_load"]

with DAG(
        dag_id="k8s_normalize_vcf_etl",
        schedule_interval="@daily",
        default_args=default_args,
        start_date=days_ago(2),
        catchup=False
) as dag:

    normalized_fhir = spark_job_pod("raw-fhir-etl-green",
                                    NAMESPACE,
                                    SPARK_IMAGE,
                                    CLIN_VARIANT_ETL_JAR,
                                    NORMALIZED_FHIR_ETL_MAIN_CLASS,
                                    RAW_FHIR_ETL_CONF,
                                    "medium-cluster")

    normalized_occurrences = spark_job_pod("raw-vcf-etl-occurrences",
                                           NAMESPACE,
                                           SPARK_IMAGE,
                                           CLIN_VARIANT_ETL_JAR,
                                           NORMALIZED_VCF_ETL_MAIN_CLASS,
                                           ["config/qa.conf", "201106_A00516_0169_AHFM3HDSXY", "occurrences", "chr11", "first_load"],
                                           "large-cluster")

    normalized_variants = spark_job_pod("raw-vcf-etl-variants",
                                        NAMESPACE,
                                        SPARK_IMAGE,
                                        CLIN_VARIANT_ETL_JAR,
                                        NORMALIZED_VCF_ETL_MAIN_CLASS,
                                        ["config/qa.conf", "201106_A00516_0169_AHFM3HDSXY", "variants", "chr11", "first_load"],
                                        "large-cluster")

    normalized_consequences = spark_job_pod("raw-vcf-etl-consequences",
                                            NAMESPACE,
                                            SPARK_IMAGE,
                                            CLIN_VARIANT_ETL_JAR,
                                            NORMALIZED_VCF_ETL_MAIN_CLASS,
                                            ["config/qa.conf", "201106_A00516_0169_AHFM3HDSXY", "consequences", "chr11", "first_load"],
                                            "large-cluster")

    enriched_variants = spark_job_pod("enriched-etl-variants",
                                      NAMESPACE,
                                      SPARK_IMAGE,
                                      CLIN_VARIANT_ETL_JAR,
                                      ENRICHED_ETL_MAIN_CLASS,
                                      ["config/qa.conf", "variants", "11", "first_load"],
                                      "large-cluster")

    enriched_consequences = spark_job_pod("enriched-etl-consequences",
                                          NAMESPACE,
                                          SPARK_IMAGE,
                                          CLIN_VARIANT_ETL_JAR,
                                          ENRICHED_ETL_MAIN_CLASS,
                                          ["config/qa.conf", "consequences", "11", "first_load"],
                                          "large-cluster")

    prepare_variant_centric = spark_job_pod("prepare-variant-centric-etl",
                                            NAMESPACE,
                                            SPARK_IMAGE,
                                            CLIN_VARIANT_ETL_JAR,
                                            PREPARE_INDEX_ETL_MAIN_CLASS,
                                            ["config/qa.conf", "variant_centric", "re_000", "first_load"],
                                            "large-cluster")

    prepare_variant_suggestions = spark_job_pod("prepare-variant-suggestions-etl",
                                                NAMESPACE,
                                                SPARK_IMAGE,
                                                CLIN_VARIANT_ETL_JAR,
                                                PREPARE_INDEX_ETL_MAIN_CLASS,
                                                ["config/qa.conf", "variant_suggestions", "re_000", "first_load"],
                                                "large-cluster")

    index_variant_centric = spark_job_pod("index-variant-centric-etl",
                                          NAMESPACE,
                                          SPARK_IMAGE,
                                          CLIN_VARIANT_ETL_JAR,
                                          INDEXER_ETL_MAIN_CLASS,
                                          [ELEASTICSEARCH_URL, "", "", "clin_qa_green_variant_centric", "re_000", "variant_centric_template.json",
                                          "variant_centric", "1900-01-01 00:00:00", "config/qa.conf"],
                                          "large-cluster")

    index_variant_suggestions = spark_job_pod("index-variant-suggestions-etl",
                                              NAMESPACE,
                                              SPARK_IMAGE,
                                              CLIN_VARIANT_ETL_JAR,
                                              INDEXER_ETL_MAIN_CLASS,
                                              [ELEASTICSEARCH_URL, "", "", "clin_qa_green_variant_suggestions", "re_000",
                                               "variant_suggestions_template.json", "variant_suggestions", "1900-01-01 00:00:00", "config/qa.conf"],
                                              "large-cluster")

    set_alias = curl_job_pod("set-green-aliases",
                             NAMESPACE)

    normalized_fhir >> normalized_occurrences >> normalized_variants >> normalized_consequences >> enriched_variants >> enriched_consequences
    enriched_consequences >> prepare_variant_centric >> prepare_variant_suggestions >> index_variant_centric >> index_variant_suggestions
    index_variant_suggestions >> set_alias

