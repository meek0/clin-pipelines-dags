import json
from enum import Enum

import kubernetes
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


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
s3_franklin = Variable.get('s3_franklin', None)
s3_franklin_bucket = Variable.get('s3_franklin_bucket', None)
franklin_url = Variable.get('franklin_url', None)
franklin_email = Variable.get('franklin_email', None)
franklin_password = Variable.get('franklin_password', None)
slack_hook_url = Variable.get('slack_hook_url', None)
show_test_dags = Variable.get('show_test_dags', None) == 'yes'
cosmic_credentials = Variable.get('cosmic_credentials', None)
topmed_bravo_credentials = Variable.get('topmed_bravo_credentials', None)
basespace_illumina_credentials = Variable.get('basespace_illumina_credentials', None)

arranger_image = 'ferlabcrsj/clin-arranger:1.3.3'
aws_image = 'amazon/aws-cli'
curl_image = 'curlimages/curl'
fhir_csv_image = 'ferlabcrsj/csv-to-fhir'
postgres_image = 'ferlabcrsj/postgres-backup:9bb43092f76e95f17cd09f03a27c65d84112a3cd'
spark_image = 'ferlabcrsj/spark:65d1946780f97a8acdd958b89b64fad118c893ee'
spark_service_account = 'spark'
batch_ids = []

clin_import_bucket = f'cqgc-{env}-app-files-import'
clin_datalake_bucket = f'cqgc-{env}-app-datalake'

if env == Env.QA:
    fhir_image = 'ferlabcrsj/clin-fhir'
    pipeline_image = 'ferlabcrsj/clin-pipelines'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = 'http://elasticsearch:9200'
    spark_jar = 'clin-variant-etl-v3.4.0.jar'
    obo_parser_spark_jar = 'obo-parser-v1.1.0.jar'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
    indexer_context = K8sContext.DEFAULT
    auth_url = 'https://auth.qa.cqgc.hsj.rtss.qc.ca'
    config_file = f'config/qa.conf'
    franklin_assay_id = '2765500d-8728-4830-94b5-269c306dbe71'
    batch_ids = [
        '201106_A00516_0169_AHFM3HDSXY',
        'test_extum', 'Batch_ParCas', 'test_franklin',
        'test_somatic_normal_part1', 'test_somatic_normal_part2',
        '2_data_to_import_germinal', 'test_dragen_4_2_4_germline', 'test_franklin']
elif env == Env.STAGING:
    fhir_image = 'ferlabcrsj/clin-fhir:a77e25a'
    pipeline_image = 'ferlabcrsj/clin-pipelines:c446a3a'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = 'http://elasticsearch:9200'
    spark_jar = 'clin-variant-etl-v3.3.4.jar'
    obo_parser_spark_jar = 'obo-parser-v1.1.0.jar'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
    indexer_context = K8sContext.DEFAULT
    auth_url = 'https://auth.staging.cqgc.hsj.rtss.qc.ca'
    config_file = f'config/staging.conf'
    franklin_assay_id = '2765500d-8728-4830-94b5-269c306dbe71'
    batch_ids = [
        '201106_A00516_0169_AHFM3HDSXY',
        '230928_A00516_0463_BHJ5NTDRX3',
        '230724_A00516_0440_BH7L2FDRX3',
        '231120_A00516_0484_BHMYT3DSX7_somatic',
        '231211_A00516_0490_BHNK22DRX3_somatic',
        '231113_A00516_0480_BHLJCMDRX3',
        '230828_A00516_0454_BHLM57DMXY.dragen.WES_somatic-tumor_only',
        '230609_A00516_0425_AHKNCFDMXY',
        '240201_A00516_0001_SYNTH',
        'test_dragen_4_2_4_germline',
    ]
elif env == Env.PROD:
    fhir_image = 'ferlabcrsj/clin-fhir:a77e25a'
    pipeline_image = 'ferlabcrsj/clin-pipelines:c446a3a'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = 'https://workers.search.cqgc.hsj.rtss.qc.ca:9200'
    spark_jar = 'clin-variant-etl-v3.3.4.jar'
    obo_parser_spark_jar = 'obo-parser-v1.1.0.jar'
    ca_certificates = 'ca-certificates-bundle'
    minio_certificate = 'ca-certificates-bundle'
    indexer_context = K8sContext.ETL
    auth_url = 'https://auth.cqgc.hsj.rtss.qc.ca'
    config_file = f'config/prod.conf'
    franklin_assay_id = 'b8a30771-5689-4189-8157-c6063ad738d1'
    batch_ids = [
        '221017_A00516_0366_BHH2T3DMXY',
        '221209_A00516_0377_BHHHJWDMXY',
        '230130_A00516_0386_BHGV3NDMXY',
        '230130_A00516_0387_AHLKYGDRX2',
        '230314_A00516_0396_AHHYHLDMXY',
        '230329_A00516_0401_AHTLY5DRX2',
        '230329_A00516_0402_BHHYGVDMXY',
        '230505_A00516_0412_BHK72VDMXY',
        '230526_A00516_0419_AHKCL2DMXY',
        '230609_A00516_0425_AHKNCFDMXY',
        '230614_A00516_0427_BHKY2LDMXY',
        '230628_A00516_0430_BHTLWMDRX2',
        '230713_A00516_0435_BHKWVGDMXY',
        '230803_A00516_0444_AHLNJKDMXY',
        '230818_A00516_0449_BH7N22DSX7',
        '231002_A00516_0466_AHM553DMXY',
        '231002_A00516_0467_BHHYV7DRX3',
        '231102_A00516_0476_AHMVFNDMXY',
        '231120_A00516_0484_BHMYT3DSX7',
        '231215_A00516_0493_BHN55LDMXY',
        '231215_A00516_0494_AHMTWFDMXY',
        '240112_A00516_0503_AHN2YTDRX3_somatic.part1',
        '240112_A00516_0503_AHN2YTDRX3_somatic.part2',
        '240119_A00516_0506_BHN3J2DMXY_germinal.part1',
        '240126_A00516_0509_BHTK5LDRX3_somatic',
        '240119_A00516_0506_BHN3J2DMXY_germinal.part3.2024-02-05',
        '240212_A00516_0516_AHNFMKDMXY_germinal',
        '240212_A00516_0517_BHNLLCDRX3_somatic',
        '240229_A00516_0524_BHWFHWDRX3_somatic',
        '240119_A00516_0506_BHN3J2DMXY_germinal.part2.2024-02-02',
        '240229_A00516_0525_AHNHWFDMXY_germinal',
        '240308_A00516_0531_BHWVJJDRX3_somatic_Dragen42',
        '240320_A00516_0532_AHW7YHDRX3_somatic',
        '240328_A00516_0537_BHWWH5DRX3_somatic',
        '240402_A00516_0538_AHWVNVDRX3_germinal',
        '240321_A00516_0534_AH5NV3DSXC_germinal_prise2',
        '240412_A00516_0544_BH5J7KDRX5_somatic',
        '240418_A00516_0546_AHTWCFDMXY_somatic',
        '240418_A00516_0546_AHTWCFDMXY_germinal',
        '240429_A00516_0551_AH5JLFDRX5_germinal',
        '240308_A00516_0531_BHWVJJDRX3_somatic_normal',
        '240328_A00516_0537_BHWWH5DRX3_somatic_normal',
        '240412_A00516_0544_BH5J7KDRX5_somatic_normal',
        '230427_A00516_0410_BH5NT3DRX3_somatic',
        '230703_A00516_0432_BHVW7WDRX2_somatic',
        '230724_A00516_0440_BH7L2FDRX3_somatic',
        '230828_A00516_0454_BHLM57DMXY_somatic',
        '230928_A00516_0463_BHJ5NTDRX3_somatic',
        '231113_A00516_0480_BHLJCMDRX3_somatic',
        '231120_A00516_0484_BHMYT3DSX7_somatic',
        '231211_A00516_0490_BHNK22DRX3_somatic',
        '240513_A00516_0555_AH5J3VDRX5_somatic',
        '240513_A00516_0554_BHTWGGDMXY_germinal',
        '240522_A00977_0744_BH5TY7DRX5_somatic',
        '231211_A00516_0490_BHNK22DRX3_somatic_normal',
        "240522_A00977_0744_BH5TY7DRX5_somatic_normal",
        "240320_A00516_0532_AHW7YHDRX3_somatic_normal",
        "240513_A00516_0555_AH5J3VDRX5_somatic_normal",
        "240603_A00516_0563_AHTWVYDMXY_germinal",
        '240418_A00516_0546_AHTWCFDMXY_somatic_normal',
        '240613_A00516_0566_AH5WJVDRX5_somatic',
        '240613_A00516_0566_AH5WJVDRX5_somatic_normal',
        '240620_A00516_0567_AHJ75TDSXC_germinal',
        '240710_A00516_0571_AHF7CYDRX5_somatic',
        '240112_A00516_0503_AHN2YTDRX3_somatic_normal',
        '240112_A00516_0503_AHN2YTDRX3_somatic_normal',
        '240522_A00977_0744_BH5TY7DRX5_somatic_normal',
        '240308_A00516_0531_BHWVJJDRX3_somatic_normal',
        '240613_A00516_0566_AH5WJVDRX5_somatic_normal',
        '240710_A00516_0571_AHF7CYDRX5_somatic_normal',
        '240712_A00516_0573_BH3WCYDRX5_somatic',
        '240724_A00516_0576_BHF7KFDRX5_somatic',
        '240725_A00516_0578_AHJCCYDSXC_germinal',
        '240729_A00516_0579_BHFFLJDRX5_germinal',
        '240808_A00516_0585_AHGC5LDRX5_somatic',
        '240809_A00516_0586_BHVGJMDMXY_germinal',
        '240815_A00516_0588_BHG7LVDRX5_somatic',
        '240821_A00516_0591_BHG7MVDRX5_somatic',
        '240816_A00516_0590_AHG7MWDRX5_germinal',
        '240815_A00516_0588_BHG7LVDRX5_somatic_normal',
        '240513_A00516_0555_AH5J3VDRX5_somatic_normal',
        '240613_A00516_0566_AH5WJVDRX5_somatic_normal',
        '240710_A00516_0571_AHF7CYDRX5_somatic_normal',
        '240712_A00516_0573_BH3WCYDRX5_somatic_normal',
        '240724_A00516_0576_BHF7KFDRX5_somatic_normal',

    ]
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
