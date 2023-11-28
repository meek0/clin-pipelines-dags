import gzip
import http.client
import json
import logging
import shutil
import tempfile
import urllib.parse
import uuid
from datetime import datetime
from enum import Enum
from io import BytesIO

from airflow.exceptions import AirflowFailException
from lib import config
from lib.config import env


# current state of an analysis is saved inside _FRANKLIN_STATUS_.txt
class FranklinStatus(Enum):
    UNKNOWN     = 0 # equivalent to never created / not found
    CREATED     = 1 # analysis created 
    READY       = 3 # status of the analysis is READY
    COMPLETED   = 4 # we have successfully retrieved the JSON and save into S3

import_bucket = f'cqgc-{env}-app-files-import'
export_bucket = f'cqgc-{env}-app-datalake'
vcf_suffix = 'hard-filtered.formatted.norm.VEP.vcf.gz'
franklin_url_parts = urllib.parse.urlparse(config.franklin_url)
familyAnalysisKeyword = 'family'

def get_metadata_content(clin_s3, batch_id):
    metadata_path = f'{batch_id}/metadata.json'
    file_obj = clin_s3.get_key(metadata_path, import_bucket)
    return json.loads(file_obj.get()['Body'].read().decode('utf-8'))

# group the metadata analyses by families or solos
def group_families_from_metadata(data):
    family_groups = {}
    analyses_without_family = []
    for analysis in data['analyses']:
        if 'familyId' in analysis['patient']:
            family_id = analysis['patient']['familyId']
            if family_id in family_groups:
                family_groups[family_id].append(analysis)
            else:
                family_groups[family_id] = [analysis]
        else:
            analyses_without_family.append(analysis)
    return [family_groups, analyses_without_family]

def transfer_vcf_to_franklin(s3_clin, s3_franklin, source_key):

    logging.info(f'Retrieve VCF content: {import_bucket}/{source_key}')
    vcf_file = s3_clin.get_key(source_key, import_bucket)
    vcf_content = vcf_file.get()['Body'].read()
    logging.info(f'VCF content size: {len(vcf_content)}')
    aliquot_ids = extract_aliquot_ids_from_vcf(vcf_content)
    logging.info(f'Aliquot IDs in VCF: {aliquot_ids}')

    # ignore upload if already on Franklin S3 with the same length
    destination_key = f'{env}/{source_key}'
    destination_vcf_content_size = get_s3_key_content_size(s3_franklin, config.s3_franklin_bucket, destination_key)
    if (destination_vcf_content_size != len(vcf_content)):
        logging.info(f'Upload to Franklin: {config.s3_franklin_bucket}/{destination_key}')
        s3_franklin.load_bytes(vcf_content, destination_key, config.s3_franklin_bucket, replace=True)

    return aliquot_ids

# took a lot of efforts to have something working, feel free to improve it in the future for fun
def extract_aliquot_ids_from_vcf(vcf_content):
    aliquot_ids = []
    with tempfile.NamedTemporaryFile(delete=True) as temp_zipped_file, tempfile.NamedTemporaryFile(delete=True) as temp_unzipped_file:
        temp_zipped_file.write(vcf_content)
        logging.info(f'VCF tmp location zipped: {temp_zipped_file.name} unzipped: {temp_unzipped_file.name}')
        with gzip.open(temp_zipped_file.name, 'rb') as f_in, open(temp_unzipped_file.name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out, length=100*1024*1024)
            with open(temp_unzipped_file.name, 'r') as file:
                for line in file:
                    if line.startswith('#CHROM'):
                        formatFound = False
                        cols = line.split('\t')
                        for col in cols:
                            if col == 'FORMAT':
                                formatFound = True
                                continue
                            if formatFound:
                                aliquot_ids.append(col.replace('\n',''))
                        break # aliquots line found, stop here
    if len(aliquot_ids) == 0:
        raise AirflowFailException(f'VCF aliquot IDs not found')
    return aliquot_ids

def attach_vcf_to_analysis(analysis, aliquot_ids):
    aliquot_id = analysis['labAliquotId']
    for vcf in aliquot_ids:
        if aliquot_id in aliquot_ids[vcf]:
            analysis['vcf'] = vcf
            return
    # did we miss one during extraction ?
    raise AirflowFailException(f'No VCF to attach: {aliquot_id}') 

# add a 'vcf' field to the analyses
def attach_vcf_to_analyses(obj, aliquot_ids):
    families = obj['families']
    solos = obj['no_family']
    for family_id, analyses in families.items():
        for analysis in analyses:
            attach_vcf_to_analysis(analysis, aliquot_ids)
    for patient in solos:
        attach_vcf_to_analysis(patient, aliquot_ids)
    return obj

def get_s3_key_content_size(s3, bucket, key): 
    if (s3.check_for_key(key, bucket)):
        file = s3.get_key(key, bucket)
        file_content = file.get()['Body'].read()
        return len(file_content)
    return 0

# avoid spamming franklin <!>
def canCreateAnalysis(clin_s3, batch_id, family_id, analyses):
    for analysis in analyses:
        aliquot_id = analysis["labAliquotId"]
        completed_analysis_keys = clin_s3.list_keys(export_bucket, buildS3AnalysesRootKey(batch_id, family_id, aliquot_id))
        for key in completed_analysis_keys:
            if 'analysis_id=' in key:   # found at least one completed analysis
                logging.info(f'Completed analysis found: {batch_id} {family_id} {aliquot_id}')
                return False
        status = checkS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id)
        if status != FranklinStatus.UNKNOWN: # fund at least one analysis with a STATUS
            logging.info(f'Created analysis found: {batch_id} {family_id} {aliquot_id}')
            return False
    return True

def checkS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id) -> FranklinStatus : 
    key = buildS3AnalysesStatusKey(batch_id, family_id, aliquot_id)
    if (clin_s3.check_for_key(key, export_bucket)):
        file = clin_s3.get_key(key, export_bucket)
        file_content = file.get()['Body'].read()
        return FranklinStatus[file_content.decode('utf-8')]
    return FranklinStatus.UNKNOWN   # analysis doesn't exist

def writeS3AnalysesStatus(clin_s3, batch_id, family_id, analyses, status, ids = None):
    for analysis in analyses:
        writeS3AnalysisStatus(clin_s3, batch_id, family_id, analysis['labAliquotId'], status, ids)

def writeS3AnalysisStatus(clin_s3, batch_id, family_id, aliquot_id, status, ids = None, id = None):
    clin_s3.load_string(status.name, buildS3AnalysesStatusKey(batch_id, family_id, aliquot_id), export_bucket, replace=True)
    if ids is not None: # save TRIO, DUO ... analyses IDs
        clin_s3.load_string(','.join(map(str, ids)), buildS3AnalysesIdsKey(batch_id, family_id, aliquot_id), export_bucket, replace=True)
    if id is not None:  # after status we can attached an ID to a specific family + aliquot id whatever it's SOLO or TRIO, DUO ...
        clin_s3.load_string(str(id), buildS3AnalysesIdKey(batch_id, family_id, aliquot_id), export_bucket, replace=True)

def buildS3AnalysesRootKey(batch_id, family_id, aliquot_id):
    return f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id or "null"}/aliquot_id={aliquot_id or "null"}'

def buildS3AnalysesJSONKey(batch_id, family_id, aliquot_id, analysis_id):
    return f'{buildS3AnalysesRootKey(batch_id, family_id, aliquot_id)}/analysis_id={analysis_id}/analysis.json'

def buildS3AnalysesStatusKey(batch_id, family_id, aliquot_id):
    return f'{buildS3AnalysesRootKey(batch_id, family_id, aliquot_id)}/_FRANKLIN_STATUS_.txt'

def buildS3AnalysesIdKey(batch_id, family_id, aliquot_id):
    return f'{buildS3AnalysesRootKey(batch_id, family_id, aliquot_id)}/_FRANKLIN_ID_.txt'

def buildS3AnalysesIdsKey(batch_id, family_id, aliquot_id):
    if family_id is not None: # IDS are stored at family level TRIO, DUO ...
        return f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/_FRANKLIN_IDS_.txt'
    else:   # SOLO
        return f'raw/landing/franklin/batch_id={batch_id}/family_id=null/aliquot_id={aliquot_id}/_FRANKLIN_IDS_.txt'

# extractParamFromS3Key('raw/landing/franklin/batch_id=foo' ,'batch_id') -> foo
def extractParamFromS3Key(key, param_name):
    for param in key.split('/'):
        if param.startswith(f"{param_name}="):
            value = param.split('=')[1]
            if value == 'null':
                return None # we want to have None when dealing with null in S3
            else:
                return value
    raise AirflowFailException(f'Cant find param: {param_name} in s3 key: {key}')

def build_sample_name(aliquot_id, family_id):
    return f'{aliquot_id} - {family_id}'    # seems to convert SOLO family_id to 'None' as a str
    
def extract_from_name_aliquot_id(name):
    id = name.split("-")[0].strip()
    # family analysis has no aliquot
    return id if id != familyAnalysisKeyword else None

def extract_from_name_family_id(name):
    id = name.split("-")[1].strip()
    return id if id != 'None' else None # cf build_sample_name()

def get_relation(relation):
    if relation == 'FTH':
        return 'father'
    elif relation == 'MTH':
        return 'mother'
    elif relation == 'PROBAND':
        return 'proband'

def format_date(input_date):
    input_format = "%d/%m/%Y"
    input_date = datetime.strptime(input_date, input_format)
    output_date = input_date.strftime("%Y-%m-%d")
    return output_date

def get_phenotypes(id, batch_id, s3):
    key = f'{batch_id}/{id}.hpo'
    if s3.check_for_key(key, import_bucket):
        file = s3.get_key(key, import_bucket)
        file_content = file.get()['Body'].read().decode('utf-8')
        logging.info(f'HPO file found: {file_content}')
        # it's not a JSON, ex: ['HP:0000001','HP:0000002','HP:0000003','HP:0000004']
        return file_content.replace("[", "").replace("]", "").replace("'", "").replace("\"", "").replace(" ", "").replace("\n", "").split(',')
    else:
        return []

def build_create_analysis_payload(family_id, analyses, batch_id, clin_s3, franklin_s3):
    family_analyses = []
    analyses_payload = []
    assay_id = str(uuid.uuid4()),
    for analysis in analyses:
        aliquot_id = analysis["labAliquotId"]
        family_member = analysis["patient"]["familyMember"]
        vcf = analysis['vcf']
        sample_name = build_sample_name(aliquot_id, family_id)
        sample = {
            "sample_name": sample_name,
            "family_relation": get_relation(family_member),
            "is_affected": analysis["patient"]["status"] == 'AFF'
        }
        if family_id:
            family_analyses.append(sample)
            if family_member == 'PROBAND':
                proband_id = aliquot_id
        else:
            proband_id = aliquot_id

        vcf_franklin_s3_full_path = f'{env}/{vcf}'
        # last resort before going further
        # check if the VCF exists in Franklin S3
        if franklin_s3.check_for_key(vcf_franklin_s3_full_path, config.s3_franklin_bucket):
            analyses_payload.append({
                "assay_id": assay_id,
                'sample_data': {
                    "sample_name": sample_name,
                    "name_in_vcf": aliquot_id,
                    "aws_files": [
                        {
                            "key": vcf,
                            "type": "VCF_SHORT"
                        }
                    ],
                    "tissue_type": "Whole Blood",
                    "patient_details": {
                        "name": analysis["patient"]["firstName"],
                        "dob": format_date(analysis["patient"]["birthDate"]),
                        "sex": analysis["patient"]["sex"].title()
                    }
                }
            })
        else:
            raise AirflowFailException(f'VCF not found: {config.s3_franklin_bucket}/{vcf_franklin_s3_full_path}')

    payload = {
        'upload_specs': {
            "source": 'AWS',
            "details": {
                "bucket": config.s3_franklin_bucket,
                'root_folder': env
            }
        },
        'analyses': analyses_payload,
    }

    if family_id:
        payload['family_analyses'] = [
            {
                'case_name': build_sample_name(familyAnalysisKeyword, family_id),
                'family_samples': family_analyses,
                "phenotypes": get_phenotypes(proband_id, batch_id, clin_s3)
            }
        ]
        payload["family_analyses_creation_specs"] = {
            "create_family_single_analyses": 'true'
        }

    return payload

def parseResponse(res):
    data = res.read()
    body = data.decode('utf-8')
    if res.status != 200:   # log if something wrong
        raise AirflowFailException(f'{res.status} - {body}')
    return body

def parseResponseJSON(res):
    return json.loads(parseResponse(res))

def get_franklin_http_conn():
    if config.franklin_url.startswith('https'):
        conn = http.client.HTTPSConnection(franklin_url_parts.hostname)
    else:
        conn = http.client.HTTPConnection(franklin_url_parts.hostname, port=franklin_url_parts.port)
    return conn

def get_franklin_token():
    conn = get_franklin_http_conn()
    payload = urllib.parse.urlencode({'email': config.franklin_email, 'password': config.franklin_password})
    conn.request("GET", franklin_url_parts.path + '/v1/auth/login?' + payload)
    conn.close
    return parseResponseJSON(conn.getresponse())['token']

def post_create_analysis(family_id, analyses, token, clin_s3, franklin_s3, batch_id):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}
    payload = json.dumps(build_create_analysis_payload(family_id, analyses, batch_id, clin_s3, franklin_s3)).encode('utf-8')
    logging.info(f'Create analysis: {family_id} {analyses}')
    conn.request("POST", franklin_url_parts.path + "/v1/analyses/create", payload, headers)
    conn.close
    return parseResponseJSON(conn.getresponse())


def get_analysis_status(started_analyses, token):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}
    payload = json.dumps({'analysis_ids': started_analyses}).encode('utf-8')
    logging.info(f'Get analysis status: {started_analyses}')
    conn.request("POST", franklin_url_parts.path + "/v1/analyses/status", payload, headers)
    conn.close
    return parseResponseJSON(conn.getresponse())


def get_completed_analysis(id, token):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}
    logging.info(f'Get completed analysis: {id}')
    conn.request("GET", franklin_url_parts.path + f"/v2/analysis/variants/snp?analysis_id={id}", "", headers)
    conn.close
    return parseResponse(conn.getresponse())
