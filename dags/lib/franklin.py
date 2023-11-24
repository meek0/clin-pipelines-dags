import json
import logging
import http.client
import urllib.parse
from datetime import datetime
from lib import config
from lib.config import env
import uuid
from airflow.exceptions import AirflowFailException
from io import BytesIO
import gzip
import tempfile

import_bucket = f'cqgc-{env}-app-files-import'
export_bucket = f'cqgc-{env}-app-datalake'
vcf_suffix = 'hard-filtered.formatted.norm.VEP.vcf.gz'
franklin_url_parts = urllib.parse.urlparse(config.franklin_url)

def get_metadata_content(clin_s3, batch_id):
    metadata_path = f'{batch_id}/metadata.json'
    file_obj = clin_s3.get_key(metadata_path, import_bucket)
    return json.loads(file_obj.get()['Body'].read().decode('utf-8'))

def get_franklin_http_conn():
    if config.franklin_url.startswith('https'):
        conn = http.client.HTTPSConnection(franklin_url_parts.hostname)
    else:
        conn = http.client.HTTPConnection(franklin_url_parts.hostname, port=franklin_url_parts.port)
    logging.info(f'Conn: {franklin_url_parts.hostname} {franklin_url_parts.port} {franklin_url_parts.path}')
    return conn

def get_franklin_token():
    conn = get_franklin_http_conn()
    payload = urllib.parse.urlencode({'email': config.franklin_email, 'password': config.franklin_password})
    path = franklin_url_parts.path + '/v1/auth/login?'
    conn.request("GET", path + payload)
    res = conn.getresponse()
    data = res.read()
    decoded_data = json.loads(data.decode('utf-8'))
    return decoded_data['token']


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
    destination_key = f'{env}/{source_key}'
    logging.info(f'Retrieve VCF content: {import_bucket}/{source_key}')
    vcf_file = s3_clin.get_key(source_key, import_bucket)
    vcf_content = vcf_file.get()['Body'].read()
    aliquot_ids = extract_aliquot_ids_from_vcf(vcf_content)
    logging.info(f'Aliquot IDs in VCF: {aliquot_ids}')
    logging.info(f'Upload to Franklin: {config.s3_franklin_bucket}/{destination_key}')
    s3_franklin.load_bytes(vcf_content, destination_key, config.s3_franklin_bucket, replace=True)
    return aliquot_ids

def extract_aliquot_ids_from_vcf(vcf_content):
    aliquot_ids = []
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        temp_file.write(vcf_content)
        with gzip.open(temp_file.name, 'rb') as f_in:
            with BytesIO(f_in.read()) as byte_stream:
                while True:
                    line_bytes = byte_stream.readline()
                    if not line_bytes:
                        break # EOF
                    line_str = line_bytes.decode('utf-8')
                    if line_str.startswith('#CHROM'):
                        formatFound = False
                        cols = line_str.split('\t')
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

def attach_vcf_to_aliquot_id(analysis, aliquot_ids):
    aliquot_id = analysis['labAliquotId']
    vcfFound = False
    for vcf in aliquot_ids:
        if aliquot_id in aliquot_ids[vcf]:
            analysis['vcf'] = vcf
            vcfFound = True
    if not vcfFound:
        raise AirflowFailException(f'No VCF to attach: {aliquot_id}') 

def attach_vcf_to_analysis(obj, aliquot_ids):
    families = obj['families']
    solos = obj['no_family']
    for family_id, analyses in families.items():
        for analysis in analyses:
            attach_vcf_to_aliquot_id(analysis, aliquot_ids)
    for patient in solos:
        attach_vcf_to_aliquot_id(patient, aliquot_ids)
    return obj


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
        logging.info(f'found the file {key}')
        try:
            file = s3.get_key(key, import_bucket)
            logging.info(f'file {file}')
            file_content = file.get()['Body'].read()
            logging.info(f'file_content {file_content}')
            return json.loads(file_content)
        except Exception as e:
            logging.info(f'Error when getting phenotypes: {e}')
            return []
    else:
        return []


def build_create_analysis_payload(family_id, analyses, batch_id, clin_s3, franklin_s3):
    family_analyses = []
    analyses_payload = []
    for analysis in analyses:
        aliquot_id = analysis["labAliquotId"]
        vcf = analysis['vcf']
        sample = {
            "sample_name": f'{aliquot_id} - {analysis["patient"]["familyMember"]}',
            "family_relation": get_relation(analysis["patient"]["familyMember"]),
            "is_affected": analysis["patient"]["status"] == 'AFF'
        }
        if family_id:
            family_analyses.append(sample)
            if analysis["patient"]["familyMember"] == 'PROBAND':
                proband_id = aliquot_id
        else:
            proband_id = aliquot_id

        vcf_franklin_s3_full_path = f'{env}/{vcf}'
        # check of the VCF exists in Franklin S3
        if franklin_s3.check_for_key(vcf_franklin_s3_full_path, config.s3_franklin_bucket):
            phenotypes = get_phenotypes(proband_id, batch_id, clin_s3) # TODO fix that
            analyses_payload.append({
                "assay_id": str(uuid.uuid4()),
                'sample_data': {
                    "sample_name": f'{aliquot_id} - {analysis["patient"]["familyMember"]}',
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
                'case_name': f'family - {family_id}',
                'family_samples': family_analyses,
                "phenotypes": phenotypes
            }
        ]
        payload["family_analyses_creation_specs"] = {
            "create_family_single_analyses": 'true'
        }

    return payload


def post_create_analysis(family_id, analyses, token, clin_s3, franklin_s3, batch_id):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}

    if family_id:
        payload = build_create_analysis_payload(family_id, analyses, batch_id, clin_s3, franklin_s3)
    else:
        payload = build_create_analysis_payload(None, analyses, batch_id, clin_s3, franklin_s3)
  
    conn.request("POST", franklin_url_parts.path + "/v1/analyses/create", json.dumps(payload).encode('utf-8'), headers)
    res = conn.getresponse()
    data = res.read()
    s = json.loads(data.decode('utf-8'))
    return s


def get_analyses_status(started_analyses, token):
    conn = http.client.HTTPSConnection(config.franklin_url)

    payload = json.dumps({'analysis_ids': started_analyses}).encode('utf-8')
    headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }
    conn.request("POST", "/v1/analyses/status", payload, headers)

    res = conn.getresponse()
    data = res.read()

    decoded = data.decode("utf-8")
    logging.info(decoded)
    return json.loads(decoded)


def get_completed_analysis(id, token):
    conn = http.client.HTTPSConnection(config.franklin_url)

    logging.info(f'id {id}')
    headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }
    conn.request("GET", f"/v2/analysis/variants/snp?analysis_id={id}", "", headers)

    res = conn.getresponse()
    data = res.read()
    logging.info(f'data {data}')

    decoded = data.decode("utf-8")
    logging.info(decoded)
    return decoded
