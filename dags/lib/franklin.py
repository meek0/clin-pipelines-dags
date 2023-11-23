import json
import logging
import http.client
import urllib.parse
from datetime import datetime
from lib import config
from lib.config import env

import_bucket = f'cqgc-{env}-app-files-import'
export_bucket = f'cqgc-{env}-app-datalake'
vcf_suffix = '.hard-filtered.formatted.norm.VEP.vcf.gz'
franklin_url_parts = urllib.parse.urlparse(config.franklin_url)

def get_metadata_content(clin_s3, batch_id):
    metadata_path = f'{batch_id}/metadata.json'
    file_obj = clin_s3.get_key(metadata_path, import_bucket)
    return json.loads(file_obj.get()['Body'].read().decode('utf-8'))

def get_franklin_conn():
    if config.franklin_url.startswith('https'):
        conn = http.client.HTTPSConnection(franklin_url_parts.hostname)
    else:
        conn = http.client.HTTPConnection(franklin_url_parts.hostname, port=franklin_url_parts.port)
    logging.info(f'Conn: {franklin_url_parts.hostname} {franklin_url_parts.port} {franklin_url_parts.path}')
    return conn

def get_franklin_token():
    conn = get_franklin_conn()
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
    logging.info(f'Upload to Franklin: {config.s3_franklin_bucket}/{destination_key}')
    s3_franklin.load_bytes(vcf_content, destination_key, config.s3_franklin_bucket, replace=True)

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


def build_payload(family_id, analyses, batch_id, s3):
    # ok mais ca devrait etre different entre deux batch ?
    assay_id = "2765500d-8728-4830-94b5-269c306dbe71"  # value given from franklin
    family_analyses = []
    analyses_payload = []
    for analysis in analyses:
        sample = {
            "sample_name": f'{analysis["labAliquotId"]} - {analysis["patient"]["familyMember"]}',
            "family_relation": get_relation(analysis["patient"]["familyMember"]),
            "is_affected": analysis["patient"]["status"] == 'AFF',
        }
        if family_id:
            family_analyses.append(sample)
            if analysis["patient"]["familyMember"] == 'PROBAND':
                proband_id = analysis["labAliquotId"]
        else:
            proband_id = analysis["labAliquotId"]

        phenotypes = get_phenotypes(proband_id, batch_id, s3)
        analyses_payload.append({
            "assay_id": assay_id,
            'sample_data': {
                "sample_name": f'{analysis["labAliquotId"]} - {analysis["patient"]["familyMember"]}',
                "name_in_vcf": analysis["labAliquotId"],
                "aws_files": [
                    {
                        "key": f'{batch_id}/{proband_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz',
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


def start_analysis(family_id, analyses, token, s3, batch_id):
    conn = get_franklin_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}

    if family_id:
        payload = build_payload(family_id, analyses, batch_id, s3)
    else:
        payload = build_payload(None, analyses, batch_id, s3)
  
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
