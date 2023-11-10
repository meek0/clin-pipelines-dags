import json
import logging
import http.client
import urllib.parse
from datetime import datetime

from lib.config import env

import_bucket = f'cqgc-{env}-app-files-import'
export_bucket = f'cqgc-{env}-app-datalake'


def authenticate(email, password):
    conn = http.client.HTTPSConnection("api.genoox.com")

    payload = urllib.parse.urlencode({'email': email, 'password': password})
    path = '/v1/auth/login?'
    conn.request("GET", path + payload,
                 "")

    res = conn.getresponse()
    data = res.read()
    decoded_data = json.loads(data.decode('utf-8'))
    return decoded_data['token']


def check_analysis_exists(s3, batch_id, family_id, aliquot_id):
    if not batch_id and not family_id and not aliquot_id:
        raise Exception('batch_id, analysis_id are required')
    if not family_id:
        path = f'raw/landing/franklin/batch_id={batch_id}/family_id=null/aliquot_id={aliquot_id}/_IN_PROGRESS_.txt'
    else:
        # todo - adjust here
        path = f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/aliquot_id=null/_IN_PROGRESS_.txt'
    if s3.check_for_key(path, export_bucket):
        logging.info("Path exists in minio")
        return True
    return False


def group_families(data):
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


def transfer_vcf_to_franklin(s3_clin, s3_franklin, source_key, batch_id):
    clin_bucket = import_bucket
    franklin_bucket = 'genoox-upload-chu-st-justine'
    destination_key = f'{env}/{source_key}'

    try:
        path = f'{batch_id}/{source_key}'
        logging.info(f'trying to copy file {path}')
        file = s3_clin.get_key(path, clin_bucket)
        file_content = file.get()['Body'].read()
        logging.info(f'found the file {file}')
        s3_franklin.load_bytes(file_content, destination_key, franklin_bucket)
        # garbage collector ??
        # del file_content
        logging.info(f'{file} was copied correctly at {destination_key}')
    except Exception as e:
        logging.info(f'Error: {e}')


def copy_files_to_franklin(s3_clin, s3_franklin, analyse, batch_id):
    files_to_transfer = []
    #  assuming the XXXXX.VEP.vcf.gz file for a family is the PROBAND's aliquot_id

    if analyse['patient']['familyMember'] == 'PROBAND':
        files_to_transfer.append(f"{analyse['labAliquotId']}.case.hard-filtered.formatted.norm.VEP.vcf.gz")
    for to_transfer in files_to_transfer:
        transfer_vcf_to_franklin(s3_clin, s3_franklin, to_transfer, batch_id)

    return True


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
    assay_id = "2765500d-8728-4830-94b5-269c306dbe71"  # value given from franklin
    family_analyses = []
    analyses_payload = []
    for analysis in analyses:
        logging.info(f'in analysis {analysis}')
        sample = {
            "sample_name": f'{analysis["labAliquotId"]} - {analysis["patient"]["familyMember"]}',
            "family_relation": get_relation(analysis["patient"]["familyMember"]),
            "is_affected": analysis["patient"]["status"] == 'AFF',
        }
        logging.info(f'sample {sample}')
        if family_id:
            family_analyses.append(sample)
            if analysis["patient"]["familyMember"] == 'PROBAND':
                proband_id = analysis["labAliquotId"]
        else:
            proband_id = analysis["labAliquotId"]

        logging.info(f'proband id {proband_id}')
        phenotypes = get_phenotypes(proband_id, batch_id, s3)
        logging.info(f'phenotypes {phenotypes}')
        analyses_payload.append({
            "assay_id": assay_id,
            'sample_data': {
                "sample_name": f'{analysis["labAliquotId"]} - {analysis["patient"]["familyMember"]}',
                "name_in_vcf": analysis["labAliquotId"],
                "aws_files": [
                    {
                        "key": f'{env}/{proband_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz',
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
                "bucket": 'genoox-upload-chu-st-justine',
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
    try:
        conn = http.client.HTTPSConnection("api.genoox.com")

        headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}
        if family_id:
            payload = build_payload(family_id, analyses, batch_id, s3)
        else:
            payload = build_payload(None, analyses, batch_id, s3)
            logging.info(f'solo payload {payload}')
        conn.request("POST", "/v1/analyses/create", json.dumps(payload).encode('utf-8'), headers)

        logging.info(f'Request sent to franklin', json.dumps(payload).encode('utf-8'))
        res = conn.getresponse()
        data = res.read()
        s = json.loads(data.decode('utf-8'))
        logging.info(s)
        return s
    except Exception as e:
        logging.info(f'Error: {e}')


def get_analyses_status(started_analyses, token):
    conn = http.client.HTTPSConnection("api.genoox.com")

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
    conn = http.client.HTTPSConnection("api.genoox.com")

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
