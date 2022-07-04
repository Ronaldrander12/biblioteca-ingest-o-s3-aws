import os
import json
import boto3

from pathlib import Path
base_path_creds = os.path.join(Path(os.getcwd()),'credentials')


def get_aws_creds() -> dict:
    aws_creds_path = os.path.join(base_path_creds, 'creds_aws.json')

    with open(f'{aws_creds_path}', 'r') as f:
        creds_aws = json.loads(f.read())

    return creds_aws

def get_aws_client(service:str) -> None:
    creds_aws = get_aws_creds()

    client_aws = boto3.client(
        service,
        aws_access_key_id=creds_aws['AWSAccessKeyId'],
        aws_secret_access_key=creds_aws['AWSSecretKey']

    )

    return client_aws

def get_aws_session() -> boto3.session.Session:
    creds_aws = get_aws_creds()

    session_aws = boto3.Session(
            aws_access_key_id=creds_aws['AWSAccessKeyId'],
            aws_secret_access_key=creds_aws['AWSSecretKey']
        )

    return session_aws

def get_pg_creds() -> dict:
    pg_creds_path = os.path.join(base_path_creds, 'creds_pg_file.json)

    with open(f'{pg_creds_path}', 'r') as f:
        creds_pg = json.loads(f.read())

    return creds_pg    