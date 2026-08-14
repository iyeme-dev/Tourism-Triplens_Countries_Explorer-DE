import os
import json
import boto3

from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()


access_key = os.getenv('MINIO_ROOT_USER')
secret_key = os.getenv('MINIO_ROOT_PASSWORD')


def load_to_bucket(data):
    bucket_name = 'triplens'
    folder_path = 'raw'
    object_name = f'{folder_path}/triplens_global.json'

    client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False
    )

    try:
        client.head_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} already exists')

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            client.create_bucket(Bucket=bucket_name)
            print(f'{bucket_name} created')

    data = json.dumps(
        data,
        ensure_ascii=False
    ).encode('utf-8')

    client.put_object(
        Bucket=bucket_name,
        Key=object_name,
        Body=data,
        ContentType='application/json'
    )

    print(f'data loaded successfully to {bucket_name}/{folder_path}')

    return None