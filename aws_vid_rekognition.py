#!/python

from datetime import datetime
from xclasses import Config
import boto3
from botocore.exceptions import ClientError
import logging
import json,sys,uuid

def create_temp_bucket(id):
    bucket_name = id + '-temp-bucket'
    s3 = boto3.client('s3')
    try:
        print(bucket_name)
        s3.create_bucket(Bucket=bucket_name)
        s3.put_bucket_lifecycle_configuration(Bucket=bucket_name,LifecycleConfiguration={'Rules':[{'ID':bucket_name,'Prefix':'','Expiration':{'Days':1},'Status': 'Enabled'}]})
    except ClientError as e:
        logging.error(e)
        return False
    return True

def main():
    cfg = Config('aws_vid_rekognition.json')
    id = str(uuid.uuid4())
    session = boto3.Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    accessKey = current_credentials.access_key
    secretKey = current_credentials.secret_key

if __name__ == '__main__':
    main()
