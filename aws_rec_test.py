import logging
import boto3
from botocore.exceptions import ClientError
import json,sys,uuid


def create_bucket(bucket_name):
    s3 = boto3.client('s3')
    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def list_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def list_bucket(bucket):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' not in response.keys():
        print('empty')
    else:
        for file in response['Contents']:
            print(file)

def get_bucket():
    s3 = boto3.client('s3')
    return [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]

def delete_bucket(bucket):
    print(f'Deleting bucket:{bucket}')
    s3 = boto3.client('s3')
    rez = s3.delete_bucket(Bucket=bucket)
    print(rez)

def delete_from_bucket(bucket,file):
    print(f'Deleting from bucket:{bucket} file:{file}')
    s3 = boto3.client('s3')
    response = s3.delete_object(Bucket = bucket,Key = file)

def upload_to_bucket(file,bucket,s3file):
    print(f'Uploading to bucket:{bucket} local file:{file} with name:{s3file}')
    s3 = boto3.client('s3')
    s3.upload_file(file,bucket,s3file)

def get_collections():
    rek = boto3.client('rekognition')
    maxResults = 100
    response=rek.list_collections(MaxResults=maxResults)
    rez = []
    while True:
        collections = response['CollectionIds']
        for collection in collections:
            rez.append(collection)
        if 'NextToken' in response:
            nextToken = response['NextToken']
            response = rek.list_collections(NextToken=nextToken,MaxResults=maxResults)
        else:
            break
    return rez

def list_faces(collectionId,maxResults = 100):
    client = boto3.client('rekognition')
    response = client.list_faces(CollectionId = collectionId,MaxResults = maxResults)
    tokens = True
    print('Faces in collection ' + collectionId)
    while tokens:
        faces = response['Faces']
        for face in faces:
            print (face)
        if 'NextToken' in response:
            nextToken = response['NextToken']
            response=client.list_faces(CollectionId = collectionId,NextToken = nextToken,MaxResults = maxResults)
        else:
            tokens = False

def index_photo(bucket,photo,collectionId):
    rek=boto3.client('rekognition')
    response=rek.index_faces(CollectionId=collectionId,Image={'S3Object':{'Bucket':bucket,'Name':photo}},ExternalImageId=photo,MaxFaces=1,QualityFilter="AUTO",DetectionAttributes=['ALL'])
    print ('Results for ' + photo)
    print('Faces indexed:')			
    for faceRecord in response['FaceRecords']:
         print('  Face ID: ' + faceRecord['Face']['FaceId'])
         print('  Location: {}'.format(faceRecord['Face']['BoundingBox']))
    print('Faces not indexed:')
    for unindexedFace in response['UnindexedFaces']:
        print(' Location: {}'.format(unindexedFace['FaceDetail']['BoundingBox']))
        print(' Reasons:')
        for reason in unindexedFace['Reasons']:
            print('   ' + reason)

def create_collection(collectionId):
    maxResults=2
    client=boto3.client('rekognition')
    print('Creating collection:' + collectionId)
    response=client.create_collection(CollectionId=collectionId)
    print('Collection ARN: ' + response['CollectionArn'])
    print('Status code: ' + str(response['StatusCode']))
    print('Done...')

def del_collection(collectionId):
    print('Attempting to delete collection ' + collectionId)
    client=boto3.client('rekognition')
    statusCode=''
    try:
        response=client.delete_collection(CollectionId=collectionId)
        statusCode=response['StatusCode']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('The collection ' + collectionId + ' was not found ')
        else:
            print('Error other than Not Found occurred: ' + e.response['Error']['Message'])
        statusCode=e.response['ResponseMetadata']['HTTPStatusCode']
    print('Operation returned Status Code: ' + str(statusCode))
    print('Done...')

def main():
    """bucket = str(uuid.uuid4())
    print(bucket)
    create_bucket(bucket)"""
    #upload_to_bucket('aws/IMG_20190620_130309.jpg',get_bucket()[0],'IMG_20190620_130309.jpg')
    #list_bucket('94c4c3e7-a5fe-5c70-b9c1-b94ad628fe2b')
    #index_photo(get_bucket()[0],'IMG_20190511_031230.jpg',get_collections()[0])
    #index_photo(get_bucket()[0],'IMG_20190611_150904.jpg',get_collections()[0])
    #index_photo(get_bucket()[0],'IMG_20190620_130309.jpg',get_collections()[0])
    #delete_from_bucket(get_bucket()[0],'VID_20190512_231642.mp4')
    #list_faces(get_collections()[0])
    #del_collection(get_collections()[0])
    #create_collection('test_collection_1')
    list_buckets()
    """sqs = boto3.client('sqs')
    sqsResponse = sqs.receive_message(QueueUrl = 'https://sqs.us-east-1.amazonaws.com/414814015346/rekognition_video',MessageAttributeNames = ['ALL'],MaxNumberOfMessages = 10)
    for message in sqsResponse['Messages']:
        print(message['Body'])
    sqs.delete_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/414814015346/rekognition_video',ReceiptHandle=message['ReceiptHandle'])"""
    """session = boto3.Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    accessKey = current_credentials.access_key
    secretKey = current_credentials.secret_key
    print(accessKey,secretKey)"""
    #delete_bucket('61e0d872-007d-4d3e-9a07-22f18b8dedee-temp-bucket')

if __name__ == '__main__':
    main()
