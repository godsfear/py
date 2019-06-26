import boto3
"""
kinesis = boto3.client('kinesis')
response = kinesis.create_stream(StreamName='Test_Data_Stream_1')
data_arn = kinesis.describe_stream(StreamName='Test_Data_Stream_1')['StreamDescription']['StreamARN']
#response = kinesis.delete_stream(StreamName='Test_Data_Stream_1')
"""
"""
kinesisvideo = boto3.client('kinesisvideo')
video_arn = kinesisvideo.create_stream(StreamName='Test_Video_Stream_1')['StreamARN']
#response = kinesisvideo.generate_presigned_url(ClientMethod,Params=None,ExpiresIn=3600,HttpMethod=None)
#response = kinesisvideo.get_data_endpoint(StreamARN=video_arn,APIName='PUT_MEDIA')
print(response)
kinesisvideo.delete_stream(StreamARN=video_arn)"""

"""
for s in kinesisvideo.list_streams()['StreamInfoList']:
    print(s['StreamName'],s['StreamARN'])
    kinesisvideo.delete_stream(StreamARN=s['StreamName'])
"""
"""
rek = boto3.client('rekognition')
#proc_arn = create_stream_processor(Input={'KinesisVideoStream':{'Arn':video_arn}},Output={'KinesisDataStream':{'Arn': data_arn}},Name='Test_Video_Rekognition_Stream_1',Settings={'FaceSearch':{'CollectionId': 'test_collection_1','FaceMatchThreshold': 90}},RoleArn='arn:aws:iam::414814015346:role/kinesis_video_rekognition')
print(response)
"""
