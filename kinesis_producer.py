import boto3
import json
from datetime import datetime
import random
import time

my_stream_name = 'Test_Data_Stream_1'
kinesis_client = boto3.client('kinesis')
#kinesis_client.create_stream(StreamName='Test_Data_Stream_1')

def put_to_stream(thing_id,property_value,property_timestamp):
    payload = {'prop': str(property_value),'timestamp': str(property_timestamp),'thing_id': thing_id}
    print(payload)
    put_response = kinesis_client.put_record(StreamName=my_stream_name,Data=json.dumps(payload),PartitionKey=thing_id)

while True:
    property_value = random.randint(40,120)
    property_timestamp = datetime.now()
    thing_id = 'aa-bb'
    put_to_stream(thing_id,property_value,property_timestamp)
    time.sleep(5)
