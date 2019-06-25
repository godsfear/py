import boto3
import json
from datetime import datetime
import time,sys

my_stream_name = 'Test_Data_Stream_1'
kinesis_client = boto3.client('kinesis')

try:
    kinesis_client.create_stream(StreamName=my_stream_name,ShardCount=1)
except (Exception,RuntimeError) as error:
    print(error)
    kinesis_client.delete_stream(StreamName=my_stream_name)
    sys.exit(1)

response = kinesis_client.describe_stream(StreamName=my_stream_name)

dotLine = 0
while len(response['StreamDescription']['Shards']) == 0:
    if dotLine < 20:
        print('.', end='')
        dotLine = dotLine + 1
    else:
        print()
        dotLine = 0
    time.sleep(5)
    response = kinesis_client.describe_stream(StreamName=my_stream_name)

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,ShardId=my_shard_id,ShardIteratorType='LATEST')
my_shard_iterator = shard_iterator['ShardIterator']
record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,Limit=2)

i = 0
while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],Limit=2)
    for rec in record_response['Records']:
        print(rec['ApproximateArrivalTimestamp'],rec['Data'].decode('utf-8'))
    time.sleep(5)
    i += 1
    if i > 10:
        break

kinesis_client.delete_stream(StreamName=my_stream_name)
