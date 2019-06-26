import boto3
import time,subprocess,os,json

from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from multiprocessing import Process, Queue

session = boto3.Session()
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()
accessKey = current_credentials.access_key
secretKey = current_credentials.secret_key
video_stream = 'Test_Video_Stream_1'
data_stream = 'Test_Data_Stream_1'
collection_id = 'test_collection_1'
region = 'us-east-1'

q = Queue()

class FilesHandler(PatternMatchingEventHandler):
    patterns = ["*.mkv", "*.mp4"]
    def process(self, event):
        if event.event_type == 'modified':
            print('modified: ' + event.src_path)
            q.put(event.src_path)

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)

def loop_send(q):
    try:
        while True:
            if not q.empty():
                print("upload file: " + q.get())
                fileName = q.get()
                if os.path.isfile(fileName):
                    subprocess.Popen(['./putMkvMedia.sh', accessKey, secretKey, region, video_stream, fileName])
    except KeyboardInterrupt:
        return

if __name__ == '__main__':
    accountID = session.client('sts').get_caller_identity()["Account"]
    kinesis = boto3.client('kinesis')
    #kinesis.create_stream(StreamName=data_stream)
    data_arn = kinesis.describe_stream(StreamName=data_stream)['StreamDescription']['StreamARN']
    kinesisvideo = boto3.client('kinesisvideo')
    video_arn = kinesisvideo.create_stream(StreamName=video_stream)['StreamARN']
    rek = boto3.client('rekognition')
    proc_arn = rek.create_stream_processor(Input={'KinesisVideoStream':{'Arn':video_arn}},Output={'KinesisDataStream':{'Arn': data_arn}},Name='Test_Video_Rekognition_1',Settings={'FaceSearch':{'CollectionId': collection_id,'FaceMatchThreshold': 85}},RoleArn='arn:aws:iam::' + accountID + ':role/kinesis_video_rekognition')
    outDirectory = './outputStream/'
    if not os.path.exists(outDirectory):
        os.makedirs(outDirectory)
    observer = Observer()
    observer.schedule(FilesHandler(), path=outDirectory)
    observer.start()
    p1 = Process(target=loop_send, args=(q,))
    p1.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    p1.join()
    observer.join()
    kinesisvideo.delete_stream(StreamARN=video_arn)
