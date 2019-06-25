import boto3
import json
import sys
from datetime import datetime

class VideoDetect:
    def __init__(self,bucket = '',video = '',CollectionId = '',queueUrl = '',roleArn = '',topicArn = ''):
        self.rek = boto3.client('rekognition')
        self.jobId = ''
        self.queueUrl = queueUrl
        self.roleArn = roleArn
        self.topicArn = topicArn
        self.bucket = bucket
        self.video = video
        self.CollectionId = CollectionId

    def main(self):
        jobFound = False
        sqs = boto3.client('sqs')
        #=====================================
        #response = self.rek.start_label_detection(Video={'S3Object': {'Bucket': self.bucket, 'Name': self.video}},NotificationChannel={'RoleArn': self.roleArn, 'SNSTopicArn': self.topicArn})
        response = self.rek.start_face_search(Video={'S3Object':{'Bucket':self.bucket,'Name':self.video}},CollectionId=self.CollectionId,NotificationChannel={'RoleArn':self.roleArn, 'SNSTopicArn':self.topicArn})
        #=====================================
        print('Start Job Id: ' + response['JobId'])
        dotLine = 0
        while jobFound == False:
            sqsResponse = sqs.receive_message(QueueUrl = self.queueUrl,MessageAttributeNames = ['ALL'],MaxNumberOfMessages = 10)
            if sqsResponse:
                if 'Messages' not in sqsResponse:
                    if dotLine < 20:
                        print('.', end='')
                        dotLine = dotLine + 1
                    else:
                        print()
                        dotLine = 0
                    sys.stdout.flush()
                    continue

                for message in sqsResponse['Messages']:
                    notification = json.loads(message['Body'])
                    rekMessage = json.loads(notification['Message'])
                    print(rekMessage['JobId'])
                    print(rekMessage['Status'])
                    if str(rekMessage['JobId']) == response['JobId']:
                        print('Matching Job Found:' + rekMessage['JobId'])
                        jobFound = True
                        #=============================================
                        #self.GetResultsLabels(rekMessage['JobId'])
                        self.GetResultsFaceSearchCollection(rekMessage['JobId'])
                        #=============================================
                        sqs.delete_message(QueueUrl=self.queueUrl,ReceiptHandle=message['ReceiptHandle'])
                    else:
                        print("Job didn't match:" + str(rekMessage['JobId']) + ' : ' + str(response['JobId']))
                    # Delete the unknown message. Consider sending to dead letter queue
                    sqs.delete_message(QueueUrl=self.queueUrl,ReceiptHandle=message['ReceiptHandle'])
        print('done')


    def GetResultsLabels(self,jobId):
        maxResults = 10
        paginationToken = ''
        finished = False
        while finished == False:
            response = self.rek.get_label_detection(JobId=jobId,MaxResults=maxResults,NextToken=paginationToken,SortBy='TIMESTAMP')
            print(response['VideoMetadata']['Codec'])
            print(str(response['VideoMetadata']['DurationMillis']))
            print(response['VideoMetadata']['Format'])
            print(response['VideoMetadata']['FrameRate'])
            for labelDetection in response['Labels']:
                label=labelDetection['Label']
                print("Timestamp: " + str(labelDetection['Timestamp']))
                print("   Label: " + label['Name'])
                print("   Confidence: " +  str(label['Confidence']))
                print("   Instances:")
                for instance in label['Instances']:
                    print("      Confidence: " + str(instance['Confidence']))
                    print("      Bounding box")
                    print("        Top: " + str(instance['BoundingBox']['Top']))
                    print("        Left: " + str(instance['BoundingBox']['Left']))
                    print("        Width: " +  str(instance['BoundingBox']['Width']))
                    print("        Height: " +  str(instance['BoundingBox']['Height']))
                    print()
                print()
                print("   Parents:")
                for parent in label['Parents']:
                    print("      " + parent['Name'])
                print()

                if 'NextToken' in response:
                    paginationToken = response['NextToken']
                else:
                    finished = True

    def GetResultsFaceSearchCollection(self,jobId):
        maxResults = 10
        paginationToken = ''
        finished = False
        while finished == False:
            response = self.rek.get_face_search(JobId=jobId,MaxResults=maxResults,NextToken=paginationToken)
            print(response['VideoMetadata']['Codec'])
            print(str(response['VideoMetadata']['DurationMillis']))
            print(response['VideoMetadata']['Format'])
            print(response['VideoMetadata']['FrameRate'])
            for personMatch in response['Persons']:
                print('Person Index: ' + str(personMatch['Person']['Index']))
                print('Timestamp: ' + str(personMatch['Timestamp']))
                if ('FaceMatches' in personMatch):
                    for faceMatch in personMatch['FaceMatches']:
                        print('Face ID: ' + faceMatch['Face']['FaceId'])
                        print('Similarity: ' + str(faceMatch['Similarity']))
                print()
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True
            print()

if __name__ == "__main__":
    analyzer=VideoDetect(bucket = '94c4c3e7-a5fe-5c70-b9c1-b94ad628fe2b',video = 'VID_20190512_231642.mp4',CollectionId = 'test_collection_1',queueUrl = 'https://sqs.us-east-1.amazonaws.com/414814015346/rekognition_video',roleArn = 'arn:aws:iam::414814015346:role/rekognition_video',topicArn = 'arn:aws:sns:us-east-1:414814015346:rekognition_video')
    beg = datetime.now()
    analyzer.main()
    print('###',datetime.now() - beg,'###')
