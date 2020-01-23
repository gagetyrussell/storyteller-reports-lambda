import boto3
import time
from trp import Document
from urllib.parse import unquote_plus


def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3BucketName,
                'Name': objectName
            }
        },
        FeatureTypes=['TABLES'],
        NotificationChannel={'RoleArn': 'arn:aws:iam::360961862032:role/AWSSNSFullAccessRole',
                             'SNSTopicArn': 'arn:aws:sns:us-east-1:360961862032:AmazonTextractReports'}
    )
    return response["JobId"]


def isJobComplete(jobId):
    # For production use cases, use SNS based notification
    # Details at: https://docs.aws.amazon.com/textract/latest/dg/api-async.html
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))
    while (status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    return status


def getJobResults(jobId):
    pages = []
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)

    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if ('NextToken' in response):
        nextToken = response['NextToken']
    while (nextToken):
        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)
        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if ('NextToken' in response):
            nextToken = response['NextToken']
    return pages

def handler(event, context):
    for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            print(bucket)
            key = unquote_plus(record['s3']['object']['key'])
            print(key)
            jobId = startJob(bucket, key)
            print("Started job with id: {}".format(jobId))

# def handler(event, context):

#     body = {
#         "message": "Go Serverless v1.0! Your function executed successfully!",
#         "input": event
#     }
#
#     response = {
#         "statusCode": 200,
#         "body": json.dumps(body)
#     }
#
#     return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    # """
    # return {
    #     "message": "Go Serverless v1.0! Your function executed successfully!",
    #     "event": event
    # }
    # """
