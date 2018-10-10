from __future__ import print_function
import json
import boto3
import os
import sys

client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        file_name = key.split('/')[-1]
        # return {'bucket':bucket,'key':key}
        new_key = 'consolidated-{}'.format(file_name)
        file_path = '/tmp/{}'.format(file_name)
        
        client.download_file(bucket, key, file_path)
        client.upload_file(file_path, bucket, 'consolidated_output/'+new_key)
