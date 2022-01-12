#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import boto3
import io
from io import StringIO
def lambda_handler(event, context):
    s3_file_key = event['Records'][0]['s3']['object']['key'];
    bucket = 'staticfilesott';
    s3 = boto3.client('s3', aws_access_key_id='AKIAVRBYUIFU5V2E7M7I',  aws_secret_access_key='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')
    obj = s3.get_object(Bucket=gitbucket, Key=s3_file_key)
    initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()));

    service_name = 's3'
    region_name = 'ap-south-1'
    aws_access_key_id = 'AKIAVRBYUIFU5V2E7M7I'
    aws_secret_access_key = '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'
    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket='targetbucketcleaned';
    df = initial_df[(initial_df.type == "Movie")];
    df1 = df.loc[:, ~df.columns.isin(['date_added', 'description', 'duration'])];
    csv_buffer = StringIO()
    df1.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key).put(Body=csv_buffer.getvalue())

