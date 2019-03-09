import boto3
import botocore
import pandas as pd
import io
import os

def pd_read_csv_s3(path, *args, **kwargs):
    # s3_client = boto3.client('s3')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY')
    )
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    print(bucket)
    print(path)
    print(key)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(e)
            return None
        else:
            print(e.response['Error']['Code'])
            return None
    return pd.read_csv(io.BytesIO(obj['Body'].read()), *args, **kwargs)
