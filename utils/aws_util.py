import boto3
import os
from config import aws_profile_name
from botocore.exceptions import ClientError

s3_client = boto3.Session(profile_name=aws_profile_name).client('s3')


def check_file_exists_in_s3(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise


def upload_to_s3(local_file_path, s3_bucket, s3_key):
    if check_file_exists_in_s3(s3_bucket, s3_key):
        print(f"File {s3_key} already exists in s3://{s3_bucket}/{s3_key}, skipping upload.")
        return
    
    print(f"Uploading {local_file_path} to s3://{s3_bucket}/{s3_key} ...")
    try:
        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"Upload complete for {local_file_path}")
        os.remove(local_file_path)
        print(f"Deleted local file {local_file_path}")
    except Exception as e:
        print(f"Failed to upload {local_file_path} to S3: {e}")