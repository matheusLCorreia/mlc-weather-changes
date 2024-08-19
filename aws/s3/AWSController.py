import boto3
from botocore.exceptions import ClientError
import logging
import LoadConfig as lc


class AWSController:
    __s3_client = None
    
    def __init__(self):    
        self.auth()
        
    def auth(self):
        access_key, secret_access_key = lc.loadConfig()
        self.__s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
        
    def getS3Client(self):
        return self.__s3_client
    
    def createBucket(self, bucket_name):
        try:
            self.getS3Client().create_bucket(Bucket=bucket_name)
        except ClientError as e:
            print("Fail on creating bucket. ", e)
            return False
            
        return True
    
    def uploadObject(self, file_name: str, bucket: str, object_name:str):
        try:
            response = self.getS3Client().upload_file(file_name, bucket, object_name)
            print(f"{object_name} file uploaded successfully!")
        except ClientError as e:
            print(f"Fail on upload file. {object_name}", e)
            return False
        
        return True
    
    def getBuckets(self):
        response = self.getS3Client().list_buckets()

        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'{bucket["Name"]}')
            
    def getObjectsFromBucket(self, bucket_name: str):
        response = self.getS3Client().list_objects(Bucket=bucket_name)

        for object in response['Contents']:
            print(object['Key'])
            
    def downloadObject(self, bucket_name: str, object_name: str, file_name: str):
        self.getS3Client().download_file(bucket_name, object_name, file_name)