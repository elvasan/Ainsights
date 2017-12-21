import boto3
import os

lambda_client = boto3.client('lambda')

env = os.environ['ENV']

bucket_name = 'jornaya-'+env+'-us-east-1-aida-insights'

def lambda_handler(event, context):

    # Parse lambda name from s3 event
    zip_file_name = event['Records'][0]['s3']['object']['key'].split("/")[1]
    lambda_name = zip_file_name.split(".")[0]

    bucket_name = event['Records'][0]['s3']['bucket']['name']

    try:
        response = lambda_client.update_function_code(
            FunctionName = lambda_name,
            S3Bucket = bucket_name,
            S3Key = 'lambda/'+zip_file_name
        )
    except Exception as e:
        print(e)
        raise e
