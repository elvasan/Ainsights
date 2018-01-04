import boto3
import os

def lambda_handler(event, context):
    """
    Lambda function that updates the function code of other lambda functions in response to s3 events
    """

    lambda_client = boto3.client('lambda')
    env = os.environ['ENV']
    bucket_name = 'jornaya-'+env+'-us-east-1-aida-insights'

    # Parse lambda name from s3 event
    zip_file_name = event['Records'][0]['s3']['object']['key'].split("/")[1]
    lambda_name = zip_file_name.split(".")[0]

    try:

        # Update the function code
        lambda_client.update_function_code(
            FunctionName=lambda_name,
            S3Bucket=bucket_name,
            S3Key='lambda/'+zip_file_name
        )

    except Exception as e:
        print(e)
        raise e
