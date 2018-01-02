import boto3
import os

s3 = boto3.client('s3')
env = os.environ['ENV']


def lambda_handler(event, context):

    try:

        client_name = event['client_name']

        bucket_name = 'jornaya-'+env+'-us-east-1-aida-insights'

        source_key = 'incoming/'+client_name+'/'+client_name+'.csv'
        destination_key = client_name+'/input/'+client_name+'.csv'

        copy_source = {
            'Bucket': bucket_name,
            'Key': source_key
        }

        s3.copy(copy_source, bucket_name, destination_key)
        s3.delete_object(Bucket=bucket_name, Key=source_key)

        return True

    except Exception as e:
        print(e)
        raise e