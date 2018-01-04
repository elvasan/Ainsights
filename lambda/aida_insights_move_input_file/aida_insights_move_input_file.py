import boto3
import os


def lambda_handler(event, context):
    """
    A Lambda function to move the Aida Insights input csv file from the /incoming folder to the /app_data folder
    for processing. Also copies the the client_overrides.csv file for a particular client job run.
    """

    s3 = boto3.client('s3')

    env = os.environ['ENV']
    bucket_name = 'jornaya-'+env+'-us-east-1-aida-insights'
    client_name = event['client_name']
    job_run_id = event['job_run_id']

    input_file_source_key = 'incoming/' + client_name + '/' + client_name + '.csv'
    client_overrides_source_key = 'app_data/' + client_name + '/client_overrides.csv'

    input_file_destination_key = 'app_data/' \
                      + client_name + '/' \
                      + client_name + '_' + job_run_id + '/' \
                      + 'input/' \
                      + client_name + '_' + job_run_id + '.csv'

    client_overrides_destination_key = 'app_data/' \
                                       + client_name + '/' \
                                       + client_name + '_' + job_run_id + '/' \
                                       + 'input/client_overrides.csv'

    input_file_copy_source = {
        'Bucket': bucket_name,
        'Key': input_file_source_key
    }

    client_overrides_copy_source = {
        'Bucket': bucket_name,
        'Key': client_overrides_source_key
    }

    try:

        # Copy and delete input file
        s3.copy(input_file_copy_source, bucket_name, input_file_destination_key)
        s3.delete_object(Bucket=bucket_name, Key=input_file_source_key)

        # Copy client overrides
        s3.copy(client_overrides_copy_source, bucket_name, client_overrides_destination_key)

        return True

    except Exception as e:
        print(e)
        raise e
