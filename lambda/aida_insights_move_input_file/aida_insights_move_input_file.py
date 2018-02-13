import os

import boto3


def lambda_handler(event, context):  # pylint:disable=unused-argument
    """
    A Lambda function to move the Aida Insights input csv file from the /incoming folder to the /app_data folder
    for processing. Also copies the the client_config.csv file for a particular client job run.
    """

    s3_client = boto3.client('s3')

    env = os.environ['ENV']
    bucket_name = 'jornaya-' + env + '-us-east-1-aida-insights'
    client_name = event['client_name']
    job_run_id = event['job_run_id']

    input_file_source_key = 'incoming/' + client_name + '/' + client_name + '.csv'
    client_config_source_key = 'app_data/' + client_name + '/client_config.csv'

    input_file_destination_key = 'app_data/' \
                                 + client_name + '/' \
                                 + client_name + '_' + job_run_id + '/' \
                                 + 'input/' \
                                 + client_name + '_' + job_run_id + '.csv'

    client_config_destination_key = 'app_data/' \
                                    + client_name + '/' \
                                    + client_name + '_' + job_run_id + '/' \
                                    + 'input/client_config.csv'

    input_file_copy_source = {
        'Bucket': bucket_name,
        'Key': input_file_source_key
    }

    client_config_copy_source = {
        'Bucket': bucket_name,
        'Key': client_config_source_key
    }

    try:

        # Copy and delete input file
        s3_client.copy(input_file_copy_source, bucket_name, input_file_destination_key)
        s3_client.delete_object(Bucket=bucket_name, Key=input_file_source_key)

        # Copy client config
        s3_client.copy(client_config_copy_source, bucket_name, client_config_destination_key)

        return True

    except Exception as exception:
        print(exception)
        raise exception
