import os
import zipfile

import boto3


def lambda_handler(event, context):  # pylint:disable=unused-argument
    """
    Lambda function to zip up and move the output files from an Aida Insights job run to the /outgoing folder, en route
    to the SFTP folder for client download.
    """
    s3_client = boto3.client('s3')
    env = os.environ['ENV']

    bucket_name = 'jornaya-' + env + '-us-east-1-aida-insights'
    client_name = event['client_name']
    job_run_id = event['job_run_id']
    external_file_name = client_name + '_' + job_run_id + '.csv'
    zip_file_name = client_name + '_' + job_run_id + '.zip'

    # We don't have control over what spark names the output files, so we use a prefix
    external_output_file_prefix = 'app_data/' \
                                  + client_name + '/' \
                                  + client_name + '_' + job_run_id + '/' \
                                  + 'output/results_external/part'

    try:

        # List the contents of the output folder to get the output file name (should only be the output file
        external_output_file_key = s3_client.list_objects(
            Bucket=bucket_name,
            Prefix=external_output_file_prefix
        )['Contents'][0]['Key']

        # Download the output file to /tmp/
        s3_client.download_file(bucket_name, external_output_file_key, '/tmp/' + external_file_name)

        # Zip up the external output file
        with zipfile.ZipFile('/tmp/' + zip_file_name, 'w') as zip_output:
            zip_output.write('/tmp/' + external_file_name, external_file_name)

        # Upload the zip file to the outgoing folder
        s3_client.upload_file('/tmp/' + zip_file_name, bucket_name, 'outgoing/' + client_name + '/' + zip_file_name)

        return True

    except Exception as exception:
        print(exception)
        raise exception
