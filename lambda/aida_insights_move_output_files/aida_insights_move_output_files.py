import os
import zipfile

import boto3


def lambda_handler(event, context):  # pylint:disable=unused-argument, too-many-locals
    """
    Lambda function to zip up and move the output files from an Aida Insights job run to the /outgoing folder, en route
    to the SFTP folder for client download.
    """
    s3_client = boto3.client('s3')
    env = os.environ['ENV']

    bucket_name = 'jornaya-' + env + '-us-east-1-aida-insights'
    client_name = event['client_name']
    job_run_id = event['job_run_id']
    external_file_name = '%s_%s.csv' % (client_name, job_run_id)
    input_summary_file_name = '%s_input_summary.csv' % client_name
    output_summary_file_name = '%s_output_summary.csv' % client_name
    zip_file_name = '%s_%s.zip' % (client_name, job_run_id)

    # We don't have control over what spark names the output files, so we use a prefix
    external_output_file_prefix = 'app_data/%s/%s_%s/output/results_external/part' \
                                  % (client_name, client_name, job_run_id)

    input_summary_file_prefix = 'app_data/%s/%s_%s/output/input_summary/part' \
                                % (client_name, client_name, job_run_id)

    output_summary_file_prefix = 'app_data/%s/%s_%s/output/output_summary/part' \
                                 % (client_name, client_name, job_run_id)

    try:

        external_output_file_key = get_bucket_key_from_prefix(s3_client, bucket_name, external_output_file_prefix)
        input_summary_file_key = get_bucket_key_from_prefix(s3_client, bucket_name, input_summary_file_prefix)
        output_summary_file_key = get_bucket_key_from_prefix(s3_client, bucket_name, output_summary_file_prefix)

        # Zip up the output files
        with zipfile.ZipFile('/tmp/' + zip_file_name, 'w') as zip_output:
            if external_output_file_key is not None:
                s3_client.download_file(bucket_name, external_output_file_key, '/tmp/%s' % external_file_name)
                zip_output.write('/tmp/%s' % external_file_name, external_file_name)

            if input_summary_file_key is not None:
                s3_client.download_file(bucket_name, input_summary_file_key, '/tmp/%s' % input_summary_file_name)
                zip_output.write('/tmp/%s' % input_summary_file_name, input_summary_file_name)

            if output_summary_file_key is not None:
                s3_client.download_file(bucket_name, output_summary_file_key, '/tmp/%s' % output_summary_file_name)
                zip_output.write('/tmp/%s' % output_summary_file_name, output_summary_file_name)

        # Upload the zip file to the outgoing folder
        s3_client.upload_file('/tmp/%s' % zip_file_name, bucket_name, 'outgoing/%s/%s' % (client_name, zip_file_name))

        return True

    except Exception as exception:
        print(exception)
        raise exception


def get_bucket_key_from_prefix(s3_client, bucket_name, prefix):

    response = s3_client.list_objects(
        Bucket=bucket_name,
        Prefix=prefix
    ).get('Contents')

    if response is not None:
        return response[0]['Key']

    return None
