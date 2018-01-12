import json
import os

import boto3


def lambda_handler(event, context):  # pylint:disable=unused-argument, too-many-locals
    """
    A Lambda Function to launch the Aida Insights EMR Cluster
    """

    emr = boto3.client('emr', region_name='us-east-1')
    s3_client = boto3.client('s3')

    env = os.environ['ENV']
    client_name = event['client_name']
    job_run_id = event['job_run_id']
    bucket_name = 'jornaya-' + env + '-us-east-1-aida-insights'

    job_run_path = bucket_name + '/app_data/' \
                   + client_name + '/' \
                   + client_name + '_' + job_run_id + '/'

    yarn_log_uri = 's3n://' + job_run_path + 'logs/yarn/'

    # Load emr cluster configuration from json files in s3
    ec2_attributes = json.loads(s3_client.get_object(Bucket=bucket_name,
                                                     Key='pyspark/config/emr-ec2-attributes.json').get('Body').read())

    instance_groups = json.loads(s3_client.get_object(Bucket=bucket_name,
                                                      Key='pyspark/config/emr-instance-groups-boto.json').get(
        'Body').read())

    emr_config = json.loads(s3_client.get_object(Bucket=bucket_name,
                                                 Key='pyspark/config/emr-config.json').get('Body').read())

    # Set spark history logging location - it's different for each execution based on job_run_id
    emr_config[1]['Properties']['spark.eventLog.dir'] = 's3://' + job_run_path + 'logs/'

    version_string = s3_client.get_object(Bucket=bucket_name,
                                          Key='pyspark/version.txt').get('Body').read().decode('UTF-8').rstrip()

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Body='',
            Key='app_data/' + client_name + '/' + client_name + '_' + job_run_id + '/logs/'
        )

        # Launch the Cluster
        response = emr.run_job_flow(
            Name="Aida Insights - " + client_name + " - " + job_run_id,
            LogUri=yarn_log_uri,
            ReleaseLabel='emr-5.11.0',
            Instances={
                'InstanceGroups': instance_groups,
                'Ec2KeyName': ec2_attributes['KeyName'],
                'Ec2SubnetId': ec2_attributes['SubnetId'],
                'EmrManagedMasterSecurityGroup': ec2_attributes['EmrManagedMasterSecurityGroup'],
                'EmrManagedSlaveSecurityGroup': ec2_attributes['EmrManagedSlaveSecurityGroup']
            },
            Applications=[
                {
                    'Name': 'Spark'
                },
                {
                    'Name': 'Hadoop'
                }
            ],
            Steps=[
                {
                    'HadoopJarStep': {
                        'Args': [
                            'spark-submit',
                            '--deploy-mode',
                            'cluster',
                            '--py-files',
                            's3://' + bucket_name + '/pyspark/jobs.zip',
                            's3://' + bucket_name + '/pyspark/main.py',
                            '--job-args',
                            'environment=' + env,
                            'client_name=' + client_name,
                            'job_run_id=' + job_run_id
                        ],
                        'Jar': 'command-runner.jar',

                    },
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'Name': 'Spark Application'
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole=ec2_attributes['InstanceProfile'],
            ServiceRole='EMR_Role',
            Configurations=emr_config,
            Tags=[
                {
                    'Key': 'Version',
                    'Value': version_string
                },
                {
                    'Key': 'Application',
                    'Value': 'aida_insights'
                },
                {
                    'Key': 'Environment',
                    'Value': env
                },
                {
                    'Key': 'Name',
                    'Value': 'Aida Insights EMR Cluster'
                }
            ]
        )

        return response['JobFlowId']

    except Exception as exception:
        print(exception)
        raise exception
