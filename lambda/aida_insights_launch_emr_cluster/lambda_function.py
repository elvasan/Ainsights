import boto3
import json
import os

print('Loading function')

emr = boto3.client('emr', region_name='us-east-1')

s3 = boto3.client('s3')

env = os.environ['ENV']

bucket_name = 'jornaya-'+env+'-us-east-1-aida-insights'

def lambda_handler(event, context):

    ec2_attributes = json.loads(s3.get_object(Bucket=bucket_name,
                                              Key='pyspark/config/emr-ec2-attributes.json').get('Body').read())

    instance_groups = json.loads(s3.get_object(Bucket=bucket_name,
                                               Key='pyspark/config/emr-instance-groups-boto.json').get('Body').read())

    emr_config = json.loads(s3.get_object(Bucket=bucket_name,
                                          Key='pyspark/config/emr-config.json').get('Body').read())

    client_name = event['client_name']

    try:
        response = emr.run_job_flow(
            Name="Aida Insights Lambda Test",
            LogUri='s3n://aws-logs-794223901232-us-east-1/elasticmapreduce/',
            ReleaseLabel='emr-5.8.0',
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
                            's3://'+bucket_name+'/pyspark/jobs.zip',
                            's3://'+bucket_name+'/pyspark/main.py',
                            '--job-args',
                            'environment='+env,
                            'client_name='+client_name
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
            Configurations=emr_config
        )

        return response['JobFlowId']

    except Exception as e:
        print(e)
        raise e
