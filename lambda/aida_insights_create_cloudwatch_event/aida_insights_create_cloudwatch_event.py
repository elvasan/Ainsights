import os

import boto3


def lambda_handler(event, context):  # pylint:disable=unused-argument
    """
    A Lambda Function to create A CloudWatch Event Rule/Target pair to trigger when EMR cluster is terminated.
    Also adds IAM permission for the CloudWatch event to execute the launch of the step function via lambda
    """
    cloudwatch_event_client = boto3.client('events')
    lambda_client = boto3.client('lambda')
    create_cloudwatch_event(event, cloudwatch_event_client, lambda_client)


def create_cloudwatch_event(event, cloudwatch_event_client, lambda_client):
    step_function_output_arn = os.environ['LAMBDA_LAUNCH_SFN_PACKAGE_OUTPUT_ARN']
    account_id = os.environ['AWS_ACCOUNT_ID']
    cluster_id = event['cluster_id']
    job_run_id = event['job_run_id']
    client_name = event['client_name']

    cloudwatch_event_input = '{' \
                             '  "cluster_id" : "' + cluster_id + '",' \
                             '  "client_name" : "' + client_name + '",' \
                             '  "job_run_id": "' + job_run_id + '"' \
                             '}'

    try:
        # Set the CloudWatch Event Filter to only pick up this particular EMR cluster, on termination.
        cloudwatch_event_pattern = '{' \
                                   '  "source": ["aws.emr"],' \
                                   '  "detail-type": ["EMR Cluster State Change"],' \
                                   '  "detail":' \
                                   '  {' \
                                   '    "state": ["TERMINATED","TERMINATED_WITH_ERRORS"],' \
                                   '    "clusterId": ["' + cluster_id + '"]' \
                                                                        '  }' \
                                                                        '}'

        # Create the CloudWatch event rule
        cloudwatch_event_client.put_rule(
            Name='AIDA_INSIGHTS_CLUSTER_' + cluster_id + '_RULE',
            EventPattern=cloudwatch_event_pattern,
            State='ENABLED'
        )

        # Create the CloudWatch event target (the lambda function that launches the step function
        cloudwatch_event_client.put_targets(
            Rule='AIDA_INSIGHTS_CLUSTER_' + cluster_id + '_RULE',
            Targets=[
                {
                    'Id': job_run_id,
                    'Arn': step_function_output_arn,
                    'Input': cloudwatch_event_input
                }
            ]
        )

        # Allow the AWS CloudWatch event to execute invoke the lambda function
        lambda_client.add_permission(
            FunctionName='aida_insights_start_sfn_package_output',
            StatementId='aida_insights_lambda_permission_' + cluster_id,
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn='arn:aws:events:us-east-1:' + account_id + ':rule/AIDA_INSIGHTS_CLUSTER_' + cluster_id + '_RULE'
        )

    except Exception as exception:
        print(exception)
        raise exception
