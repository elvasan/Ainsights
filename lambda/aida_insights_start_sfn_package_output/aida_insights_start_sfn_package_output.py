import boto3
import os


def lambda_handler(event, context):
    """
    Lambda function to start the step function that packages the output of an Aida Insights job run
    """

    sfn_client = boto3.client('stepfunctions')
    account_id = os.environ['account_id']

    step_function_input = '{' \
                          '  "client_name" : "' + event['client_name'] + '",' \
                          '  "job_run_id" : "' + event['job_run_id'] + '",' \
                          '  "cluster_id": "' + event['cluster_id'] + '"' \
                          '}'
    try:
        # Execute the step function
        sfn_client.start_execution(
            stateMachineArn='arn:aws:states:us-east-1:'
                            + account_id
                            + ':stateMachine:aida_insights_package_output_state_machine',
            input=step_function_input
        )

    except Exception as e:
        print(e)
        raise e
