import boto3
import datetime
import os


def lambda_handler(event, context):
    """
    Lambda function to trigger the launch of the Aida Insights step function that launches the EMR cluster
    """

    sfn_client = boto3.client('stepfunctions')

    account_id = os.environ['account_id']
    client_name = event['Records'][0]['s3']['object']['key'].split("/")[1]

    # Generate the job_run_id using a timestamp
    now = datetime.datetime.now()
    job_run_id = now.strftime('%Y_%m_%d_%H_%M_%S_%f')

    state_machine_input = '{' \
                          '  "client_name" : "'+client_name+'",' \
                          '  "job_run_id" : "'+job_run_id+'"' \
                          '}'

    try:
        # Invoke the step function
        sfn_client.start_execution(
            stateMachineArn='arn:aws:states:us-east-1:'
                            + account_id
                            + ':stateMachine:aida_insights_launch_cluster_state_machine',
            input=state_machine_input
        )

    except Exception as e:
        print(e)
        raise e
