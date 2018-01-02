import boto3

sfn_client = boto3.client('stepfunctions')


def lambda_handler(event, context):

    client_name = event['Records'][0]['s3']['object']['key'].split("/")[1]

    try:
        response = sfn_client.start_execution(
            stateMachineArn='arn:aws:states:us-east-1:794223901232:stateMachine:aida_insights_state_machine',
            input='{\"client_name\" : \"'+client_name+'\"}'
        )
        return response['executionArn']

    except Exception as e:
        print(e)
        raise e
