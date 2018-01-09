from unittest.mock import patch

import boto3
from botocore.stub import Stubber, ANY

from aida_insights_create_cloudwatch_event import create_cloudwatch_event


def test_cloudwatch_event_created():
    # Arrange

    cloudwatch_event_client = boto3.client('events', 'us-east-1')
    lambda_client = boto3.client('lambda', 'us-east-1')
    cloudwatch_event_stubber = Stubber(cloudwatch_event_client)
    lambda_stubber = Stubber(lambda_client)

    test_event = {
        'cluster_id': '1',
        'job_run_id': '2',
        'client_name': 'A'
    }
    lambda_arn = 'arn'

    put_rule_expected_params = {
        'Name': 'AIDA_INSIGHTS_CLUSTER_1_RULE',
        'EventPattern': ANY,
        'State': 'ENABLED'
    }
    cloudwatch_event_stubber.add_response('put_rule', {}, put_rule_expected_params)

    put_targets_expected_params = {
        'Rule': 'AIDA_INSIGHTS_CLUSTER_1_RULE',
        'Targets': [{
            'Id': '2',
            'Arn': lambda_arn,
            'Input': ANY
        }]
    }
    cloudwatch_event_stubber.add_response('put_targets', {}, put_targets_expected_params)

    add_permission_expected_params = {
        'FunctionName': 'aida_insights_start_sfn_package_output',
        'StatementId': 'aida_insights_lambda_permission_1',
        'Action': 'lambda:InvokeFunction',
        'Principal': 'events.amazonaws.com',
        'SourceArn': 'arn:aws:events:us-east-1:3:rule/AIDA_INSIGHTS_CLUSTER_1_RULE'
    }
    lambda_stubber.add_response('add_permission', {}, add_permission_expected_params)

    envars = {
        'AWS_ACCOUNT_ID': '3',
        'LAMBDA_LAUNCH_SFN_PACKAGE_OUTPUT_ARN': lambda_arn
    }

    cloudwatch_event_stubber.activate()
    lambda_stubber.activate()

    # Act

    with patch.dict('os.environ', envars):
        create_cloudwatch_event(test_event, cloudwatch_event_client, lambda_client)

    # Assert

    lambda_stubber.assert_no_pending_responses()
    lambda_stubber.assert_no_pending_responses()
