import boto3


def lambda_handler(event, context):  # pylint:disable=unused-argument
    """
    Lambda function to removed the cloudwatch event rule and lambda execution permission after an Aida Insights job
    run is completed.
    """

    cloudwatch_events_client = boto3.client('events')
    lambda_client = boto3.client('lambda')

    cluster_id = event['cluster_id']
    job_run_id = event['job_run_id']

    try:

        # Remove the execution permission
        lambda_client.remove_permission(
            FunctionName='aida_insights_start_sfn_package_output',
            StatementId='aida_insights_lambda_permission_' + cluster_id
        )

        # Remove the event target
        cloudwatch_events_client.remove_targets(
            Rule='AIDA_INSIGHTS_CLUSTER_' + cluster_id + "_RULE",
            Ids=[job_run_id]
        )

        # Remove the rule
        cloudwatch_events_client.delete_rule(
            Name='AIDA_INSIGHTS_CLUSTER_' + cluster_id + "_RULE"
        )

    except Exception as exception:
        print(exception)
        raise exception
