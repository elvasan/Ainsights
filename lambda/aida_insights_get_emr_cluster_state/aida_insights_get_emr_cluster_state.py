import boto3


def get_emr_cluster_state(event, emr_client):
    """
    Lambda function that returns the current state of an EMR cluster
    """

    try:

        cluster_id = event['cluster_id']

        cluster_response = emr_client.describe_cluster(ClusterId=cluster_id)

        cluster_state = cluster_response['Cluster']['Status']['State']

        if cluster_state == 'TERMINATED':
            return 'SUCCEEDED'
        elif cluster_state == 'TERMINATED_WITH_ERRORS':
            return 'FAILED'

        return 'RUNNING'

    except Exception as exception:
        print(exception)
        raise exception


def lambda_handler(event, context):  # pylint:disable=unused-argument
    emr_client = boto3.client('emr', region_name='us-east-1')

    return get_emr_cluster_state(event, emr_client)
