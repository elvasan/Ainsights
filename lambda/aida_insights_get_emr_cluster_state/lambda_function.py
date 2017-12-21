import boto3

emr = boto3.client('emr', region_name='us-east-1')


def lambda_handler(event, context):

    cluster_id = event['cluster_id']

    cluster_response = emr.describe_cluster(ClusterId=cluster_id)

    cluster_state = cluster_response['Cluster']['Status']['State']

    if cluster_state == 'TERMINATED':
        return 'SUCCEEDED'
    elif cluster_state == 'TERMINATED_WITH_ERRORS':
        return 'FAILED'
    else:
        return 'RUNNING'
