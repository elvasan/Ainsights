# noinspection PyPackageRequirements
import boto3
import time
import json
import sys


class AWSUtils:
    ENV = None
    REGION = None
    BUCKET = None

    SESSION = None
    S3_CLIENT = None
    GLUE_CLIENT = None
    ATHENA_CLIENT = None

    instance = None

    JOB_STATE_RUNNING = "RUNNING"
    JOB_STATE_SUCCEEDED = "SUCCEEDED"
    JOB_STATE_STOPPING = "STOPPING"
    JOB_STATE_FAILED = "FAILED"

    def __init__(self):
        print("Constructor")

    @staticmethod
    def get_instance(context):
        if AWSUtils.instance is None:
            sys.stderr.flush()
            AWSUtils.instance = AWSUtils()
            AWSUtils.instance.init_session(context)
        return AWSUtils.instance

    def init_session(self, context):
        AWSUtils.instance.ENV = context.envName
        AWSUtils.instance.BUCKET = context.bucketName
        AWSUtils.instance.REGION = context.region

        AWSUtils.instance.SESSION = boto3.Session(profile_name=AWSUtils.instance.ENV)
        AWSUtils.instance.S3_CLIENT = AWSUtils.instance.SESSION.client('s3', region_name=AWSUtils.instance.REGION)
        AWSUtils.instance.GLUE_CLIENT = AWSUtils.instance.SESSION.client('glue', region_name=AWSUtils.instance.REGION)
        AWSUtils.instance.ATHENA_CLIENT = AWSUtils.instance.SESSION.client('athena', region_name=AWSUtils.instance.REGION)

    def set_bucket(self, bucket_name):
        AWSUtils.instance.BUCKET = bucket_name

    def set_default_bucket(self, context):
        AWSUtils.instance.BUCKET = context.bucketName

    def check_bucket_exists(self):
        buckets_list = AWSUtils.instance.S3_CLIENT.list_buckets()
        buckets = [bucket['Name'] for bucket in buckets_list['Buckets']]
        return AWSUtils.instance.BUCKET in buckets

    def upload_file(self, local_file, prefix):
        data = open(local_file, 'rb')
        AWSUtils.instance.S3_CLIENT.put_object(Bucket=AWSUtils.instance.BUCKET, Key=prefix, Body=data)

    def download_file(self, file_path, local_file):
        AWSUtils.instance.S3_CLIENT.download_file(Bucket=AWSUtils.instance.BUCKET, Key=file_path, Filename=local_file)

    def delete_file(self, prefix):
        AWSUtils.instance.S3_CLIENT.delete_object(Bucket=AWSUtils.instance.BUCKET, Key=prefix)

    def delete_directory(self, prefix):
        file_list = AWSUtils.instance.S3_CLIENT.list_objects(Bucket=AWSUtils.instance.BUCKET, Prefix=prefix + '/', Delimiter='/')
        for key in file_list['Contents']:
            to_delete = key['Key']
            print(to_delete)
            AWSUtils.instance.S3_CLIENT.delete_object(Bucket=AWSUtils.instance.BUCKET, Key=to_delete)

    def check_file_or_direcory_present(self, file_path):
        results = AWSUtils.instance.S3_CLIENT.list_objects(Bucket=AWSUtils.instance.BUCKET, Prefix=file_path)
        return 'Contents' in results

    def list_files_in_folder(self, file_path):
        return AWSUtils.instance.S3_CLIENT.list_objects(Bucket=AWSUtils.instance.BUCKET, Prefix=file_path)

    def run_glue_job_and_wait_for_end(self, job_name, arguments):

        escaped_job_name = job_name.replace('/', r'/')
        data = json.loads(str(arguments))
        job_run = AWSUtils.instance.GLUE_CLIENT.start_job_run(JobName=escaped_job_name, Arguments=data)
        job_run_id = job_run['JobRunId']

        while True:
            status = AWSUtils.instance.GLUE_CLIENT.get_job_run(JobName=escaped_job_name, RunId=job_run_id)
            if status['JobRun']['JobRunState'] != AWSUtils.instance.JOB_STATE_RUNNING:
                break
            time.sleep(20) #20 seconds

    def run_athena_query_and_wait_for_result(self, query_string, output_location):
        s3_output_location = "s3://{}/{}".format(AWSUtils.instance.BUCKET, output_location)
        query_execution = AWSUtils.instance.ATHENA_CLIENT.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={
                'OutputLocation': s3_output_location,
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
        )
        qeid = query_execution['QueryExecutionId']

        while True:
            response = AWSUtils.instance.ATHENA_CLIENT.get_query_execution(
                QueryExecutionId=qeid
            )
            status = response['QueryExecution']['Status']['State']
            if status != AWSUtils.instance.JOB_STATE_RUNNING:
                break
            time.sleep(5) #5 seconds
        return status
