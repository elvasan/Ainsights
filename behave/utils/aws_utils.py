import boto3
import configparser
import json
import os
import time
from datetime import datetime, timezone
from dateutil import parser
from os.path import expanduser


class AWSUtils(object):
    env = None
    region = None
    bucket = None

    session = None
    s3_client = None
    glue_client = None
    athena_client = None

    instance = None

    job_state_running = "RUNNING"
    job_state_starting = "STARTING"

    bucket_format = "{}-{}"
    bucket_with_prefix_format = "{}/{}"
    prefix_format = '{}_{}'

    def __init__(self):
        print("Constructor")

    @staticmethod
    def get_instance(context):
        if None == AWSUtils.instance:
            AWSUtils.instance = AWSUtils()
            AWSUtils.instance._init_session(context)
        return AWSUtils.instance

    def _init_session(self, context):
        self.env = context.envName
        self.bucket = context.bucketName
        self.region = context.region

        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, '../temp_cred.json')

        if os.path.exists(file_path):
            with open(file_path) as config_file:
                temp_cred = json.load(config_file)
        else:
            if os.environ.get('mfa') is None:
                print("Some error message that we need the mfa on the first run")
                raise SystemExit(1)
            temp_cred = dict()

        session_exists = False
        if len(temp_cred) == 0 or self.env not in temp_cred.keys():
            temp_cred[self.env] = {}

        env_data = temp_cred[self.env]
        if('AWS_TOKEN_EXPIRATION_TIME' in env_data.keys()
            and 'AWS_ACCESS_KEY_ID' in env_data.keys()
            and 'AWS_SECRET_ACCESS_KEY' in env_data.keys()
            and 'AWS_SESSION_TOKEN' in env_data.keys()):
            now = datetime.now(timezone.utc)
            exp_date = parser.parse(env_data['AWS_TOKEN_EXPIRATION_TIME'])
            session_exists = now < exp_date

        if not session_exists:

            config = configparser.ConfigParser()
            home = expanduser("~")
            conf = os.path.join(home, '.aws/config')
            config.read(conf)
            profile = "profile " + self.env
            role_arn = config[profile]['role_arn']
            mfa_serial = config[profile]['mfa_serial']
            role_session_name = role_arn.split('/')[1]

            sts = boto3.client('sts')

            assume_role_response = sts.assume_role(RoleArn=role_arn,
                                               RoleSessionName=role_session_name,
                                               SerialNumber=mfa_serial,
                                               TokenCode=context.mfa)

            env_data['AWS_ACCESS_KEY_ID'] = assume_role_response['Credentials']['AccessKeyId']
            env_data['AWS_SECRET_ACCESS_KEY'] = assume_role_response['Credentials']['SecretAccessKey']
            env_data['AWS_SESSION_TOKEN'] = assume_role_response['Credentials']['SessionToken']
            env_data['AWS_TOKEN_EXPIRATION_TIME'] = str(assume_role_response['Credentials']['Expiration'])

            with open(file_path, 'w') as outfile:
                json.dump(temp_cred, outfile)

        self.session = boto3.Session(profile_name=self.env,
                                                  region_name=self.region,
                                                  aws_access_key_id=env_data['AWS_ACCESS_KEY_ID'],
                                                  aws_secret_access_key=env_data['AWS_SECRET_ACCESS_KEY'],
                                                  aws_session_token=env_data['AWS_SESSION_TOKEN'])

        self.s3_client = self.session.client('s3', region_name=self.region)
        self.glue_client = self.session.client('glue', region_name=self.region)
        self.athena_client = self.session.client('athena', region_name=self.region)

    def set_bucket(self, bucket_name):
        self.bucket = bucket_name

    def set_default_bucket(self, context):
        self.bucket = context.bucketName

    def check_bucket_exists(self):
        buckets_list = self.s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in buckets_list['Buckets']]
        return self.bucket in buckets

    def upload_file(self, local_file, bucket, prefix):
        with open(local_file, 'rb') as data:
            self.s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
        data.close()

    def download_file(self, file_path, local_file):
        self.s3_client.download_file(Bucket=self.bucket, Key=file_path, Filename=local_file)

    def delete_file(self, prefix):
        self.s3_client.delete_object(Bucket=self.bucket, Key=prefix)

    def delete_directory(self, prefix):
        list = self.s3_client.list_objects(Bucket=self.bucket, Prefix=prefix + '/',
                                                        Delimiter='/')
        for key in list['Contents']:
            to_delete = key['Key']
            self.s3_client.delete_object(Bucket=self.bucket, Key=to_delete)

    def check_file_or_direcory_present(self, bucket, file_path):
        results = self.s3_client.list_objects(Bucket=bucket, Prefix=file_path)
        return 'Contents' in results

    def list_files_in_folder(self, file_path):
        return self.s3_client.list_objects(Bucket=self.bucket, Prefix=file_path)


    def run_glue_job_and_wait_for_end(self, job_name, arguments):
        escaped_job_name = job_name.replace('/', r'/')
        job_run = self.glue_client.start_job_run(JobName=escaped_job_name, Arguments=arguments)
        job_run_id = job_run['JobRunId']

        while True:
            status = self.glue_client.get_job_run(JobName=escaped_job_name, RunId=job_run_id)
            if status['JobRun']['JobRunState'] not in [self.job_state_starting,
                                                       self.job_state_running]:
                break
            time.sleep(20)

    def run_athena_query_and_wait_for_result(self, query_string, output_location):
        s3_output_location = "s3://{}/{}".format(self.bucket, output_location)
        query_execution = self.athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={
                'OutputLocation': s3_output_location,
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }
            }
        )
        id = query_execution['QueryExecutionId']

        while True:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=id
            )
            status = response['QueryExecution']['Status']['State']
            if status not in [self.job_state_starting, self.job_state_running]:
                break
            time.sleep(5)
        return status

    def generate_bucket(self, bucket_name, layer, prefix=None):
        bucket = self.bucket_format.format(bucket_name, layer)

        if prefix:
            bucket = self.bucket_with_prefix_format.format(bucket, prefix)

        return bucket

    def generate_prefixed_name(self, prefix, name):
        return self.prefix_format.format(prefix, name)
