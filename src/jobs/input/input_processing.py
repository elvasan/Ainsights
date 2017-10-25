from pyspark.sql.functions import lit  # pylint:disable=no-name-in-module
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType

from shared.utilities import InputColumnNames, Environments, GenericColumnNames


def process_input_file(spark, logger, client_name, environment):
    logger.info("input_processing: start to read file")
    full_file_name = build_input_csv_file_name(environment, client_name)

    logger.info("input_processing: trying to read file named: " + full_file_name)
    raw_data_frame = load_csv_file(spark, full_file_name)
    # TODO: file validations (TBD)
    # transform file
    logger.info("input_processing: returning transformed file")
    return transform_input_csv(raw_data_frame)


def build_input_csv_file_name(environment, client_name):
    # file name should be environment, aws_region, client_name
    # S3://jornaya-dev-us-east-1-aida-insights/beestest/input/beestest.csv
    # ../samples/beestest/input/beestest.csv
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 'S3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    return '{0}{1}/input/{2}.csv'.format(bucket_prefix, client_name, client_name)


def load_csv_file(spark, full_file_name):
    # Note: FailFast will cause Java read errors for any invalid formatted row! This will fail the whole file read
    # we need to revisit this once we understand input file validation rules
    return spark.read.csv(full_file_name, schema=input_csv_schema(), header=True, mode='FailFast')


def transform_input_csv(input_csv_df):
    # Basic transform for single lead_1 value (this will expand greatly once we support other types
    return input_csv_df.select(input_csv_df.record_id, input_csv_df.lead_1, input_csv_df.as_of_time) \
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(GenericColumnNames.LEAD_ID)) \
        .withColumn(InputColumnNames.HAS_ERROR, lit(False)) \
        .withColumn(InputColumnNames.ERROR_MESSAGE, lit(None)) \
        .withColumnRenamed("lead_1", InputColumnNames.INPUT_ID)


def input_csv_schema():
    # Existing input.csv file has following columns:
    # recordid phone01 phone02 phone03 email01 email02 email03 leadid01 leadid02 leadid03 asof
    # we're reading it in as:
    # record_id, phone_1, phone_2, phone_3, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), False),
         StructField("phone_1", StringType(), True),
         StructField("phone_2", StringType(), True),
         StructField("phone_3", StringType(), True),
         StructField("email_1", StringType(), True),
         StructField("email_2", StringType(), True),
         StructField("email_3", StringType(), True),
         StructField("lead_1", StringType(), True),
         StructField("lead_2", StringType(), True),
         StructField("lead_3", StringType(), True),
         StructField(InputColumnNames.AS_OF_TIME, StringType(), True)])


def input_csv_transformed_schema():
    # record_id, input_id, input_id_type, as_of_time, has_error, error_message
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), True),
         StructField(InputColumnNames.INPUT_ID, StringType(), True),
         StructField(InputColumnNames.INPUT_ID_TYPE, StringType(), True),
         StructField(InputColumnNames.AS_OF_TIME, StringType(), True),
         StructField(InputColumnNames.HAS_ERROR, BooleanType(), True),
         StructField(InputColumnNames.ERROR_MESSAGE, StringType(), True)])
