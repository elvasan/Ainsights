from pyspark.sql.functions import lit  # pylint:disable=no-name-in-module
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType
from shared.utilities import ENV_LOCAL, LOCAL_BUCKET_PREFIX, ID_TYPE_LEAD_ID, COL_NAME_ERROR_MESSAGE, \
    COL_NAME_HAS_ERROR, COL_NAME_RECORD_ID, COL_NAME_INPUT_ID, COL_NAME_INPUT_ID_TYPE, COL_NAME_AS_OF_TIME


def process_input_file(spark, logger, client_name, environment, aws_region):
    logger.info("input_processing: start to read file")
    full_file_name = build_input_csv_file_name(environment, aws_region, client_name)

    logger.info("input_processing: trying to read file named: " + full_file_name)
    raw_data_frame = load_csv_file(spark, full_file_name)
    # do file validations (TBD)
    # transform file
    lead_id_data_frame = transform_input_csv(raw_data_frame)
    logger.info("input_processing: returning transformed file")
    return lead_id_data_frame


def build_input_csv_file_name(environment, aws_region, client_name):
    # file name should be environment, aws_region, client_name
    # S3://jornaya-dev-us-east-1-aida-insights/beestest/input/beestest.csv
    # ../samples/beestest/input/beestest.csv
    if environment == ENV_LOCAL:
        bucket_prefix = LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 'S3://jornaya-{0}-{1}-aida-insights/'.format(environment, aws_region)
    name = '{0}{1}/input/{2}.csv'.format(bucket_prefix, client_name, client_name)
    return name


def load_csv_file(spark, full_file_name):
    # Note: FailFast will cause Java read errors for any invalid formatted row! This will fail the whole file read
    # we need to revisit this once we understand input file validation rules
    csv_data_frame = spark.read.csv(full_file_name, schema=input_csv_schema(), header=True, mode='FailFast')
    return csv_data_frame


def transform_input_csv(input_csv_df):
    # Basic transform for single lead_1 value (this will expand greatly once we support other types
    transformed_data_frame = input_csv_df.select(input_csv_df.record_id, input_csv_df.lead_1, input_csv_df.as_of_time) \
        .withColumn(COL_NAME_INPUT_ID_TYPE, lit(ID_TYPE_LEAD_ID)) \
        .withColumn(COL_NAME_HAS_ERROR, lit(False))\
        .withColumn(COL_NAME_ERROR_MESSAGE, lit(None))\
        .withColumnRenamed("lead_1", COL_NAME_INPUT_ID)
    return transformed_data_frame


def input_csv_schema():
    # Existing input.csv file has following columns:
    # recordid phone01 phone02 phone03 email01 email02 email03 leadid01 leadid02 leadid03 asof
    # we're reading it in as:
    # record_id, phone_1, phone_2, phone_3, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    csv_schema = StructType(
        [StructField(COL_NAME_RECORD_ID, LongType(), False),
         StructField("phone_1", StringType(), True),
         StructField("phone_2", StringType(), True),
         StructField("phone_3", StringType(), True),
         StructField("email_1", StringType(), True),
         StructField("email_2", StringType(), True),
         StructField("email_3", StringType(), True),
         StructField("lead_1", StringType(), True),
         StructField("lead_2", StringType(), True),
         StructField("lead_3", StringType(), True),
         StructField("as_of_time", StringType(), True)])
    return csv_schema


def input_csv_transformed_schema():
    # record_id, input_id, input_id_type, as_of_time, has_error, error_message
    transformed_csv_schema = StructType(
        [StructField(COL_NAME_RECORD_ID, LongType(), True),
         StructField(COL_NAME_INPUT_ID, StringType(), True),
         StructField(COL_NAME_INPUT_ID_TYPE, StringType(), True),
         StructField(COL_NAME_AS_OF_TIME, StringType(), True),
         StructField(COL_NAME_HAS_ERROR, BooleanType(), True),
         StructField(COL_NAME_ERROR_MESSAGE, StringType(), True)])
    return transformed_csv_schema
