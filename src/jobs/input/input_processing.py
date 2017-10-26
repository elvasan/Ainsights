from pyspark.sql.functions import lit  # pylint:disable=no-name-in-module
from pyspark.sql.types import StructField, StructType, StringType, LongType

from shared.utilities import InputColumnNames, Environments, GenericColumnNames


def process_input_file(spark, logger, client_name, environment):
    """
    Given the client name and environment, we attempt to grab the client's CSV file and convert it to a DataFrame
    that is used for the execution of our application.
    :param spark: The spark context
    :param logger: The underlying JVM logger
    :param client_name: The name of the client
    :param environment: The current deployment environment
    :return: A DataFrame consisting of input data that was read in from a CSV
    """
    logger.info("input_processing: start to read file")
    full_file_name = build_input_csv_file_name(environment, client_name)

    logger.info("input_processing: trying to read file named: " + full_file_name)
    raw_data_frame = load_csv_file(spark, full_file_name)
    # TODO: file validations (TBD)
    logger.info("input_processing: returning transformed file")
    return transform_input_csv(raw_data_frame)


def build_input_csv_file_name(environment, client_name):
    """
    Constructs a known file location based on environment and client_name
    File name should be environment, aws_region, client_name
    Development example: s3://jornaya-dev-us-east-1-aida-insights/beestest/input/beestest.csv
    Local example: ../samples/beestest/input/beestest.csv
    :param environment: The current execution environment
    :param client_name: The name of the client
    :return: A string representing the location of the input csv file.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    return '{0}{1}/input/{2}.csv'.format(bucket_prefix, client_name, client_name)


def load_csv_file(spark, full_file_name):
    """
    Loads a CSV file from a given location.
    :param spark: The spark context
    :param full_file_name: The absolute path to the location of the CSV file.
    :return: A DataFrame from the input CSV.
    """
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
    """
    Defines the schema of the input file.
    :return: A StructType object that defines a DataFrame's schema
    """
    # Existing input.csv file has following columns:
    # recordid phone01 phone02 phone03 email01 email02 email03 leadid01 leadid02 leadid03 asof
    # we're reading it in as:
    # record_id, phone_1, phone_2, phone_3, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), False),
         StructField("phone_1", StringType()),
         StructField("phone_2", StringType()),
         StructField("phone_3", StringType()),
         StructField("email_1", StringType()),
         StructField("email_2", StringType()),
         StructField("email_3", StringType()),
         StructField("lead_1", StringType()),
         StructField("lead_2", StringType()),
         StructField("lead_3", StringType()),
         StructField(InputColumnNames.AS_OF_TIME, StringType())])
