from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import lit, concat_ws, split, explode, when  # pylint:disable=no-name-in-module
from shared.utilities import InputColumnNames, Environments, IdentifierTypes, RawInputCSVColumnNames

ID_SEPARATOR = ':'


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

    logger.info("input_processing: trying to read file named: {0}".format(full_file_name))
    raw_data_frame = load_csv_file(spark, full_file_name)
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
    # collapse all 'phone', 'email', 'lead' columns to concatenated string so can make ArrayType
    # define functions to combine all 'type' columns 'Phone1:Phone2:Phone3'
    concatenated_phones = concat_ws(ID_SEPARATOR, RawInputCSVColumnNames.PHONE_1, RawInputCSVColumnNames.PHONE_2,
                                    RawInputCSVColumnNames.PHONE_3, RawInputCSVColumnNames.PHONE_4).alias(
        'all_phones_string')
    concatenated_emails = concat_ws(ID_SEPARATOR, RawInputCSVColumnNames.EMAIL_1, RawInputCSVColumnNames.EMAIL_2,
                                    RawInputCSVColumnNames.EMAIL_3).alias('all_emails_string')
    concatenated_leads = concat_ws(ID_SEPARATOR, RawInputCSVColumnNames.LEAD_1, RawInputCSVColumnNames.LEAD_2,
                                   RawInputCSVColumnNames.LEAD_3).alias('all_leads_string')
    # select out record_id, and all_phones, all_emails, all_leads
    all_concat_df = input_csv_df.select(input_csv_df.record_id, concatenated_phones, concatenated_emails,
                                        concatenated_leads)

    # once have string of all values split by : to single column with array of string values
    phone_splitter = when(all_concat_df.all_phones_string == '', None).otherwise(
        split(all_concat_df.all_phones_string, ID_SEPARATOR)).alias("phones_array")
    email_splitter = when(all_concat_df.all_emails_string == '', None).otherwise(
        split(all_concat_df.all_emails_string, ID_SEPARATOR)).alias("emails_array")
    lead_splitter = when(all_concat_df.all_leads_string == '', None).otherwise(
        split(all_concat_df.all_leads_string, ID_SEPARATOR)).alias("leads_array")
    all_as_array_df = all_concat_df.select(input_csv_df.record_id, phone_splitter, email_splitter, lead_splitter)

    # Explode our array type columns into separate rows
    # Input => 100, [], [PHONE111,PHONE222], []
    # Output => record_id, input_id_raw, input_id_type
    #   100, PHONE111, phone
    #   100, PHONE222, phone
    phones_exploded_df = all_as_array_df.withColumn(InputColumnNames.INPUT_ID_RAW,
                                                    explode(all_as_array_df.phones_array)) \
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(IdentifierTypes.PHONE)) \
        .select(all_as_array_df.record_id, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE)

    # Explode our array type columns into separate rows
    # Input => 100, [], [], [EMAIL111,EMAIL222]
    # Output => record_id, input_id_raw, input_id_type
    #   100, EMAIL111, email
    #   100, EMAIL222, email
    emails_exploded_df = all_as_array_df.withColumn(InputColumnNames.INPUT_ID_RAW,
                                                    explode(all_as_array_df.emails_array)) \
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(IdentifierTypes.EMAIL)) \
        .select(all_as_array_df.record_id, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE)

    # Explode our array type columns into separate rows
    # Input => 100, [LEAD111,LEAD222], [], []
    # Output => record_id, input_id_raw, input_id_type
    #   100, LEAD111, leadid
    #   100, LEAD222, leadid
    leads_exploded_df = all_as_array_df.withColumn(InputColumnNames.INPUT_ID_RAW, explode(all_as_array_df.leads_array))\
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(IdentifierTypes.LEADID)) \
        .select(all_as_array_df.record_id, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE)

    # union all the tables back
    result_df = phones_exploded_df.union(emails_exploded_df).union(leads_exploded_df) \
        .withColumn(InputColumnNames.HAS_ERROR, lit(False)) \
        .withColumn(InputColumnNames.ERROR_MESSAGE, lit(None)) \
        .orderBy(all_as_array_df.record_id)
    return result_df


def input_csv_schema():
    """
    Defines the schema of the input file.
    :return: A StructType object that defines a DataFrame's schema
    """
    # Existing input.csv file has following columns:
    # recordid phone01 phone02 phone03 phone04 email01 email02 email03 leadid01 leadid02 leadid03 asof
    # we're reading it in as:
    # record_id, phone_1, phone_2, phone_3, phone_4, email_1, email_2, email_3, lead_1, lead_2, lead_3, as_of_time
    return StructType(
        [StructField(InputColumnNames.RECORD_ID, LongType(), False),
         StructField(RawInputCSVColumnNames.PHONE_1, StringType(), True),
         StructField(RawInputCSVColumnNames.PHONE_2, StringType(), True),
         StructField(RawInputCSVColumnNames.PHONE_3, StringType(), True),
         StructField(RawInputCSVColumnNames.PHONE_4, StringType(), True),
         StructField(RawInputCSVColumnNames.EMAIL_1, StringType(), True),
         StructField(RawInputCSVColumnNames.EMAIL_2, StringType(), True),
         StructField(RawInputCSVColumnNames.EMAIL_3, StringType(), True),
         StructField(RawInputCSVColumnNames.LEAD_1, StringType(), True),
         StructField(RawInputCSVColumnNames.LEAD_2, StringType(), True),
         StructField(RawInputCSVColumnNames.LEAD_3, StringType(), True),
         StructField(InputColumnNames.AS_OF_TIME, StringType(), True)])
