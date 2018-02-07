from pyspark.sql.functions import lit, concat_ws, split, explode, when, \
    count as f_count  # pylint:disable=no-name-in-module
from pyspark.sql.types import StructField, StructType, StringType, LongType

from shared.constants import InputColumnNames, Environments, IdentifierTypes, RawInputCSVColumnNames, \
    GenericColumnNames

ID_SEPARATOR = ':'


def process_input_file(spark, logger, client_name, environment, job_run_id):
    """
    Given the client name and environment, we attempt to grab the client's CSV file and convert it to a DataFrame
    that is used for the execution of our application.
    :param spark: The spark context
    :param logger: The underlying JVM logger
    :param client_name: The name of the client
    :param environment: The current deployment environment
    :param job_run_id: The id of the job run
    :return: A DataFrame consisting of input data that was read in from a CSV
    """
    logger.info("input_processing: start to read file")
    full_file_name = build_input_csv_file_name(environment, client_name, job_run_id)

    logger.info("input_processing: trying to read file named: {0}".format(full_file_name))
    raw_data_frame = load_csv_file(spark, full_file_name, input_csv_schema())
    logger.info("input_processing: returning transformed file")
    return transform_input_csv(raw_data_frame)


def build_input_csv_file_name(environment, client_name, job_run_id):
    """
    Constructs a known file location based on environment and client_name
    File name should be environment, aws_region, client_name
    Development example:
        s3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_yyyy_mm_dd/input/beestest_yyyy_mm_dd.csv
    Local example:
        ../samples/app_data/beestest/beestest_yyyy_mm_dd/input/beestest_yyyy_mm_dd.csv
    :param environment: The current execution environment
    :param client_name: The name of the client
    :param job_run_id: The id of the job run
    :return: A string representing the location of the input csv file.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    return '{0}app_data/{1}/{1}_{2}/input/{1}_{2}.csv'.format(bucket_prefix, client_name, job_run_id)


def load_csv_file(spark, full_file_name, schema):
    """
    Loads a CSV file from a given location.
    :param spark: The spark context
    :param full_file_name: The absolute path to the location of the CSV file.
    :param schema: The schema to apply to the csv when reading.
    :return: A DataFrame from the input CSV.
    """
    # Note: FailFast will cause Java read errors for any invalid formatted row! This will fail the whole file read
    # we need to revisit this once we understand input file validation rules
    return spark.read.csv(full_file_name, schema=schema, header=True, mode='FailFast')


def load_parquet_into_df(spark_session, file_location):
    """
    Loads a parquet file into a DataFrame.
    :param spark_session: The spark context initialized in on the application start up
    :param file_location: The absolute path to the parquet file
    :return: A DataFrame from the parquet location
    """
    return spark_session.read.parquet(file_location)


def transform_input_csv(input_csv_df):
    """
    Collapse all 'phone', 'email', 'lead' columns to concatenated string so we have an ArrayType
    Define functions to combine all 'type' columns 'Phone1:Phone2:Phone3'
    :param input_csv_df: The initial customer input CSV file
    :return: A DataFrame broken out row by row with
    """
    concatenated_phones = concat_ws(
        ID_SEPARATOR,
        RawInputCSVColumnNames.PHONE_1,
        RawInputCSVColumnNames.PHONE_2,
        RawInputCSVColumnNames.PHONE_3,
        RawInputCSVColumnNames.PHONE_4) \
        .alias('all_phones_string')

    concatenated_emails = concat_ws(
        ID_SEPARATOR,
        RawInputCSVColumnNames.EMAIL_1,
        RawInputCSVColumnNames.EMAIL_2,
        RawInputCSVColumnNames.EMAIL_3) \
        .alias('all_emails_string')

    concatenated_leads = concat_ws(
        ID_SEPARATOR,
        RawInputCSVColumnNames.LEAD_1,
        RawInputCSVColumnNames.LEAD_2,
        RawInputCSVColumnNames.LEAD_3) \
        .alias('all_leads_string')
    # select out record_id, and all_phones, all_emails, all_leads
    all_concat_df = input_csv_df. \
        select(input_csv_df.record_id, concatenated_phones, concatenated_emails, concatenated_leads)

    # once have string of all values split by : to single column with array of string values
    phone_splitter = when(all_concat_df.all_phones_string == '', None) \
        .otherwise(split(all_concat_df.all_phones_string, ID_SEPARATOR)) \
        .alias("phones_array")

    email_splitter = when(all_concat_df.all_emails_string == '', None) \
        .otherwise(split(all_concat_df.all_emails_string, ID_SEPARATOR)) \
        .alias("emails_array")

    lead_splitter = when(all_concat_df.all_leads_string == '', None) \
        .otherwise(split(all_concat_df.all_leads_string, ID_SEPARATOR)) \
        .alias("leads_array")

    all_as_array_df = all_concat_df.select(input_csv_df.record_id, phone_splitter, email_splitter, lead_splitter)

    # Explode our array type columns into separate rows
    # Input => 100, [], [PHONE111,PHONE222], []
    # Output => record_id, input_id_raw, input_id_type
    #   100, PHONE111, phone
    #   100, PHONE222, phone
    phones_exploded_df = all_as_array_df \
        .withColumn(InputColumnNames.INPUT_ID_RAW, explode(all_as_array_df.phones_array)) \
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(IdentifierTypes.PHONE)) \
        .select(all_as_array_df.record_id, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE)

    # Explode our array type columns into separate rows
    # Input => 100, [], [], [EMAIL111,EMAIL222]
    # Output => record_id, input_id_raw, input_id_type
    #   100, EMAIL111, email
    #   100, EMAIL222, email
    emails_exploded_df = all_as_array_df \
        .withColumn(InputColumnNames.INPUT_ID_RAW, explode(all_as_array_df.emails_array)) \
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(IdentifierTypes.EMAIL)) \
        .select(all_as_array_df.record_id, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE)

    # Explode our array type columns into separate rows
    # Input => 100, [LEAD111,LEAD222], [], []
    # Output => record_id, input_id_raw, input_id_type
    #   100, LEAD111, leadid
    #   100, LEAD222, leadid
    leads_exploded_df = all_as_array_df \
        .withColumn(InputColumnNames.INPUT_ID_RAW, explode(all_as_array_df.leads_array)) \
        .withColumn(InputColumnNames.INPUT_ID_TYPE, lit(IdentifierTypes.LEADID)) \
        .select(all_as_array_df.record_id, InputColumnNames.INPUT_ID_RAW, InputColumnNames.INPUT_ID_TYPE)

    # union all the tables back and return the result
    return phones_exploded_df.union(emails_exploded_df) \
        .union(leads_exploded_df) \
        .withColumn(InputColumnNames.HAS_ERROR, lit(False)) \
        .withColumn(InputColumnNames.ERROR_MESSAGE, lit(None)) \
        .orderBy(all_as_array_df.record_id)


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
         StructField(RawInputCSVColumnNames.PHONE_1, StringType()),
         StructField(RawInputCSVColumnNames.PHONE_2, StringType()),
         StructField(RawInputCSVColumnNames.PHONE_3, StringType()),
         StructField(RawInputCSVColumnNames.PHONE_4, StringType()),
         StructField(RawInputCSVColumnNames.EMAIL_1, StringType()),
         StructField(RawInputCSVColumnNames.EMAIL_2, StringType()),
         StructField(RawInputCSVColumnNames.EMAIL_3, StringType()),
         StructField(RawInputCSVColumnNames.LEAD_1, StringType()),
         StructField(RawInputCSVColumnNames.LEAD_2, StringType()),
         StructField(RawInputCSVColumnNames.LEAD_3, StringType()),
         StructField(InputColumnNames.AS_OF_TIME, StringType())])


def summarize_input_df(spark, input_file_df):  # pylint:disable=too-many-locals
    """
    Summarizes the client input file with a count of total records, a count of unique phones, emails, and leads,
    followed by identifier coverage. Identifier Coverage is the percent of records with at least one identifier per
    column type divided by the total number of records multiplied by 100 with a percent sign added for Steve.

    Example:

    +---------------+------------------+-------------------+
    |Total Records  |17                |                   |
    |               |                  |                   |
    |               |Unique Identifiers|Identifier Coverage|
    |Phone Numbers  |27                |100.0 %            |
    |Email Addresses|21                |100.0 %            |
    |LeadiDs        |11                |70.6 %             |
    +---------------+------------------+-------------------+

    :param spark: The application spark context
    :param input_file_df: The customers input file containing recordids, phones, emails, and leadids as a DataFrame.
    :return: A DataFrame summarizing the results of the input file.
    """
    # Drop these columns since they're meaningless here
    truncated_input_df = input_file_df.drop(InputColumnNames.HAS_ERROR, InputColumnNames.ERROR_MESSAGE)
    record_ids = truncated_input_df.select(InputColumnNames.RECORD_ID).distinct()
    total_record_count = record_ids.count()
    total_records = record_ids.select([
        lit('Total Records').alias("labels"),
        f_count(record_ids.record_id).alias('unique_records'),
        lit('').alias('identifier_coverage'),
    ])

    blank_row = create_three_column_row(spark, '', '', '')
    header_row = create_three_column_row(spark, '', 'Unique Identifiers', 'Identifier Coverage')

    pivoted_df = truncated_input_df.groupby(truncated_input_df.record_id, truncated_input_df.input_id_raw) \
        .pivot(InputColumnNames.INPUT_ID_TYPE) \
        .count()

    pivoted_df = validate_column_names(pivoted_df)

    unique_phone_count = get_unique_identifier_count(pivoted_df,
                                                     [InputColumnNames.INPUT_ID_RAW, GenericColumnNames.PHONE],
                                                     GenericColumnNames.PHONE)
    unique_email_count = get_unique_identifier_count(pivoted_df,
                                                     [InputColumnNames.INPUT_ID_RAW, GenericColumnNames.EMAIL],
                                                     GenericColumnNames.EMAIL)

    unique_lead_count = get_unique_identifier_count(pivoted_df,
                                                    [InputColumnNames.INPUT_ID_RAW, GenericColumnNames.LEAD_ID],
                                                    GenericColumnNames.LEAD_ID)

    phone_count = get_record_count_for_identifier(pivoted_df, GenericColumnNames.PHONE)
    email_count = get_record_count_for_identifier(pivoted_df, GenericColumnNames.EMAIL)
    leads_count = get_record_count_for_identifier(pivoted_df, GenericColumnNames.LEAD_ID)

    phone_coverage = calculate_identifier_coverage(phone_count, total_record_count)
    email_coverage = calculate_identifier_coverage(email_count, total_record_count)
    lead_coverage = calculate_identifier_coverage(leads_count, total_record_count)

    phones_row = create_three_column_row(spark, 'Phone Numbers', unique_phone_count, phone_coverage)
    emails_row = create_three_column_row(spark, 'Email Addresses', unique_email_count, email_coverage)
    leads_row = create_three_column_row(spark, 'LeadiDs', unique_lead_count, lead_coverage)

    return total_records.union(blank_row.union(header_row.union(phones_row.union(emails_row.union(leads_row)))))


def get_record_count_for_identifier(dataframe, col_name):
    """
    Gets a list of unique record ids that have at least one identifier for a given type. For example if a column
    has 6 phone numbers across 3 unique record ids, only those 3 record ids will be returned.

    :param dataframe: A PySpark DataFrame
    :param col_name: The column representing the identifier type
    :return: A count of record ids that have at least one identifier for a given identifier type
    """
    return dataframe.select(InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID_RAW, col_name) \
        .dropna(subset=[col_name]) \
        .select(InputColumnNames.RECORD_ID) \
        .distinct() \
        .count()


def get_unique_identifier_count(dataframe, columns_to_select, dropna_column):
    """
    Gets the number of unique identifiers for a given column in a DataFrame

    :param dataframe: A PySpark DataFrame
    :param columns_to_select: A list of columns to select from a DataFrame
    :param dropna_column: The column which to drop null values
    :return:
    """
    return dataframe.select(*columns_to_select) \
        .dropna(subset=[dropna_column]) \
        .distinct() \
        .count()


def validate_column_names(dataframe):
    """
    Validates whether a DataFrame has the required columns for the input summary.

    :param dataframe: A DataFrame to validate
    :return: A DataFrame with all of the necessary columns used for the input summary
    """
    for name in [GenericColumnNames.LEAD_ID, GenericColumnNames.PHONE, GenericColumnNames.EMAIL]:
        if name not in dataframe.schema.names:
            dataframe = dataframe.withColumn(name, lit(None))
    return dataframe


def create_three_column_row(spark, column_1, column_2, column_3):
    """
    Convenience function to help create rows for our summary DataFrames

    :param spark: The application spark context
    :param column_1: The value in column 1
    :param column_2: The value in column 2
    :param column_3: The value in column 3
    :return: A three column DataFrame
    """
    return spark.createDataFrame([(column_1, column_2, column_3)])


def calculate_identifier_coverage(identifier_count, total_record_count):
    """
    Returns a string value representing the identifier coverage (percent of records with at least one identifier per
    column type divided by the total number of records)

    :param identifier_count: An int representing at least one identifier per column type
    :param total_record_count: The total number of records in the csv
    :return: A string representing the identifier coverage
    """
    return format((identifier_count / total_record_count) * 100, '.0f') + ' %'
