from jobs.input.input_processing import load_parquet_into_df
from shared.utilities import InputColumnNames, Environments, IdentifierTypes, HashMappingColumnNames


class PIIInternalColumnNames:  # pylint:disable=too-few-public-methods
    CANONICAL_HASH_VALUE = "canonical_hash_value"
    RAW_HASH_VALUE = "raw_hash_value"


def transform_raw_inputs(spark, logger, input_df, environment):
    # get hash mapping dataframe
    hash_mapping_df = get_hash_mapping_df(spark, environment, logger)
    return populate_all_raw_inputs(input_df, hash_mapping_df)


def populate_all_raw_inputs(input_df, hash_mapping_df):
    # Inputs
    # record_id, input_id_raw, input_id_type, has_error, error_messages
    # 100, PPPPP, phone, False, None
    # 100, EEEEE, email, False, None
    # 101, LLLLL, leadid, False, None
    # 101, EEEEE, email, False, None
    #
    # Outputs
    # record_id, input_id_raw, input_id_type, input_id
    # 100, PPPPP, phone, CCPPP  (input_id is canonical hash value of input_id_raw value)
    # 100, EEEEE, email, CCEEE  (input_id is canonical hash value of input_id_raw value)
    # 101, LLLLL, leadid, LLLLL (input_id is just leadid copied over)
    # 101, EEEEE, email, CCEEE  (input_id is canonical hash value of input_id_raw value)

    # select distinct raw_hash_value from only phones, emails
    # raw_hash_value
    # PPPPP
    # EEEEE
    phones_emails_df = select_distinct_raw_hash_values_for_phones_emails(input_df)

    # lookup canonical hashes for phones_emails
    # raw_hash_value, canonical_hash_value
    # PPPPP, CPPPPP
    # EEEEE, CEEEEE
    canonical_to_raw_hash_df = lookup_canonical_hashes(hash_mapping_df, phones_emails_df)

    # join in phone,email canonical hash values (to a new input_id column)
    # record_id, input_id_raw, input_id_type, input_id
    # 100, PPPPP, phone, CPPPPP
    # 100, EEEEE, email, CEEEEE
    # 101, EEEEE, email, CEEEEE
    phones_emails_df = join_canonical_hash_values_to_phones_emails(input_df, canonical_to_raw_hash_df)

    # transform lead values to input_id column
    # record_id, input_id_raw, input_id_type, input_id
    # 101, LLLLL, leadid, LLLLL
    leads_df = transform_lead_values_input_id_column(input_df)

    # union leads_df and phones_emails_df for complete set
    return phones_emails_df.union(leads_df).orderBy(InputColumnNames.RECORD_ID)


def join_canonical_hash_values_to_phones_emails(input_df, canonical_hash_values):
    result_df = input_df.filter(input_df.input_id_type.isin([IdentifierTypes.PHONE, IdentifierTypes.EMAIL]))
    # left join to hash_values and add just canonical_hash_value column
    join_expression = result_df.input_id_raw == canonical_hash_values.raw_hash_value
    return result_df.join(canonical_hash_values, join_expression, "left") \
        .select(result_df.record_id,
                result_df.input_id_raw,
                result_df.input_id_type,
                HashMappingColumnNames.CANONICAL_HASH_VALUE) \
        .withColumnRenamed(HashMappingColumnNames.CANONICAL_HASH_VALUE, InputColumnNames.INPUT_ID)


def transform_lead_values_input_id_column(input_df):
    return input_df.filter(input_df.input_id_type.isin([IdentifierTypes.LEADID])) \
        .withColumn(InputColumnNames.INPUT_ID, input_df.input_id_raw) \
        .select(InputColumnNames.RECORD_ID,
                InputColumnNames.INPUT_ID_RAW,
                InputColumnNames.INPUT_ID_TYPE,
                InputColumnNames.INPUT_ID)


def lookup_canonical_hashes(hash_mapping_df, phones_emails_df):
    # left join to hash_values and add just canonical_hash_value column
    join_expression = hash_mapping_df.hash_value == phones_emails_df.raw_hash_value
    return phones_emails_df \
        .join(hash_mapping_df, join_expression, "left") \
        .drop(HashMappingColumnNames.HASH_VALUE)


def select_distinct_raw_hash_values_for_phones_emails(input_df):
    # get unique set of hashed_values of PHONES, EMAIL types only
    return input_df.filter(input_df.input_id_type.isin([IdentifierTypes.EMAIL, IdentifierTypes.PHONE])) \
        .select(input_df.input_id_raw.alias(PIIInternalColumnNames.RAW_HASH_VALUE)) \
        .distinct()


def get_hash_mapping_df(spark_session, environment, logger):
    """
    Retrieves the hash mapping tables as a DataFrame consisting of two columns: canonical hash value and raw input value
    :param spark_session: The Spark Session
    :param environment: The current environment (local, dev, qa, prod)
    :param logger: The application logger
    :return: A DataFrame with two columns, canonical hash value and raw input value
    """
    schema_location = get_hash_mapping_schema_location(environment)
    logger.info("Reading hash_mapping file from {location}".format(location=schema_location))
    hash_map_df = load_parquet_into_df(spark_session, schema_location)
    # select out only the canonical_hash_value and hash_value columns
    # TODO: look into only pulling the two supported hash value types
    return hash_map_df.select(hash_map_df.canonical_hash_value, hash_map_df.hash_value)


def get_hash_mapping_schema_location(environment):
    """
    Builds an absolute path to the hash mapping schema`

    :param environment: The current environment (local, dev, qa, prod)
    :return: A string for locating hash mapping parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-fdl/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'hash_mapping'
