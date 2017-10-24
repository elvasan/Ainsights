from shared.utilities import *


def classify(spark_session, logger, input_df, environment, aws_region):
    """
    Classify the leads that are provided in the input data frame.

    :param spark_session: Spark session created from our main file.
    :param logger: The underlying spark log4j logger
    :param input_df: A data frame consisting of leads that need to be classified
    :param environment: The current environment (local, dev, qa, prod)
    :param aws_region: The aws region where the classification bucket resides
    :return: A data frame that contains the leads from the input and their classifications
    """
    lead_df = get_classification_lead_df(spark_session, environment, aws_region)
    logger.debug('Printing classif_lead dataframe.')
    logger.debug(lead_df.show(15, True))

    classification_set_df = get_classification_set_elem_xref_df(spark_session, environment, aws_region)
    logger.debug('Printing classif_set_elem_xref dataframe.')
    logger.debug(classification_set_df.show(15, True))

    # Transform the input dataframe by joining it to the classif_lead dataframe
    input_lead_df = join_input_to_lead_df(input_df, lead_df)
    logger.debug('Printing input joined to classif_lead dataframe.')
    logger.debug(input_lead_df.show(15, True))

    # Transform the input and lead dataframe by joining it to the classif_set dataframe
    input_lead_set_df = join_input_to_classification_set_df(input_lead_df, classification_set_df)
    logger.debug('Printing transformed input joined to classif_set dataframe.')
    logger.debug(input_lead_set_df.show(15, True))

    return input_lead_set_df


def join_input_to_lead_df(input_df, lead_df):
    """
    Joins the input dataframe received by pii hashing to the classification lead table based on lead id.
    :param input_df:
    :param lead_df: The classif_lead table as a dataframe
    :return:
    """
    join_expression = input_df.input_id == lead_df.lead_id
    return input_df \
        .join(lead_df, join_expression, "left") \
        .drop(COL_NAME_INPUT_ID_TYPE,
              COL_NAME_LEAD_ID,
              COL_NAME_CLASSIF_TIMESTAMP,
              COL_NAME_INSERTED_TIMESTAMP)


def join_input_to_classification_set_df(input_df, classification_set_df):
    """

    :param input_df: The original input dataframe joined with the lead table that includes classif_set_key
        so we can join into the classif_set_element_xref table
    :param classification_set_df: The classif_set_element_xref table as a dataframe
    :return: A transformed dataframe
    """
    join_expression = input_df.classif_set_key == classification_set_df.classif_set_key
    return input_df \
        .join(classification_set_df, join_expression, "left") \
        .drop(COL_NAME_CLASSIF_SET_KEY,
              COL_NAME_CLASSIF_ELEMENT_KEY,
              COL_NAME_CLASSIF_CATEGORY_KEY,
              COL_NAME_INSERTED_TIMESTAMP,
              COL_NAME_HAS_ERROR,
              COL_NAME_ERROR_MESSAGE,
              COL_NAME_AS_OF_TIME)


def get_classification_lead_df(spark_session, environment, aws_region):
    """
    Returns a data frame consisting of the classification leads table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :param aws_region: The aws region of the classification bucket
    :return: A data frame
    """
    schema_location = get_classification_schema_location(environment, aws_region, CLASSIFICATION_LEAD_SCHEMA_NAME)
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_set_elem_xref_df(spark_session, environment, aws_region):
    """
    Returns a data frame consisting of the classification set_element_xref table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :param aws_region: The aws region of the classification bucket
    :return: A data frame
    """
    schema_location = get_classification_schema_location(environment, aws_region,
                                                         CLASSIFICATION_SET_ELEMENT_XREF_SCHEMA_NAME)
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_subcategory_df(spark_session, environment, aws_region):
    """
    Returns a data frame consisting of the classification subcategory table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :param aws_region: The aws region of the classification bucket
    :return: A data frame
    """
    schema_location = get_classification_schema_location(environment, aws_region,
                                                         CLASSIFICATION_SUBCATEGORY_SCHEMA_NAME)
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_schema_location(environment, aws_region, schema_name):
    """
    Builds an absolute path to a file for classifications

    :param environment: The current environment (local, dev, qa, prod)
    :param aws_region: The AWS region (us-east-1)
    :param schema_name: The name of the table being retrieved
    :return: A string for retrieving classification parquet files.
    """
    if environment == ENV_LOCAL:
        bucket_prefix = LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 'S3://jornaya-{0}-{1}-prj/'.format(environment, aws_region)
    return bucket_prefix + 'classification/' + schema_name + '/'


def load_parquet_into_df(spark_session, file_location):
    """
    Loads a parquet file into a data frame.
    :param spark_session: The spark context initialized in on the application start up
    :param file_location: The absolute path to the parquet file
    :return: A DataFrame from the parquet location
    """
    return spark_session.read.parquet(file_location)
