from jobs.input.input_processing import load_parquet_into_df
from shared.utilities import Environments, ClassificationLead, ClassificationSetElementXref, \
    ClassificationSubcategory, InputColumnNames


def classify(spark_session, logger, input_df, environment):
    """
    Classify the leads that are provided in the input DataFrame.

    :param spark_session: Spark session created from our main file.
    :param logger: The underlying spark log4j logger
    :param input_df: A DataFrame consisting of leads that need to be classified
    :param environment: The current environment (local, dev, qa, prod)
    :return: A DataFrame that contains the leads from the input and their classifications
    """
    lead_df = get_classification_lead_df(spark_session, environment, logger)
    logger.info("LEAD_DF PARTITION SIZE: {size}".format(size=lead_df.rdd.getNumPartitions()))

    classification_set_df = get_classification_set_elem_xref_df(spark_session, environment, logger)
    logger.info(
        "CLASSIFICATION_SET_DF PARTITION SIZE: {size}".format(size=classification_set_df.rdd.getNumPartitions()))

    # Transform the input DataFrame by joining it to the classif_lead DataFrame
    input_lead_df = join_input_to_lead_df(input_df, lead_df)
    logger.info("INPUT_LEAD_DF PARTITION SIZE: {size}".format(size=input_lead_df.rdd.getNumPartitions()))

    # Transform the input and lead DataFrame by joining it to the classif_set DataFrame
    input_lead_set_df = join_input_to_classification_set_df(input_lead_df, classification_set_df)
    logger.info("INPUT_LEAD_SET_DF PARTITION SIZE: {size}".format(size=input_lead_set_df.rdd.getNumPartitions()))

    return input_lead_set_df


def join_input_to_lead_df(input_df, lead_df):
    """
    Joins the input DataFrame received from pii hashing to the classification lead table based on lead id.
    :param input_df: A DataFrame consisting of leads that need to be classified
    :param lead_df: The classif_lead table as a DataFrame
    :return: A DataFrame consisting of the initial input DataFrame joined to the Classification Lead DataFrame minus
    timestamps.
    """
    modified_input = input_df.select(
        InputColumnNames.RECORD_ID,
        InputColumnNames.INPUT_ID
    )
    join_expression = modified_input.input_id == lead_df.token
    return modified_input \
        .join(lead_df, join_expression, "left") \
        .drop(ClassificationLead.TOKEN)


def join_input_to_classification_set_df(input_df, classification_set_df):
    """

    :param input_df: The original input DataFrame joined with the lead table that includes classif_set_key
        so we can join into the classif_set_element_xref table
    :param classification_set_df: The classif_set_element_xref table as a DataFrame
    :return: A DataFrame consisting of the transformed input DataFrame joined to the Classification Set DataFrame minus
    unnecessary columns.
    """
    modified_input = input_df.drop(InputColumnNames.INPUT_ID)
    join_expression = modified_input.classif_set_key == classification_set_df.classif_set_key
    return modified_input \
        .join(classification_set_df, join_expression, "left") \
        .drop(ClassificationLead.CLASSIF_SET_KEY)


def get_classification_lead_df(spark_session, environment, logger):
    """
    Returns a DataFrame consisting of the classification leads table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :return: The Classification Lead table as a DataFrame
    """
    schema_location = get_classification_schema_location(environment, ClassificationLead.SCHEMA_NAME)
    logger.info("Reading classif_lead file from {location}".format(location=schema_location))
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_set_elem_xref_df(spark_session, environment, logger):
    """
    Returns a DataFrame consisting of the classification set_element_xref table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :return: The Classification Set Element XREF table as a DataFrame
    """
    schema_location = get_classification_schema_location(environment, ClassificationSetElementXref.SCHEMA_NAME)
    logger.info("Reading classif_set_element_xref file from {location}".format(location=schema_location))
    classification_set_elem_xref_df = load_parquet_into_df(spark_session, schema_location)
    return classification_set_elem_xref_df.select(classification_set_elem_xref_df.classif_set_key,
                                                  classification_set_elem_xref_df.classif_subcategory_key)


def get_classification_subcategory_df(spark_session, environment, logger):
    """
    Returns a DataFrame consisting of the classification subcategory table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :return: The Classification Subcategory table as a DataFrame
    """
    schema_location = get_classification_schema_location(environment, ClassificationSubcategory.SCHEMA_NAME)
    logger.info("Reading classif_subcategory file from {location}".format(location=schema_location))
    classif_subcategory_df = load_parquet_into_df(spark_session, schema_location)
    return classif_subcategory_df.select(classif_subcategory_df.classif_subcategory_key,
                                         classif_subcategory_df.subcategory_cd,
                                         classif_subcategory_df.subcategory_display_nm)


def get_classification_schema_location(environment, schema_name):
    """
    Builds an absolute path to a file for classifications

    :param environment: The current environment (local, dev, qa, prod)
    :param schema_name: The name of the table being retrieved
    :return: A string for locating classification parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-prj/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'classification/' + schema_name + '/'
